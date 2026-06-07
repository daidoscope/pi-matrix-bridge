import type { ILogger } from "matrix-bot-sdk";
import {
  AutojoinRoomsMixin,
  LogService,
  MatrixClient,
  RustSdkCryptoStorageProvider,
  RustSdkCryptoStoreType,
  SimpleFsStorageProvider,
} from "matrix-bot-sdk";
import * as os from "os";
import * as path from "path";
import type { ChallengeAuth } from "../auth/challenge-auth.js";
import type { ExternalMessage } from "../types.js";
import type { ITransportProvider } from "./interface.js";
import {
  extractUsername,
  formatForMatrix,
  getRetryAfterMs,
  RATE_LIMIT_NOTE,
  shouldSkipEvent,
  stripBotMention,
  wasBotMentioned,
} from "./matrix-utils.js";

/** One queued outbound request (new message or in-place edit). */
interface OutboundJob {
  kind: "send" | "edit";
  chatId: string;
  text: string;
  /** Target event id — required for edits. */
  messageId?: string;
  resolve: (eventId: string) => void;
  reject: (err: Error) => void;
  /** How many times dispatch has been attempted (for the rate-limit retry cap). */
  attempts: number;
}

/** Give up on a single job after this many rate-limited retries. */
const MAX_SEND_ATTEMPTS = 10;

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// matrix-bot-sdk logs to the console, which corrupts pi's interactive TUI (the
// crypto/sync logs land mid-render, e.g. eating an incoming message). Silence it
// entirely — real connection failures still throw from client.start() and are
// surfaced via the transport error handler / connect() rejection.
const SILENT_LOGGER: ILogger = {
  info: () => {},
  warn: () => {},
  debug: () => {},
  trace: () => {},
  error: () => {},
};

/**
 * Matrix transport provider using matrix-bot-sdk
 * Works with any Matrix homeserver — Element X, Element Web, FluffyChat, etc.
 */
export class MatrixProvider implements ITransportProvider {
  readonly type = "matrix";
  private client?: MatrixClient;
  private _isConnected = false;
  private messageHandler?: (message: ExternalMessage) => void;
  private errorHandler?: (error: Error) => void;
  private botUserId?: string;
  private joinedRooms = new Set<string>();
  private roomMemberCount = new Map<string, number>();
  private connectedAt = 0;

  // Serialized outbound queue. Every send/edit goes through it so a 429 backs off
  // and retries instead of dropping the message. `rateLimitedUntil` is a global
  // pause (Matrix rate-limits per connection, not per room); `rateLimitNotePending`
  // makes the next new message carry a small "delayed" note.
  private sendQueue: OutboundJob[] = [];
  private processing = false;
  private rateLimitedUntil = 0;
  private rateLimitNotePending = false;

  constructor(
    private config: { homeserverUrl: string; accessToken: string; encryption?: boolean },
    private auth: ChallengeAuth
  ) {}

  get isConnected(): boolean {
    return this._isConnected;
  }

  // Formatting delegated to matrix-utils.ts (pure, testable)

  async connect(): Promise<void> {
    if (this._isConnected) return;

    // Silence the SDK before any client activity so it never writes to the TUI.
    LogService.setLogger(SILENT_LOGGER);

    const { homeserverUrl, accessToken } = this.config;

    if (!homeserverUrl || !accessToken) {
      throw new Error("Matrix homeserver URL and access token required");
    }

    const storagePath = path.join(
      os.homedir(),
      ".pi",
      "matrix-bridge-store.json"
    );
    const storage = new SimpleFsStorageProvider(storagePath);

    // Set up E2EE crypto storage if encryption is enabled.
    // Uses @matrix-org/matrix-sdk-crypto-nodejs (native Rust, SQLite on disk).
    // Crypto state persists across restarts — same device, same keys.
    // The device must be verified once from another Matrix client (Element, etc).
    let cryptoProvider: RustSdkCryptoStorageProvider | undefined;
    if (this.config.encryption !== false) {
      try {
        const cryptoStorePath = path.join(
          os.homedir(),
          ".pi",
          "matrix-bridge-crypto"
        );
        cryptoProvider = new RustSdkCryptoStorageProvider(cryptoStorePath, RustSdkCryptoStoreType.Sqlite);
      } catch {
        // E2EE crypto unavailable — continue without encryption.
      }
    }

    this.client = new MatrixClient(
      homeserverUrl,
      accessToken,
      storage,
      cryptoProvider
    );

    // Auto-join rooms the bot is invited to
    AutojoinRoomsMixin.setupOnClient(this.client);

    // Cache bot user ID (never changes)
    this.botUserId = await this.client.getUserId();

    // Track room membership and member counts
    this.client.on("room.join", (roomId: string) => {
      this.joinedRooms.add(roomId);
      // Refresh member count asynchronously
      this.client?.getJoinedRoomMembers(roomId)
        .then(members => this.roomMemberCount.set(roomId, members.length))
        .catch(() => {});
    });
    this.client.on("room.leave", (roomId: string) => {
      this.joinedRooms.delete(roomId);
      this.roomMemberCount.delete(roomId);
    });

    // Handle incoming messages
    this.client.on("room.message", async (roomId: string, event: any) => {
      try {
        await this.handleMessage(roomId, event);
      } catch (err) {
        if (this.errorHandler) {
          this.errorHandler(err as Error);
        }
      }
    });

    try {
      await this.client.start();
    } catch (error) {
      // Clean up dangling state so connect() can be retried
      this.client = undefined;
      this.botUserId = undefined;
      this.joinedRooms.clear();
      this.roomMemberCount.clear();
      throw error;
    }

    // Seed joined rooms and member count caches
    const rooms = await this.client.getJoinedRooms();
    this.joinedRooms = new Set(rooms);
    await Promise.all(rooms.map(async (roomId) => {
      try {
        const members = await this.client!.getJoinedRoomMembers(roomId);
        this.roomMemberCount.set(roomId, members.length);
      } catch {
        // Will be fetched on first message if needed
      }
    }));
    this.connectedAt = Date.now();
    this._isConnected = true;
  }

  async disconnect(): Promise<void> {
    if (!this._isConnected || !this.client) return;

    // Fail any queued sends so awaiting callers don't hang on a dead connection.
    const pending = this.sendQueue;
    this.sendQueue = [];
    this.rateLimitedUntil = 0;
    this.rateLimitNotePending = false;
    for (const job of pending) {
      job.reject(new Error("Matrix transport disconnected"));
    }

    this.client.stop();
    this._isConnected = false;
    this.client = undefined;
    this.botUserId = undefined;
    this.joinedRooms.clear();
    this.roomMemberCount.clear();
    this.connectedAt = 0;
  }

  async sendMessage(chatId: string, text: string): Promise<string> {
    if (!this.client) {
      throw new Error("Matrix client not connected");
    }
    if (!text?.trim()) return "";

    return this.enqueue({ kind: "send", chatId, text });
  }

  async editMessage(chatId: string, messageId: string, text: string): Promise<void> {
    if (!this.client || !messageId || !text?.trim()) return;

    await this.enqueue({ kind: "edit", chatId, messageId, text });
  }

  /**
   * Append a job to the outbound queue and start the processor. Consecutive
   * pending edits to the same message are coalesced (the queued text is replaced)
   * so a backoff doesn't flush a run of stale intermediate edits.
   */
  private enqueue(spec: Pick<OutboundJob, "kind" | "chatId" | "text" | "messageId">): Promise<string> {
    if (spec.kind === "edit") {
      const pending = this.findCoalescableEdit(spec.chatId, spec.messageId);
      if (pending) {
        pending.text = spec.text;
        return new Promise((resolve, reject) => {
          // Ride along with the existing job's outcome.
          const prevResolve = pending.resolve;
          const prevReject = pending.reject;
          pending.resolve = (id) => { prevResolve(id); resolve(id); };
          pending.reject = (err) => { prevReject(err); reject(err); };
        });
      }
    }

    return new Promise<string>((resolve, reject) => {
      this.sendQueue.push({ ...spec, resolve, reject, attempts: 0 });
      void this.processQueue();
    });
  }

  /** Find a not-yet-dispatched edit for the same message that can be coalesced. */
  private findCoalescableEdit(chatId: string, messageId?: string): OutboundJob | undefined {
    if (!messageId) return undefined;
    // Skip the head while it's actively being dispatched — only later jobs are safe to mutate.
    const start = this.processing ? 1 : 0;
    for (let i = start; i < this.sendQueue.length; i++) {
      const job = this.sendQueue[i];
      if (job.kind === "edit" && job.chatId === chatId && job.messageId === messageId) {
        return job;
      }
    }
    return undefined;
  }

  /**
   * Drain the outbound queue one job at a time, honoring the rate-limit backoff.
   * On a 429 the head job stays in place and is retried after the server's
   * retry_after delay; other errors reject the job and move on.
   */
  private async processQueue(): Promise<void> {
    if (this.processing) return;
    this.processing = true;
    try {
      while (this.sendQueue.length > 0) {
        const wait = this.rateLimitedUntil - Date.now();
        if (wait > 0) await sleep(wait);

        const job = this.sendQueue[0];
        job.attempts++;
        try {
          const eventId = await this.dispatch(job);
          this.sendQueue.shift();
          job.resolve(eventId);
        } catch (err) {
          const retryMs = getRetryAfterMs(err);
          if (retryMs != null && job.attempts < MAX_SEND_ATTEMPTS) {
            // Rate limited: pause all sends, flag the note, retry this same job.
            this.rateLimitedUntil = Date.now() + retryMs;
            this.rateLimitNotePending = true;
          } else {
            this.sendQueue.shift();
            job.reject(err as Error);
          }
        }
      }
    } finally {
      this.processing = false;
    }
  }

  /** Perform the actual SDK call for a single queued job. */
  private async dispatch(job: OutboundJob): Promise<string> {
    if (!this.client) throw new Error("Matrix client not connected");

    if (job.kind === "send") {
      let text = job.text;
      if (this.rateLimitNotePending) {
        text = `${RATE_LIMIT_NOTE}\n\n${text}`;
        this.rateLimitNotePending = false;
      }
      const { body, formattedBody } = formatForMatrix(text);
      return await this.client.sendMessage(job.chatId, {
        msgtype: "m.text",
        body,
        ...(formattedBody && {
          format: "org.matrix.custom.html",
          formatted_body: formattedBody,
        }),
      });
    }

    const { body, formattedBody } = formatForMatrix(job.text);
    const newContent = {
      msgtype: "m.text",
      body,
      ...(formattedBody && {
        format: "org.matrix.custom.html",
        formatted_body: formattedBody,
      }),
    };

    // m.replace edit. The top-level body carries the "* " fallback shown by
    // clients that don't render edits; m.new_content holds the real replacement.
    await this.client.sendEvent(job.chatId, "m.room.message", {
      ...newContent,
      body: `* ${body}`,
      ...(formattedBody && { formatted_body: `* ${formattedBody}` }),
      "m.new_content": newContent,
      "m.relates_to": { rel_type: "m.replace", event_id: job.messageId },
    });
    return "";
  }

  async sendTyping(chatId: string): Promise<void> {
    // Don't pile typing requests onto a server that's already rate-limiting us.
    if (!this.client || this.rateLimitedUntil > Date.now()) return;
    try {
      await this.client.setTyping(chatId, true, 10000);
    } catch {
      // Ignore typing indicator errors
    }
  }

  async stopTyping(chatId: string): Promise<void> {
    if (!this.client || this.rateLimitedUntil > Date.now()) return;
    try {
      await this.client.setTyping(chatId, false);
    } catch {
      // Ignore typing indicator errors
    }
  }

  onMessage(handler: (message: ExternalMessage) => void): void {
    this.messageHandler = handler;
  }

  onError(handler: (error: Error) => void): void {
    this.errorHandler = handler;
  }

  private async handleMessage(roomId: string, event: any): Promise<void> {
    if (!this.client || !this.botUserId) return;

    // Pure filter — delegates to testable utility
    const skipReason = shouldSkipEvent(event, this.botUserId, this.connectedAt, this.joinedRooms, roomId);
    if (skipReason) return;

    const chatId = roomId;
    const userId = event.sender; // e.g. @user:matrix.org
    const username = extractUsername(userId);
    const messageText = event.content.body;
    const messageId = event.event_id;

    // Determine if group chat from cached member count (no API call per message)
    let memberCount = this.roomMemberCount.get(roomId);
    if (memberCount === undefined) {
      // Cache miss — fetch once and cache
      try {
        const members = await this.client.getJoinedRoomMembers(roomId);
        memberCount = members.length;
        this.roomMemberCount.set(roomId, memberCount);
      } catch {
        memberCount = 2; // Default to DM if we can't check
      }
    }
    const isGroupChat = memberCount > 2;

    // Check if bot was mentioned (pure utility)
    const wasMentioned = isGroupChat ? wasBotMentioned(messageText, this.botUserId) : false;

    // Check authorization
    const sendMessageToUser = async (cId: string, text: string) => {
      await this.sendMessage(cId, text);
    };

    const isAuthorized = await this.auth.checkAuthorization(
      userId,
      chatId,
      username,
      isGroupChat,
      wasMentioned,
      sendMessageToUser,
      this.type
    );

    // Handle challenge codes and commands in DMs. Admin commands accept either
    // a / or ! prefix (normalised in handleAdminCommand).
    if (!isGroupChat && (messageText.startsWith("/") || messageText.startsWith("!") || messageText.match(/^\d{6}$/))) {
      const handled = await this.auth.handleAdminCommand(
        messageText,
        chatId,
        userId,
        async (text) => {
          await this.sendMessage(chatId, text);
        },
        this.type
      );
      if (handled) return;
    }

    if (!isAuthorized) return;

    // Strip bot mention from message (pure utility)
    const cleanContent = wasMentioned && this.botUserId
      ? stripBotMention(messageText, this.botUserId)
      : messageText;

    // Forward to message handler
    if (this.messageHandler && cleanContent) {
      const externalMessage: ExternalMessage = {
        chatId,
        transport: this.type,
        content: cleanContent,
        username,
        userId,
        timestamp: new Date(event.origin_server_ts || Date.now()),
        messageId,
        isGroupChat,
        wasMentioned,
      };

      this.messageHandler(externalMessage);
    }
  }

}
