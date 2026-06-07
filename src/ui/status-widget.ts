import type { TransportStatus } from "../types.js";

const TRANSPORT_ABBREV: Record<string, string> = {
  matrix: "mx",
};

/**
 * Status widget showing remote pilot connection status
 */
export function createStatusWidget(
  transports: TransportStatus[],
  usersByTransport: Record<string, string[]>
): string | undefined {
  // Only show the widget when something is actually connected — keeps a
  // desktop pi (transport configured but not auto-connected) free of noise.
  const connected = transports.filter((t) => t.connected);
  if (connected.length === 0) {
    return undefined;
  }

  const transportList = connected
    .map((t) => {
      const abbrev = TRANSPORT_ABBREV[t.type] || t.type.slice(0, 3);
      const userCount = usersByTransport[t.type]?.length || 0;
      const userSuffix = userCount > 0 ? `:${userCount}` : "";
      return `[${abbrev}${userSuffix}]`;
    })
    .join("");

  return `💬 ${transportList}`;
}
