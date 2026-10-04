import type { ReactNode } from "react";
import { ExternalLink, RefreshCw, Store } from "lucide-react";
import { Badge, Button, Card, Skeleton, StateMessage } from "./ui";

export type ConnectionCardStatus = "connected" | "not_connected" | "needs_reauth" | "error";

type ConnectionCardProps = {
  platformName: string;
  description?: string;
  status: ConnectionCardStatus;
  accountIdentifier?: string;
  lastSyncedAt?: string | null;
  icon?: ReactNode;
  loading?: boolean;
  loadError?: string;
  actionError?: string;
  busy?: boolean;
  disconnecting?: boolean;
  connectDisabledReason?: string;
  onConnect?: () => void;
  onSync?: () => void;
  onDisconnect?: () => void;
  onRetryLoad?: () => void;
  onRetryAction?: () => void;
};

export function ConnectionCard({
  platformName,
  description,
  status,
  accountIdentifier,
  lastSyncedAt,
  icon,
  loading = false,
  loadError,
  actionError,
  busy = false,
  disconnecting = false,
  connectDisabledReason,
  onConnect,
  onSync,
  onDisconnect,
  onRetryLoad,
  onRetryAction,
}: ConnectionCardProps) {
  if (loading) return <Card className="connection-card"><Skeleton lines={3} /></Card>;
  if (loadError) {
    return <StateMessage kind="error" title={`${platformName} status could not load`}>
      {loadError} {onRetryLoad && <button className="text-action" onClick={onRetryLoad}>Try again</button>}
    </StateMessage>;
  }

  const badgeStatus = status === "connected" ? "connected" : status;
  const connectLabel = status === "needs_reauth" || status === "error" ? `Reconnect ${platformName}` : `Connect ${platformName}`;

  return <>
    <Card className="connection-card">
    <div className="provider-icon">{icon || <Store size={22} />}</div>
    <div className="provider-main">
      <div className="provider-title"><h2>{platformName}</h2><Badge status={badgeStatus} /></div>
      <p className="provider-description">{status === "needs_reauth" ? "Authorization required" : description || "Ecommerce platform"}</p>
      <div className="provider-meta">
        {accountIdentifier && <span>{accountIdentifier}</span>}
        {status === "connected" && <>
          {accountIdentifier && <span className="meta-dot" />}
          {lastSyncedAt ? <LastSync timestamp={lastSyncedAt} /> : <span>Not synced yet</span>}
        </>}
      </div>
      {connectDisabledReason && status !== "connected" && <p className="connection-note">{connectDisabledReason}</p>}
    </div>
    <div className="provider-actions">
      {status === "connected" ? <>
        <Button className="primary-button" onClick={onSync} disabled={!onSync || busy || disconnecting}>
          <RefreshCw size={16} className={busy ? "spin" : ""} />{busy ? "Syncing…" : "Sync now"}
        </Button>
        <Button className="quiet-button" onClick={onDisconnect} disabled={!onDisconnect || busy || disconnecting}>
          {disconnecting ? "Disconnecting…" : "Disconnect"}
        </Button>
      </> : <Button className="primary-button" onClick={onConnect} disabled={!onConnect || Boolean(connectDisabledReason) || busy || disconnecting}>
        <ExternalLink size={16} />{connectLabel}
      </Button>}
    </div>
    </Card>
    {actionError && <StateMessage kind="error" title={`${platformName} action failed`}>
      {actionError} {onRetryAction && <button className="text-action" onClick={onRetryAction} disabled={busy || disconnecting}>Retry</button>}
    </StateMessage>}
  </>;
}

function LastSync({ timestamp }: { timestamp: string }) {
  const date = new Date(timestamp);
  if (Number.isNaN(date.getTime())) return <span>Last sync unknown</span>;
  return <>
    <span>Last synced</span>
    <time title={date.toLocaleString()}>{relativeTime(date)}</time>
  </>;
}

function relativeTime(date: Date): string {
  const seconds = Math.round((date.getTime() - Date.now()) / 1000);
  const unit: Intl.RelativeTimeFormatUnit = Math.abs(seconds) < 60 ? "second" : Math.abs(seconds) < 3600 ? "minute" : Math.abs(seconds) < 86400 ? "hour" : "day";
  const divisor = unit === "second" ? 1 : unit === "minute" ? 60 : unit === "hour" ? 3600 : 86400;
  return new Intl.RelativeTimeFormat(undefined, { numeric: "auto" }).format(Math.round(seconds / divisor), unit);
}