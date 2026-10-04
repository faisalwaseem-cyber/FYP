import { forwardRef, useEffect, useRef } from "react";
import { AlertCircle, CheckCircle2, Circle, LoaderCircle, Moon, Sun, X } from "lucide-react";
import type { ButtonHTMLAttributes, ReactNode } from "react";
import { formatPkr } from "../format";

export const Button = forwardRef<HTMLButtonElement, ButtonHTMLAttributes<HTMLButtonElement>>(function Button(
  { className = "", ...props }, ref,
) {
  return <button ref={ref} className={`button ${className}`} {...props} />;
});

export function Card({ children, className = "" }: { children: ReactNode; className?: string }) {
  return <section className={`card ${className}`}>{children}</section>;
}

const badgeCopy: Record<string, string> = {
  connected: "Connected",
  error: "Error",
  not_connected: "Not connected",
  running: "Running",
  success: "Success",
  failed: "Failed",
  needs_reauth: "Needs reauthorization",
};

export function Badge({ status }: { status: keyof typeof badgeCopy }) {
  const Icon = status === "connected" || status === "success" ? CheckCircle2
    : status === "running" ? LoaderCircle
      : status === "failed" || status === "error" || status === "needs_reauth" ? AlertCircle : Circle;
  return (
    <span className={`badge badge-${status}`} role="status" aria-label={`Status: ${badgeCopy[status]}`}>
      <Icon size={14} aria-hidden="true" />{badgeCopy[status]}
    </span>
  );
}

export function PageHeader({ eyebrow, title, description, action }: {
  eyebrow: string;
  title: string;
  description: string;
  action?: ReactNode;
}) {
  return (
    <header className="page-header">
      <div><p className="eyebrow">{eyebrow}</p><h1>{title}</h1><p className="page-description">{description}</p></div>
      {action}
    </header>
  );
}

export function Skeleton({ lines = 2 }: { lines?: number }) {
  return <div className="skeleton-stack" aria-label="Loading" role="status">
    {Array.from({ length: lines }, (_, index) => <span className="skeleton" key={index} />)}
  </div>;
}

export function StateMessage({ kind, title, children }: {
  kind: "empty" | "error" | "info";
  title: string;
  children: ReactNode;
}) {
  return <div className={`state-message state-${kind}`} role={kind === "error" ? "alert" : "status"}>
    <strong>{title}</strong><p>{children}</p>
  </div>;
}

export function FormField({ id, label, hint, ...props }: {
  id: string;
  label: string;
  hint?: string;
} & React.InputHTMLAttributes<HTMLInputElement>) {
  return <div className="form-field">
    <label htmlFor={id}>{label}</label>
    <input id={id} aria-describedby={hint ? `${id}-hint` : undefined} {...props} />
    {hint && <small id={`${id}-hint`}>{hint}</small>}
  </div>;
}

export function ThemeToggle({ theme, onToggle }: { theme: "light" | "dark"; onToggle: () => void }) {
  const Icon = theme === "dark" ? Sun : Moon;
  return <Button className="icon-button quiet-button" onClick={onToggle} aria-label={`Switch to ${theme === "dark" ? "light" : "dark"} theme`} title="Theme">
    <Icon size={18} aria-hidden="true" />
  </Button>;
}

export function ConfirmDialog({ title, children, confirmLabel, onConfirm, onCancel, busy = false }: {
  title: string;
  children: ReactNode;
  confirmLabel: string;
  onConfirm: () => void;
  onCancel: () => void;
  busy?: boolean;
}) {
  const cancelRef = useRef<HTMLButtonElement>(null);
  useEffect(() => {
    cancelRef.current?.focus();
    const handleKey = (event: KeyboardEvent) => {
      if (event.key === "Escape" && !busy) onCancel();
    };
    window.addEventListener("keydown", handleKey);
    return () => window.removeEventListener("keydown", handleKey);
  }, [busy, onCancel]);

  return <div className="dialog-backdrop" onMouseDown={(event) => event.target === event.currentTarget && onCancel()}>
    <section className="dialog" role="alertdialog" aria-modal="true" aria-labelledby="confirm-title" aria-describedby="confirm-description">
      <button className="dialog-close" onClick={onCancel} aria-label="Close confirmation" disabled={busy}><X size={18} /></button>
      <h2 id="confirm-title">{title}</h2><div id="confirm-description">{children}</div>
      <div className="dialog-actions"><Button className="quiet-button" onClick={onCancel} disabled={busy} ref={cancelRef}>Cancel</Button>
        <Button className="danger-button" onClick={onConfirm} disabled={busy}>{busy ? "Disconnecting…" : confirmLabel}</Button></div>
    </section>
  </div>;
}

export function Toast({ message, onDismiss }: { message: string; onDismiss: () => void }) {
  useEffect(() => {
    const timeout = window.setTimeout(onDismiss, 5000);
    return () => window.clearTimeout(timeout);
  }, [message, onDismiss]);
  return <div className="toast" role="status" aria-live="polite"><span>{message}</span>
    <button onClick={onDismiss} aria-label="Dismiss message"><X size={16} /></button></div>;
}

export function MoneyValue({ value }: { value: string | number }) {
  const negative = String(value).trim().startsWith("-");
  return <span className={`money-value${negative ? " money-negative" : ""}`} aria-label={`${negative ? "Negative " : ""}${formatPkr(value)}`}>
    {formatPkr(value)}
  </span>;
}