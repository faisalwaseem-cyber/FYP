import { createContext, useCallback, useContext, useEffect, useState } from "react";
import type { FormEvent } from "react";
import { QueryClient, QueryClientProvider, useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { Activity, AlertCircle, ArrowUpRight, Check, ChevronDown, CircleHelp, CreditCard, LayoutGrid, LogOut, Megaphone, RefreshCw, ShieldCheck, Store } from "lucide-react";
import { BrowserRouter, Link, Navigate, NavLink, Outlet, Route, Routes, useLocation, useNavigate } from "react-router-dom";
import { ApiError, disconnectMeta, disconnectShopify, getMetaStatus, getShopifyStatus, startMetaConnect, startShopifyConnect, triggerMetaSync, triggerShopifySync, type MetaAccount, type ShopifyStatus } from "./api";
import { ConnectionCard, type ConnectionCardStatus } from "./components/ConnectionCard";
import { Badge, Button, Card, ConfirmDialog, FormField, PageHeader, Skeleton, StateMessage, ThemeToggle, Toast } from "./components/ui";
import { navigateToProvider } from "./navigation";

type Theme = "light" | "dark";
type SessionValue = { token: string; signOut: () => void; setToken: (token: string) => void };
const SessionContext = createContext<SessionValue | null>(null);
const queryClient = new QueryClient({ defaultOptions: { queries: { retry: 1, refetchOnWindowFocus: false } } });
const SESSION_TOKEN_KEY = "bridge-session-token";

function useSession() {
  const session = useContext(SessionContext);
  if (!session) throw new Error("Session context is unavailable.");
  return session;
}

function preferredTheme(): Theme {
  const saved = localStorage.getItem("bridge-theme");
  if (saved === "dark" || saved === "light") return saved;
  return typeof window.matchMedia === "function" && window.matchMedia("(prefers-color-scheme: dark)").matches ? "dark" : "light";
}

export default function App() {
  const [token, setTokenState] = useState(() => sessionStorage.getItem(SESSION_TOKEN_KEY) || "");
  const [theme, setTheme] = useState<Theme>(preferredTheme);
  const setToken = useCallback((accessToken: string) => {
    setTokenState(accessToken);
    if (accessToken) sessionStorage.setItem(SESSION_TOKEN_KEY, accessToken);
    else sessionStorage.removeItem(SESSION_TOKEN_KEY);
  }, []);
  const signOut = useCallback(() => {
    setToken("");
    queryClient.clear();
  }, [setToken]);

  useEffect(() => {
    document.documentElement.dataset.theme = theme;
    localStorage.setItem("bridge-theme", theme);
  }, [theme]);

  const session = { token, setToken, signOut };
  return <QueryClientProvider client={queryClient}>
    <SessionContext.Provider value={session}>
      <BrowserRouter>
        <Routes>
          <Route path="/login" element={<LoginPage />} />
          <Route element={<ProtectedShell theme={theme} onThemeToggle={() => setTheme(theme === "light" ? "dark" : "light")} />}>
            <Route path="/" element={<ConnectionsPage />} />
            <Route path="/sync" element={<SyncPage />} />
            <Route path="/payments" element={<PaymentsPage />} />
          </Route>
          <Route path="*" element={<Navigate to="/" replace />} />
        </Routes>
      </BrowserRouter>
    </SessionContext.Provider>
  </QueryClientProvider>;
}

function LoginPage() {
  const [input, setInput] = useState("");
  const [error, setError] = useState("");
  const { token, setToken } = useSession();
  const navigate = useNavigate();
  const location = useLocation();
  const expired = Boolean((location.state as { expired?: boolean } | null)?.expired);
  if (token) return <Navigate to="/" replace />;

  function submit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const accessToken = input.trim();
    setInput("");
    if (!accessToken) {
      setError("Enter an access token to continue.");
      return;
    }
    setError("");
    setToken(accessToken);
    navigate((location.state as { from?: string } | null)?.from || "/", { replace: true });
  }

  return <main className="auth-page">
    <div className="auth-brand"><span className="brand-mark"><Activity size={20} /></span><span>bridge<span className="brand-light">flow</span></span></div>
    <Card className="auth-card">
      <div className="auth-icon"><ShieldCheck size={22} /></div>
      <p className="eyebrow">WORKSPACE ACCESS</p><h1>Connect to your workspace</h1>
      <p className="auth-copy">Use a Bearer token issued by your authentication service. It is kept for this tab and cleared when you sign out.</p>
      {expired && <StateMessage kind="info" title="Session expired">Enter a fresh access token to continue.</StateMessage>}
      <form onSubmit={submit}>
        <FormField id="api-token" label="API access token" type="password" autoComplete="off" value={input} onChange={(event) => setInput(event.target.value)} required />
        {error && <p className="field-error" role="alert">{error}</p>}
        <Button className="primary-button submit-button" type="submit">Continue <ArrowUpRight size={16} /></Button>
      </form>
      <p className="auth-footnote"><CircleHelp size={14} /> Backend login and token-issuing endpoints are not available.</p>
    </Card>
    <p className="auth-caption">A clearer view of your commerce operations</p>
  </main>;
}

function ProtectedShell({ theme, onThemeToggle }: { theme: Theme; onThemeToggle: () => void }) {
  const { token } = useSession();
  const location = useLocation();
  if (!token) return <Navigate to="/login" replace state={{ from: location.pathname }} />;
  return <div className="app-shell">
    <aside className="sidebar">
      <Link className="brand" to="/" aria-label="Bridge Flow home"><span className="brand-mark"><Activity size={20} /></span><span>bridge<span className="brand-light">flow</span></span></Link>
      <div className="workspace-label">WORKSPACE</div><div className="workspace-name"><span className="workspace-avatar">B</span><span>Business workspace</span><ChevronDown size={14} /></div>
      <div className="nav-label">OPERATIONS</div>
      <nav className="side-nav" aria-label="Main navigation">
        <NavLink to="/" end><LayoutGrid size={18} /><span>Connections</span></NavLink>
        <NavLink to="/sync"><RefreshCw size={18} /><span>Sync</span></NavLink>
        <NavLink to="/payments"><CreditCard size={18} /><span>Payments</span></NavLink>
      </nav>
      <div className="sidebar-bottom"><div className="secure-note"><ShieldCheck size={16} /><span>Credentials stay encrypted</span></div></div>
    </aside>
    <div className="main-column">
      <header className="topbar"><div className="breadcrumb">Business workspace <span>/</span> <strong>Operations</strong></div>
        <div className="topbar-actions"><ThemeToggle theme={theme} onToggle={onThemeToggle} /><UserMenu /></div>
      </header>
      <main className="page-content"><Outlet /></main>
      <footer className="page-footer"><span>Bridge Flow</span><span>Connected services are managed securely</span></footer>
    </div>
  </div>;
}

function UserMenu() {
  const [open, setOpen] = useState(false);
  const { signOut } = useSession();
  return <div className="user-menu-wrap">
    <Button className="user-menu" aria-expanded={open} aria-haspopup="menu" onClick={() => setOpen(!open)}>
      <span className="user-avatar">U</span><span className="user-menu-label">Workspace user</span><ChevronDown size={14} />
    </Button>
    {open && <div className="user-popover" role="menu"><p>Authenticated with Bearer token</p><button role="menuitem" onClick={signOut}><LogOut size={16} /> Sign out</button></div>}
  </div>;
}

function useApiError(error: unknown) {
  const { signOut } = useSession();
  const navigate = useNavigate();
  useEffect(() => {
    if (error instanceof ApiError && error.status === 401) {
      signOut();
      navigate("/login", { replace: true, state: { expired: true } });
    }
  }, [error, navigate, signOut]);
}

function useShopifyStatus() {
  const { token } = useSession();
  return useQuery({ queryKey: ["shopify-status"], queryFn: () => getShopifyStatus(token) });
}

function useMetaStatus() {
  const { token } = useSession();
  return useQuery({ queryKey: ["meta-status"], queryFn: () => getMetaStatus(token) });
}

function ConnectionsPage() {
  const statusQuery = useShopifyStatus();
  const metaStatusQuery = useMetaStatus();
  const queryClient = useQueryClient();
  const { token } = useSession();
  const location = useLocation();
  const navigate = useNavigate();
  const [confirm, setConfirm] = useState(false);
  const [toast, setToast] = useState("");
  const [connectOpen, setConnectOpen] = useState(false);
  const [shopInput, setShopInput] = useState("");
  const [metaDisconnectTarget, setMetaDisconnectTarget] = useState<MetaAccount | null>(null);
  useApiError(statusQuery.error);
  useApiError(metaStatusQuery.error);
  useEffect(() => {
    const params = new URLSearchParams(location.search);
    const shopifyOutcome = params.get("shopify");
    const metaOutcome = params.get("meta");
    if (shopifyOutcome === "connected" || shopifyOutcome === "error") {
      setToast(shopifyOutcome === "connected" ? "Shopify connected." : shopifyOAuthErrorMessage(params.get("reason")));
    } else if (metaOutcome === "connected" || metaOutcome === "error") {
      setToast(metaOutcome === "connected" ? metaOAuthSuccessMessage(params.get("accounts")) : metaOAuthErrorMessage(params.get("reason")));
      if (metaOutcome === "connected") void queryClient.invalidateQueries({ queryKey: ["meta-status"] });
    } else {
      return;
    }
    navigate(location.pathname, { replace: true });
  }, [location.pathname, location.search, navigate, queryClient]);
  const connect = useMutation({
    mutationFn: (shop: string) => startShopifyConnect(token, shop),
    onSuccess: ({ authorization_url }) => window.location.assign(authorization_url),
  });
  const disconnect = useMutation({
    mutationFn: () => disconnectShopify(token),
    onSuccess: async (result) => {
      setConfirm(false);
      setToast("Shopify disconnected.");
      if (result.status === "success") {
        queryClient.setQueryData<ShopifyStatus>(["shopify-status"], (current) => current ? { ...current, connected: false, status: "disconnected" } : current);
        await queryClient.invalidateQueries({ queryKey: ["shopify-status"] });
      }
    },
  });
  const sync = useMutation({
    mutationFn: () => triggerShopifySync(token),
    onSuccess: async (result) => {
      if (result.status === "success") {
        setToast("Shopify sync completed.");
        await queryClient.invalidateQueries({ queryKey: ["shopify-status"] });
      }
    },
  });
  const metaConnect = useMutation({
    mutationFn: () => startMetaConnect(token),
    onSuccess: ({ authorization_url }) => navigateToProvider(authorization_url),
  });
  const metaDisconnect = useMutation({
    mutationFn: (accountIdentifier: string) => disconnectMeta(token, accountIdentifier),
    onSuccess: async () => {
      setMetaDisconnectTarget(null);
      setToast("Meta ad account disconnected.");
      await queryClient.invalidateQueries({ queryKey: ["meta-status"] });
    },
  });
  const metaSync = useMutation({
    mutationFn: (accountIdentifier: string) => triggerMetaSync(token, accountIdentifier),
    onSuccess: async (result) => {
      setToast(`Meta sync completed: ${result.campaigns_synced} campaigns, ${result.spend_inserted} spend rows inserted.`);
      await queryClient.invalidateQueries({ queryKey: ["meta-status"] });
    },
  });
  useApiError(connect.error);
  useApiError(disconnect.error);
  useApiError(sync.error);
  useApiError(metaConnect.error);
  useApiError(metaDisconnect.error);
  useApiError(metaSync.error);
  const status = statusQuery.data;
  const metaAccounts = metaStatusQuery.data?.accounts || [];

  return <>
    <PageHeader eyebrow="INTEGRATIONS" title="Connections" description="Manage the commerce and marketing platforms connected to this workspace." />
    <ConnectionCard
      platformName="Shopify"
      status={shopifyCardStatus(status)}
      accountIdentifier={status?.store}
      lastSyncedAt={status?.last_synced_at}
      loading={statusQuery.isPending}
      loadError={statusQuery.isError ? safeMessage(statusQuery.error) : undefined}
      actionError={sync.isError ? safeMessage(sync.error) : disconnect.isError ? safeMessage(disconnect.error) : undefined}
      busy={sync.isPending}
      disconnecting={disconnect.isPending}
      onConnect={() => {
        setShopInput(status?.store || "");
        connect.reset();
        setConnectOpen(true);
      }}
      onSync={() => sync.mutate()}
      onDisconnect={() => setConfirm(true)}
      onRetryLoad={() => void statusQuery.refetch()}
      onRetryAction={sync.isError ? () => sync.mutate() : disconnect.isError ? () => disconnect.mutate() : undefined}
    />
    {metaAccounts.length > 0 ? metaAccounts.map((account) => {
      const accountLabel = account.account_name
        ? `${account.account_name} · ${account.account_identifier}`
        : account.account_identifier;
      const cardStatus = metaCardStatus(account);
      const syncFailed = metaSync.isError && metaSync.variables === account.account_identifier;
      const disconnectFailed = metaDisconnect.isError && metaDisconnect.variables === account.account_identifier;
      return <ConnectionCard
        key={account.account_identifier}
        platformName="Meta Ads"
        description="Advertising data source"
        status={cardStatus}
        accountIdentifier={accountLabel}
        lastSyncedAt={account.last_synced_at}
        icon={<Megaphone size={22} />}
        loading={metaStatusQuery.isPending}
        loadError={metaStatusQuery.isError ? safeMessage(metaStatusQuery.error) : undefined}
        actionError={syncFailed ? safeMessage(metaSync.error) : disconnectFailed ? safeMessage(metaDisconnect.error) : metaConnect.isError ? safeMessage(metaConnect.error) : undefined}
        busy={metaSync.isPending && metaSync.variables === account.account_identifier || metaConnect.isPending}
        disconnecting={metaDisconnect.isPending && metaDisconnect.variables === account.account_identifier}
        onConnect={() => metaConnect.mutate()}
        onSync={() => metaSync.mutate(account.account_identifier)}
        onDisconnect={() => setMetaDisconnectTarget(account)}
        onRetryLoad={() => void metaStatusQuery.refetch()}
        onRetryAction={syncFailed
          ? () => metaSync.mutate(account.account_identifier)
          : disconnectFailed
            ? () => metaDisconnect.mutate(account.account_identifier)
            : metaConnect.isError ? () => metaConnect.mutate() : undefined}
      />;
    }) : <ConnectionCard
      platformName="Meta Ads"
      description="Advertising data source"
      status="not_connected"
      icon={<Megaphone size={22} />}
      loading={metaStatusQuery.isPending}
      loadError={metaStatusQuery.isError ? safeMessage(metaStatusQuery.error) : undefined}
      actionError={metaConnect.isError ? safeMessage(metaConnect.error) : undefined}
      busy={metaConnect.isPending}
      onConnect={() => metaConnect.mutate()}
      onRetryLoad={() => void metaStatusQuery.refetch()}
      onRetryAction={metaConnect.isError ? () => metaConnect.mutate() : undefined}
    />}
    {connectOpen && <Card className="connect-shopify-card">
      <form onSubmit={(event) => { event.preventDefault(); connect.mutate(shopInput.trim()); }}>
        <FormField id="shopify-domain" label="Shopify store domain" placeholder="your-store.myshopify.com" autoComplete="url" value={shopInput} onChange={(event) => setShopInput(event.target.value)} required />
        {connect.isError && <StateMessage kind="error" title="Shopify connection could not start">{safeMessage(connect.error)} <button className="text-action" type="button" onClick={() => connect.mutate(shopInput.trim())}>Retry</button></StateMessage>}
        <div className="provider-actions">
          <Button className="primary-button" type="submit" disabled={connect.isPending || !shopInput.trim()}>{connect.isPending ? "Preparing…" : "Continue to Shopify"}</Button>
          <Button className="quiet-button" type="button" onClick={() => { setConnectOpen(false); connect.reset(); }}>Cancel</Button>
        </div>
      </form>
    </Card>}
    {confirm && <ConfirmDialog title="Disconnect Shopify?" confirmLabel="Disconnect" busy={disconnect.isPending} onCancel={() => setConfirm(false)} onConfirm={() => disconnect.mutate()}>
      <p>Shopify data will no longer sync for this workspace. You can reconnect after OAuth setup is available.</p>
      {disconnect.isError && <p className="field-error" role="alert">{safeMessage(disconnect.error)}</p>}
    </ConfirmDialog>}
    {metaDisconnectTarget && <ConfirmDialog
      title={`Disconnect ${metaDisconnectTarget.account_name || "Meta ad account"}?`}
      confirmLabel="Disconnect"
      busy={metaDisconnect.isPending}
      onCancel={() => setMetaDisconnectTarget(null)}
      onConfirm={() => metaDisconnect.mutate(metaDisconnectTarget.account_identifier)}
    >
      <p>This Meta ad account will no longer sync for this workspace. Its encrypted connection remains unavailable until reconnected.</p>
      {metaDisconnect.isError && <p className="field-error" role="alert">{safeMessage(metaDisconnect.error)}</p>}
    </ConfirmDialog>}
    {toast && <Toast message={toast} onDismiss={() => setToast("")} />}
  </>;
}

function SyncPage() {
  const statusQuery = useShopifyStatus();
  const queryClient = useQueryClient();
  const { token } = useSession();
  const mutation = useMutation({
    mutationFn: () => triggerShopifySync(token),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ["shopify-status"] }),
  });
  useApiError(statusQuery.error);
  useApiError(mutation.error);
  const status = statusQuery.data;
  const connected = status?.connected === true;

  return <>
    <PageHeader eyebrow="OPERATIONS" title="Sync" description="Run a data sync for an active platform connection." />
    <Card className="sync-card">
      <div className="sync-heading"><div className="provider-icon shopify-icon"><Store size={22} /></div><div><p className="eyebrow">PLATFORM</p><h2>Shopify</h2></div>
        {mutation.isPending ? <Badge status="running" /> : mutation.isSuccess ? <Badge status="success" /> : mutation.isError ? <Badge status="failed" /> : status && <Badge status={shopifyCardStatus(status)} />}
      </div>
      {statusQuery.isPending ? <Skeleton lines={2} /> : statusQuery.isError ? <StateMessage kind="error" title="Connection status unavailable">{safeMessage(statusQuery.error)} <button className="text-action" onClick={() => void statusQuery.refetch()}>Try again</button></StateMessage> :
        <>
          {!connected && <StateMessage kind="info" title="Shopify is not connected">Connect an active Shopify store before starting a sync.</StateMessage>}
          {connected && <p className="sync-copy">Sync {status?.store || "the connected Shopify store"}. This request runs synchronously and reports when it finishes.</p>}
          <Button className="primary-button sync-button" disabled={!connected || mutation.isPending} onClick={() => mutation.mutate()}>
            <RefreshCw size={16} className={mutation.isPending ? "spin" : ""} />{mutation.isPending ? "Sync running…" : "Start sync"}
          </Button>
        </>}
    </Card>
    {mutation.isPending && <Card className="run-result"><div className="result-title"><Badge status="running" /><strong>Sync request in progress</strong></div><Skeleton lines={2} /></Card>}
    {mutation.isSuccess && <Card className="run-result"><div className="result-title"><Badge status="success" /><strong>Sync completed</strong></div>
      <div className="result-grid">{metric("Records fetched", mutation.data.metrics?.total_fetched)}{metric("Inserted", mutation.data.metrics?.total_inserted)}{metric("Updated", mutation.data.metrics?.total_updated)}</div>
      <p className="muted-note">Historical sync runs are not available from the backend.</p>
    </Card>}
    {mutation.isError && <StateMessage kind="error" title="Sync failed">{safeMessage(mutation.error)} <button className="text-action" onClick={() => mutation.mutate()}>Retry</button></StateMessage>}
    {status && <p className="page-footnote"><AlertCircle size={15} /> Sync history and run timestamps are not exposed by the backend.</p>}
  </>;
}

function PaymentsPage() {
  return <>
    <PageHeader eyebrow="SETTLEMENTS" title="Payment imports" description="Import provider settlement statements and review validation results." />
    <Card className="unavailable-card"><span className="unavailable-icon"><CreditCard size={23} /></span>
      <h2>Payment import is not available yet</h2>
      <p>The backend has payment parsing and sync pipeline code, but no HTTP upload endpoint. No file is uploaded and no settlement counts are simulated.</p>
      <StateMessage kind="info" title="Backend endpoint needed">An authenticated CSV/Excel upload route with accepted, duplicate, and rejected row results is required.</StateMessage>
      <Link to="/" className="text-link">Return to connections <ArrowUpRight size={15} /></Link>
    </Card>
  </>;
}

function metric(label: string, value: number | undefined) {
  return <div className="metric" key={label}><span>{label}</span><strong>{value === undefined ? "—" : value.toLocaleString()}</strong></div>;
}

function shopifyCardStatus(status: ShopifyStatus | undefined): ConnectionCardStatus {
  if (status?.status === "needs_reauth") return "needs_reauth";
  if (status?.status === "error") return "error";
  if (status?.connected) return "connected";
  return "not_connected";
}

function metaCardStatus(account: MetaAccount): ConnectionCardStatus {
  if (account.status === "needs_reauth") return "needs_reauth";
  if (account.status === "error") return "error";
  return account.status === "active" ? "connected" : "not_connected";
}

function metaOAuthSuccessMessage(accounts: string | null): string {
  const accountCount = Number(accounts);
  return Number.isInteger(accountCount) && accountCount > 0
    ? `Meta Ads connected. ${accountCount} ad account${accountCount === 1 ? "" : "s"} available.`
    : "Meta Ads connected.";
}

function metaOAuthErrorMessage(reason: string | null): string {
  switch (reason) {
    case "invalid_state": return "Meta authorization expired or could not be verified. Please reconnect.";
    case "authorization_denied": return "Meta authorization was denied.";
    case "no_accessible_ad_accounts": return "No accessible Meta ad accounts were found.";
    case "connection_storage_failed": return "The Meta connection could not be saved. Please try again.";
    default: return "Meta Ads connection could not be completed. Please try again.";
  }
}

function shopifyOAuthErrorMessage(reason: string | null): string {
  switch (reason) {
    case "invalid_request": return "Shopify did not return all required authorization details.";
    case "invalid_hmac": return "Shopify authorization could not be verified.";
    case "invalid_state": return "Shopify authorization expired or could not be verified. Please reconnect.";
    case "invalid_shop": return "The Shopify store domain could not be verified.";
    case "token_exchange_failed": return "Shopify could not complete authorization. Please try again.";
    case "connection_storage_failed": return "The Shopify connection could not be saved. Please try again.";
    default: return "Shopify connection could not be completed. Please try again.";
  }
}

function safeMessage(error: unknown): string {
  return error instanceof ApiError ? error.message : "The request could not be completed. Try again later.";
}
