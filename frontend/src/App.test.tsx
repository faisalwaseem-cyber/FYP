import { cleanup, render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";
vi.mock("./navigation", () => ({ navigateToProvider: vi.fn() }));
import { navigateToProvider } from "./navigation";
import App from "./App";

function connectionCard(accountLabelText: string) {
  const accountLabel = screen.getByText(accountLabelText);
  const card = accountLabel.closest(".connection-card");
  if (!(card instanceof HTMLElement)) throw new Error(`Missing connection card for ${accountLabelText}`);
  return within(card);
}

function accountCard(accountName: string, accountIdentifier: string) {
  return connectionCard(`${accountName} · ${accountIdentifier}`);
}

afterEach(() => {
  cleanup();
  vi.clearAllMocks();
  localStorage.clear();
  sessionStorage.clear();
  vi.unstubAllEnvs();
  vi.unstubAllGlobals();
});

describe("frontend access and supported connection state", () => {
  it("redirects protected pages to access when no token is in memory", () => {
    window.history.replaceState({}, "", "/sync");
    render(<App />);
    expect(screen.getByRole("heading", { name: "Connect to your workspace" })).toBeInTheDocument();
  });

  it("submits an ephemeral token, clears the input, and shows the real connection state", async () => {
    vi.stubEnv("VITE_API_BASE_URL", "https://api.example.test");
    const fetchMock = vi.fn().mockResolvedValue(new Response(JSON.stringify({
      connected: true,
      platform: "shopify",
      store: "shop.myshopify.com",
      status: "active",
      last_synced_at: null,
    }), { status: 200 }));
    vi.stubGlobal("fetch", fetchMock);
    window.history.replaceState({}, "", "/");
    const user = userEvent.setup();
    render(<App />);

    const tokenField = screen.getByLabelText("API access token");
    await user.type(tokenField, "temporary-access-token");
    await user.click(screen.getByRole("button", { name: "Continue" }));

    expect(await screen.findByRole("status", { name: "Status: Connected" })).toBeInTheDocument();
    expect(localStorage.getItem("bridge-access-token")).toBeNull();
    expect(fetchMock.mock.calls[0][1]?.headers).toMatchObject({ Authorization: "Bearer temporary-access-token" });
    expect(screen.queryByText("temporary-access-token")).not.toBeInTheDocument();
  });

  it("restores the tab session after OAuth and loads the real Shopify status", async () => {
    vi.stubEnv("VITE_API_BASE_URL", "https://api.example.test");
    sessionStorage.setItem("bridge-session-token", "session-api-token");
    const fetchMock = vi.fn().mockResolvedValue(new Response(JSON.stringify({
      connected: true,
      platform: "shopify",
      store: "shop.myshopify.com",
      status: "active",
    }), { status: 200 }));
    vi.stubGlobal("fetch", fetchMock);
    window.history.replaceState({}, "", "/?shopify=connected");
    render(<App />);

    expect(await screen.findByRole("status", { name: "Status: Connected" })).toBeInTheDocument();
    expect(await screen.findByText("Shopify connected.")).toBeInTheDocument();
    expect(fetchMock).toHaveBeenCalledWith(
      "https://api.example.test/integrations/shopify/status",
      expect.objectContaining({ headers: expect.objectContaining({ Authorization: "Bearer session-api-token" }) }),
    );
  });

  it("renders an errored connection as Error, not Not connected", async () => {
    vi.stubEnv("VITE_API_BASE_URL", "https://api.example.test");
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(new Response(JSON.stringify({
      connected: false,
      platform: "shopify",
      store: "shop.myshopify.com",
      status: "error",
    }), { status: 200 })));
    window.history.replaceState({}, "", "/");
    const user = userEvent.setup();
    render(<App />);

    await user.type(screen.getByLabelText("API access token"), "temporary-access-token");
    await user.click(screen.getByRole("button", { name: "Continue" }));

    expect(await screen.findByRole("status", { name: "Status: Error" })).toBeInTheDocument();
    expect(screen.queryByRole("status", { name: "Status: Not connected" })).not.toBeInTheDocument();
  });

  it("triggers a supported Shopify sync and renders returned counts", async () => {
    vi.stubEnv("VITE_API_BASE_URL", "https://api.example.test");
    const fetchMock = vi.fn(async (_input: RequestInfo | URL, init?: RequestInit) => {
      if (init?.method === "POST") {
        return new Response(JSON.stringify({
          status: "success",
          metrics: { total_fetched: 12, total_inserted: 3, total_updated: 9 },
        }), { status: 200 });
      }
      return new Response(JSON.stringify({ connected: true, platform: "shopify", status: "active" }), { status: 200 });
    });
    vi.stubGlobal("fetch", fetchMock);
    window.history.replaceState({}, "", "/");
    const user = userEvent.setup();
    render(<App />);

    await user.type(screen.getByLabelText("API access token"), "temporary-access-token");
    await user.click(screen.getByRole("button", { name: "Continue" }));
    await user.click(screen.getByRole("link", { name: /^Sync$/ }));
    await user.click(await screen.findByRole("button", { name: "Start sync" }));

    expect(await screen.findByText("Sync completed")).toBeInTheDocument();
    expect(screen.getByText("12")).toBeInTheDocument();
    expect(fetchMock).toHaveBeenCalledWith("https://api.example.test/integrations/shopify/sync", expect.objectContaining({ method: "POST" }));
  });

  it("syncs from the connection card, refreshes status, and reflects a confirmed disconnect", async () => {
    vi.stubEnv("VITE_API_BASE_URL", "https://api.example.test");
    let statusRequestCount = 0;
    const fetchMock = vi.fn(async (_input: RequestInfo | URL, init?: RequestInit) => {
      const url = String(_input);
      if (init?.method === "POST" && String(_input).endsWith("/sync")) {
        return new Response(JSON.stringify({ status: "success", metrics: { total_fetched: 2 } }), { status: 200 });
      }
      if (init?.method === "POST" && String(_input).endsWith("/disconnect")) {
        return new Response(JSON.stringify({ status: "success", message: "Disconnected" }), { status: 200 });
      }
      if (!url.endsWith("/integrations/shopify/status")) {
        return new Response(JSON.stringify({ connected: false, platform: "meta_ads", accounts: [] }), { status: 200 });
      }
      statusRequestCount += 1;
      return new Response(JSON.stringify(statusRequestCount < 3
        ? { connected: true, platform: "shopify", store: "shop.myshopify.com", status: "active", last_synced_at: null }
        : { connected: false, platform: "shopify", store: "shop.myshopify.com", status: "disconnected" }), { status: 200 });
    });
    vi.stubGlobal("fetch", fetchMock);
    window.history.replaceState({}, "", "/");
    const user = userEvent.setup();
    render(<App />);

    await user.type(screen.getByLabelText("API access token"), "temporary-access-token");
    await user.click(screen.getByRole("button", { name: "Continue" }));
    expect(await screen.findByText("shop.myshopify.com")).toBeInTheDocument();
    expect(screen.getByText("Not synced yet")).toBeInTheDocument();

    await user.click(screen.getByRole("button", { name: "Sync now" }));
    expect(await screen.findByText("Shopify sync completed.")).toBeInTheDocument();
    expect(statusRequestCount).toBe(2);

    await user.click(screen.getByRole("button", { name: "Disconnect" }));
    const dialog = screen.getByRole("alertdialog");
    await user.click(within(dialog).getByRole("button", { name: "Disconnect" }));
    expect(await connectionCard("shop.myshopify.com").findByRole("status", { name: "Status: Not connected" })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Connect Shopify" })).toBeEnabled();
    expect(statusRequestCount).toBe(3);
  });

  it("handles a Meta OAuth return and renders each connected account safely", async () => {
    vi.stubEnv("VITE_API_BASE_URL", "https://api.example.test");
    sessionStorage.setItem("bridge-session-token", "session-api-token");
    const fetchMock = vi.fn(async (input: RequestInfo | URL) => {
      if (String(input).endsWith("/integrations/meta/status")) {
        return new Response(JSON.stringify({
          connected: true,
          platform: "meta_ads",
          accounts: [
            { account_identifier: "act_123", account_name: "North Shop", status: "active", last_synced_at: null },
            { account_identifier: "act_456", account_name: "South Shop", status: "active", last_synced_at: null },
          ],
        }), { status: 200 });
      }
      return new Response(JSON.stringify({ connected: false, platform: "shopify", status: "not_connected" }), { status: 200 });
    });
    vi.stubGlobal("fetch", fetchMock);
    window.history.replaceState({}, "", "/?meta=connected&accounts=2");
    render(<App />);

    expect(await screen.findByText("Meta Ads connected. 2 ad accounts available.")).toBeInTheDocument();
    expect(await screen.findByText("North Shop · act_123")).toBeInTheDocument();
    expect(await screen.findByText("South Shop · act_456")).toBeInTheDocument();
    expect(window.location.search).toBe("");
    expect(sessionStorage.getItem("bridge-session-token")).toBe("session-api-token");
    expect(screen.queryByText("fixture-meta-access-token")).not.toBeInTheDocument();
  });

  it("offers reconnect for needs_reauth and calls Meta OAuth start", async () => {
    vi.stubEnv("VITE_API_BASE_URL", "https://api.example.test");
    sessionStorage.setItem("bridge-session-token", "session-api-token");
    const fetchMock = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      if (String(input).endsWith("/integrations/meta/status")) {
        return new Response(JSON.stringify({
          connected: false,
          platform: "meta_ads",
          accounts: [{ account_identifier: "act_123", account_name: "North Shop", status: "needs_reauth" }],
        }), { status: 200 });
      }
      if (String(input).endsWith("/integrations/meta/connect") && init?.method === "POST") {
        return new Response(JSON.stringify({ authorization_url: "https://facebook.example/oauth" }), { status: 200 });
      }
      return new Response(JSON.stringify({ connected: false, platform: "shopify", status: "not_connected" }), { status: 200 });
    });
    vi.stubGlobal("fetch", fetchMock);
    window.history.replaceState({}, "", "/");
    const user = userEvent.setup();
    render(<App />);

    const metaCard = accountCard("North Shop", "act_123");
    expect(await metaCard.findByRole("status", { name: "Status: Needs reauthorization" })).toBeInTheDocument();
    await user.click(metaCard.getByRole("button", { name: "Reconnect Meta Ads" }));
    await waitFor(() => expect(navigateToProvider).toHaveBeenCalledWith("https://facebook.example/oauth"));
    expect(fetchMock).toHaveBeenCalledWith(
      "https://api.example.test/integrations/meta/connect",
      expect.objectContaining({ method: "POST", headers: expect.objectContaining({ Authorization: "Bearer session-api-token" }) }),
    );
  });

  it("syncs only the selected Meta account, blocks duplicate clicks, and refreshes status", async () => {
    vi.stubEnv("VITE_API_BASE_URL", "https://api.example.test");
    sessionStorage.setItem("bridge-session-token", "session-api-token");
    let resolveSync: ((response: Response) => void) | undefined;
    let metaStatusRequests = 0;
    const fetchMock = vi.fn((input: RequestInfo | URL, init?: RequestInit) => {
      const url = String(input);
      if (url.endsWith("/integrations/meta/status")) {
        metaStatusRequests += 1;
        return Promise.resolve(new Response(JSON.stringify({
          connected: true,
          platform: "meta_ads",
          accounts: [{ account_identifier: "act_123", account_name: "North Shop", status: "active" }],
        }), { status: 200 }));
      }
      if (new URL(url).pathname === "/integrations/meta/sync") {
        return new Promise<Response>((resolve) => { resolveSync = resolve; });
      }
      return Promise.resolve(new Response(JSON.stringify({ connected: false, platform: "shopify", status: "not_connected" }), { status: 200 }));
    });
    vi.stubGlobal("fetch", fetchMock);
    window.history.replaceState({}, "", "/");
    const user = userEvent.setup();
    render(<App />);

    const metaCard = accountCard("North Shop", "act_123");
    expect(await metaCard.findByRole("status", { name: "Status: Connected" })).toBeInTheDocument();
    const syncButton = metaCard.getByRole("button", { name: "Sync now" });
    await user.click(syncButton);
    const syncingButton = await metaCard.findByRole("button", { name: "Syncing…" });
    expect(syncingButton).toBeDisabled();
    await user.click(syncingButton);
    expect(fetchMock.mock.calls.filter(([url]) => String(url).includes("/integrations/meta/sync"))).toHaveLength(1);

    resolveSync?.(new Response(JSON.stringify({
      status: "success", accounts_synced: 1, campaigns_synced: 2, insights_fetched: 3,
      spend_inserted: 1, spend_updated: 0, date_range: { since: "2026-10-01", until: "2026-10-03" },
    }), { status: 200 }));
    expect(await screen.findByText("Meta sync completed: 2 campaigns, 1 spend rows inserted.")).toBeInTheDocument();
    expect(metaStatusRequests).toBeGreaterThan(1);
    expect(fetchMock).toHaveBeenCalledWith(
      "https://api.example.test/integrations/meta/sync?account_identifier=act_123&days_back=7",
      expect.objectContaining({ method: "POST" }),
    );
  });

  it("disconnects a selected Meta account and displays OAuth errors safely", async () => {
    vi.stubEnv("VITE_API_BASE_URL", "https://api.example.test");
    sessionStorage.setItem("bridge-session-token", "session-api-token");
    let disconnected = false;
    const fetchMock = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = String(input);
      if (url.endsWith("/integrations/meta/status")) {
        return new Response(JSON.stringify({
          connected: !disconnected,
          platform: "meta_ads",
          accounts: [{ account_identifier: "act_123", account_name: "North Shop", status: disconnected ? "disconnected" : "active" }],
        }), { status: 200 });
      }
      if (url.includes("/integrations/meta/disconnect")) {
        disconnected = true;
        return new Response(JSON.stringify({ status: "disconnected", accounts_disconnected: 1 }), { status: 200 });
      }
      return new Response(JSON.stringify({ connected: false, platform: "shopify", status: "not_connected" }), { status: 200 });
    });
    vi.stubGlobal("fetch", fetchMock);
    window.history.replaceState({}, "", "/?meta=error&reason=authorization_failed");
    const user = userEvent.setup();
    render(<App />);

    expect(await screen.findByText("Meta Ads connection could not be completed. Please try again.")).toBeInTheDocument();
    const metaCard = accountCard("North Shop", "act_123");
    await user.click(metaCard.getByRole("button", { name: "Disconnect" }));
    const dialog = screen.getByRole("alertdialog");
    await user.click(within(dialog).getByRole("button", { name: "Disconnect" }));
    expect(metaCard.getByRole("button", { name: "Connect Meta Ads" })).toBeInTheDocument();
    expect(fetchMock).toHaveBeenCalledWith(
      "https://api.example.test/integrations/meta/disconnect?account_identifier=act_123",
      expect.objectContaining({ method: "POST" }),
    );
    expect(screen.queryByText("fixture-meta-access-token")).not.toBeInTheDocument();
  });
});