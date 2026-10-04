import { afterEach, describe, expect, it, vi } from "vitest";
import { ApiError, disconnectMeta, getMetaStatus, getShopifyStatus, startMetaConnect, startShopifyConnect, triggerMetaSync, triggerShopifySync } from "./api";
import { formatPkr } from "./format";

afterEach(() => {
  vi.unstubAllEnvs();
  vi.unstubAllGlobals();
});

describe("Shopify API client", () => {
  it("uses the configured API and Bearer token without sending a tenant selector", async () => {
    vi.stubEnv("VITE_API_BASE_URL", "https://api.example.test/");
    const fetchMock = vi.fn().mockResolvedValue(new Response(JSON.stringify({
      connected: true,
      platform: "shopify",
      status: "active",
      store: "shop.myshopify.com",
    }), { status: 200 }));
    vi.stubGlobal("fetch", fetchMock);

    await getShopifyStatus("ephemeral-token");
    const [url, options] = fetchMock.mock.calls[0] as [string, RequestInit];
    expect(url).toBe("https://api.example.test/integrations/shopify/status");
    expect(new Headers(options.headers).get("Authorization")).toBe("Bearer ephemeral-token");
    expect(url).not.toContain("business_id");
    expect(options.body).toBeUndefined();
  });

  it("starts Shopify OAuth through the authenticated API client", async () => {
    vi.stubEnv("VITE_API_BASE_URL", "https://api.example.test");
    const fetchMock = vi.fn().mockResolvedValue(new Response(JSON.stringify({
      authorization_url: "https://shop.myshopify.com/admin/oauth/authorize?state=one-time-state",
    }), { status: 200 }));
    vi.stubGlobal("fetch", fetchMock);

    const result = await startShopifyConnect("ephemeral-token", "shop.myshopify.com");

    expect(result.authorization_url).toContain("https://shop.myshopify.com/");
    expect(fetchMock).toHaveBeenCalledWith(
      "https://api.example.test/integrations/shopify/connect?shop=shop.myshopify.com",
      expect.objectContaining({ method: "POST", headers: expect.objectContaining({ Authorization: "Bearer ephemeral-token" }) }),
    );
  });

  it("never exposes raw backend error details", async () => {
    vi.stubEnv("VITE_API_BASE_URL", "https://api.example.test");
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(new Response(JSON.stringify({ detail: "Traceback: secret-key-value" }), { status: 500 })));

    await expect(triggerShopifySync("token")).rejects.toMatchObject({
      message: "The service could not complete this request. Try again later.",
      status: 500,
    } satisfies Partial<ApiError>);
  });
});

describe("PKR formatting", () => {
  it.each([
    ["1250.50", "PKR 1,250.50"],
    ["-1250.50", "-PKR 1,250.50"],
    ["0", "PKR 0"],
    ["9876543210.25", "PKR 9,876,543,210.25"],
  ])("formats %s as %s", (input, expected) => {
    expect(formatPkr(input)).toBe(expected);
  });
});

describe("Meta Ads API client", () => {
  it("loads safe multi-account status using the authenticated request", async () => {
    vi.stubEnv("VITE_API_BASE_URL", "https://api.example.test/");
    const fetchMock = vi.fn().mockResolvedValue(new Response(JSON.stringify({
      connected: true,
      platform: "meta_ads",
      accounts: [{ account_identifier: "act_123", status: "active" }],
    }), { status: 200 }));
    vi.stubGlobal("fetch", fetchMock);

    const result = await getMetaStatus("bridge-session-token");
    expect(result.accounts).toHaveLength(1);
    const [url, options] = fetchMock.mock.calls[0] as [string, RequestInit];
    expect(url).toBe("https://api.example.test/integrations/meta/status");
    expect(url).not.toContain("business_id");
    expect(new Headers(options.headers).get("Authorization")).toBe("Bearer bridge-session-token");
  });

  it("starts OAuth on the backend and returns only its navigation URL", async () => {
    vi.stubEnv("VITE_API_BASE_URL", "https://api.example.test");
    const authorizationUrl = "https://www.facebook.com/v25.0/dialog/oauth?scope=ads_read&state=opaque";
    const fetchMock = vi.fn().mockResolvedValue(new Response(JSON.stringify({ authorization_url: authorizationUrl }), { status: 200 }));
    vi.stubGlobal("fetch", fetchMock);

    const result = await startMetaConnect("bridge-session-token");
    expect(result.authorization_url).toBe(authorizationUrl);
    expect(fetchMock).toHaveBeenCalledWith(
      "https://api.example.test/integrations/meta/connect",
      expect.objectContaining({ method: "POST", headers: expect.objectContaining({ Authorization: "Bearer bridge-session-token" }) }),
    );
  });

  it("selects one account for sync and disconnect without sending a tenant id", async () => {
    vi.stubEnv("VITE_API_BASE_URL", "https://api.example.test");
    const fetchMock = vi.fn()
      .mockResolvedValueOnce(new Response(JSON.stringify({ status: "success", accounts_disconnected: 1 }), { status: 200 }))
      .mockResolvedValueOnce(new Response(JSON.stringify({
        status: "success", accounts_synced: 1, campaigns_synced: 2, insights_fetched: 3,
        spend_inserted: 1, spend_updated: 2, date_range: { since: "2026-10-01", until: "2026-10-03" },
      }), { status: 200 }));
    vi.stubGlobal("fetch", fetchMock);

    await disconnectMeta("bridge-session-token", "act_12/3");
    await triggerMetaSync("bridge-session-token", "act_12/3", 5);
    expect(fetchMock.mock.calls[0][0]).toBe("https://api.example.test/integrations/meta/disconnect?account_identifier=act_12%2F3");
    expect(fetchMock.mock.calls[1][0]).toBe("https://api.example.test/integrations/meta/sync?account_identifier=act_12%2F3&days_back=5");
    for (const [, options] of fetchMock.mock.calls as [string, RequestInit][]) {
      expect(new Headers(options.headers).get("Authorization")).toBe("Bearer bridge-session-token");
      expect(options.body).toBeUndefined();
    }
    expect(fetchMock.mock.calls.every(([url]) => !String(url).includes("business_id"))).toBe(true);
  });
});