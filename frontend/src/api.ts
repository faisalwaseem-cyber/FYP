export type ShopifyStatus = {
  connected: boolean;
  platform: "shopify";
  store?: string;
  status: "active" | "error" | "needs_reauth" | "disconnected" | "not_connected" | string;
  installed_at?: string | null;
  last_synced_at?: string | null;
};

export type ShopifySyncResult = {
  status: "success";
  metrics?: {
    total_fetched?: number;
    total_inserted?: number;
    total_updated?: number;
    recalculated_orders?: number;
  };
};

export type ShopifyConnectStart = {
  authorization_url: string;
};

export type MetaAccount = {
  account_identifier: string;
  account_name?: string | null;
  status: "active" | "needs_reauth" | "disconnected" | "error" | string;
  scopes?: string | null;
  installed_at?: string | null;
  last_synced_at?: string | null;
};

export type MetaStatus = {
  connected: boolean;
  platform: "meta_ads";
  accounts: MetaAccount[];
};

export type MetaSyncResult = {
  status: "success";
  accounts_synced: number;
  campaigns_synced: number;
  insights_fetched: number;
  spend_inserted: number;
  spend_updated: number;
  date_range: { since: string; until: string };
};

export class ApiError extends Error {
  constructor(
    message: string,
    readonly status: number,
  ) {
    super(message);
    this.name = "ApiError";
  }
}

function apiBaseUrl(): string {
  const baseUrl = import.meta.env.VITE_API_BASE_URL?.trim().replace(/\/$/, "");
  if (!baseUrl) {
    throw new ApiError("API base URL is not configured.", 0);
  }
  return baseUrl;
}

async function request<T>(path: string, token: string, init: RequestInit = {}): Promise<T> {
  let response: Response;
  try {
    response = await fetch(`${apiBaseUrl()}${path}`, {
      ...init,
      headers: {
        Accept: "application/json",
        Authorization: `Bearer ${token}`,
        ...init.headers,
      },
    });
  } catch {
    throw new ApiError("The service could not be reached. Check the API URL and try again.", 0);
  }

  if (!response.ok) {
    const message = response.status === 401
      ? "Your access session has expired. Sign in again."
      : response.status === 403
        ? "This account is not authorized for this workspace."
        : response.status >= 500
          ? "The service could not complete this request. Try again later."
          : "The request could not be completed. Check the details and try again.";
    throw new ApiError(message, response.status);
  }

  if (response.status === 204) return undefined as T;
  try {
    return await response.json() as T;
  } catch {
    throw new ApiError("The service returned an unreadable response.", response.status);
  }
}

export function getShopifyStatus(token: string): Promise<ShopifyStatus> {
  return request("/integrations/shopify/status", token);
}

export function startShopifyConnect(token: string, shop: string): Promise<ShopifyConnectStart> {
  return request(`/integrations/shopify/connect?shop=${encodeURIComponent(shop)}`, token, { method: "POST" });
}

export function disconnectShopify(token: string): Promise<{ status: string }> {
  return request("/integrations/shopify/disconnect", token, { method: "POST" });
}

export function triggerShopifySync(token: string): Promise<ShopifySyncResult> {
  return request("/integrations/shopify/sync", token, { method: "POST" });
}

export function getMetaStatus(token: string): Promise<MetaStatus> {
  return request("/integrations/meta/status", token);
}

export function startMetaConnect(token: string): Promise<ShopifyConnectStart> {
  return request("/integrations/meta/connect", token, { method: "POST" });
}

export function disconnectMeta(token: string, accountIdentifier: string): Promise<{ status: string; accounts_disconnected: number }> {
  return request(
    `/integrations/meta/disconnect?account_identifier=${encodeURIComponent(accountIdentifier)}`,
    token,
    { method: "POST" },
  );
}

export function triggerMetaSync(
  token: string,
  accountIdentifier: string,
  daysBack = 7,
): Promise<MetaSyncResult> {
  return request(
    `/integrations/meta/sync?account_identifier=${encodeURIComponent(accountIdentifier)}&days_back=${daysBack}`,
    token,
    { method: "POST" },
  );
}