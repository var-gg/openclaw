import { resolveFetch } from "openclaw/plugin-sdk/fetch-runtime";
import {
  resolveRetryConfig,
  retryAsync,
  type RetryConfig,
} from "openclaw/plugin-sdk/retry-runtime";

const DISCORD_API_BASE = "https://discord.com/api/v10";
const DISCORD_API_RETRY_DEFAULTS = {
  attempts: 3,
  minDelayMs: 500,
  maxDelayMs: 30_000,
  jitter: 0.1,
};

type DiscordApiErrorPayload = {
  message?: string;
  retry_after?: number;
  code?: number;
  global?: boolean;
};

function parseDiscordApiErrorPayload(text: string): DiscordApiErrorPayload | null {
  const trimmed = text.trim();
  if (!trimmed.startsWith("{") || !trimmed.endsWith("}")) {
    return null;
  }
  try {
    const payload = JSON.parse(trimmed);
    if (payload && typeof payload === "object") {
      return payload as DiscordApiErrorPayload;
    }
  } catch {
    return null;
  }
  return null;
}

function parseRetryAfterSeconds(text: string, response: Response): number | undefined {
  const payload = parseDiscordApiErrorPayload(text);
  const retryAfter =
    payload && typeof payload.retry_after === "number" && Number.isFinite(payload.retry_after)
      ? payload.retry_after
      : undefined;
  if (retryAfter !== undefined) {
    return retryAfter;
  }
  const header = response.headers.get("Retry-After");
  if (!header) {
    return undefined;
  }
  const parsed = Number(header);
  return Number.isFinite(parsed) ? parsed : undefined;
}

function formatRetryAfterSeconds(value: number | undefined): string | undefined {
  if (value === undefined || !Number.isFinite(value) || value < 0) {
    return undefined;
  }
  const rounded = value < 10 ? value.toFixed(1) : Math.round(value).toString();
  return `${rounded}s`;
}

function formatDiscordApiErrorText(text: string): string | undefined {
  const trimmed = text.trim();
  if (!trimmed) {
    return undefined;
  }
  const payload = parseDiscordApiErrorPayload(trimmed);
  if (!payload) {
    const looksJson = trimmed.startsWith("{") && trimmed.endsWith("}");
    return looksJson ? "unknown error" : trimmed;
  }
  const message =
    typeof payload.message === "string" && payload.message.trim()
      ? payload.message.trim()
      : "unknown error";
  const retryAfter = formatRetryAfterSeconds(
    typeof payload.retry_after === "number" ? payload.retry_after : undefined,
  );
  return retryAfter ? `${message} (retry after ${retryAfter})` : message;
}

export class DiscordApiError extends Error {
  status: number;
  retryAfter?: number;
  method?: string;
  path?: string;
  code?: "http" | "network" | "timeout";

  constructor(
    message: string,
    status: number,
    retryAfter?: number,
    options?: {
      method?: string;
      path?: string;
      code?: "http" | "network" | "timeout";
    },
  ) {
    super(message);
    this.status = status;
    this.retryAfter = retryAfter;
    this.method = options?.method;
    this.path = options?.path;
    this.code = options?.code;
  }
}

export type DiscordFetchOptions = {
  retry?: RetryConfig;
  label?: string;
};

export type DiscordApiMethod = "DELETE" | "GET" | "PATCH" | "POST" | "PUT";

export type DiscordApiRequestOptions = {
  method?: DiscordApiMethod;
  body?: unknown;
  retry?: RetryConfig;
  label?: string;
  timeoutMs?: number;
  headers?: Record<string, string>;
  shouldRetry?: (err: DiscordApiError, attempt: number) => boolean;
};

export type DiscordApiRequestResult<T> = {
  data: T;
  durationMs: number;
  retryAfter?: number;
  status: number;
};

function isAbortError(err: unknown) {
  return (
    (err instanceof DOMException && err.name === "AbortError") ||
    (err instanceof Error && err.name === "AbortError")
  );
}

function classifyTransportError(params: {
  err: unknown;
  method: DiscordApiMethod;
  path: string;
  timedOut: boolean;
}): DiscordApiError {
  if (params.err instanceof DiscordApiError) {
    return params.err;
  }
  if (params.timedOut || isAbortError(params.err)) {
    return new DiscordApiError(
      `Discord API ${params.method} ${params.path} timed out`,
      0,
      undefined,
      {
        method: params.method,
        path: params.path,
        code: "timeout",
      },
    );
  }
  return new DiscordApiError(
    `Discord API ${params.method} ${params.path} failed: ${String(params.err)}`,
    0,
    undefined,
    {
      method: params.method,
      path: params.path,
      code: "network",
    },
  );
}

export async function requestDiscordApi<T>(
  path: string,
  token: string,
  fetcher: typeof fetch = fetch,
  options?: DiscordApiRequestOptions,
): Promise<DiscordApiRequestResult<T>> {
  const fetchImpl = resolveFetch(fetcher);
  if (!fetchImpl) {
    throw new Error("fetch is not available");
  }

  const method = options?.method ?? "GET";
  const retryConfig = resolveRetryConfig(DISCORD_API_RETRY_DEFAULTS, options?.retry);
  return retryAsync(
    async () => {
      const startedAt = Date.now();
      const controller = typeof AbortController !== "undefined" ? new AbortController() : undefined;
      let timedOut = false;
      const timeoutMs =
        typeof options?.timeoutMs === "number" && Number.isFinite(options.timeoutMs)
          ? Math.max(0, options.timeoutMs)
          : 0;
      const timer =
        controller && timeoutMs > 0
          ? setTimeout(() => {
              timedOut = true;
              controller.abort();
            }, timeoutMs)
          : null;
      try {
        const res = await fetchImpl(`${DISCORD_API_BASE}${path}`, {
          method,
          headers: {
            Authorization: `Bot ${token}`,
            ...(options?.body !== undefined ? { "Content-Type": "application/json" } : {}),
            ...options?.headers,
          },
          ...(options?.body !== undefined ? { body: JSON.stringify(options.body) } : {}),
          ...(controller ? { signal: controller.signal } : {}),
        });
        const durationMs = Date.now() - startedAt;
        const retryAfter = parseRetryAfterSeconds("", res);
        if (!res.ok) {
          const text = await res.text().catch(() => "");
          const detail = formatDiscordApiErrorText(text);
          const suffix = detail ? `: ${detail}` : "";
          throw new DiscordApiError(
            `Discord API ${method} ${path} failed (${res.status})${suffix}`,
            res.status,
            res.status === 429 ? parseRetryAfterSeconds(text, res) : retryAfter,
            {
              method,
              path,
              code: "http",
            },
          );
        }
        if (res.status === 204) {
          return {
            data: undefined as T,
            durationMs,
            ...(retryAfter !== undefined ? { retryAfter } : {}),
            status: res.status,
          };
        }
        return {
          data: (await res.json()) as T,
          durationMs,
          ...(retryAfter !== undefined ? { retryAfter } : {}),
          status: res.status,
        };
      } catch (err) {
        throw classifyTransportError({
          err,
          method,
          path,
          timedOut,
        });
      } finally {
        if (timer) {
          clearTimeout(timer);
        }
      }
    },
    {
      ...retryConfig,
      label: options?.label ?? `${method} ${path}`,
      shouldRetry: (err, attempt) => {
        if (!(err instanceof DiscordApiError)) {
          return false;
        }
        if (options?.shouldRetry) {
          return options.shouldRetry(err, attempt);
        }
        return err.status === 429 || err.status >= 500 || err.code === "network" || err.code === "timeout";
      },
      retryAfterMs: (err) =>
        err instanceof DiscordApiError && typeof err.retryAfter === "number"
          ? err.retryAfter * 1000
          : undefined,
    },
  );
}

export async function fetchDiscord<T>(
  path: string,
  token: string,
  fetcher: typeof fetch = fetch,
  options?: DiscordFetchOptions,
): Promise<T> {
  const retryConfig = resolveRetryConfig(DISCORD_API_RETRY_DEFAULTS, options?.retry);
  try {
    const result = await requestDiscordApi<T>(path, token, fetcher, {
      method: "GET",
      label: options?.label ?? path,
      retry: retryConfig,
      shouldRetry: (err) => err.status === 429,
    });
    return result.data;
  } catch (err) {
    if (err instanceof DiscordApiError) {
      throw new DiscordApiError(
        err.message.replace(`Discord API GET ${path}`, `Discord API ${path}`),
        err.status,
        err.retryAfter,
        {
          code: err.code,
          method: err.method,
          path: err.path,
        },
      );
    }
    throw err;
  }
}
