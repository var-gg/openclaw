import { describe, expect, it } from "vitest";
import { withFetchPreconnect } from "../test-utils/fetch-mock.js";
import { DiscordApiError, fetchDiscord, requestDiscordApi } from "./api.js";
import { jsonResponse } from "./test-http-helpers.js";

describe("fetchDiscord", () => {
  it("formats rate limit payloads without raw JSON", async () => {
    const fetcher = withFetchPreconnect(async () =>
      jsonResponse(
        {
          message: "You are being rate limited.",
          retry_after: 0.631,
          global: false,
        },
        429,
      ),
    );

    let error: unknown;
    try {
      await fetchDiscord("/users/@me/guilds", "test", fetcher, {
        retry: { attempts: 1 },
      });
    } catch (err) {
      error = err;
    }

    const message = String(error);
    expect(message).toContain("Discord API /users/@me/guilds failed (429)");
    expect(message).toContain("You are being rate limited.");
    expect(message).toContain("retry after 0.6s");
    expect(message).not.toContain("{");
    expect(message).not.toContain("retry_after");
  });

  it("preserves non-JSON error text", async () => {
    const fetcher = withFetchPreconnect(async () => new Response("Not Found", { status: 404 }));
    await expect(
      fetchDiscord("/users/@me/guilds", "test", fetcher, {
        retry: { attempts: 1 },
      }),
    ).rejects.toThrow("Discord API /users/@me/guilds failed (404): Not Found");
  });

  it("retries rate limits before succeeding", async () => {
    let calls = 0;
    const fetcher = withFetchPreconnect(async () => {
      calls += 1;
      if (calls === 1) {
        return jsonResponse(
          {
            message: "You are being rate limited.",
            retry_after: 0,
            global: false,
          },
          429,
        );
      }
      return jsonResponse([{ id: "1", name: "Guild" }], 200);
    });

    const result = await fetchDiscord<Array<{ id: string; name: string }>>(
      "/users/@me/guilds",
      "test",
      fetcher,
      { retry: { attempts: 2, minDelayMs: 0, maxDelayMs: 0 } },
    );

    expect(result).toHaveLength(1);
    expect(calls).toBe(2);
  });
});

describe("requestDiscordApi", () => {
  it("sends PATCH bodies and honors retry_after on 429", async () => {
    let calls = 0;
    let lastMethod = "";
    let lastBody = "";
    const fetcher = withFetchPreconnect(async (_input, init) => {
      calls += 1;
      lastMethod = init?.method ?? "";
      lastBody = String(init?.body ?? "");
      if (calls === 1) {
        return jsonResponse(
          {
            message: "You are being rate limited.",
            retry_after: 0,
            global: false,
          },
          429,
        );
      }
      return jsonResponse({ id: "123", name: "⚙️-main" }, 200);
    });

    const result = await requestDiscordApi<{ id: string; name: string }>(
      "/channels/123",
      "test",
      fetcher,
      {
        body: { name: "⚙️-main" },
        method: "PATCH",
        retry: { attempts: 2, minDelayMs: 0, maxDelayMs: 0 },
      },
    );

    expect(calls).toBe(2);
    expect(lastMethod).toBe("PATCH");
    expect(JSON.parse(lastBody)).toEqual({ name: "⚙️-main" });
    expect(result.data.name).toBe("⚙️-main");
    expect(result.status).toBe(200);
  });

  it("retries transient network failures", async () => {
    let calls = 0;
    const fetcher = withFetchPreconnect(async () => {
      calls += 1;
      if (calls === 1) {
        throw new TypeError("socket hang up");
      }
      return jsonResponse({ id: "123", name: "🟢-main" }, 200);
    });

    const result = await requestDiscordApi<{ id: string; name: string }>(
      "/channels/123",
      "test",
      fetcher,
      {
        retry: { attempts: 2, minDelayMs: 0, maxDelayMs: 0 },
      },
    );

    expect(calls).toBe(2);
    expect(result.data.name).toBe("🟢-main");
  });

  it("classifies abort timeouts", async () => {
    const fetcher = withFetchPreconnect(
      (_input, init) =>
        new Promise<Response>((_resolve, reject) => {
          init?.signal?.addEventListener("abort", () => {
            reject(new DOMException("Aborted", "AbortError"));
          });
        }),
    );

    let error: unknown;
    try {
      await requestDiscordApi("/channels/123", "test", fetcher, {
        retry: { attempts: 1, minDelayMs: 0, maxDelayMs: 0 },
        timeoutMs: 5,
      });
    } catch (err) {
      error = err;
    }

    expect(error).toBeInstanceOf(DiscordApiError);
    expect((error as DiscordApiError).code).toBe("timeout");
  });

  it("does not retry definitive 403 failures", async () => {
    let calls = 0;
    const fetcher = withFetchPreconnect(async () => {
      calls += 1;
      return new Response("Forbidden", { status: 403 });
    });

    await expect(
      requestDiscordApi("/channels/123", "test", fetcher, {
        retry: { attempts: 3, minDelayMs: 0, maxDelayMs: 0 },
      }),
    ).rejects.toThrow("Discord API GET /channels/123 failed (403): Forbidden");
    expect(calls).toBe(1);
  });
});
