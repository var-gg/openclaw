import { describe, expect, test } from "vitest";
import {
  clearAgentRunContext,
  emitAgentEvent,
  getAgentRunContext,
  onAgentEvent,
  registerAgentRunContext,
  resetAgentRunContextForTest,
} from "./agent-events.js";

describe("agent-events sequencing", () => {
  test("stores and clears run context", async () => {
    resetAgentRunContextForTest();
    registerAgentRunContext("run-1", { sessionKey: "main" });
    expect(getAgentRunContext("run-1")?.sessionKey).toBe("main");
    clearAgentRunContext("run-1");
    expect(getAgentRunContext("run-1")).toBeUndefined();
  });

  test("maintains monotonic seq per runId", async () => {
    const seen: Record<string, number[]> = {};
    const stop = onAgentEvent((evt) => {
      const list = seen[evt.runId] ?? [];
      seen[evt.runId] = list;
      list.push(evt.seq);
    });

    emitAgentEvent({ runId: "run-1", stream: "lifecycle", data: {} });
    emitAgentEvent({ runId: "run-1", stream: "lifecycle", data: {} });
    emitAgentEvent({ runId: "run-2", stream: "lifecycle", data: {} });
    emitAgentEvent({ runId: "run-1", stream: "lifecycle", data: {} });

    stop();

    expect(seen["run-1"]).toEqual([1, 2, 3]);
    expect(seen["run-2"]).toEqual([1]);
  });

  test("preserves compaction ordering on the event bus", async () => {
    const phases: Array<string> = [];
    const stop = onAgentEvent((evt) => {
      if (evt.runId !== "run-1") {
        return;
      }
      if (evt.stream !== "compaction") {
        return;
      }
      if (typeof evt.data?.phase === "string") {
        phases.push(evt.data.phase);
      }
    });

    emitAgentEvent({ runId: "run-1", stream: "compaction", data: { phase: "start" } });
    emitAgentEvent({
      runId: "run-1",
      stream: "compaction",
      data: { phase: "end", willRetry: false },
    });

    stop();

    expect(phases).toEqual(["start", "end"]);
  });

  test("preserves sessionKey on the internal event bus even when Control UI is hidden", async () => {
    resetAgentRunContextForTest();
    registerAgentRunContext("run-hidden", {
      sessionKey: "session-imessage",
      isControlUiVisible: false,
    });

    let receivedSessionKey: string | undefined;
    const stop = onAgentEvent((evt) => {
      receivedSessionKey = evt.sessionKey;
    });
    emitAgentEvent({
      runId: "run-hidden",
      stream: "assistant",
      data: { text: "hi" },
    });
    stop();

    expect(receivedSessionKey).toBe("session-imessage");
  });

  test("can explicitly hide sessionKey from internal listeners", async () => {
    resetAgentRunContextForTest();
    registerAgentRunContext("run-internal-hidden", {
      sessionKey: "session-secret",
      isSessionKeyVisibleToInternalListeners: false,
    });

    let receivedSessionKey: string | undefined;
    const stop = onAgentEvent((evt) => {
      receivedSessionKey = evt.sessionKey;
    });
    emitAgentEvent({
      runId: "run-internal-hidden",
      stream: "assistant",
      data: { text: "hi" },
    });
    stop();

    expect(receivedSessionKey).toBeUndefined();
  });

  test("stores bus state on globalThis so split bundles share one listener set", async () => {
    resetAgentRunContextForTest();

    const globalState = globalThis as typeof globalThis & {
      __openclawAgentEventsState?: {
        seqByRun: Map<string, number>;
        listeners: Set<(evt: unknown) => void>;
        runContextById: Map<string, unknown>;
      };
    };

    expect(globalState.__openclawAgentEventsState).toBeDefined();

    const stop = onAgentEvent(() => {});
    expect(globalState.__openclawAgentEventsState?.listeners.size).toBe(1);
    stop();
    expect(globalState.__openclawAgentEventsState?.listeners.size).toBe(0);

    registerAgentRunContext("run-global", { sessionKey: "session-global" });
    expect(globalState.__openclawAgentEventsState?.runContextById.get("run-global")).toEqual({
      sessionKey: "session-global",
    });
  });
});
