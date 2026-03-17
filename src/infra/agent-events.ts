import type { VerboseLevel } from "../auto-reply/thinking.js";

export type AgentEventStream = "lifecycle" | "tool" | "assistant" | "error" | (string & {});

export type AgentEventPayload = {
  runId: string;
  seq: number;
  stream: AgentEventStream;
  ts: number;
  data: Record<string, unknown>;
  sessionKey?: string;
};

export type AgentRunContext = {
  sessionKey?: string;
  verboseLevel?: VerboseLevel;
  isHeartbeat?: boolean;
  /** Whether control UI clients should receive chat/agent updates for this run. */
  isControlUiVisible?: boolean;
  /**
   * Whether internal event-bus listeners should receive the run sessionKey.
   * Keep this separate from Control UI visibility so internal monitors can
   * correlate runs without exposing session identifiers to external clients.
   */
  isSessionKeyVisibleToInternalListeners?: boolean;
};

type AgentEventsGlobalState = {
  seqByRun: Map<string, number>;
  listeners: Set<(evt: AgentEventPayload) => void>;
  runContextById: Map<string, AgentRunContext>;
};

function getAgentEventsState(): AgentEventsGlobalState {
  const globalStore = globalThis as typeof globalThis & {
    __openclawAgentEventsState?: AgentEventsGlobalState;
  };
  if (!globalStore.__openclawAgentEventsState) {
    globalStore.__openclawAgentEventsState = {
      seqByRun: new Map<string, number>(),
      listeners: new Set<(evt: AgentEventPayload) => void>(),
      runContextById: new Map<string, AgentRunContext>(),
    };
  }
  return globalStore.__openclawAgentEventsState;
}

export function registerAgentRunContext(runId: string, context: AgentRunContext) {
  if (!runId) {
    return;
  }
  const state = getAgentEventsState();
  const existing = state.runContextById.get(runId);
  if (!existing) {
    state.runContextById.set(runId, { ...context });
    return;
  }
  if (context.sessionKey && existing.sessionKey !== context.sessionKey) {
    existing.sessionKey = context.sessionKey;
  }
  if (context.verboseLevel && existing.verboseLevel !== context.verboseLevel) {
    existing.verboseLevel = context.verboseLevel;
  }
  if (context.isControlUiVisible !== undefined) {
    existing.isControlUiVisible = context.isControlUiVisible;
  }
  if (
    context.isSessionKeyVisibleToInternalListeners !== undefined &&
    existing.isSessionKeyVisibleToInternalListeners !== context.isSessionKeyVisibleToInternalListeners
  ) {
    existing.isSessionKeyVisibleToInternalListeners = context.isSessionKeyVisibleToInternalListeners;
  }
  if (context.isHeartbeat !== undefined && existing.isHeartbeat !== context.isHeartbeat) {
    existing.isHeartbeat = context.isHeartbeat;
  }
}

export function getAgentRunContext(runId: string) {
  return getAgentEventsState().runContextById.get(runId);
}

export function clearAgentRunContext(runId: string) {
  getAgentEventsState().runContextById.delete(runId);
}

export function resetAgentRunContextForTest() {
  const state = getAgentEventsState();
  state.runContextById.clear();
  state.seqByRun.clear();
  state.listeners.clear();
}

export function emitAgentEvent(event: Omit<AgentEventPayload, "seq" | "ts">) {
  const state = getAgentEventsState();
  const nextSeq = (state.seqByRun.get(event.runId) ?? 0) + 1;
  state.seqByRun.set(event.runId, nextSeq);
  const context = state.runContextById.get(event.runId);
  const isSessionKeyVisibleToInternalListeners =
    context?.isSessionKeyVisibleToInternalListeners ?? true;
  const eventSessionKey =
    typeof event.sessionKey === "string" && event.sessionKey.trim() ? event.sessionKey : undefined;
  const sessionKey = isSessionKeyVisibleToInternalListeners
    ? (eventSessionKey ?? context?.sessionKey)
    : undefined;
  const enriched: AgentEventPayload = {
    ...event,
    sessionKey,
    seq: nextSeq,
    ts: Date.now(),
  };
  for (const listener of state.listeners) {
    try {
      listener(enriched);
    } catch {
      /* ignore */
    }
  }
}

export function onAgentEvent(listener: (evt: AgentEventPayload) => void) {
  const state = getAgentEventsState();
  state.listeners.add(listener);
  return () => state.listeners.delete(listener);
}
