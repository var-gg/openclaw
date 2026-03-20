import {
  diagnosticLogger as diag,
  logMessageQueued,
  logSessionStateChange,
} from "../../logging/diagnostic.js";
import {
  emitActivityObserverEvent,
  type ActivityObserverSourceKind,
} from "../activity-observer.js";

type EmbeddedPiQueueHandle = {
  queueMessage: (text: string) => Promise<void>;
  isStreaming: () => boolean;
  isCompacting: () => boolean;
  abort: (reason?: unknown) => void;
};

type ActiveEmbeddedRunEntry = {
  handle: EmbeddedPiQueueHandle;
  runId?: string;
  sessionKey?: string;
  sourceKind: ActivityObserverSourceKind;
  startedAt: number;
};

export type ActiveEmbeddedRunSnapshotEntry = {
  sessionId: string;
  runId?: string;
  sessionKey?: string;
  sourceKind: ActivityObserverSourceKind;
  startedAt: number;
};

const ACTIVE_EMBEDDED_RUNS = new Map<string, ActiveEmbeddedRunEntry>();
type EmbeddedRunWaiter = {
  resolve: (ended: boolean) => void;
  timer: NodeJS.Timeout;
};
const EMBEDDED_RUN_WAITERS = new Map<string, Set<EmbeddedRunWaiter>>();

export function queueEmbeddedPiMessage(sessionId: string, text: string): boolean {
  const entry = ACTIVE_EMBEDDED_RUNS.get(sessionId);
  if (!entry) {
    diag.debug(`queue message failed: sessionId=${sessionId} reason=no_active_run`);
    return false;
  }
  const { handle } = entry;
  if (!handle.isStreaming()) {
    diag.debug(`queue message failed: sessionId=${sessionId} reason=not_streaming`);
    return false;
  }
  if (handle.isCompacting()) {
    diag.debug(`queue message failed: sessionId=${sessionId} reason=compacting`);
    return false;
  }
  logMessageQueued({ sessionId, source: "pi-embedded-runner" });
  void handle.queueMessage(text);
  return true;
}

export function abortEmbeddedPiRun(sessionId: string, reason?: unknown): boolean {
  const entry = ACTIVE_EMBEDDED_RUNS.get(sessionId);
  if (!entry) {
    diag.debug(`abort failed: sessionId=${sessionId} reason=no_active_run`);
    return false;
  }
  const { handle } = entry;
  diag.debug(`aborting run: sessionId=${sessionId}`);
  handle.abort(reason);
  return true;
}

export function isEmbeddedPiRunActive(sessionId: string): boolean {
  const active = ACTIVE_EMBEDDED_RUNS.has(sessionId);
  if (active) {
    diag.debug(`run active check: sessionId=${sessionId} active=true`);
  }
  return active;
}

export function isEmbeddedPiRunStreaming(sessionId: string): boolean {
  const entry = ACTIVE_EMBEDDED_RUNS.get(sessionId);
  if (!entry) {
    return false;
  }
  const { handle } = entry;
  return handle.isStreaming();
}

export function getActiveEmbeddedRunCount(): number {
  return ACTIVE_EMBEDDED_RUNS.size;
}

export function getActiveEmbeddedRunsSnapshot(): ActiveEmbeddedRunSnapshotEntry[] {
  return [...ACTIVE_EMBEDDED_RUNS.entries()].map(([sessionId, entry]) => ({
    sessionId,
    ...(entry.runId ? { runId: entry.runId } : {}),
    ...(entry.sessionKey ? { sessionKey: entry.sessionKey } : {}),
    sourceKind: entry.sourceKind,
    startedAt: entry.startedAt,
  }));
}

function resolveEmbeddedRunSourceKind(params: {
  runId?: string;
  sessionKey?: string;
}): ActivityObserverSourceKind {
  const runId = params.runId?.trim().toLowerCase() ?? "";
  const sessionKey = params.sessionKey?.trim().toLowerCase() ?? "";
  if (runId.startsWith("announce:v1:")) {
    return "announce";
  }
  if (/^agent:[^:]+:discord:channel:[^:]+(?::thread:[^:]+)?$/.test(sessionKey)) {
    return "root";
  }
  if (sessionKey.includes(":subagent:") || sessionKey.startsWith("agent:cron:")) {
    return "subagent";
  }
  return "unknown";
}

export function waitForEmbeddedPiRunEnd(sessionId: string, timeoutMs = 15_000): Promise<boolean> {
  if (!sessionId || !ACTIVE_EMBEDDED_RUNS.has(sessionId)) {
    return Promise.resolve(true);
  }
  diag.debug(`waiting for run end: sessionId=${sessionId} timeoutMs=${timeoutMs}`);
  return new Promise((resolve) => {
    const waiters = EMBEDDED_RUN_WAITERS.get(sessionId) ?? new Set();
    const waiter: EmbeddedRunWaiter = {
      resolve,
      timer: setTimeout(
        () => {
          waiters.delete(waiter);
          if (waiters.size === 0) {
            EMBEDDED_RUN_WAITERS.delete(sessionId);
          }
          diag.warn(`wait timeout: sessionId=${sessionId} timeoutMs=${timeoutMs}`);
          resolve(false);
        },
        Math.max(100, timeoutMs),
      ),
    };
    waiters.add(waiter);
    EMBEDDED_RUN_WAITERS.set(sessionId, waiters);
    if (!ACTIVE_EMBEDDED_RUNS.has(sessionId)) {
      waiters.delete(waiter);
      if (waiters.size === 0) {
        EMBEDDED_RUN_WAITERS.delete(sessionId);
      }
      clearTimeout(waiter.timer);
      resolve(true);
    }
  });
}

function notifyEmbeddedRunEnded(sessionId: string) {
  const waiters = EMBEDDED_RUN_WAITERS.get(sessionId);
  if (!waiters || waiters.size === 0) {
    return;
  }
  EMBEDDED_RUN_WAITERS.delete(sessionId);
  diag.debug(`notifying waiters: sessionId=${sessionId} waiterCount=${waiters.size}`);
  for (const waiter of waiters) {
    clearTimeout(waiter.timer);
    waiter.resolve(true);
  }
}

export function setActiveEmbeddedRun(
  sessionId: string,
  handle: EmbeddedPiQueueHandle,
  sessionKey?: string,
  meta?: {
    runId?: string;
    sourceKind?: ActivityObserverSourceKind;
    startedAt?: number;
  },
) {
  const wasActive = ACTIVE_EMBEDDED_RUNS.has(sessionId);
  const sourceKind =
    meta?.sourceKind ?? resolveEmbeddedRunSourceKind({ runId: meta?.runId, sessionKey });
  ACTIVE_EMBEDDED_RUNS.set(sessionId, {
    handle,
    ...(meta?.runId ? { runId: meta.runId } : {}),
    ...(sessionKey ? { sessionKey } : {}),
    sourceKind,
    startedAt: meta?.startedAt ?? Date.now(),
  });
  logSessionStateChange({
    sessionId,
    sessionKey,
    state: "processing",
    reason: wasActive ? "run_replaced" : "run_started",
  });
  if (!sessionId.startsWith("probe-")) {
    diag.debug(`run registered: sessionId=${sessionId} totalActive=${ACTIVE_EMBEDDED_RUNS.size}`);
  }
  emitActivityObserverEvent({
    source: "embedded-run",
    action: "run_registered",
    ...(meta?.runId ? { runId: meta.runId } : {}),
    ...(sessionKey ? { sessionKeys: [sessionKey] } : {}),
    sourceKind,
  });
}

export function clearActiveEmbeddedRun(
  sessionId: string,
  handle: EmbeddedPiQueueHandle,
  sessionKey?: string,
) {
  const entry = ACTIVE_EMBEDDED_RUNS.get(sessionId);
  if (entry?.handle === handle) {
    ACTIVE_EMBEDDED_RUNS.delete(sessionId);
    logSessionStateChange({ sessionId, sessionKey, state: "idle", reason: "run_completed" });
    if (!sessionId.startsWith("probe-")) {
      diag.debug(`run cleared: sessionId=${sessionId} totalActive=${ACTIVE_EMBEDDED_RUNS.size}`);
    }
    emitActivityObserverEvent({
      source: "embedded-run",
      action: "run_cleared",
      ...(entry.runId ? { runId: entry.runId } : {}),
      ...(entry.sessionKey ? { sessionKeys: [entry.sessionKey] } : {}),
      sourceKind: entry.sourceKind,
    });
    notifyEmbeddedRunEnded(sessionId);
  } else {
    diag.debug(`run clear skipped: sessionId=${sessionId} reason=handle_mismatch`);
  }
}

export function resetEmbeddedPiRunsForTests() {
  ACTIVE_EMBEDDED_RUNS.clear();
  for (const waiters of EMBEDDED_RUN_WAITERS.values()) {
    for (const waiter of waiters) {
      clearTimeout(waiter.timer);
    }
  }
  EMBEDDED_RUN_WAITERS.clear();
}

export type { EmbeddedPiQueueHandle };
