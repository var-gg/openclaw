export type ActivityObserverSource = "embedded-run" | "subagent-announce" | "subagent-registry";

export type ActivityObserverAction =
  | "announce_finished"
  | "announce_started"
  | "cleanup_completed"
  | "cleanup_pending"
  | "run_cleared"
  | "run_registered"
  | "subagent_run_created"
  | "subagent_run_ended"
  | "watchdog_terminalized";

export type ActivityObserverSourceKind = "announce" | "root" | "subagent" | "unknown";

export type ActivityObserverEvent = {
  source: ActivityObserverSource;
  action: ActivityObserverAction;
  runId?: string;
  sessionKeys?: string[];
  sourceKind?: ActivityObserverSourceKind;
};

type ActivityObserverListener = (event: ActivityObserverEvent) => void;

type ActivityObserverState = {
  listeners: Set<ActivityObserverListener>;
};

function getActivityObserverState(): ActivityObserverState {
  const globalStore = globalThis as typeof globalThis & {
    __openclawActivityObserverState?: ActivityObserverState;
  };
  if (!globalStore.__openclawActivityObserverState) {
    globalStore.__openclawActivityObserverState = {
      listeners: new Set<ActivityObserverListener>(),
    };
  }
  return globalStore.__openclawActivityObserverState;
}

export function emitActivityObserverEvent(event: ActivityObserverEvent) {
  const normalizedSessionKeys = [
    ...new Set(
      (event.sessionKeys ?? [])
        .map((entry) => entry.trim().toLowerCase())
        .filter((entry) => entry.length > 0),
    ),
  ];
  const payload: ActivityObserverEvent = {
    ...event,
    ...(normalizedSessionKeys.length > 0 ? { sessionKeys: normalizedSessionKeys } : {}),
  };
  for (const listener of getActivityObserverState().listeners) {
    try {
      listener(payload);
    } catch {
      // Best-effort diagnostic channel only.
    }
  }
}

export function onActivityObserverEvent(listener: ActivityObserverListener) {
  const state = getActivityObserverState();
  state.listeners.add(listener);
  return () => state.listeners.delete(listener);
}

export function resetActivityObserverForTests() {
  getActivityObserverState().listeners.clear();
}
