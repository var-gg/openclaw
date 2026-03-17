import fs from "node:fs/promises";
import path from "node:path";
import type { OpenClawConfig } from "../../config/config.js";
import type { SessionEntry } from "../../config/sessions/types.js";
import type { AgentEventPayload } from "../../infra/agent-events.js";
import { onAgentEvent } from "../../infra/agent-events.js";
import { createSubsystemLogger } from "../../logging/subsystem.js";
import { resolveThreadParentSessionKey } from "../../sessions/session-key-utils.js";
import { loadCombinedSessionStoreForGateway } from "../../gateway/session-utils.js";
import { editChannelDiscord } from "../send.js";
import { fetchChannelInfoDiscord } from "../send.guild.js";

const log = createSubsystemLogger("discord-agent-activity");

export type AgentActivityState = "idle" | "running" | "error";

type ChannelMapEntry = {
  channelId: string;
  baseName: string;
  accountId?: string;
};

type ChannelMap = {
  version?: number;
  staleRunningMs?: number;
  channels?: Record<string, ChannelMapEntry>;
};

type PersistedRunRecord = {
  sessionKey?: string;
  status: "running" | "ok" | "error";
  startedAt: number;
  updatedAt: number;
  lastError?: string;
};

type PersistedChannelRecord = {
  state: AgentActivityState;
  updatedAt: number;
  lastEventAt?: number;
  lastStartAt?: number;
  lastTargetName?: string;
  lastAppliedAt?: number;
  lastError?: string;
  runs?: Record<string, PersistedRunRecord>;
};

type PersistedAgentActivityState = {
  version: 2;
  staleRunningMs: number;
  channels: Record<string, PersistedChannelRecord>;
};

type DiscordChannelRenameFn = (params: {
  channelId: string;
  name: string;
  accountId?: string;
}) => Promise<void>;

type DiscordChannelFetchFn = (params: {
  channelId: string;
  accountId?: string;
}) => Promise<{ name?: string | null }>;

type AgentChannelActivityMonitorOptions = {
  workspaceDir: string;
  cfg?: OpenClawConfig;
  renameChannel?: DiscordChannelRenameFn;
  fetchChannel?: DiscordChannelFetchFn;
  now?: () => number;
  staleSweepIntervalMs?: number;
};

const DEFAULT_STALE_RUNNING_MS = 6 * 60 * 60 * 1000;
const DEFAULT_SWEEP_INTERVAL_MS = 10 * 60 * 1000;
const CONFIG_RELATIVE_PATH = path.join("ops", "config", "agent-channel-map.json");
const STATE_RELATIVE_PATH = path.join("ops", "state", "agent-activity.json");

const STATE_TO_EMOJI: Record<AgentActivityState, string> = {
  idle: "🟢",
  running: "⚙️",
  error: "🔴",
};

function isObject(value: unknown): value is Record<string, unknown> {
  return !!value && typeof value === "object" && !Array.isArray(value);
}

function normalizeChannelMapEntry(value: unknown): ChannelMapEntry | null {
  if (!isObject(value)) {
    return null;
  }
  const channelId = typeof value.channelId === "string" ? value.channelId.trim() : "";
  const baseName = typeof value.baseName === "string" ? value.baseName.trim() : "";
  const accountId = typeof value.accountId === "string" && value.accountId.trim() ? value.accountId.trim() : undefined;
  if (!channelId || !baseName) {
    return null;
  }
  return { channelId, baseName, ...(accountId ? { accountId } : {}) };
}

function normalizeMap(raw: unknown): ChannelMap {
  if (!isObject(raw)) {
    return {};
  }

  const channels: Record<string, ChannelMapEntry> = {};
  const channelsRaw = isObject(raw.channels) ? raw.channels : {};
  for (const [rootKey, value] of Object.entries(channelsRaw)) {
    const normalized = normalizeChannelMapEntry(value);
    const normalizedRootKey = normalizeSessionKey(rootKey);
    if (!normalized || !normalizedRootKey) {
      continue;
    }
    channels[normalizedRootKey] = normalized;
  }

  // Backward compatibility with v0 agent-keyed config.
  const agentsRaw = isObject(raw.agents) ? raw.agents : {};
  for (const [agentId, value] of Object.entries(agentsRaw)) {
    const normalized = normalizeChannelMapEntry(value);
    if (!normalized) {
      continue;
    }
    channels[`agent:${agentId.trim().toLowerCase()}:discord:channel:${normalized.channelId}`] = normalized;
  }

  const staleRunningMs =
    typeof raw.staleRunningMs === "number" && Number.isFinite(raw.staleRunningMs) && raw.staleRunningMs > 0
      ? Math.floor(raw.staleRunningMs)
      : undefined;
  return {
    version: typeof raw.version === "number" ? raw.version : undefined,
    ...(staleRunningMs ? { staleRunningMs } : {}),
    channels,
  };
}

function normalizeRunRecord(value: unknown): PersistedRunRecord | null {
  if (!isObject(value)) {
    return null;
  }
  const status = value.status;
  if (status !== "running" && status !== "ok" && status !== "error") {
    return null;
  }
  const startedAt = typeof value.startedAt === "number" ? value.startedAt : undefined;
  const updatedAt = typeof value.updatedAt === "number" ? value.updatedAt : undefined;
  if (startedAt === undefined || updatedAt === undefined) {
    return null;
  }
  return {
    status,
    startedAt,
    updatedAt,
    ...(typeof value.sessionKey === "string" && value.sessionKey.trim() ? { sessionKey: value.sessionKey.trim().toLowerCase() } : {}),
    ...(typeof value.lastError === "string" && value.lastError.trim() ? { lastError: value.lastError.trim() } : {}),
  };
}

function normalizeChannelRecord(value: unknown): PersistedChannelRecord | null {
  if (!isObject(value)) {
    return null;
  }
  const state = value.state;
  if (state !== "idle" && state !== "running" && state !== "error") {
    return null;
  }
  const updatedAt = typeof value.updatedAt === "number" ? value.updatedAt : Date.now();
  const runsRaw = isObject(value.runs) ? value.runs : {};
  const runs: Record<string, PersistedRunRecord> = {};
  for (const [runId, runValue] of Object.entries(runsRaw)) {
    const normalizedRun = normalizeRunRecord(runValue);
    if (normalizedRun) {
      runs[runId] = normalizedRun;
    }
  }
  return {
    state,
    updatedAt,
    ...(typeof value.lastEventAt === "number" ? { lastEventAt: value.lastEventAt } : {}),
    ...(typeof value.lastStartAt === "number" ? { lastStartAt: value.lastStartAt } : {}),
    ...(typeof value.lastTargetName === "string" && value.lastTargetName ? { lastTargetName: value.lastTargetName } : {}),
    ...(typeof value.lastAppliedAt === "number" ? { lastAppliedAt: value.lastAppliedAt } : {}),
    ...(typeof value.lastError === "string" && value.lastError ? { lastError: value.lastError } : {}),
    ...(Object.keys(runs).length > 0 ? { runs } : {}),
  };
}

function normalizeState(raw: unknown): PersistedAgentActivityState {
  if (!isObject(raw)) {
    return { version: 2, staleRunningMs: DEFAULT_STALE_RUNNING_MS, channels: {} };
  }

  const channels: Record<string, PersistedChannelRecord> = {};
  const channelsRaw = isObject(raw.channels) ? raw.channels : {};
  for (const [rootKey, value] of Object.entries(channelsRaw)) {
    const normalizedKey = normalizeSessionKey(rootKey);
    const normalizedValue = normalizeChannelRecord(value);
    if (normalizedKey && normalizedValue) {
      channels[normalizedKey] = normalizedValue;
    }
  }

  // Backward compatibility with v0 persisted state.
  if (Object.keys(channels).length === 0 && isObject(raw.agents)) {
    for (const [agentId, value] of Object.entries(raw.agents)) {
      const normalizedValue = normalizeChannelRecord(value);
      if (!normalizedValue) {
        continue;
      }
      channels[`agent:${agentId.trim().toLowerCase()}:discord:channel:legacy`] = normalizedValue;
    }
  }

  return {
    version: 2,
    staleRunningMs:
      typeof raw.staleRunningMs === "number" && Number.isFinite(raw.staleRunningMs) && raw.staleRunningMs > 0
        ? Math.floor(raw.staleRunningMs)
        : DEFAULT_STALE_RUNNING_MS,
    channels,
  };
}

async function readJsonFile(filePath: string): Promise<unknown | null> {
  try {
    const text = await fs.readFile(filePath, "utf8");
    return JSON.parse(text) as unknown;
  } catch (err) {
    if ((err as NodeJS.ErrnoException)?.code === "ENOENT") {
      return null;
    }
    throw err;
  }
}

async function writeJsonFile(filePath: string, value: unknown) {
  await fs.mkdir(path.dirname(filePath), { recursive: true });
  await fs.writeFile(filePath, `${JSON.stringify(value, null, 2)}\n`, "utf8");
}

function synthesizeChannelName(baseName: string, state: AgentActivityState) {
  return `${STATE_TO_EMOJI[state]}-${baseName}`;
}

function normalizeSessionKey(value: string | undefined | null): string | null {
  const trimmed = (value ?? "").trim().toLowerCase();
  return trimmed || null;
}

function isDiscordChannelRootSessionKey(sessionKey: string | undefined | null): boolean {
  const normalized = normalizeSessionKey(sessionKey);
  if (!normalized) {
    return false;
  }
  return /^agent:[^:]+:discord:channel:[^:]+$/.test(normalized);
}

function extractDiscordChannelRootSessionKey(sessionKey: string | undefined | null): string | null {
  const normalized = normalizeSessionKey(sessionKey);
  if (!normalized) {
    return null;
  }
  const parent = resolveThreadParentSessionKey(normalized);
  if (isDiscordChannelRootSessionKey(parent)) {
    return parent;
  }
  return isDiscordChannelRootSessionKey(normalized) ? normalized : null;
}

function extractDiscordChannelIdFromRootSessionKey(sessionKey: string | undefined | null): string | null {
  const normalized = extractDiscordChannelRootSessionKey(sessionKey);
  if (!normalized) {
    return null;
  }
  return normalized.split(":").at(-1) ?? null;
}

function synthesizeRecordState(record: PersistedChannelRecord): AgentActivityState {
  const runs = Object.values(record.runs ?? {});
  if (runs.some((run) => run.status === "error")) {
    return "error";
  }
  if (runs.some((run) => run.status === "running")) {
    return "running";
  }
  return "idle";
}

function collectLatestError(runs: Record<string, PersistedRunRecord> | undefined): string | undefined {
  const ordered = Object.values(runs ?? {}).sort((a, b) => b.updatedAt - a.updatedAt);
  return ordered.find((run) => run.status === "error")?.lastError;
}

function findFallbackRunningRunId(
  runs: Record<string, PersistedRunRecord>,
  sessionKey: string | undefined,
): string | null {
  const runningEntries = Object.entries(runs).filter(([, run]) => run.status === "running");
  if (runningEntries.length === 0) {
    return null;
  }
  const normalizedSessionKey = normalizeSessionKey(sessionKey);
  if (normalizedSessionKey) {
    const exactMatch = runningEntries.find(
      ([, run]) => normalizeSessionKey(run.sessionKey) === normalizedSessionKey,
    );
    if (exactMatch) {
      return exactMatch[0];
    }
  }
  if (runningEntries.length === 1) {
    return runningEntries[0]?.[0] ?? null;
  }
  const latestRunning = [...runningEntries].sort((a, b) => {
    const aTime = a[1].updatedAt ?? a[1].startedAt;
    const bTime = b[1].updatedAt ?? b[1].startedAt;
    return bTime - aTime;
  })[0];
  return latestRunning?.[0] ?? null;
}

export class AgentChannelActivityMonitor {
  private readonly workspaceDir: string;
  private readonly cfg?: OpenClawConfig;
  private readonly configPath: string;
  private readonly statePath: string;
  private readonly renameChannel: DiscordChannelRenameFn;
  private readonly fetchChannel: DiscordChannelFetchFn;
  private readonly now: () => number;
  private readonly staleSweepIntervalMs: number;
  private unsub: (() => void) | null = null;
  private sweepTimer: ReturnType<typeof setInterval> | null = null;
  private state: PersistedAgentActivityState = {
    version: 2,
    staleRunningMs: DEFAULT_STALE_RUNNING_MS,
    channels: {},
  };
  private queue: Promise<void> = Promise.resolve();

  constructor(options: AgentChannelActivityMonitorOptions) {
    this.workspaceDir = options.workspaceDir;
    this.cfg = options.cfg;
    this.configPath = path.join(options.workspaceDir, CONFIG_RELATIVE_PATH);
    this.statePath = path.join(options.workspaceDir, STATE_RELATIVE_PATH);
    this.renameChannel =
      options.renameChannel ??
      (async ({ channelId, name, accountId }) => {
        await editChannelDiscord({ channelId, name }, accountId ? { accountId } : undefined);
      });
    this.fetchChannel =
      options.fetchChannel ??
      (async ({ channelId, accountId }) => {
        const channel = await fetchChannelInfoDiscord(channelId, accountId ? { accountId } : undefined);
        return { name: "name" in channel ? ((channel as { name?: string | null }).name ?? undefined) : undefined };
      });
    this.now = options.now ?? (() => Date.now());
    this.staleSweepIntervalMs = options.staleSweepIntervalMs ?? DEFAULT_SWEEP_INTERVAL_MS;
  }

  async start() {
    this.state = normalizeState(await readJsonFile(this.statePath));
    await this.persistState();
    this.unsub = onAgentEvent((evt) => {
      void this.enqueue(() => this.handleEvent(evt));
    });
    this.sweepTimer = setInterval(() => {
      void this.enqueue(() => this.recoverStaleRunningChannels());
    }, this.staleSweepIntervalMs);
    if (typeof this.sweepTimer.unref === "function") {
      this.sweepTimer.unref();
    }
    await this.enqueue(() => this.recoverStaleRunningChannels());
  }

  async stop() {
    this.unsub?.();
    this.unsub = null;
    if (this.sweepTimer) {
      clearInterval(this.sweepTimer);
      this.sweepTimer = null;
    }
    await this.queue.catch(() => {});
  }

  private enqueue(task: () => Promise<void>) {
    this.queue = this.queue.then(task, task);
    return this.queue;
  }

  private async readMap() {
    return normalizeMap(await readJsonFile(this.configPath));
  }

  private async persistState() {
    await writeJsonFile(this.statePath, this.state);
  }

  private loadSessionStore(): Record<string, SessionEntry> {
    if (!this.cfg) {
      return {};
    }
    try {
      return loadCombinedSessionStoreForGateway(this.cfg).store;
    } catch (err) {
      log.warn(`failed to load session store for discord activity monitor: ${String(err)}`);
      return {};
    }
  }

  private resolveRootSessionKey(params: {
    sessionKey?: string;
    mappedRoots: Set<string>;
    store: Record<string, SessionEntry>;
  }): string | null {
    const eventRoot = extractDiscordChannelRootSessionKey(params.sessionKey);
    if (eventRoot && params.mappedRoots.has(eventRoot)) {
      return eventRoot;
    }
    const eventChannelId = extractDiscordChannelIdFromRootSessionKey(eventRoot ?? params.sessionKey);
    if (eventChannelId) {
      const matchingRoots = [...params.mappedRoots].filter(
        (root) => extractDiscordChannelIdFromRootSessionKey(root) === eventChannelId,
      );
      if (matchingRoots.length === 1) {
        return matchingRoots[0] ?? null;
      }
    }

    const normalizedSessionKey = normalizeSessionKey(params.sessionKey);
    if (!normalizedSessionKey) {
      return null;
    }

    const visited = new Set<string>();
    let current: string | null = normalizedSessionKey;
    while (current && !visited.has(current)) {
      visited.add(current);
      const channelRoot = extractDiscordChannelRootSessionKey(current);
      if (channelRoot && params.mappedRoots.has(channelRoot)) {
        return channelRoot;
      }
      const traversedChannelId = extractDiscordChannelIdFromRootSessionKey(channelRoot ?? current);
      if (traversedChannelId) {
        const matchingRoots = [...params.mappedRoots].filter(
          (root) => extractDiscordChannelIdFromRootSessionKey(root) === traversedChannelId,
        );
        if (matchingRoots.length === 1) {
          return matchingRoots[0] ?? null;
        }
      }
      const entry = params.store[current];
      const spawnedBy = normalizeSessionKey(entry?.spawnedBy);
      current = spawnedBy ?? null;
    }

    return null;
  }

  private async handleEvent(evt: AgentEventPayload) {
    if (evt.stream !== "lifecycle") {
      return;
    }
    const phase = typeof evt.data?.phase === "string" ? evt.data.phase : "";
    if (phase !== "start" && phase !== "end" && phase !== "error") {
      return;
    }

    const config = await this.readMap();
    const mappedRoots = new Set(Object.keys(config.channels ?? {}));
    if (mappedRoots.size === 0) {
      return;
    }

    this.state.staleRunningMs = config.staleRunningMs ?? DEFAULT_STALE_RUNNING_MS;
    const store = this.loadSessionStore();
    const rootSessionKey = this.resolveRootSessionKey({
      sessionKey: evt.sessionKey,
      mappedRoots,
      store,
    });
    if (!rootSessionKey) {
      return;
    }
    const mapping = config.channels?.[rootSessionKey];
    if (!mapping) {
      return;
    }

    const now = this.now();
    const current = this.state.channels[rootSessionKey] ?? { state: "idle", updatedAt: now, runs: {} };
    const runs = { ...(current.runs ?? {}) };

    if (phase === "start" && rootSessionKey === extractDiscordChannelRootSessionKey(evt.sessionKey)) {
      for (const [runId, run] of Object.entries(runs)) {
        if (run.status !== "running") {
          delete runs[runId];
        }
      }
    }

    const previousRun = runs[evt.runId];
    let fallbackRunId: string | null = null;
    if (phase === "start") {
      runs[evt.runId] = {
        sessionKey: normalizeSessionKey(evt.sessionKey) ?? previousRun?.sessionKey,
        status: "running",
        startedAt: previousRun?.startedAt ?? now,
        updatedAt: now,
      };
    } else if (previousRun) {
      runs[evt.runId] = {
        ...previousRun,
        status: phase === "error" ? "error" : "ok",
        updatedAt: now,
        ...(phase === "error" && typeof evt.data?.error === "string" && evt.data.error.trim()
          ? { lastError: evt.data.error.trim() }
          : {}),
      };
    } else {
      fallbackRunId = findFallbackRunningRunId(runs, evt.sessionKey);
      if (!fallbackRunId) {
        log.info(
          `discord-agent-activity orphan completion ignored: phase=${phase} runId=${evt.runId} sessionKey=${JSON.stringify(evt.sessionKey ?? null)} root=${rootSessionKey}`,
        );
        return;
      }
      const fallbackRun = runs[fallbackRunId];
      if (!fallbackRun) {
        return;
      }
      log.info(
        `discord-agent-activity orphan completion fallback: phase=${phase} runId=${evt.runId} fallbackRunId=${fallbackRunId} sessionKey=${JSON.stringify(evt.sessionKey ?? null)} root=${rootSessionKey}`,
      );
      runs[fallbackRunId] = {
        ...fallbackRun,
        sessionKey: normalizeSessionKey(evt.sessionKey) ?? fallbackRun.sessionKey,
        status: phase === "error" ? "error" : "ok",
        updatedAt: now,
        ...(phase === "error" && typeof evt.data?.error === "string" && evt.data.error.trim()
          ? { lastError: evt.data.error.trim() }
          : {}),
      };
    }

    log.info(
      `discord-agent-activity lifecycle handled: phase=${phase} runId=${evt.runId} sessionKey=${JSON.stringify(evt.sessionKey ?? null)} root=${rootSessionKey} previousRun=${previousRun ? "yes" : "no"} fallbackRunId=${fallbackRunId ?? ""}`,
    );

    const next: PersistedChannelRecord = {
      ...current,
      updatedAt: now,
      lastEventAt: now,
      ...(phase === "start" ? { lastStartAt: now } : {}),
      runs,
      state: current.state,
    };
    next.state = synthesizeRecordState(next);
    const latestError = collectLatestError(runs);
    if (latestError) {
      next.lastError = latestError;
    } else {
      delete next.lastError;
    }

    await this.applyChannelState(rootSessionKey, mapping, next);
  }

  private async applyChannelState(
    rootSessionKey: string,
    mapping: ChannelMapEntry,
    next: PersistedChannelRecord,
  ) {
    const targetName = synthesizeChannelName(mapping.baseName, next.state);
    const previous = this.state.channels[rootSessionKey];
    const now = this.now();
    let observedName = previous?.lastTargetName;

    try {
      const channel = await this.fetchChannel({
        channelId: mapping.channelId,
        ...(mapping.accountId ? { accountId: mapping.accountId } : {}),
      });
      observedName = typeof channel.name === "string" && channel.name.trim() ? channel.name.trim() : observedName;
    } catch (err) {
      log.warn(`failed to fetch Discord channel before rename for root ${rootSessionKey}: ${String(err)}`);
    }

    const shouldRename = observedName !== targetName;
    let renameAppliedAt = previous?.lastAppliedAt;
    let finalName = observedName ?? previous?.lastTargetName ?? targetName;

    if (shouldRename) {
      log.info(`discord-agent-activity rename requested: root=${rootSessionKey} channelId=${mapping.channelId} from=${JSON.stringify(observedName ?? null)} to=${JSON.stringify(targetName)}`);
      try {
        await this.renameChannel({
          channelId: mapping.channelId,
          name: targetName,
          ...(mapping.accountId ? { accountId: mapping.accountId } : {}),
        });
        renameAppliedAt = now;
      } catch (err) {
        log.warn(`failed to rename Discord channel for root ${rootSessionKey}: ${String(err)}`);
      }

      try {
        const channel = await this.fetchChannel({
          channelId: mapping.channelId,
          ...(mapping.accountId ? { accountId: mapping.accountId } : {}),
        });
        finalName = typeof channel.name === "string" && channel.name.trim() ? channel.name.trim() : finalName;
      } catch (err) {
        log.warn(`failed to fetch Discord channel after rename for root ${rootSessionKey}: ${String(err)}`);
      }

      if (finalName !== targetName) {
        log.warn(
          `discord-agent-activity rename drift: root=${rootSessionKey} channelId=${mapping.channelId} expected=${JSON.stringify(targetName)} actual=${JSON.stringify(finalName ?? null)}`,
        );
      }
    }

    this.state.channels[rootSessionKey] = {
      ...next,
      lastTargetName: finalName,
      ...(renameAppliedAt ? { lastAppliedAt: renameAppliedAt } : previous?.lastAppliedAt ? { lastAppliedAt: previous.lastAppliedAt } : {}),
    };
    await this.persistState();
  }

  private async recoverStaleRunningChannels() {
    const config = await this.readMap();
    this.state.staleRunningMs = config.staleRunningMs ?? DEFAULT_STALE_RUNNING_MS;
    const cutoffMs = this.state.staleRunningMs;
    const now = this.now();
    let changed = false;

    for (const [rootSessionKey, record] of Object.entries(this.state.channels)) {
      const mapping = config.channels?.[rootSessionKey];
      if (!mapping) {
        continue;
      }
      const runs = { ...(record.runs ?? {}) };
      let recoveredAny = false;
      for (const [runId, run] of Object.entries(runs)) {
        if (run.status !== "running") {
          continue;
        }
        const lastTouchAt = run.updatedAt ?? run.startedAt;
        if (now - lastTouchAt < cutoffMs) {
          continue;
        }
        runs[runId] = {
          ...run,
          status: "ok",
          updatedAt: now,
        };
        recoveredAny = true;
      }
      if (!recoveredAny) {
        continue;
      }
      const next: PersistedChannelRecord = {
        ...record,
        updatedAt: now,
        lastEventAt: now,
        runs,
        state: "idle",
      };
      next.state = synthesizeRecordState(next);
      const latestError = collectLatestError(runs);
      if (latestError) {
        next.lastError = latestError;
      } else {
        delete next.lastError;
      }
      await this.applyChannelState(rootSessionKey, mapping, next);
      changed = true;
    }

    if (!changed) {
      await this.persistState();
    }
  }
}

export async function startAgentChannelActivityMonitor(options: AgentChannelActivityMonitorOptions) {
  const monitor = new AgentChannelActivityMonitor(options);
  await monitor.start();
  return {
    stop: () => monitor.stop(),
  };
}

export const __test__ = {
  normalizeMap,
  normalizeState,
  synthesizeChannelName,
  extractDiscordChannelRootSessionKey,
  extractDiscordChannelIdFromRootSessionKey,
  synthesizeRecordState,
};
