import fs from "node:fs/promises";
import path from "node:path";
import { Routes, type APIChannel } from "discord-api-types/v10";
import { getActiveEmbeddedRunsSnapshot } from "../../../../src/agents/pi-embedded-runner/runs.js";
import {
  listDescendantRunsForRequester,
  summarizePendingDescendantRuns,
} from "../../../../src/agents/subagent-registry.js";
import {
  onActivityObserverEvent,
  type ActivityObserverEvent,
} from "../../../../src/agents/activity-observer.js";
import { loadConfig, type OpenClawConfig } from "../../../../src/config/config.js";
import type { SessionEntry } from "../../../../src/config/sessions/types.js";
import type { AgentEventPayload } from "../../../../src/infra/agent-events.js";
import { onAgentEvent } from "../../../../src/infra/agent-events.js";
import { createSubsystemLogger } from "../../../../src/logging/subsystem.js";
import { resolveThreadParentSessionKey } from "../../../../src/sessions/session-key-utils.js";
import { withTimeout } from "../../../../src/utils/with-timeout.js";
import { loadCombinedSessionStoreForGateway } from "../../../../src/gateway/session-utils.js";
import { resolveDiscordAccount } from "../accounts.js";
import { DiscordApiError, requestDiscordApi } from "../api.js";

const log = createSubsystemLogger("discord-agent-activity");

export type AgentActivityState = "idle" | "running" | "error";
type ActivityRunningReason =
  | "active_descendant_run"
  | "announce_run"
  | "pending_descendant_cleanup"
  | "root_lifecycle"
  | "stale_recovery";

type ChannelCategories = {
  blocked?: string;
  idle: string;
  running: string;
};

type ChannelMapEntry = {
  channelId: string;
  accountId?: string;
  baseName?: string;
  categories?: ChannelCategories;
  guildId?: string;
  sortKey?: number;
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
  lastBusyAt?: number;
  manualHold?: boolean;
  desiredState?: AgentActivityState;
  desiredName?: string;
  desiredParentId?: string;
  desiredVersion?: number;
  lastDesiredAt?: number;
  observedState?: AgentActivityState;
  lastObservedName?: string;
  lastObservedParentId?: string;
  lastObservedAt?: number;
  lastTargetName?: string;
  lastAppliedAt?: number;
  lastError?: string;
  lastRenameAttemptAt?: number;
  lastRenameStatus?: string;
  lastRenameDurationMs?: number;
  lastRenameError?: string;
  nextRetryAt?: number;
  guildId?: string;
  sortKey?: number;
  transportState?: "degraded" | "idle" | "in_flight" | "retry_wait";
  runs?: Record<string, PersistedRunRecord>;
};

type PersistedAgentActivityState = {
  version: 5;
  staleRunningMs: number;
  channels: Record<string, PersistedChannelRecord>;
};

type DiscordChannelRenameFn = (params: {
  channelId: string;
  name: string;
  accountId?: string;
}) => Promise<{
  durationMs?: number;
  httpStatus?: number;
  retryAfterMs?: number;
} | void>;

type DiscordChannelFetchFn = (params: {
  channelId: string;
  accountId?: string;
}) => Promise<{
  durationMs?: number;
  guildId?: string;
  httpStatus?: number;
  name?: string | null;
  parentId?: string | null;
  position?: number;
  retryAfterMs?: number;
}>;

type DiscordCategoryMoveFn = (params: {
  accountId?: string;
  guildId: string;
  moves: Array<{
    channelId: string;
    parentId: string;
    position?: number;
  }>;
}) => Promise<{
  durationMs?: number;
  httpStatus?: number;
  retryAfterMs?: number;
} | void>;

type AgentChannelActivityMonitorOptions = {
  workspaceDir: string;
  cfg?: OpenClawConfig;
  renameChannel?: DiscordChannelRenameFn;
  fetchChannel?: DiscordChannelFetchFn;
  moveChannels?: DiscordCategoryMoveFn;
  channelOpTimeoutMs?: number;
  channelFetchTimeoutMs?: number;
  channelRenameTimeoutMs?: number;
  now?: () => number;
  reconcileIntervalMs?: number;
  staleSweepIntervalMs?: number;
};

type ActivityReconcileDecision = {
  trackedSessionKeys: string[];
  activeTrackedRuns: Array<{
    runId?: string;
    sessionId: string;
    sessionKey?: string;
    sourceKind: string;
    startedAt: number;
  }>;
  pendingDescendantRuns: number;
  stateBefore: AgentActivityState;
  stateAfter: AgentActivityState;
  desiredState: AgentActivityState;
  desiredName: string;
  desiredParentId?: string;
  lastObservedName?: string;
  lastObservedParentId?: string;
  reconcileSuppressedByGrace: boolean;
  reason: ActivityRunningReason | null;
};

const DEFAULT_STALE_RUNNING_MS = 6 * 60 * 60 * 1000;
const DEFAULT_RECONCILE_INTERVAL_MS = 5_000;
const DEFAULT_CHANNEL_FETCH_TIMEOUT_MS = 5_000;
const DEFAULT_CHANNEL_RENAME_TIMEOUT_MS = 45_000;
const DEFAULT_RUNNING_CONFIRMATION_GRACE_MS = 10_000;
const DEFAULT_IDLE_QUIET_GRACE_MS = 60_000;
const DEFAULT_RENAME_RETRY_COOLDOWN_MS = 10_000;
const DEFAULT_RENAME_SETTLE_DELAY_MS = 1_000;
const DEFAULT_RENAME_SETTLE_POLLS = 3;
const CONFIG_RELATIVE_PATH = path.join("ops", "config", "agent-channel-map.json");
const STATE_RELATIVE_PATH = path.join("ops", "state", "agent-activity.json");

type RenameWorkerState = {
  activeDesiredName?: string;
  activeDesiredVersion?: number;
  pendingDesiredName?: string;
  pendingDesiredVersion?: number;
  running: boolean;
};

type CategoryWorkerState = {
  queued: boolean;
  running: boolean;
};

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
  const guildId = typeof value.guildId === "string" && value.guildId.trim() ? value.guildId.trim() : undefined;
  const sortKey =
    typeof value.sortKey === "number" && Number.isFinite(value.sortKey) ? Math.floor(value.sortKey) : undefined;
  const rawCategories = isObject(value.categories) ? value.categories : undefined;
  const categories =
    rawCategories &&
    typeof rawCategories.idle === "string" &&
    rawCategories.idle.trim() &&
    typeof rawCategories.running === "string" &&
    rawCategories.running.trim()
      ? ({
          idle: rawCategories.idle.trim(),
          running: rawCategories.running.trim(),
          ...(typeof rawCategories.blocked === "string" && rawCategories.blocked.trim()
            ? { blocked: rawCategories.blocked.trim() }
            : {}),
        } satisfies ChannelCategories)
      : undefined;
  if (!channelId || (!baseName && !categories)) {
    return null;
  }
  return {
    channelId,
    ...(baseName ? { baseName } : {}),
    ...(accountId ? { accountId } : {}),
    ...(guildId ? { guildId } : {}),
    ...(sortKey !== undefined ? { sortKey } : {}),
    ...(categories ? { categories } : {}),
  };
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
    ...(typeof value.lastBusyAt === "number" ? { lastBusyAt: value.lastBusyAt } : {}),
    ...(typeof value.manualHold === "boolean" ? { manualHold: value.manualHold } : {}),
    ...(value.desiredState === "idle" || value.desiredState === "running" || value.desiredState === "error"
      ? { desiredState: value.desiredState }
      : {}),
    ...(typeof value.desiredName === "string" && value.desiredName ? { desiredName: value.desiredName } : {}),
    ...(typeof value.desiredParentId === "string" && value.desiredParentId
      ? { desiredParentId: value.desiredParentId }
      : {}),
    ...(typeof value.desiredVersion === "number" ? { desiredVersion: value.desiredVersion } : {}),
    ...(typeof value.lastDesiredAt === "number" ? { lastDesiredAt: value.lastDesiredAt } : {}),
    ...(value.observedState === "idle" || value.observedState === "running" || value.observedState === "error"
      ? { observedState: value.observedState }
      : {}),
    ...(typeof value.lastObservedName === "string" && value.lastObservedName
      ? { lastObservedName: value.lastObservedName }
      : typeof value.lastTargetName === "string" && value.lastTargetName
        ? { lastObservedName: value.lastTargetName }
        : {}),
    ...(typeof value.lastObservedAt === "number" ? { lastObservedAt: value.lastObservedAt } : {}),
    ...(typeof value.lastObservedParentId === "string" && value.lastObservedParentId
      ? { lastObservedParentId: value.lastObservedParentId }
      : {}),
    ...(typeof value.lastTargetName === "string" && value.lastTargetName ? { lastTargetName: value.lastTargetName } : {}),
    ...(typeof value.lastAppliedAt === "number" ? { lastAppliedAt: value.lastAppliedAt } : {}),
    ...(typeof value.lastError === "string" && value.lastError ? { lastError: value.lastError } : {}),
    ...(typeof value.lastRenameAttemptAt === "number" ? { lastRenameAttemptAt: value.lastRenameAttemptAt } : {}),
    ...(typeof value.lastRenameStatus === "string" && value.lastRenameStatus
      ? { lastRenameStatus: value.lastRenameStatus }
      : {}),
    ...(typeof value.lastRenameDurationMs === "number" ? { lastRenameDurationMs: value.lastRenameDurationMs } : {}),
    ...(typeof value.lastRenameError === "string" && value.lastRenameError
      ? { lastRenameError: value.lastRenameError }
      : {}),
    ...(typeof value.nextRetryAt === "number" ? { nextRetryAt: value.nextRetryAt } : {}),
    ...(typeof value.guildId === "string" && value.guildId ? { guildId: value.guildId } : {}),
    ...(typeof value.sortKey === "number" ? { sortKey: value.sortKey } : {}),
    ...(value.transportState === "degraded" ||
    value.transportState === "idle" ||
    value.transportState === "in_flight" ||
    value.transportState === "retry_wait"
      ? { transportState: value.transportState }
      : {}),
    ...(Object.keys(runs).length > 0 ? { runs } : {}),
  };
}

function normalizeState(raw: unknown): PersistedAgentActivityState {
  if (!isObject(raw)) {
    return { version: 5, staleRunningMs: DEFAULT_STALE_RUNNING_MS, channels: {} };
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
    version: 5,
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

function isCategoryModeMapping(mapping: ChannelMapEntry) {
  return Boolean(mapping.categories?.idle && mapping.categories?.running);
}

function synthesizeDesiredParentId(mapping: ChannelMapEntry, state: "idle" | "running") {
  if (!mapping.categories) {
    return undefined;
  }
  return state === "running" ? mapping.categories.running : mapping.categories.idle;
}

function inferObservedCategoryState(
  parentId: string | undefined,
  mapping: ChannelMapEntry,
): "blocked" | "idle" | "running" | undefined {
  if (!parentId || !mapping.categories) {
    return undefined;
  }
  if (mapping.categories.blocked && parentId === mapping.categories.blocked) {
    return "blocked";
  }
  if (parentId === mapping.categories.running) {
    return "running";
  }
  if (parentId === mapping.categories.idle) {
    return "idle";
  }
  return undefined;
}

function inferObservedState(
  channelName: string | undefined,
  baseName: string,
): AgentActivityState | undefined {
  if (!channelName) {
    return undefined;
  }
  if (channelName === synthesizeChannelName(baseName, "idle")) {
    return "idle";
  }
  if (channelName === synthesizeChannelName(baseName, "running")) {
    return "running";
  }
  if (channelName === synthesizeChannelName(baseName, "error")) {
    return "error";
  }
  return undefined;
}

function isTransientDiscordRenameError(err: DiscordApiError) {
  return err.status === 429 || err.status >= 500 || err.code === "network" || err.code === "timeout";
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

function findSessionEntryByKey(
  store: Record<string, SessionEntry>,
  sessionKey: string | undefined | null,
): SessionEntry | undefined {
  const normalized = normalizeSessionKey(sessionKey);
  if (!normalized) {
    return undefined;
  }
  const direct = store[normalized];
  if (direct) {
    return direct;
  }
  for (const [key, entry] of Object.entries(store)) {
    if (normalizeSessionKey(key) === normalized) {
      return entry;
    }
  }
  return undefined;
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
  private readonly moveChannels: DiscordCategoryMoveFn;
  private readonly fetchChannel: DiscordChannelFetchFn;
  private readonly customChannelTransport: boolean;
  private readonly channelFetchTimeoutMs: number;
  private readonly channelRenameTimeoutMs: number;
  private readonly now: () => number;
  private readonly reconcileIntervalMs: number;
  private unsub: (() => void) | null = null;
  private activityUnsub: (() => void) | null = null;
  private reconcileTimer: ReturnType<typeof setInterval> | null = null;
  private state: PersistedAgentActivityState = {
    version: 5,
    staleRunningMs: DEFAULT_STALE_RUNNING_MS,
    channels: {},
  };
  private queue: Promise<void> = Promise.resolve();
  private readonly renameWorkerState = new Map<string, RenameWorkerState>();
  private readonly renameWorkerMapping = new Map<string, ChannelMapEntry>();
  private readonly renameWorkerPromise = new Map<string, Promise<void>>();
  private readonly categoryWorkerState = new Map<string, CategoryWorkerState>();
  private readonly categoryWorkerPromise = new Map<string, Promise<void>>();
  private readonly categoryWorkerMappings = new Map<string, Map<string, ChannelMapEntry>>();

  constructor(options: AgentChannelActivityMonitorOptions) {
    this.workspaceDir = options.workspaceDir;
    this.cfg = options.cfg;
    this.configPath = path.join(options.workspaceDir, CONFIG_RELATIVE_PATH);
    this.statePath = path.join(options.workspaceDir, STATE_RELATIVE_PATH);
    this.customChannelTransport = Boolean(options.renameChannel || options.fetchChannel);
    this.renameChannel =
      options.renameChannel ??
      (async ({ channelId, name, accountId }) => {
        const cfg = this.cfg ?? loadConfig();
        const account = resolveDiscordAccount({ cfg, accountId });
        const result = await requestDiscordApi<APIChannel>(
          Routes.channel(channelId),
          account.token,
          fetch,
          {
            body: { name },
            method: "PATCH",
            retry: { attempts: 4, jitter: 0.1, maxDelayMs: 45_000, minDelayMs: 1_000 },
            // Let the worker own 429 backoff so the transport does not stay
            // "in_flight" for minutes while retry_after elapses.
            shouldRetry: (err) =>
              (err.status >= 500 || err.code === "network" || err.code === "timeout") &&
              isTransientDiscordRenameError(err),
            timeoutMs: this.channelRenameTimeoutMs,
          },
        );
        return {
          durationMs: result.durationMs,
          httpStatus: result.status,
          ...(typeof result.retryAfter === "number" ? { retryAfterMs: result.retryAfter * 1000 } : {}),
        };
      });
    this.moveChannels =
      options.moveChannels ??
      (async ({ accountId, guildId, moves }) => {
        const cfg = this.cfg ?? loadConfig();
        const account = resolveDiscordAccount({ cfg, accountId });
        const result = await requestDiscordApi<void>(
          Routes.guildChannels(guildId),
          account.token,
          fetch,
          {
            body: moves.map((entry) => ({
              id: entry.channelId,
              parent_id: entry.parentId,
              ...(entry.position !== undefined ? { position: entry.position } : {}),
            })),
            method: "PATCH",
            retry: { attempts: 4, jitter: 0.1, maxDelayMs: 45_000, minDelayMs: 1_000 },
            shouldRetry: (err) =>
              (err.status === 429 || err.status >= 500 || err.code === "network" || err.code === "timeout") &&
              isTransientDiscordRenameError(err),
            timeoutMs: this.channelRenameTimeoutMs,
          },
        );
        return {
          durationMs: result.durationMs,
          httpStatus: result.status,
          ...(typeof result.retryAfter === "number" ? { retryAfterMs: result.retryAfter * 1000 } : {}),
        };
      });
    this.fetchChannel =
      options.fetchChannel ??
      (async ({ channelId, accountId }) => {
        const cfg = this.cfg ?? loadConfig();
        const account = resolveDiscordAccount({ cfg, accountId });
        const result = await requestDiscordApi<APIChannel>(
          Routes.channel(channelId),
          account.token,
          fetch,
          {
            method: "GET",
            retry: { attempts: 3, jitter: 0.1, maxDelayMs: 5_000, minDelayMs: 500 },
            shouldRetry: (err) => err.status === 429 || err.status >= 500,
            timeoutMs: this.channelFetchTimeoutMs,
          },
        );
        return {
          durationMs: result.durationMs,
          guildId:
            "guild_id" in result.data
              ? ((result.data as { guild_id?: string | null }).guild_id ?? undefined)
              : undefined,
          httpStatus: result.status,
          name: "name" in result.data ? ((result.data as { name?: string | null }).name ?? undefined) : undefined,
          parentId:
            "parent_id" in result.data
              ? ((result.data as { parent_id?: string | null }).parent_id ?? undefined)
              : undefined,
          position:
            "position" in result.data && typeof (result.data as { position?: number }).position === "number"
              ? (result.data as { position: number }).position
              : undefined,
          ...(typeof result.retryAfter === "number" ? { retryAfterMs: result.retryAfter * 1000 } : {}),
        };
      });
    this.channelFetchTimeoutMs =
      options.channelFetchTimeoutMs ??
      options.channelOpTimeoutMs ??
      DEFAULT_CHANNEL_FETCH_TIMEOUT_MS;
    this.channelRenameTimeoutMs =
      options.channelRenameTimeoutMs ??
      options.channelOpTimeoutMs ??
      DEFAULT_CHANNEL_RENAME_TIMEOUT_MS;
    this.now = options.now ?? (() => Date.now());
    this.reconcileIntervalMs =
      options.reconcileIntervalMs ??
      options.staleSweepIntervalMs ??
      DEFAULT_RECONCILE_INTERVAL_MS;
  }

  async start() {
    this.state = normalizeState(await readJsonFile(this.statePath));
    await this.persistState();
    this.unsub = onAgentEvent((evt) => {
      void this.enqueue(() => this.handleEvent(evt)).catch((err) => {
        log.warn(`discord-agent-activity event handling failed: ${String(err)}`);
      });
    });
    this.activityUnsub = onActivityObserverEvent((evt) => {
      void this.enqueue(() => this.handleActivityObservationEvent(evt)).catch((err) => {
        log.warn(`discord-agent-activity observation handling failed: ${String(err)}`);
      });
    });
    this.reconcileTimer = setInterval(() => {
      void this.enqueue(() => this.reconcileChannels());
    }, this.reconcileIntervalMs);
    if (typeof this.reconcileTimer.unref === "function") {
      this.reconcileTimer.unref();
    }
    await this.enqueue(() => this.reconcileChannels());
  }

  async stop() {
    this.unsub?.();
    this.unsub = null;
    this.activityUnsub?.();
    this.activityUnsub = null;
    if (this.reconcileTimer) {
      clearInterval(this.reconcileTimer);
      this.reconcileTimer = null;
    }
    await this.queue.catch(() => {});
    while (this.renameWorkerPromise.size > 0) {
      await Promise.allSettled(
        [...this.renameWorkerPromise.keys()].map((rootSessionKey) =>
          this.waitForRenameWorker(rootSessionKey),
        ),
      );
    }
    while (this.categoryWorkerPromise.size > 0) {
      await Promise.allSettled(
        [...this.categoryWorkerPromise.values()].map((workerPromise) => workerPromise),
      );
    }
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

  private waitForRenameWorker(rootSessionKey: string) {
    return this.renameWorkerPromise.get(rootSessionKey) ?? Promise.resolve();
  }

  private renameTransportNeedsWork(
    record: PersistedChannelRecord,
    mapping: ChannelMapEntry,
    now: number,
    options?: { force?: boolean },
  ) {
    const desiredName =
      record.desiredName ??
      synthesizeChannelName(mapping.baseName ?? "channel", record.desiredState ?? record.state);
    const observedName = record.lastObservedName ?? record.lastTargetName;
    if (options?.force) {
      return true;
    }
    if (record.transportState === "retry_wait") {
      return now >= (record.nextRetryAt ?? 0);
    }
    if (record.transportState === "degraded") {
      return (record.lastDesiredAt ?? 0) > (record.lastRenameAttemptAt ?? 0);
    }
    return observedName !== desiredName;
  }

  private maybeScheduleRenameWorker(
    rootSessionKey: string,
    mapping: ChannelMapEntry,
    options?: { force?: boolean },
  ) {
    const record = this.state.channels[rootSessionKey];
    if (!record) {
      return;
    }
    const desiredName =
      record.desiredName ??
      synthesizeChannelName(mapping.baseName ?? "channel", record.desiredState ?? record.state);
    if (!this.renameTransportNeedsWork(record, mapping, this.now(), options)) {
      return;
    }

    this.renameWorkerMapping.set(rootSessionKey, mapping);
    const current = this.renameWorkerState.get(rootSessionKey) ?? {
      running: false,
    };
    const desiredVersion = record.desiredVersion ?? 0;
    if (current.running) {
      current.pendingDesiredVersion = desiredVersion;
      current.pendingDesiredName = desiredName;
      this.renameWorkerState.set(rootSessionKey, current);
      log.info(
        current.activeDesiredVersion !== undefined && desiredVersion > current.activeDesiredVersion
          ? `discord-agent-activity desired superseded: root=${rootSessionKey} channelId=${mapping.channelId} previousDesiredVersion=${current.activeDesiredVersion} nextDesiredVersion=${desiredVersion} desired=${JSON.stringify(desiredName)}`
          : `discord-agent-activity transport still in flight: root=${rootSessionKey} channelId=${mapping.channelId} desiredVersion=${desiredVersion} desired=${JSON.stringify(desiredName)}`,
      );
      return;
    }

    current.running = true;
    current.activeDesiredVersion = desiredVersion;
    current.activeDesiredName = desiredName;
    current.pendingDesiredVersion = desiredVersion;
    current.pendingDesiredName = desiredName;
    this.renameWorkerState.set(rootSessionKey, current);
    const workerPromise = this.runRenameWorker(rootSessionKey)
      .catch((err) => {
        log.warn(`discord-agent-activity rename worker failed: root=${rootSessionKey} error=${JSON.stringify(String(err))}`);
      })
      .finally(() => {
        this.renameWorkerPromise.delete(rootSessionKey);
        this.renameWorkerState.delete(rootSessionKey);
      });
    this.renameWorkerPromise.set(rootSessionKey, workerPromise);
  }

  private async fetchObservedChannelInfo(params: {
    rootSessionKey: string;
    mapping: ChannelMapEntry;
    stage: string;
  }) {
    try {
      const fetchPromise = this.fetchChannel({
        channelId: params.mapping.channelId,
        ...(params.mapping.accountId ? { accountId: params.mapping.accountId } : {}),
      });
      const channel =
        this.customChannelTransport && this.channelFetchTimeoutMs > 0
          ? await withTimeout(fetchPromise, this.channelFetchTimeoutMs)
          : await fetchPromise;
      return {
        durationMs: channel.durationMs,
        guildId: channel.guildId,
        httpStatus: channel.httpStatus,
        name: typeof channel.name === "string" && channel.name.trim() ? channel.name.trim() : undefined,
        parentId:
          typeof channel.parentId === "string" && channel.parentId.trim() ? channel.parentId.trim() : undefined,
        position: typeof channel.position === "number" ? channel.position : undefined,
        retryAfterMs: channel.retryAfterMs,
      };
    } catch (err) {
      log.warn(
        `discord-agent-activity transport fetch failure: root=${params.rootSessionKey} channelId=${params.mapping.channelId} stage=${params.stage} error=${JSON.stringify(String(err))}`,
      );
      return {
        error: String(err),
      };
    }
  }

  private computeRenameRetryAt(params: { err?: DiscordApiError; now: number }) {
    const retryAfterMs =
      typeof params.err?.retryAfter === "number" ? Math.max(0, params.err.retryAfter * 1000) : undefined;
    return params.now + (retryAfterMs ?? DEFAULT_RENAME_RETRY_COOLDOWN_MS);
  }

  private async commitRenameState(
    rootSessionKey: string,
    updater: (record: PersistedChannelRecord) => PersistedChannelRecord,
  ) {
    const latest = this.state.channels[rootSessionKey];
    if (!latest) {
      return;
    }
    this.state.channels[rootSessionKey] = updater(latest);
    await this.persistState();
  }

  private async verifyDesiredChannelName(params: {
    desiredName: string;
    mapping: ChannelMapEntry;
    rootSessionKey: string;
  }) {
    let lastObserved = await this.fetchObservedChannelInfo({
      rootSessionKey: params.rootSessionKey,
      mapping: params.mapping,
      stage: "after",
    });
    if (lastObserved.name === params.desiredName) {
      return lastObserved;
    }
    for (let attempt = 1; attempt <= DEFAULT_RENAME_SETTLE_POLLS; attempt += 1) {
      await new Promise((resolve) => setTimeout(resolve, DEFAULT_RENAME_SETTLE_DELAY_MS));
      lastObserved = await this.fetchObservedChannelInfo({
        rootSessionKey: params.rootSessionKey,
        mapping: params.mapping,
        stage: `settle:${attempt}`,
      });
      if (lastObserved.name === params.desiredName) {
        return lastObserved;
      }
    }
    return lastObserved;
  }

  private async runRenameWorker(rootSessionKey: string) {
    while (true) {
      const mapping = this.renameWorkerMapping.get(rootSessionKey);
      const current = this.state.channels[rootSessionKey];
      const workerState = this.renameWorkerState.get(rootSessionKey);
      if (!mapping || !current || !workerState) {
        return;
      }
      const desiredVersion = workerState.activeDesiredVersion ?? current.desiredVersion ?? 0;
      const desiredName =
        workerState.activeDesiredName ??
        current.desiredName ??
        synthesizeChannelName(mapping.baseName ?? "channel", current.desiredState ?? current.state);
      const observedNameBefore = current.lastObservedName ?? current.lastTargetName;
      const shouldProcessActiveDesired =
        current.lastObservedAt === undefined ||
        observedNameBefore !== desiredName ||
        current.transportState === "retry_wait" ||
        current.transportState === "degraded";
      if (!shouldProcessActiveDesired) {
        if (
          (workerState.pendingDesiredVersion ?? desiredVersion) > desiredVersion &&
          workerState.pendingDesiredName
        ) {
          workerState.activeDesiredVersion = workerState.pendingDesiredVersion;
          workerState.activeDesiredName = workerState.pendingDesiredName;
          continue;
        }
        return;
      }
      const transportStateBefore = current.transportState ?? "idle";
      const attemptId = `${rootSessionKey}:${desiredVersion}:${this.now()}`;
      workerState.activeDesiredVersion = desiredVersion;
      workerState.activeDesiredName = desiredName;

      log.info(
        `discord-agent-activity rename transport start: root=${rootSessionKey} channelId=${mapping.channelId} desiredVersion=${desiredVersion} desiredName=${JSON.stringify(desiredName)} attemptId=${JSON.stringify(attemptId)} transportStateBefore=${transportStateBefore}`,
      );

      await this.commitRenameState(rootSessionKey, (latest) => ({
        ...latest,
        lastRenameAttemptAt: this.now(),
        lastRenameStatus: "in_flight",
        transportState: "in_flight",
      }));

      const beforeObserved = await this.fetchObservedChannelInfo({
        rootSessionKey,
        mapping,
        stage: "before",
      });
      const observedBefore = beforeObserved.name ?? current.lastObservedName ?? current.lastTargetName;
      if (observedBefore === desiredName) {
        log.info(
          `discord-agent-activity rename verified: root=${rootSessionKey} channelId=${mapping.channelId} desiredVersion=${desiredVersion} observed=${JSON.stringify(observedBefore)} attemptId=${JSON.stringify(attemptId)}`,
        );
        await this.commitRenameState(rootSessionKey, (latest) => {
          const next: PersistedChannelRecord = {
            ...latest,
            lastObservedAt: this.now(),
            lastObservedName: observedBefore,
            lastTargetName: observedBefore,
            lastRenameDurationMs: beforeObserved.durationMs,
            lastRenameStatus: "verified",
            lastAppliedAt: this.now(),
            observedState: inferObservedState(observedBefore, mapping.baseName ?? "channel"),
            transportState: "idle",
          };
          delete next.lastRenameError;
          delete next.nextRetryAt;
          return next;
        });
        const latestDesiredVersion = this.state.channels[rootSessionKey]?.desiredVersion ?? desiredVersion;
        if (latestDesiredVersion > desiredVersion) {
          workerState.activeDesiredVersion = workerState.pendingDesiredVersion ?? latestDesiredVersion;
          workerState.activeDesiredName = workerState.pendingDesiredName;
          continue;
        }
        return;
      }

      let renameResult:
        | {
            durationMs?: number;
            httpStatus?: number;
            retryAfterMs?: number;
          }
        | undefined;
      let renameError: DiscordApiError | undefined;
      try {
        const renamePromise = this.renameChannel({
          channelId: mapping.channelId,
          name: desiredName,
          ...(mapping.accountId ? { accountId: mapping.accountId } : {}),
        });
        renameResult =
          ((this.customChannelTransport && this.channelRenameTimeoutMs > 0
            ? await withTimeout(renamePromise, this.channelRenameTimeoutMs)
            : await renamePromise) as {
            durationMs?: number;
            httpStatus?: number;
            retryAfterMs?: number;
          } | void) ?? { httpStatus: 200 };
      } catch (err) {
        renameError =
          err instanceof DiscordApiError
            ? err
            : new DiscordApiError(String(err), 0, undefined, {
                code: String(err).includes("timeout") ? "timeout" : "network",
                method: "PATCH",
                path: Routes.channel(mapping.channelId),
              });
      }

      const latestDesiredVersion = this.state.channels[rootSessionKey]?.desiredVersion ?? desiredVersion;
      const obsoleteDesired = latestDesiredVersion > desiredVersion;

      if (renameError) {
        const transient = isTransientDiscordRenameError(renameError);
        const transportStateAfter = obsoleteDesired
          ? "idle"
          : transient
            ? "retry_wait"
            : "degraded";
        const retryAt =
          !obsoleteDesired && transient
            ? this.computeRenameRetryAt({ err: renameError, now: this.now() })
            : undefined;
        log.warn(
          `discord-agent-activity rename transport result: root=${rootSessionKey} channelId=${mapping.channelId} desiredVersion=${desiredVersion} desiredName=${JSON.stringify(desiredName)} attemptId=${JSON.stringify(attemptId)} httpStatus=${renameError.status} retryAfterMs=${Math.round((renameError.retryAfter ?? 0) * 1000)} durationMs=${renameResult?.durationMs ?? 0} transportStateBefore=${transportStateBefore} transportStateAfter=${transportStateAfter} obsoleteDesired=${obsoleteDesired ? "1" : "0"} error=${JSON.stringify(renameError.message)}`,
        );
        await this.commitRenameState(rootSessionKey, (latest) => {
          const next: PersistedChannelRecord = {
            ...latest,
            lastRenameDurationMs: renameResult?.durationMs,
            lastRenameStatus: obsoleteDesired
              ? "obsolete"
              : transient
                ? renameError.code ?? "retry_wait"
                : "degraded",
            transportState: transportStateAfter,
          };
          if (!obsoleteDesired) {
            next.lastRenameError = renameError.message;
          } else {
            delete next.lastRenameError;
          }
          if (retryAt !== undefined) {
            next.nextRetryAt = retryAt;
          } else {
            delete next.nextRetryAt;
          }
          return next;
        });
        if (obsoleteDesired) {
          workerState.activeDesiredVersion = workerState.pendingDesiredVersion ?? latestDesiredVersion;
          workerState.activeDesiredName = workerState.pendingDesiredName;
          continue;
        }
        return;
      }

      const verified = await this.verifyDesiredChannelName({
        desiredName,
        mapping,
        rootSessionKey,
      });
      const finalDesiredVersion = this.state.channels[rootSessionKey]?.desiredVersion ?? desiredVersion;
      const obsoleteAfterVerify = finalDesiredVersion > desiredVersion;
      const observedName = verified.name ?? observedBefore;
      const matched = observedName === desiredName;
      const transportStateAfter = obsoleteAfterVerify
        ? "idle"
        : matched
          ? "idle"
          : "retry_wait";

      log.info(
        `discord-agent-activity rename transport result: root=${rootSessionKey} channelId=${mapping.channelId} desiredVersion=${desiredVersion} desiredName=${JSON.stringify(desiredName)} attemptId=${JSON.stringify(attemptId)} httpStatus=${renameResult?.httpStatus ?? 200} retryAfterMs=${renameResult?.retryAfterMs ?? 0} durationMs=${renameResult?.durationMs ?? 0} transportStateBefore=${transportStateBefore} transportStateAfter=${transportStateAfter} obsoleteDesired=${obsoleteAfterVerify ? "1" : "0"}`,
      );

      if (matched) {
        log.info(
          `discord-agent-activity rename verified: root=${rootSessionKey} channelId=${mapping.channelId} desiredVersion=${desiredVersion} observed=${JSON.stringify(observedName)} attemptId=${JSON.stringify(attemptId)}`,
        );
        await this.commitRenameState(rootSessionKey, (latest) => {
          const next: PersistedChannelRecord = {
            ...latest,
            lastAppliedAt: this.now(),
            lastObservedAt: this.now(),
            lastObservedName: observedName,
            lastTargetName: observedName,
            lastRenameDurationMs: renameResult?.durationMs,
            lastRenameStatus: obsoleteAfterVerify ? "obsolete" : "verified",
            observedState: inferObservedState(observedName, mapping.baseName ?? "channel"),
            transportState: transportStateAfter,
          };
          delete next.lastRenameError;
          delete next.nextRetryAt;
          return next;
        });
      } else {
        log.warn(
          `discord-agent-activity rename drift: root=${rootSessionKey} channelId=${mapping.channelId} expected=${JSON.stringify(desiredName)} actual=${JSON.stringify(observedName ?? null)} attemptId=${JSON.stringify(attemptId)} obsoleteDesired=${obsoleteAfterVerify ? "1" : "0"}`,
        );
        await this.commitRenameState(rootSessionKey, (latest) => {
          const next: PersistedChannelRecord = {
            ...latest,
            ...(observedName
              ? { lastObservedAt: this.now(), lastObservedName: observedName, lastTargetName: observedName }
              : {}),
            lastRenameDurationMs: renameResult?.durationMs,
            lastRenameStatus: obsoleteAfterVerify ? "obsolete" : "drift",
            observedState: inferObservedState(observedName, mapping.baseName ?? "channel"),
            transportState: transportStateAfter,
          };
          if (!obsoleteAfterVerify) {
            next.lastRenameError = "rename drift";
            next.nextRetryAt = this.now() + DEFAULT_RENAME_RETRY_COOLDOWN_MS;
          } else {
            delete next.lastRenameError;
            delete next.nextRetryAt;
          }
          return next;
        });
      }

      if (obsoleteAfterVerify) {
        workerState.activeDesiredVersion = workerState.pendingDesiredVersion ?? finalDesiredVersion;
        workerState.activeDesiredName = workerState.pendingDesiredName;
        continue;
      }
      if (
        (workerState.pendingDesiredVersion ?? desiredVersion) > desiredVersion &&
        workerState.pendingDesiredName
      ) {
        workerState.activeDesiredVersion = workerState.pendingDesiredVersion;
        workerState.activeDesiredName = workerState.pendingDesiredName;
        continue;
      }
      return;
    }
  }

  private resolveVisibleStateForCategory(params: {
    now: number;
    previous?: PersistedChannelRecord;
    record: PersistedChannelRecord;
  }): "idle" | "running" {
    if (params.record.state === "running") {
      return "running";
    }
    const previousVisibleRunning =
      params.previous?.desiredState === "running" || params.record.desiredState === "running";
    if (!previousVisibleRunning) {
      return "idle";
    }
    const quietUntil = (params.record.lastBusyAt ?? 0) + DEFAULT_IDLE_QUIET_GRACE_MS;
    const blockedUntil = params.record.nextRetryAt ?? 0;
    return params.now < Math.max(quietUntil, blockedUntil) ? "running" : "idle";
  }

  private getCategoryGroupKey(mapping: ChannelMapEntry, record: PersistedChannelRecord): string | null {
    if (!isCategoryModeMapping(mapping)) {
      return null;
    }
    const guildId = mapping.guildId ?? record.guildId;
    if (!guildId) {
      return null;
    }
    return `${mapping.accountId ?? ""}:${guildId}`.replace(/:+/g, ":");
  }

  private categoryTransportNeedsWork(record: PersistedChannelRecord, now: number) {
    if (record.manualHold) {
      return false;
    }
    if (!record.desiredParentId) {
      return false;
    }
    if (record.transportState === "retry_wait") {
      return now >= (record.nextRetryAt ?? 0);
    }
    if (record.transportState === "degraded") {
      return (record.lastDesiredAt ?? 0) > (record.lastRenameAttemptAt ?? 0);
    }
    return record.lastObservedParentId !== record.desiredParentId;
  }

  private async refreshObservedCategoryState(params: {
    current: PersistedChannelRecord;
    mapping: ChannelMapEntry;
    rootSessionKey: string;
    trigger: string;
  }) {
    const observed = await this.fetchObservedChannelInfo({
      rootSessionKey: params.rootSessionKey,
      mapping: params.mapping,
      stage: `reconcile:${params.trigger}`,
    });
    if ("error" in observed) {
      return params.current;
    }
    if (observed.guildId && !params.mapping.guildId) {
      params.mapping.guildId = observed.guildId;
    }
    const inferredCategoryState = inferObservedCategoryState(observed.parentId, params.mapping);
    const manualHold = inferredCategoryState === "blocked";
    if (manualHold !== (params.current.manualHold ?? false)) {
      log.info(
        manualHold
          ? `discord-agent-activity manual blocked override: root=${params.rootSessionKey} channelId=${params.mapping.channelId} guildId=${JSON.stringify(observed.guildId ?? params.current.guildId ?? params.mapping.guildId ?? null)} actualParentId=${JSON.stringify(observed.parentId ?? null)}`
          : `discord-agent-activity manual blocked released: root=${params.rootSessionKey} channelId=${params.mapping.channelId} guildId=${JSON.stringify(observed.guildId ?? params.current.guildId ?? params.mapping.guildId ?? null)} actualParentId=${JSON.stringify(observed.parentId ?? null)}`,
      );
    }
    return {
      ...params.current,
      ...(observed.guildId ? { guildId: observed.guildId } : {}),
      ...(observed.position !== undefined
        ? { sortKey: params.current.sortKey ?? params.mapping.sortKey ?? observed.position }
        : {}),
      ...(observed.name ? { lastObservedName: observed.name, lastTargetName: observed.name } : {}),
      ...(observed.parentId ? { lastObservedParentId: observed.parentId } : {}),
      ...(inferredCategoryState === "idle" || inferredCategoryState === "running"
        ? { observedState: inferredCategoryState }
        : {}),
      ...(manualHold ? { manualHold: true } : { manualHold: false }),
      lastObservedAt: this.now(),
    } satisfies PersistedChannelRecord;
  }

  private maybeScheduleCategoryWorker(rootSessionKey: string, mapping: ChannelMapEntry) {
    const record = this.state.channels[rootSessionKey];
    if (!record) {
      return;
    }
    const groupKey = this.getCategoryGroupKey(mapping, record);
    if (!groupKey || !this.categoryTransportNeedsWork(record, this.now())) {
      return;
    }
    const groupMappings = this.categoryWorkerMappings.get(groupKey) ?? new Map<string, ChannelMapEntry>();
    groupMappings.set(rootSessionKey, mapping);
    this.categoryWorkerMappings.set(groupKey, groupMappings);
    const workerState = this.categoryWorkerState.get(groupKey) ?? { queued: false, running: false };
    if (workerState.running) {
      workerState.queued = true;
      this.categoryWorkerState.set(groupKey, workerState);
      return;
    }
    workerState.running = true;
    workerState.queued = false;
    this.categoryWorkerState.set(groupKey, workerState);
    const workerPromise = this.runCategoryWorker(groupKey)
      .catch((err) => {
        log.warn(`discord-agent-activity category worker failed: group=${groupKey} error=${JSON.stringify(String(err))}`);
      })
      .finally(() => {
        this.categoryWorkerPromise.delete(groupKey);
        this.categoryWorkerState.delete(groupKey);
      });
    this.categoryWorkerPromise.set(groupKey, workerPromise);
  }

  private async runCategoryWorker(groupKey: string) {
    while (true) {
      const workerState = this.categoryWorkerState.get(groupKey);
      const mappings = this.categoryWorkerMappings.get(groupKey);
      if (!workerState || !mappings) {
        return;
      }
      workerState.queued = false;
      const entries = [...mappings.entries()]
        .map(([rootSessionKey, mapping]) => ({
          mapping,
          record: this.state.channels[rootSessionKey],
          rootSessionKey,
        }))
        .filter(
          (entry): entry is { mapping: ChannelMapEntry; record: PersistedChannelRecord; rootSessionKey: string } =>
            Boolean(entry.record && isCategoryModeMapping(entry.mapping)),
        );
      if (entries.length === 0) {
        return;
      }
      const guildId = entries[0]?.mapping.guildId ?? entries[0]?.record.guildId;
      if (!guildId) {
        return;
      }
      const moves = entries
        .filter((entry) => this.categoryTransportNeedsWork(entry.record, this.now()))
        .map((entry) => ({
          channelId: entry.mapping.channelId,
          desiredParentId:
            entry.record.desiredParentId ??
            synthesizeDesiredParentId(
              entry.mapping,
              entry.record.desiredState === "running" ? "running" : "idle",
            ),
          position: entry.record.sortKey ?? entry.mapping.sortKey,
          rootSessionKey: entry.rootSessionKey,
        }))
        .filter((entry): entry is typeof entry & { desiredParentId: string } => Boolean(entry.desiredParentId))
        .sort((left, right) => (left.position ?? Number.MAX_SAFE_INTEGER) - (right.position ?? Number.MAX_SAFE_INTEGER));
      if (moves.length === 0) {
        if (workerState.queued) {
          continue;
        }
        return;
      }
      const accountId = entries[0]?.mapping.accountId;
      for (const move of moves) {
        const mapping = mappings.get(move.rootSessionKey);
        if (!mapping) {
          continue;
        }
        const attemptId = `${groupKey}:${move.rootSessionKey}:${this.now()}`;
        log.info(
          `discord-agent-activity category move requested: root=${move.rootSessionKey} channelId=${mapping.channelId} guildId=${guildId} desiredParentId=${JSON.stringify(move.desiredParentId)} attemptId=${JSON.stringify(attemptId)}`,
        );
        await this.commitRenameState(move.rootSessionKey, (latest) => ({
          ...latest,
          lastRenameAttemptAt: this.now(),
          lastRenameStatus: "requested",
          transportState: "in_flight",
        }));

        let moveResult:
          | Awaited<ReturnType<DiscordCategoryMoveFn>>
          | undefined;
        let moveError: DiscordApiError | undefined;
        try {
          moveResult = await this.moveChannels({
            ...(accountId ? { accountId } : {}),
            guildId,
            moves: [
              {
                channelId: move.channelId,
                parentId: move.desiredParentId,
                ...(move.position !== undefined ? { position: move.position } : {}),
              },
            ],
          });
        } catch (err) {
          moveError = err instanceof DiscordApiError ? err : undefined;
        }

        if (moveError) {
          const retryAt = this.computeRenameRetryAt({ err: moveError, now: this.now() });
          const transportState =
            moveError.status === 401 || moveError.status === 403 || moveError.status === 404
              ? "degraded"
              : "retry_wait";
          log.warn(
            `discord-agent-activity category move failure: root=${move.rootSessionKey} channelId=${mapping.channelId} guildId=${guildId} desiredParentId=${JSON.stringify(move.desiredParentId)} retryAfterMs=${Math.round((moveError.retryAfter ?? 0) * 1000)} attemptId=${JSON.stringify(attemptId)} error=${JSON.stringify(moveError.message)}`,
          );
          await this.commitRenameState(move.rootSessionKey, (latest) => {
            const next = {
              ...latest,
              lastRenameDurationMs: moveResult?.durationMs,
              lastRenameError: moveError.message,
              lastRenameStatus: transportState === "degraded" ? "degraded" : "retry_wait",
              transportState,
            } satisfies PersistedChannelRecord;
            if (transportState === "retry_wait") {
              next.nextRetryAt = retryAt;
            }
            return next;
          });
          continue;
        }

        const observed = await this.fetchObservedChannelInfo({
          rootSessionKey: move.rootSessionKey,
          mapping,
          stage: "category-after",
        });
        const matched = observed.parentId === move.desiredParentId;
        if (matched) {
          log.info(
            `discord-agent-activity category move applied: root=${move.rootSessionKey} channelId=${mapping.channelId} guildId=${guildId} desiredParentId=${JSON.stringify(move.desiredParentId)} actualParentId=${JSON.stringify(observed.parentId ?? null)} retryAfterMs=${moveResult?.retryAfterMs ?? 0} attemptId=${JSON.stringify(attemptId)}`,
          );
          await this.commitRenameState(move.rootSessionKey, (latest) => {
            const next = {
              ...latest,
              ...(observed.guildId ? { guildId: observed.guildId } : {}),
              ...(observed.position !== undefined ? { sortKey: latest.sortKey ?? observed.position } : {}),
              ...(observed.name ? { lastObservedName: observed.name, lastTargetName: observed.name } : {}),
              ...(observed.parentId ? { lastObservedParentId: observed.parentId } : {}),
              lastObservedAt: this.now(),
              lastAppliedAt: this.now(),
              lastRenameDurationMs: moveResult?.durationMs,
              lastRenameStatus: "applied",
              transportState: "idle",
            } satisfies PersistedChannelRecord;
            delete next.lastRenameError;
            delete next.nextRetryAt;
            return next;
          });
          continue;
        }

        log.warn(
          `discord-agent-activity category move failure: root=${move.rootSessionKey} channelId=${mapping.channelId} guildId=${guildId} desiredParentId=${JSON.stringify(move.desiredParentId)} actualParentId=${JSON.stringify(observed.parentId ?? null)} retryAfterMs=${moveResult?.retryAfterMs ?? 0} attemptId=${JSON.stringify(attemptId)} error=${JSON.stringify("category move drift")}`,
        );
        await this.commitRenameState(move.rootSessionKey, (latest) => ({
          ...latest,
          ...(observed.guildId ? { guildId: observed.guildId } : {}),
          ...(observed.position !== undefined ? { sortKey: latest.sortKey ?? observed.position } : {}),
          ...(observed.name ? { lastObservedName: observed.name, lastTargetName: observed.name } : {}),
          ...(observed.parentId ? { lastObservedParentId: observed.parentId } : {}),
          lastObservedAt: this.now(),
          lastRenameDurationMs: moveResult?.durationMs,
          lastRenameError: "category move drift",
          lastRenameStatus: "drift",
          nextRetryAt: this.now() + DEFAULT_RENAME_RETRY_COOLDOWN_MS,
          transportState: "retry_wait",
        }));
      }
      if (!workerState.queued) {
        return;
      }
    }
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

    for (const mappedRoot of params.mappedRoots) {
      const descendants = listDescendantRunsForRequester(mappedRoot);
      if (
        descendants.some(
          (entry) => normalizeSessionKey(entry.childSessionKey) === normalizedSessionKey,
        )
      ) {
        return mappedRoot;
      }
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
    const activeRuns = getActiveEmbeddedRunsSnapshot();
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
      for (const runId of Object.keys(runs)) {
        if (runId !== evt.runId) {
          delete runs[runId];
        }
      }
    }

    const previousRun = runs[evt.runId];
    let fallbackRunId: string | null = null;
    if (phase === "start") {
      const normalizedEventSessionKey = normalizeSessionKey(evt.sessionKey);
      for (const [existingRunId, existingRun] of Object.entries(runs)) {
        if (
          existingRunId === evt.runId ||
          existingRun.status !== "running" ||
          normalizeSessionKey(existingRun.sessionKey) !== normalizedEventSessionKey
        ) {
          continue;
        }
        // A newer root lifecycle start supersedes stale root runs for the same session.
        runs[existingRunId] = {
          ...existingRun,
          status: "ok",
          updatedAt: now,
        };
      }
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
    await this.reconcileRootChannel({
      rootSessionKey,
      mapping,
      current: next,
      store,
      activeRuns,
      now,
      optimisticRunning: true,
      trigger: `lifecycle:${phase}`,
    });
  }

  private resolveObservedRoots(params: {
    event: ActivityObserverEvent;
    mappedRoots: Set<string>;
    store: Record<string, SessionEntry>;
  }): string[] {
    const roots = new Set<string>();
    for (const sessionKey of params.event.sessionKeys ?? []) {
      const rootSessionKey = this.resolveRootSessionKey({
        sessionKey,
        mappedRoots: params.mappedRoots,
        store: params.store,
      });
      if (rootSessionKey) {
        roots.add(rootSessionKey);
      }
    }
    return [...roots];
  }

  private async handleActivityObservationEvent(evt: ActivityObserverEvent) {
    const config = await this.readMap();
    const mappedRoots = new Set(Object.keys(config.channels ?? {}));
    if (mappedRoots.size === 0) {
      return;
    }
    this.state.staleRunningMs = config.staleRunningMs ?? DEFAULT_STALE_RUNNING_MS;
    const store = this.loadSessionStore();
    const activeRuns = getActiveEmbeddedRunsSnapshot();
    const observedRoots = this.resolveObservedRoots({
      event: evt,
      mappedRoots,
      store,
    });
    await this.reconcileRootChannels({
      config,
      roots: observedRoots.length > 0 ? observedRoots : [...mappedRoots],
      store,
      activeRuns,
      now: this.now(),
      trigger: `${evt.source}:${evt.action}`,
    });
  }

  private async reconcileRootChannels(params: {
    config: ChannelMap;
    roots: string[];
    store: Record<string, SessionEntry>;
    activeRuns: ReturnType<typeof getActiveEmbeddedRunsSnapshot>;
    now: number;
    trigger: string;
  }) {
    for (const rootSessionKey of [...new Set(params.roots)]) {
      const mapping = params.config.channels?.[rootSessionKey];
      if (!mapping) {
        continue;
      }
      await this.reconcileRootChannel({
        rootSessionKey,
        mapping,
        current:
          this.state.channels[rootSessionKey] ??
          ({
            state: "idle",
            updatedAt: params.now,
            runs: {},
          } satisfies PersistedChannelRecord),
        store: params.store,
        activeRuns: params.activeRuns,
        now: params.now,
        trigger: params.trigger,
      });
    }
  }

  private async reconcileRootChannel(params: {
    rootSessionKey: string;
    mapping: ChannelMapEntry;
    current: PersistedChannelRecord;
    store: Record<string, SessionEntry>;
    activeRuns: ReturnType<typeof getActiveEmbeddedRunsSnapshot>;
    now: number;
    trigger: string;
    optimisticRunning?: boolean;
  }) {
    let current = params.current;
    if (
      isCategoryModeMapping(params.mapping) &&
      (params.trigger === "interval" ||
        current.lastObservedAt === undefined ||
        current.manualHold === true ||
        current.guildId === undefined ||
        current.sortKey === undefined)
    ) {
      current = await this.refreshObservedCategoryState({
        current,
        mapping: params.mapping,
        rootSessionKey: params.rootSessionKey,
        trigger: params.trigger,
      });
    }
    log.info(
      `discord-agent-activity activity reconcile start: root=${params.rootSessionKey} trigger=${params.trigger} stateBefore=${current.state}`,
    );
    const reconciled = this.buildReconciledChannelRecord({
      rootSessionKey: params.rootSessionKey,
      mapping: params.mapping,
      record: current,
      store: params.store,
      activeRuns: params.activeRuns,
      now: params.now,
      optimisticRunning: params.optimisticRunning,
    });
    const decision = reconciled.decision;
    log.info(
      `discord-agent-activity activity reconcile decision: root=${params.rootSessionKey} trigger=${params.trigger} trackedSessionKeys=${JSON.stringify(decision.trackedSessionKeys)} activeTrackedRuns=${JSON.stringify(decision.activeTrackedRuns)} pendingDescendantRuns=${decision.pendingDescendantRuns} stateBefore=${decision.stateBefore} stateAfter=${decision.stateAfter} desiredState=${decision.desiredState} desiredName=${JSON.stringify(decision.desiredName)} desiredParentId=${JSON.stringify(decision.desiredParentId ?? null)} lastObservedName=${JSON.stringify(decision.lastObservedName ?? null)} lastObservedParentId=${JSON.stringify(decision.lastObservedParentId ?? null)} reconcileSuppressedByGrace=${decision.reconcileSuppressedByGrace ? "1" : "0"} reason=${JSON.stringify(decision.reason)}`,
    );
    await this.applyChannelState(params.rootSessionKey, params.mapping, reconciled.record);
  }

  private async applyChannelState(
    rootSessionKey: string,
    mapping: ChannelMapEntry,
    next: PersistedChannelRecord,
  ) {
    const previous = this.state.channels[rootSessionKey];
    const now = this.now();
    if (isCategoryModeMapping(mapping)) {
      const visibleDesiredState = this.resolveVisibleStateForCategory({
        now,
        previous,
        record: next,
      });
      const desiredParentId = synthesizeDesiredParentId(
        mapping,
        visibleDesiredState === "running" ? "running" : "idle",
      );
      const desiredChanged =
        previous?.desiredState !== visibleDesiredState ||
        previous?.desiredParentId !== desiredParentId;
      const desiredVersion = desiredChanged
        ? (previous?.desiredVersion ?? 0) + 1
        : previous?.desiredVersion ?? next.desiredVersion ?? 0;
      const merged: PersistedChannelRecord = {
        ...previous,
        ...next,
        desiredState: visibleDesiredState,
        ...(desiredParentId ? { desiredParentId } : {}),
        desiredVersion,
        lastDesiredAt: desiredChanged ? now : previous?.lastDesiredAt ?? now,
        ...(previous?.lastObservedName ?? next.lastObservedName
          ? {
              lastObservedName:
                previous?.lastObservedName ?? previous?.lastTargetName ?? next.lastObservedName,
              lastTargetName:
                previous?.lastObservedName ?? previous?.lastTargetName ?? next.lastObservedName,
            }
          : {}),
        ...(previous?.lastObservedParentId ?? next.lastObservedParentId
          ? {
              lastObservedParentId:
                previous?.lastObservedParentId ?? next.lastObservedParentId,
            }
          : {}),
        ...(mapping.guildId ?? previous?.guildId ?? next.guildId
          ? { guildId: mapping.guildId ?? previous?.guildId ?? next.guildId }
          : {}),
        ...(mapping.sortKey ?? previous?.sortKey ?? next.sortKey
          ? { sortKey: mapping.sortKey ?? previous?.sortKey ?? next.sortKey }
          : {}),
        ...(previous?.lastAppliedAt !== undefined ? { lastAppliedAt: previous.lastAppliedAt } : {}),
        ...(previous?.lastRenameAttemptAt !== undefined ? { lastRenameAttemptAt: previous.lastRenameAttemptAt } : {}),
        ...(previous?.lastRenameStatus ? { lastRenameStatus: previous.lastRenameStatus } : {}),
        ...(previous?.lastRenameDurationMs !== undefined
          ? { lastRenameDurationMs: previous.lastRenameDurationMs }
          : {}),
        ...(previous?.lastRenameError ? { lastRenameError: previous.lastRenameError } : {}),
        ...(previous?.nextRetryAt !== undefined ? { nextRetryAt: previous.nextRetryAt } : {}),
        transportState: previous?.transportState ?? "idle",
      };
      this.state.channels[rootSessionKey] = merged;
      await this.persistState();
      if (!merged.manualHold) {
        this.maybeScheduleCategoryWorker(rootSessionKey, mapping);
      }
      return;
    }

    const targetName = synthesizeChannelName(mapping.baseName ?? "channel", next.state);
    const desiredChanged =
      previous?.desiredState !== next.state || previous?.desiredName !== targetName;
    const desiredVersion = desiredChanged
      ? (previous?.desiredVersion ?? 0) + 1
      : previous?.desiredVersion ?? next.desiredVersion ?? 0;
    this.state.channels[rootSessionKey] = {
      ...previous,
      ...next,
      desiredState: next.state,
      desiredName: targetName,
      desiredVersion,
      lastDesiredAt: desiredChanged ? now : previous?.lastDesiredAt ?? now,
      lastObservedName: previous?.lastObservedName ?? previous?.lastTargetName ?? next.lastObservedName,
      lastTargetName: previous?.lastObservedName ?? previous?.lastTargetName ?? next.lastObservedName,
      observedState:
        previous?.observedState ??
        inferObservedState(
          previous?.lastObservedName ?? previous?.lastTargetName ?? next.lastObservedName,
          mapping.baseName ?? "channel",
        ),
      ...(previous?.lastAppliedAt !== undefined ? { lastAppliedAt: previous.lastAppliedAt } : {}),
      ...(previous?.lastRenameAttemptAt !== undefined ? { lastRenameAttemptAt: previous.lastRenameAttemptAt } : {}),
      ...(previous?.lastRenameStatus ? { lastRenameStatus: previous.lastRenameStatus } : {}),
      ...(previous?.lastRenameDurationMs !== undefined
        ? { lastRenameDurationMs: previous.lastRenameDurationMs }
        : {}),
      ...(previous?.lastRenameError ? { lastRenameError: previous.lastRenameError } : {}),
      ...(previous?.nextRetryAt !== undefined ? { nextRetryAt: previous.nextRetryAt } : {}),
      transportState: previous?.transportState ?? "idle",
    };
    await this.persistState();
    this.maybeScheduleRenameWorker(rootSessionKey, mapping, {
      force:
        desiredChanged ||
        previous?.desiredName === undefined ||
        previous?.lastObservedName === undefined,
    });
  }

  private sessionBelongsToRoot(params: {
    rootSessionKey: string;
    sessionKey: string;
    store: Record<string, SessionEntry>;
  }): boolean {
    const normalizedRoot = normalizeSessionKey(params.rootSessionKey);
    const normalizedSessionKey = normalizeSessionKey(params.sessionKey);
    if (!normalizedRoot || !normalizedSessionKey) {
      return false;
    }
    const directRoot = extractDiscordChannelRootSessionKey(normalizedSessionKey);
    if (directRoot === normalizedRoot) {
      return true;
    }
    const visited = new Set<string>();
    let current: string | null = normalizedSessionKey;
    while (current && !visited.has(current)) {
      visited.add(current);
      const currentRoot = extractDiscordChannelRootSessionKey(current);
      if (currentRoot === normalizedRoot) {
        return true;
      }
      current = normalizeSessionKey(params.store[current]?.spawnedBy);
    }
    return false;
  }

  private collectTrackedSessionKeys(params: {
    rootSessionKey: string;
    record: PersistedChannelRecord;
    store: Record<string, SessionEntry>;
  }): Set<string> {
    const tracked = new Set<string>();
    tracked.add(params.rootSessionKey);
    for (const run of Object.values(params.record.runs ?? {})) {
      const sessionKey = normalizeSessionKey(run.sessionKey);
      if (sessionKey) {
        tracked.add(sessionKey);
      }
    }
    for (const descendant of listDescendantRunsForRequester(params.rootSessionKey)) {
      const childSessionKey = normalizeSessionKey(descendant.childSessionKey);
      if (childSessionKey) {
        tracked.add(childSessionKey);
      }
    }
    for (const sessionKey of Object.keys(params.store)) {
      const normalizedSessionKey = normalizeSessionKey(sessionKey);
      if (
        normalizedSessionKey &&
        this.sessionBelongsToRoot({
          rootSessionKey: params.rootSessionKey,
          sessionKey: normalizedSessionKey,
          store: params.store,
        })
      ) {
        tracked.add(normalizedSessionKey);
      }
    }
    return tracked;
  }

  private collectActiveTrackedRuns(params: {
    trackedSessionKeys: Set<string>;
    store: Record<string, SessionEntry>;
    activeRuns: ReturnType<typeof getActiveEmbeddedRunsSnapshot>;
  }) {
    const activeSessionKeys = new Set(
      params.activeRuns
        .map((entry) => normalizeSessionKey(entry.sessionKey))
        .filter((entry): entry is string => Boolean(entry)),
    );
    const activeSessionIds = new Set(
      params.activeRuns
        .map((entry) => entry.sessionId.trim())
        .filter((entry) => entry.length > 0),
    );

    const matchingRuns: ReturnType<typeof getActiveEmbeddedRunsSnapshot> = [];
    for (const sessionKey of params.trackedSessionKeys) {
      const sessionId = findSessionEntryByKey(params.store, sessionKey)?.sessionId?.trim();
      for (const entry of params.activeRuns) {
        const normalizedEntrySessionKey = normalizeSessionKey(entry.sessionKey);
        const normalizedSessionId = entry.sessionId.trim();
        if (normalizedEntrySessionKey && activeSessionKeys.has(sessionKey) && normalizedEntrySessionKey === sessionKey) {
          matchingRuns.push(entry);
          continue;
        }
        if (sessionId && activeSessionIds.has(sessionId) && normalizedSessionId === sessionId) {
          matchingRuns.push(entry);
        }
      }
    }
    return [...new Map(matchingRuns.map((entry) => [entry.sessionId, entry])).values()];
  }

  private resolveRunningReason(params: {
    rootSessionKey: string;
    activeTrackedRuns: ReturnType<typeof getActiveEmbeddedRunsSnapshot>;
    pendingDescendantRuns: ReturnType<typeof summarizePendingDescendantRuns>;
    reconcileSuppressedByGrace: boolean;
    runsChanged: boolean;
    nextState: AgentActivityState;
  }): ActivityRunningReason | null {
    if (params.nextState !== "running") {
      return params.runsChanged ? "stale_recovery" : null;
    }
    if (params.reconcileSuppressedByGrace) {
      return "root_lifecycle";
    }
    if (params.activeTrackedRuns.some((entry) => entry.sourceKind === "announce")) {
      return "announce_run";
    }
    if (
      params.activeTrackedRuns.some(
        (entry) =>
          normalizeSessionKey(entry.sessionKey) === params.rootSessionKey || entry.sourceKind === "root",
      )
    ) {
      return "root_lifecycle";
    }
    if (params.activeTrackedRuns.length > 0 || params.pendingDescendantRuns.active > 0) {
      return "active_descendant_run";
    }
    if (params.pendingDescendantRuns.total > 0) {
      return "pending_descendant_cleanup";
    }
    return null;
  }

  private buildReconciledChannelRecord(params: {
    rootSessionKey: string;
    mapping: ChannelMapEntry;
    record: PersistedChannelRecord;
    store: Record<string, SessionEntry>;
    activeRuns: ReturnType<typeof getActiveEmbeddedRunsSnapshot>;
    now: number;
    optimisticRunning?: boolean;
  }): {
    record: PersistedChannelRecord;
    decision: ActivityReconcileDecision;
  } {
    const trackedSessionKeys = this.collectTrackedSessionKeys({
      rootSessionKey: params.rootSessionKey,
      record: params.record,
      store: params.store,
    });
    const activeTrackedRuns = this.collectActiveTrackedRuns({
      trackedSessionKeys,
      store: params.store,
      activeRuns: params.activeRuns,
    });
    const pendingDescendantRuns = summarizePendingDescendantRuns(params.rootSessionKey);

    const runs = { ...(params.record.runs ?? {}) };
    let runsChanged = false;
    const currentSynthesizedState = synthesizeRecordState({
      ...params.record,
      runs,
    });
    const hasRunningRunPastStaleTimeout = Object.values(runs).some((run) => {
      if (run.status !== "running") {
        return false;
      }
      const lastTouchAt = run.updatedAt ?? run.startedAt;
      return params.now - lastTouchAt >= this.state.staleRunningMs;
    });
    const runningSignalAt = Math.max(params.record.lastStartAt ?? 0, params.record.lastEventAt ?? 0);
    const reconcileSuppressedByGrace =
      currentSynthesizedState === "running" &&
      runningSignalAt > 0 &&
      params.now - runningSignalAt < DEFAULT_RUNNING_CONFIRMATION_GRACE_MS &&
      !hasRunningRunPastStaleTimeout;

    if (
      activeTrackedRuns.length === 0 &&
      pendingDescendantRuns.total === 0 &&
      !(params.optimisticRunning && currentSynthesizedState === "running") &&
      !reconcileSuppressedByGrace
    ) {
      for (const [runId, run] of Object.entries(runs)) {
        if (run.status !== "running") {
          continue;
        }
        const lastTouchAt = run.updatedAt ?? run.startedAt;
        const staleByTimeout = params.now - lastTouchAt >= this.state.staleRunningMs;
        const pastConfirmationGrace =
          params.now - lastTouchAt >= DEFAULT_RUNNING_CONFIRMATION_GRACE_MS;
        if (!staleByTimeout && !pastConfirmationGrace) {
          continue;
        }
        runs[runId] = {
          ...run,
          status: "ok",
          updatedAt: params.now,
        };
        runsChanged = true;
      }
    }

    const synthesizedState = synthesizeRecordState({
      ...params.record,
      runs,
    });
    const nextState =
      activeTrackedRuns.length > 0 || pendingDescendantRuns.total > 0 ? "running" : synthesizedState;
    const latestError = collectLatestError(runs);

    const next: PersistedChannelRecord = {
      ...params.record,
      runs,
      state: nextState,
      updatedAt:
        runsChanged || params.record.state !== nextState ? params.now : params.record.updatedAt,
    };
    if (nextState === "running") {
      next.lastBusyAt = params.now;
    } else if (params.record.lastBusyAt !== undefined) {
      next.lastBusyAt = params.record.lastBusyAt;
    }
    if (runsChanged) {
      next.lastEventAt = params.now;
    }
    if (latestError) {
      next.lastError = latestError;
    } else {
      delete next.lastError;
    }
    const visibleDesiredState = isCategoryModeMapping(params.mapping)
      ? this.resolveVisibleStateForCategory({
          now: params.now,
          previous: params.record,
          record: next,
        })
      : next.state;
    const desiredName = params.mapping.baseName
      ? synthesizeChannelName(params.mapping.baseName, visibleDesiredState)
      : next.desiredName ?? "";
    const desiredParentId = isCategoryModeMapping(params.mapping)
      ? synthesizeDesiredParentId(
          params.mapping,
          visibleDesiredState === "running" ? "running" : "idle",
        )
      : undefined;
    return {
      record: next,
      decision: {
        trackedSessionKeys: [...trackedSessionKeys],
        activeTrackedRuns: activeTrackedRuns.map((entry) => ({
          ...(entry.runId ? { runId: entry.runId } : {}),
          sessionId: entry.sessionId,
          ...(entry.sessionKey ? { sessionKey: entry.sessionKey } : {}),
          sourceKind: entry.sourceKind,
          startedAt: entry.startedAt,
        })),
        pendingDescendantRuns: pendingDescendantRuns.total,
        stateBefore: params.record.state,
        stateAfter: next.state,
        desiredState: visibleDesiredState,
        desiredName,
        ...(desiredParentId ? { desiredParentId } : {}),
        lastObservedName: next.lastObservedName ?? next.lastTargetName,
        ...(next.lastObservedParentId ? { lastObservedParentId: next.lastObservedParentId } : {}),
        reconcileSuppressedByGrace,
        reason: this.resolveRunningReason({
          rootSessionKey: params.rootSessionKey,
          activeTrackedRuns,
          pendingDescendantRuns,
          reconcileSuppressedByGrace,
          runsChanged,
          nextState,
        }),
      },
    };
  }

  private async reconcileChannels() {
    const config = await this.readMap();
    this.state.staleRunningMs = config.staleRunningMs ?? DEFAULT_STALE_RUNNING_MS;
    const now = this.now();
    const store = this.loadSessionStore();
    const activeRuns = getActiveEmbeddedRunsSnapshot();
    await this.reconcileRootChannels({
      config,
      roots: Object.keys(config.channels ?? {}),
      store,
      activeRuns,
      now,
      trigger: "interval",
    });
    await this.persistState();
  }

  private async recoverStaleRunningChannels() {
    const config = await this.readMap();
    this.state.staleRunningMs = config.staleRunningMs ?? DEFAULT_STALE_RUNNING_MS;
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
        if (now - lastTouchAt < this.state.staleRunningMs) {
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
  findSessionEntryByKey,
};
