import fs from "node:fs/promises";
import path from "node:path";
import type { AgentEventPayload } from "../../infra/agent-events.js";
import { onAgentEvent } from "../../infra/agent-events.js";
import { createSubsystemLogger } from "../../logging/subsystem.js";
import { editChannelDiscord } from "../send.js";

const log = createSubsystemLogger("discord-agent-activity");

export type AgentActivityState = "idle" | "running" | "error";

type AgentChannelMapEntry = {
  channelId: string;
  baseName: string;
  accountId?: string;
};

type AgentChannelMap = {
  version?: number;
  staleRunningMs?: number;
  agents?: Record<string, AgentChannelMapEntry>;
};

type PersistedAgentRecord = {
  state: AgentActivityState;
  activeRunId?: string;
  updatedAt: number;
  lastEventAt?: number;
  lastStartAt?: number;
  lastTargetName?: string;
  lastAppliedAt?: number;
  lastError?: string;
};

type PersistedAgentActivityState = {
  version: 1;
  staleRunningMs: number;
  agents: Record<string, PersistedAgentRecord>;
};

type DiscordChannelRenameFn = (params: {
  channelId: string;
  name: string;
  accountId?: string;
}) => Promise<void>;

type AgentChannelActivityMonitorOptions = {
  workspaceDir: string;
  renameChannel?: DiscordChannelRenameFn;
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

function normalizeMap(raw: unknown): AgentChannelMap {
  if (!isObject(raw)) {
    return {};
  }
  const agentsRaw = isObject(raw.agents) ? raw.agents : {};
  const agents: Record<string, AgentChannelMapEntry> = {};
  for (const [agentId, value] of Object.entries(agentsRaw)) {
    if (!isObject(value)) {
      continue;
    }
    const channelId = typeof value.channelId === "string" ? value.channelId.trim() : "";
    const baseName = typeof value.baseName === "string" ? value.baseName.trim() : "";
    const accountId = typeof value.accountId === "string" && value.accountId.trim() ? value.accountId.trim() : undefined;
    if (!channelId || !baseName) {
      continue;
    }
    agents[agentId] = { channelId, baseName, ...(accountId ? { accountId } : {}) };
  }
  const staleRunningMs =
    typeof raw.staleRunningMs === "number" && Number.isFinite(raw.staleRunningMs) && raw.staleRunningMs > 0
      ? Math.floor(raw.staleRunningMs)
      : undefined;
  return {
    version: typeof raw.version === "number" ? raw.version : undefined,
    ...(staleRunningMs ? { staleRunningMs } : {}),
    agents,
  };
}

function normalizeState(raw: unknown): PersistedAgentActivityState {
  if (!isObject(raw) || !isObject(raw.agents)) {
    return { version: 1, staleRunningMs: DEFAULT_STALE_RUNNING_MS, agents: {} };
  }
  const agents: Record<string, PersistedAgentRecord> = {};
  for (const [agentId, value] of Object.entries(raw.agents)) {
    if (!isObject(value)) {
      continue;
    }
    const state = value.state;
    if (state !== "idle" && state !== "running" && state !== "error") {
      continue;
    }
    const updatedAt = typeof value.updatedAt === "number" ? value.updatedAt : Date.now();
    agents[agentId] = {
      state,
      updatedAt,
      ...(typeof value.activeRunId === "string" && value.activeRunId ? { activeRunId: value.activeRunId } : {}),
      ...(typeof value.lastEventAt === "number" ? { lastEventAt: value.lastEventAt } : {}),
      ...(typeof value.lastStartAt === "number" ? { lastStartAt: value.lastStartAt } : {}),
      ...(typeof value.lastTargetName === "string" && value.lastTargetName ? { lastTargetName: value.lastTargetName } : {}),
      ...(typeof value.lastAppliedAt === "number" ? { lastAppliedAt: value.lastAppliedAt } : {}),
      ...(typeof value.lastError === "string" && value.lastError ? { lastError: value.lastError } : {}),
    };
  }
  return {
    version: 1,
    staleRunningMs:
      typeof raw.staleRunningMs === "number" && Number.isFinite(raw.staleRunningMs) && raw.staleRunningMs > 0
        ? Math.floor(raw.staleRunningMs)
        : DEFAULT_STALE_RUNNING_MS,
    agents,
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

export class AgentChannelActivityMonitor {
  private readonly workspaceDir: string;
  private readonly configPath: string;
  private readonly statePath: string;
  private readonly renameChannel: DiscordChannelRenameFn;
  private readonly now: () => number;
  private readonly staleSweepIntervalMs: number;
  private unsub: (() => void) | null = null;
  private sweepTimer: ReturnType<typeof setInterval> | null = null;
  private state: PersistedAgentActivityState = {
    version: 1,
    staleRunningMs: DEFAULT_STALE_RUNNING_MS,
    agents: {},
  };
  private queue: Promise<void> = Promise.resolve();

  constructor(options: AgentChannelActivityMonitorOptions) {
    this.workspaceDir = options.workspaceDir;
    this.configPath = path.join(options.workspaceDir, CONFIG_RELATIVE_PATH);
    this.statePath = path.join(options.workspaceDir, STATE_RELATIVE_PATH);
    this.renameChannel =
      options.renameChannel ??
      (async ({ channelId, name, accountId }) => {
        await editChannelDiscord({ channelId, name }, accountId ? { accountId } : undefined);
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
      void this.enqueue(() => this.recoverStaleRunningAgents());
    }, this.staleSweepIntervalMs);
    if (typeof this.sweepTimer.unref === "function") {
      this.sweepTimer.unref();
    }
    await this.enqueue(() => this.recoverStaleRunningAgents());
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

  private getAgentId(evt: AgentEventPayload): string | null {
    const candidate = evt.sessionKey?.match(/^agent:([^:]+)/)?.[1]?.trim();
    return candidate || null;
  }

  private async handleEvent(evt: AgentEventPayload) {
    if (evt.stream !== "lifecycle") {
      return;
    }
    const phase = typeof evt.data?.phase === "string" ? evt.data.phase : "";
    if (phase !== "start" && phase !== "end" && phase !== "error") {
      return;
    }
    const agentId = this.getAgentId(evt);
    if (!agentId) {
      return;
    }
    const config = await this.readMap();
    const mapping = config.agents?.[agentId];
    if (!mapping) {
      return;
    }
    this.state.staleRunningMs = config.staleRunningMs ?? DEFAULT_STALE_RUNNING_MS;
    const now = this.now();
    const current = this.state.agents[agentId] ?? { state: "idle", updatedAt: now };

    if (phase === "start") {
      const next: PersistedAgentRecord = {
        ...current,
        state: "running",
        activeRunId: evt.runId,
        updatedAt: now,
        lastEventAt: now,
        lastStartAt: now,
      };
      delete next.lastError;
      await this.applyAgentState(agentId, mapping, next);
      return;
    }

    if (current.activeRunId !== evt.runId) {
      return;
    }

    const next: PersistedAgentRecord = {
      ...current,
      state: phase === "error" ? "error" : "idle",
      updatedAt: now,
      lastEventAt: now,
      ...(phase === "error" && typeof evt.data?.error === "string" && evt.data.error.trim()
        ? { lastError: evt.data.error.trim() }
        : {}),
    };
    delete next.activeRunId;
    await this.applyAgentState(agentId, mapping, next);
  }

  private async applyAgentState(
    agentId: string,
    mapping: AgentChannelMapEntry,
    next: PersistedAgentRecord,
  ) {
    const targetName = synthesizeChannelName(mapping.baseName, next.state);
    const previous = this.state.agents[agentId];
    const nameChanged = previous?.lastTargetName !== targetName;

    this.state.agents[agentId] = {
      ...next,
      lastTargetName: targetName,
      ...(nameChanged ? { lastAppliedAt: this.now() } : { lastAppliedAt: previous?.lastAppliedAt }),
    };
    await this.persistState();

    if (!nameChanged) {
      return;
    }

    try {
      await this.renameChannel({
        channelId: mapping.channelId,
        name: targetName,
        ...(mapping.accountId ? { accountId: mapping.accountId } : {}),
      });
    } catch (err) {
      log.warn(`failed to rename Discord channel for agent ${agentId}: ${String(err)}`);
    }
  }

  private async recoverStaleRunningAgents() {
    const config = await this.readMap();
    this.state.staleRunningMs = config.staleRunningMs ?? DEFAULT_STALE_RUNNING_MS;
    const cutoffMs = this.state.staleRunningMs;
    const now = this.now();
    let changed = false;

    for (const [agentId, record] of Object.entries(this.state.agents)) {
      if (record.state !== "running") {
        continue;
      }
      const mapping = config.agents?.[agentId];
      if (!mapping) {
        continue;
      }
      const lastStartAt = record.lastStartAt ?? record.lastEventAt ?? record.updatedAt;
      if (now - lastStartAt < cutoffMs) {
        continue;
      }
      const recovered: PersistedAgentRecord = {
        ...record,
        state: "idle",
        updatedAt: now,
        lastEventAt: now,
      };
      delete recovered.activeRunId;
      await this.applyAgentState(agentId, mapping, recovered);
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
};
