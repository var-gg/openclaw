import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import {
  clearActiveEmbeddedRun,
  resetEmbeddedPiRunsForTests,
  setActiveEmbeddedRun,
  type EmbeddedPiQueueHandle,
} from "../../agents/pi-embedded-runner/runs.js";
import { resetActivityObserverForTests } from "../../agents/activity-observer.js";
import {
  addSubagentRunForTests,
  releaseSubagentRun,
  resetSubagentRegistryForTests,
} from "../../agents/subagent-registry.js";
import type { OpenClawConfig } from "../../config/config.js";
import {
  emitAgentEvent,
  registerAgentRunContext,
  resetAgentRunContextForTest,
} from "../../infra/agent-events.js";
import { DiscordApiError } from "../api.js";
import { AgentChannelActivityMonitor, __test__ } from "./agent-channel-activity.js";

async function makeWorkspace() {
  return await fs.mkdtemp(path.join(os.tmpdir(), "openclaw-agent-channel-activity-"));
}

async function writeMap(workspaceDir: string, value: Record<string, unknown>) {
  const filePath = path.join(workspaceDir, "ops", "config", "agent-channel-map.json");
  await fs.mkdir(path.dirname(filePath), { recursive: true });
  await fs.writeFile(filePath, `${JSON.stringify(value, null, 2)}\n`, "utf8");
}

async function readState(workspaceDir: string) {
  const filePath = path.join(workspaceDir, "ops", "state", "agent-activity.json");
  return JSON.parse(await fs.readFile(filePath, "utf8")) as {
    version: number;
    staleRunningMs: number;
    channels: Record<string, Record<string, unknown>>;
  };
}

async function writeSessionStore(workspaceDir: string, store: Record<string, unknown>) {
  const filePath = path.join(workspaceDir, ".openclaw-state", "agents", "main", "sessions", "sessions.json");
  await fs.mkdir(path.dirname(filePath), { recursive: true });
  await fs.writeFile(filePath, `${JSON.stringify(store, null, 2)}\n`, "utf8");
}

function makeCfg(workspaceDir: string): OpenClawConfig {
  return {
    session: {
      store: path.join(workspaceDir, ".openclaw-state", "agents", "{agentId}", "sessions", "sessions.json"),
    },
    agents: {
      main: {},
    },
  } as OpenClawConfig;
}

function createChannelMocks(initialName: string) {
  let currentName = initialName;
  const renameChannel = vi.fn(async ({ name }: { channelId: string; name: string; accountId?: string }) => {
    currentName = name;
  });
  const fetchChannel = vi.fn(async () => ({ name: currentName }));
  return {
    renameChannel,
    fetchChannel,
  };
}

function createCategoryChannelMocks(params: {
  channelId?: string;
  guildId?: string;
  initialName?: string;
  initialParentId: string;
  position?: number;
  moveImpl?: (ctx: {
    moves: Array<{ channelId: string; parentId: string; position?: number }>;
    setParentId: (parentId: string) => void;
    setPosition: (position: number) => void;
  }) => Promise<{ durationMs?: number; httpStatus?: number; retryAfterMs?: number } | void>;
}) {
  const channelId = params.channelId ?? "123";
  const guildId = params.guildId ?? "guild-1";
  const name = params.initialName ?? "main";
  let currentParentId = params.initialParentId;
  let currentPosition = params.position ?? 0;
  const fetchChannel = vi.fn(async () => ({
    guildId,
    name,
    parentId: currentParentId,
    position: currentPosition,
  }));
  const moveChannels = vi.fn(
    async ({
      moves,
    }: {
      accountId?: string;
      guildId: string;
      moves: Array<{ channelId: string; parentId: string; position?: number }>;
    }) => {
      if (params.moveImpl) {
        return await params.moveImpl({
          moves,
          setParentId: (parentId) => {
            currentParentId = parentId;
          },
          setPosition: (position) => {
            currentPosition = position;
          },
        });
      }
      for (const move of moves) {
        if (move.channelId !== channelId) {
          continue;
        }
        currentParentId = move.parentId;
        if (move.position !== undefined) {
          currentPosition = move.position;
        }
      }
      return { httpStatus: 200 };
    },
  );
  return {
    fetchChannel,
    getParentId: () => currentParentId,
    moveChannels,
    setParentId: (parentId: string) => {
      currentParentId = parentId;
    },
  };
}

function makeEmbeddedHandle(): EmbeddedPiQueueHandle {
  return {
    queueMessage: vi.fn(async () => {}),
    isStreaming: () => true,
    isCompacting: () => false,
    abort: vi.fn(),
  };
}

async function flushMonitorQueue(monitor?: AgentChannelActivityMonitor) {
  await Promise.resolve();
  await Promise.resolve();
  if (monitor) {
    const internals = monitor as unknown as {
      categoryWorkerPromise?: Map<string, Promise<void>>;
      queue: Promise<void>;
      renameWorkerPromise?: Map<string, Promise<void>>;
    };
    await internals.queue.catch(() => {});
    const workerPromises = [
      ...(internals.renameWorkerPromise ? [...internals.renameWorkerPromise.values()] : []),
      ...(internals.categoryWorkerPromise ? [...internals.categoryWorkerPromise.values()] : []),
    ];
    if (workerPromises.length > 0) {
      await Promise.allSettled(workerPromises);
      await internals.queue.catch(() => {});
    }
  }
}

describe("agent channel activity monitor", () => {
  beforeEach(() => {
    resetAgentRunContextForTest();
    resetActivityObserverForTests();
    resetEmbeddedPiRunsForTests();
    resetSubagentRegistryForTests({ persist: false });
  });

  afterEach(() => {
    vi.useRealTimers();
    resetAgentRunContextForTest();
    resetActivityObserverForTests();
    resetEmbeddedPiRunsForTests();
    resetSubagentRegistryForTests({ persist: false });
  });

  it("synthesizes canonical emoji-prefixed channel names", () => {
    expect(__test__.synthesizeChannelName("main", "idle")).toBe("🟢-main");
    expect(__test__.synthesizeChannelName("main", "running")).toBe("⚙️-main");
    expect(__test__.synthesizeChannelName("main", "error")).toBe("🔴-main");
  });

  it("detects discord channel root keys from channel and thread sessions", () => {
    expect(__test__.extractDiscordChannelRootSessionKey("agent:main:discord:channel:123")).toBe(
      "agent:main:discord:channel:123",
    );
    expect(
      __test__.extractDiscordChannelRootSessionKey("agent:main:discord:channel:123:thread:999"),
    ).toBe("agent:main:discord:channel:123");
  });

  it("tracks mapped root channel sessions directly and only renames on target change", async () => {
    const workspaceDir = await makeWorkspace();
    await writeMap(workspaceDir, {
      version: 2,
      channels: {
        "agent:main:discord:channel:123": {
          channelId: "123",
          baseName: "vargg-growth",
        },
      },
    });

    let now = 1_000;
    const { renameChannel, fetchChannel } = createChannelMocks("🟢-vargg-growth");
    const monitor = new AgentChannelActivityMonitor({
      workspaceDir,
      renameChannel,
      fetchChannel,
      now: () => now,
      staleSweepIntervalMs: 60_000,
    });
    await monitor.start();

    emitAgentEvent({
      runId: "run-1",
      stream: "lifecycle",
      sessionKey: "agent:main:discord:channel:123",
      data: { phase: "start" },
    });
    emitAgentEvent({
      runId: "run-1",
      stream: "lifecycle",
      sessionKey: "agent:main:discord:channel:123",
      data: { phase: "start" },
    });
    emitAgentEvent({
      runId: "run-1",
      stream: "lifecycle",
      sessionKey: "agent:main:discord:channel:123",
      data: { phase: "end" },
    });
    await monitor.stop();

    expect(renameChannel).toHaveBeenCalledTimes(2);
    expect(renameChannel.mock.calls.at(0)?.[0]).toEqual({ channelId: "123", name: "⚙️-vargg-growth" });
    expect(renameChannel.mock.calls.at(1)?.[0]).toEqual({ channelId: "123", name: "🟢-vargg-growth" });

    const state = await readState(workspaceDir);
    expect(state.channels["agent:main:discord:channel:123"]?.state).toBe("idle");
  });

  it("tracks direct Discord root runs from run context when lifecycle events are hidden from Control UI", async () => {
    const workspaceDir = await makeWorkspace();
    await writeMap(workspaceDir, {
      version: 2,
      channels: {
        "agent:main:discord:channel:123": {
          channelId: "123",
          baseName: "ops-room",
        },
      },
    });

    let now = 2_000;
    const { renameChannel, fetchChannel } = createChannelMocks("🟢-ops-room");
    const monitor = new AgentChannelActivityMonitor({
      workspaceDir,
      renameChannel,
      fetchChannel,
      now: () => now,
      staleSweepIntervalMs: 60_000,
    });
    await monitor.start();

    registerAgentRunContext("run-discord-direct", {
      sessionKey: "agent:main:discord:channel:123",
      isControlUiVisible: false,
    });

    emitAgentEvent({
      runId: "run-discord-direct",
      stream: "lifecycle",
      data: { phase: "start" },
    });
    now += 10;
    emitAgentEvent({
      runId: "run-discord-direct",
      stream: "lifecycle",
      data: { phase: "error", error: "boom" },
    });
    await monitor.stop();

    expect(renameChannel).toHaveBeenCalledTimes(2);
    expect(renameChannel.mock.calls.at(0)?.[0]).toEqual({ channelId: "123", name: "⚙️-ops-room" });
    expect(renameChannel.mock.calls.at(1)?.[0]).toEqual({ channelId: "123", name: "🔴-ops-room" });

    const state = await readState(workspaceDir);
    expect(state.channels["agent:main:discord:channel:123"]?.state).toBe("error");
  });

  it("recovers after a stalled rename so later runs still project state", async () => {
    const workspaceDir = await makeWorkspace();
    await writeMap(workspaceDir, {
      version: 2,
      channels: {
        "agent:main:discord:channel:123": {
          channelId: "123",
          baseName: "main",
        },
      },
    });

    let currentName = "🟢-main";
    let renameCall = 0;
    const renameChannel = vi.fn(async ({ name }: { channelId: string; name: string; accountId?: string }) => {
      renameCall += 1;
      if (renameCall === 1) {
        await new Promise<void>(() => {});
      }
      currentName = name;
    });
    const fetchChannel = vi.fn(async () => ({ name: currentName }));
    const monitor = new AgentChannelActivityMonitor({
      workspaceDir,
      renameChannel,
      fetchChannel,
      channelOpTimeoutMs: 20,
      staleSweepIntervalMs: 60_000,
    });
    await monitor.start();

    emitAgentEvent({
      runId: "run-1",
      stream: "lifecycle",
      sessionKey: "agent:main:discord:channel:123",
      data: { phase: "start" },
    });
    await new Promise((resolve) => setTimeout(resolve, 40));
    await flushMonitorQueue(monitor);

    emitAgentEvent({
      runId: "run-1",
      stream: "lifecycle",
      sessionKey: "agent:main:discord:channel:123",
      data: { phase: "end" },
    });
    await flushMonitorQueue(monitor);

    emitAgentEvent({
      runId: "run-2",
      stream: "lifecycle",
      sessionKey: "agent:main:discord:channel:123",
      data: { phase: "start" },
    });
    await new Promise((resolve) => setTimeout(resolve, 40));
    await flushMonitorQueue(monitor);

    emitAgentEvent({
      runId: "run-2",
      stream: "lifecycle",
      sessionKey: "agent:main:discord:channel:123",
      data: { phase: "end" },
    });
    await flushMonitorQueue(monitor);
    await monitor.stop();

    expect(renameChannel).toHaveBeenCalledTimes(3);
    expect(renameChannel.mock.calls.at(0)?.[0]).toEqual({ channelId: "123", name: "⚙️-main" });
    expect(renameChannel.mock.calls.at(1)?.[0]).toEqual({ channelId: "123", name: "⚙️-main" });
    expect(renameChannel.mock.calls.at(2)?.[0]).toEqual({ channelId: "123", name: "🟢-main" });

    const state = await readState(workspaceDir);
    expect(state.channels["agent:main:discord:channel:123"]?.state).toBe("idle");
  });

  it("keeps parent channel running until spawned child work completes and surfaces child errors", async () => {
    const workspaceDir = await makeWorkspace();
    await writeMap(workspaceDir, {
      version: 2,
      channels: {
        "agent:main:discord:channel:123": {
          channelId: "123",
          baseName: "chief-of-staff",
        },
      },
    });
    await writeSessionStore(workspaceDir, {
      "agent:main:discord:channel:123": {
        sessionId: "root",
        updatedAt: 1,
      },
      "agent:main:subagent:child": {
        sessionId: "child",
        updatedAt: 2,
        spawnedBy: "agent:main:discord:channel:123",
      },
    });

    let now = 5_000;
    const { renameChannel, fetchChannel } = createChannelMocks("🟢-chief-of-staff");
    const monitor = new AgentChannelActivityMonitor({
      workspaceDir,
      cfg: makeCfg(workspaceDir),
      renameChannel,
      fetchChannel,
      now: () => now,
      staleSweepIntervalMs: 60_000,
    });
    await monitor.start();

    emitAgentEvent({
      runId: "root-run",
      stream: "lifecycle",
      sessionKey: "agent:main:discord:channel:123",
      data: { phase: "start" },
    });
    now += 10;
    emitAgentEvent({
      runId: "child-run",
      stream: "lifecycle",
      sessionKey: "agent:main:subagent:child",
      data: { phase: "start" },
    });
    now += 10;
    emitAgentEvent({
      runId: "root-run",
      stream: "lifecycle",
      sessionKey: "agent:main:discord:channel:123",
      data: { phase: "end" },
    });
    now += 10;
    emitAgentEvent({
      runId: "child-run",
      stream: "lifecycle",
      sessionKey: "agent:main:subagent:child",
      data: { phase: "error", error: "boom" },
    });
    await monitor.stop();

    expect(renameChannel).toHaveBeenCalledTimes(2);
    expect(renameChannel.mock.calls.at(0)?.[0]).toEqual({ channelId: "123", name: "⚙️-chief-of-staff" });
    expect(renameChannel.mock.calls.at(1)?.[0]).toEqual({ channelId: "123", name: "🔴-chief-of-staff" });

    const state = await readState(workspaceDir);
    expect(state.channels["agent:main:discord:channel:123"]?.state).toBe("error");
  });

  it("ignores orphan child completions whose run never started under a tracked tree", async () => {
    const workspaceDir = await makeWorkspace();
    await writeMap(workspaceDir, {
      version: 2,
      channels: {
        "agent:main:discord:channel:123": {
          channelId: "123",
          baseName: "vargg-growth",
        },
      },
    });
    await writeSessionStore(workspaceDir, {
      "agent:main:subagent:child": {
        sessionId: "child",
        updatedAt: 2,
        spawnedBy: "agent:main:discord:channel:123",
      },
    });

    const renameChannel = vi.fn(async (_params: { channelId: string; name: string; accountId?: string }) => {});
    const fetchChannel = vi.fn(async () => ({ name: "🟢-vargg-growth" }));
    const monitor = new AgentChannelActivityMonitor({
      workspaceDir,
      cfg: makeCfg(workspaceDir),
      renameChannel,
      fetchChannel,
      now: () => Date.now(),
      staleSweepIntervalMs: 60_000,
    });
    await monitor.start();

    emitAgentEvent({
      runId: "child-run",
      stream: "lifecycle",
      sessionKey: "agent:main:subagent:child",
      data: { phase: "end" },
    });
    await monitor.stop();

    expect(renameChannel).toHaveBeenCalledTimes(0);
  });

  it("falls back to the only running tracked run for orphan completions", async () => {
    const workspaceDir = await makeWorkspace();
    await writeMap(workspaceDir, {
      version: 2,
      channels: {
        "agent:main:discord:channel:123": {
          channelId: "123",
          baseName: "main",
        },
      },
    });
    await writeSessionStore(workspaceDir, {
      "agent:main:subagent:child": {
        sessionId: "child",
        updatedAt: 2,
        spawnedBy: "agent:main:discord:channel:123",
      },
    });

    const { renameChannel, fetchChannel } = createChannelMocks("🟢-main");
    const monitor = new AgentChannelActivityMonitor({
      workspaceDir,
      cfg: makeCfg(workspaceDir),
      renameChannel,
      fetchChannel,
      now: () => Date.now(),
      staleSweepIntervalMs: 60_000,
    });
    await monitor.start();

    emitAgentEvent({
      runId: "root-run",
      stream: "lifecycle",
      sessionKey: "agent:main:discord:channel:123",
      data: { phase: "start" },
    });
    emitAgentEvent({
      runId: "orphan-end-run",
      stream: "lifecycle",
      sessionKey: "agent:main:subagent:child",
      data: { phase: "end" },
    });
    await monitor.stop();

    expect(renameChannel).toHaveBeenCalledTimes(2);
    expect(renameChannel.mock.calls.at(0)?.[0]).toEqual({ channelId: "123", name: "⚙️-main" });
    expect(renameChannel.mock.calls.at(1)?.[0]).toEqual({ channelId: "123", name: "🟢-main" });

    const state = await readState(workspaceDir);
    expect(state.channels["agent:main:discord:channel:123"]?.state).toBe("idle");
  });

  it("recovers stale running channels back to idle conservatively", async () => {
    const workspaceDir = await makeWorkspace();
    await writeMap(workspaceDir, {
      staleRunningMs: 1_000,
      channels: {
        "agent:main:discord:channel:123": {
          channelId: "123",
          baseName: "main",
        },
      },
    });

    let now = 10_000;
    const { renameChannel, fetchChannel } = createChannelMocks("🟢-main");
    const monitor = new AgentChannelActivityMonitor({
      workspaceDir,
      renameChannel,
      fetchChannel,
      now: () => now,
      staleSweepIntervalMs: 60_000,
    });
    await monitor.start();

    emitAgentEvent({
      runId: "run-1",
      stream: "lifecycle",
      sessionKey: "agent:main:discord:channel:123",
      data: { phase: "start" },
    });
    await monitor.stop();

    now += 1_500;
    const recoveryMonitor = new AgentChannelActivityMonitor({
      workspaceDir,
      renameChannel,
      fetchChannel,
      now: () => now,
      staleSweepIntervalMs: 60_000,
    });
    await recoveryMonitor.start();
    await recoveryMonitor.stop();

    expect(renameChannel).toHaveBeenCalledTimes(2);
    expect(renameChannel.mock.calls.at(0)?.[0]).toEqual({ channelId: "123", name: "⚙️-main" });
    expect(renameChannel.mock.calls.at(1)?.[0]).toEqual({ channelId: "123", name: "🟢-main" });

    const state = await readState(workspaceDir);
    expect(state.channels["agent:main:discord:channel:123"]?.state).toBe("idle");
  });

  it("accepts legacy agent keyed config and maps it to channel roots", () => {
    const normalized = __test__.normalizeMap({
      version: 1,
      agents: {
        main: {
          channelId: "123",
          baseName: "legacy-main",
        },
      },
    });
    expect(normalized.channels).toEqual({
      "agent:main:discord:channel:123": {
        channelId: "123",
        baseName: "legacy-main",
      },
    });
  });

  it("falls back to a uniquely mapped channelId when the event sessionKey agent id changed", async () => {
    const workspaceDir = await makeWorkspace();
    await writeMap(workspaceDir, {
      version: 2,
      channels: {
        "agent:main:discord:channel:123": {
          channelId: "123",
          baseName: "korean-tax-agent",
        },
      },
    });

    const { renameChannel, fetchChannel } = createChannelMocks("🟢-korean-tax-agent");
    const monitor = new AgentChannelActivityMonitor({
      workspaceDir,
      renameChannel,
      fetchChannel,
      now: () => Date.now(),
      staleSweepIntervalMs: 60_000,
    });
    await monitor.start();

    emitAgentEvent({
      runId: "run-bound-agent",
      stream: "lifecycle",
      sessionKey: "agent:agent_chief_of_staff:discord:channel:123",
      data: { phase: "start" },
    });
    emitAgentEvent({
      runId: "run-bound-agent",
      stream: "lifecycle",
      sessionKey: "agent:agent_chief_of_staff:discord:channel:123",
      data: { phase: "end" },
    });
    await monitor.stop();

    expect(renameChannel).toHaveBeenCalledTimes(2);
    expect(renameChannel.mock.calls.at(0)?.[0]).toEqual({ channelId: "123", name: "⚙️-korean-tax-agent" });
    expect(renameChannel.mock.calls.at(1)?.[0]).toEqual({ channelId: "123", name: "🟢-korean-tax-agent" });
  });

  it("reconciles drift when persisted state says running but the actual channel is idle", async () => {
    const workspaceDir = await makeWorkspace();
    await writeMap(workspaceDir, {
      version: 2,
      channels: {
        "agent:main:discord:channel:123": {
          channelId: "123",
          baseName: "main",
        },
      },
    });

    const statePath = path.join(workspaceDir, "ops", "state", "agent-activity.json");
    await fs.mkdir(path.dirname(statePath), { recursive: true });
    await fs.writeFile(
      statePath,
      `${JSON.stringify({
        version: 2,
        staleRunningMs: 21600000,
        channels: {
          "agent:main:discord:channel:123": {
            state: "running",
            updatedAt: 1000,
            runs: {
              "run-1": {
                sessionKey: "agent:main:discord:channel:123",
                status: "running",
                startedAt: 1000,
                updatedAt: 1000,
              },
            },
            lastTargetName: "⚙️-main",
            lastAppliedAt: 1000,
          },
        },
      }, null, 2)}\n`,
      "utf8",
    );

    const { renameChannel, fetchChannel } = createChannelMocks("🟢-main");
    const monitor = new AgentChannelActivityMonitor({
      workspaceDir,
      renameChannel,
      fetchChannel,
      now: () => 2000,
      staleSweepIntervalMs: 60_000,
    });
    await monitor.start();

    emitAgentEvent({
      runId: "run-1",
      stream: "lifecycle",
      sessionKey: "agent:main:discord:channel:123",
      data: { phase: "start" },
    });
    await monitor.stop();

    expect(renameChannel).toHaveBeenCalledTimes(1);
    expect(renameChannel.mock.calls.at(0)?.[0]).toEqual({ channelId: "123", name: "⚙️-main" });
  });

  it("clears older root running runs when a new root run starts", async () => {
    const workspaceDir = await makeWorkspace();
    await writeMap(workspaceDir, {
      version: 2,
      channels: {
        "agent:main:discord:channel:123": {
          channelId: "123",
          baseName: "main",
        },
      },
    });

    const statePath = path.join(workspaceDir, "ops", "state", "agent-activity.json");
    await fs.mkdir(path.dirname(statePath), { recursive: true });
    await fs.writeFile(
      statePath,
      `${JSON.stringify({
        version: 2,
        staleRunningMs: 21600000,
        channels: {
          "agent:main:discord:channel:123": {
            state: "running",
            updatedAt: 1000,
            runs: {
              "zombie-1": {
                sessionKey: "agent:main:discord:channel:123",
                status: "running",
                startedAt: 1000,
                updatedAt: 1000,
              },
              "zombie-2": {
                sessionKey: "agent:main:discord:channel:123",
                status: "running",
                startedAt: 1100,
                updatedAt: 1100,
              },
            },
            lastTargetName: "⚙️-main",
            lastAppliedAt: 1000,
          },
        },
      }, null, 2)}\n`,
      "utf8",
    );

    const { renameChannel, fetchChannel } = createChannelMocks("⚙️-main");
    let now = 2000;
    const monitor = new AgentChannelActivityMonitor({
      workspaceDir,
      renameChannel,
      fetchChannel,
      now: () => now,
      staleSweepIntervalMs: 60_000,
    });
    await monitor.start();

    emitAgentEvent({
      runId: "fresh-run",
      stream: "lifecycle",
      sessionKey: "agent:main:discord:channel:123",
      data: { phase: "start" },
    });
    now = 3000;
    emitAgentEvent({
      runId: "fresh-run",
      stream: "lifecycle",
      sessionKey: "agent:main:discord:channel:123",
      data: { phase: "end" },
    });
    await monitor.stop();

    const state = JSON.parse(await fs.readFile(statePath, "utf8")) as {
      channels: Record<string, { state: string; runs: Record<string, { status: string }> }>;
    };
    expect(state.channels["agent:main:discord:channel:123"]?.state).toBe("idle");
    expect(Object.keys(state.channels["agent:main:discord:channel:123"]?.runs ?? {})).toEqual(["fresh-run"]);
    expect(state.channels["agent:main:discord:channel:123"]?.runs?.["fresh-run"]?.status).toBe("ok");
    expect(renameChannel.mock.calls.map((call) => call[0])).toEqual([{ channelId: "123", name: "🟢-main" }]);
  });

  it("reconciles a missed lifecycle end back to idle once runtime activity disappears", async () => {
    vi.useFakeTimers();
    const workspaceDir = await makeWorkspace();
    await writeMap(workspaceDir, {
      version: 2,
      channels: {
        "agent:main:discord:channel:123": {
          channelId: "123",
          baseName: "main",
        },
      },
    });

    const { renameChannel, fetchChannel } = createChannelMocks("🟢-main");
    const monitor = new AgentChannelActivityMonitor({
      workspaceDir,
      renameChannel,
      fetchChannel,
      reconcileIntervalMs: 5_000,
      now: () => Date.now(),
    });
    await monitor.start();

    const handle = makeEmbeddedHandle();
    setActiveEmbeddedRun("sess-root", handle, "agent:main:discord:channel:123");
    emitAgentEvent({
      runId: "run-1",
      stream: "lifecycle",
      sessionKey: "agent:main:discord:channel:123",
      data: { phase: "start" },
    });
    await flushMonitorQueue(monitor);

    clearActiveEmbeddedRun("sess-root", handle, "agent:main:discord:channel:123");
    await vi.advanceTimersByTimeAsync(10_000);
    await flushMonitorQueue(monitor);
    await monitor.stop();

    expect(renameChannel.mock.calls.map((call) => call[0])).toEqual([
      { channelId: "123", name: "⚙️-main" },
      { channelId: "123", name: "🟢-main" },
    ]);
  });

  it("keeps the channel running during quick overlap between finishing root work and active child work", async () => {
    const workspaceDir = await makeWorkspace();
    await writeMap(workspaceDir, {
      version: 2,
      channels: {
        "agent:main:discord:channel:123": {
          channelId: "123",
          baseName: "chief-of-staff",
        },
      },
    });
    await writeSessionStore(workspaceDir, {
      "agent:main:discord:channel:123": {
        sessionId: "root",
        updatedAt: 1,
      },
      "agent:main:subagent:child": {
        sessionId: "child",
        updatedAt: 2,
        spawnedBy: "agent:main:discord:channel:123",
      },
    });

    const { renameChannel, fetchChannel } = createChannelMocks("🟢-chief-of-staff");
    const monitor = new AgentChannelActivityMonitor({
      workspaceDir,
      cfg: makeCfg(workspaceDir),
      renameChannel,
      fetchChannel,
      now: () => Date.now(),
      reconcileIntervalMs: 5_000,
    });
    await monitor.start();

    const rootHandle = makeEmbeddedHandle();
    setActiveEmbeddedRun("sess-root", rootHandle, "agent:main:discord:channel:123");
    emitAgentEvent({
      runId: "root-run",
      stream: "lifecycle",
      sessionKey: "agent:main:discord:channel:123",
      data: { phase: "start" },
    });

    const childHandle = makeEmbeddedHandle();
    setActiveEmbeddedRun("sess-child", childHandle, "agent:main:subagent:child");
    emitAgentEvent({
      runId: "child-run",
      stream: "lifecycle",
      sessionKey: "agent:main:subagent:child",
      data: { phase: "start" },
    });
    await flushMonitorQueue(monitor);

    clearActiveEmbeddedRun("sess-root", rootHandle, "agent:main:discord:channel:123");
    emitAgentEvent({
      runId: "root-run",
      stream: "lifecycle",
      sessionKey: "agent:main:discord:channel:123",
      data: { phase: "end" },
    });

    clearActiveEmbeddedRun("sess-child", childHandle, "agent:main:subagent:child");
    emitAgentEvent({
      runId: "child-run",
      stream: "lifecycle",
      sessionKey: "agent:main:subagent:child",
      data: { phase: "end" },
    });
    await flushMonitorQueue(monitor);
    await monitor.stop();

    expect(renameChannel.mock.calls.map((call) => call[0])).toEqual([
      { channelId: "123", name: "⚙️-chief-of-staff" },
      { channelId: "123", name: "🟢-chief-of-staff" },
    ]);
  });

  it("projects child embedded activity immediately even when child lifecycle events are missing", async () => {
    const workspaceDir = await makeWorkspace();
    await writeMap(workspaceDir, {
      version: 2,
      channels: {
        "agent:main:discord:channel:123": {
          channelId: "123",
          baseName: "main",
        },
      },
    });
    await writeSessionStore(workspaceDir, {
      "agent:main:discord:channel:123": {
        sessionId: "root",
        updatedAt: 1,
      },
      "agent:main:subagent:child": {
        sessionId: "child",
        updatedAt: 2,
        spawnedBy: "agent:main:discord:channel:123",
      },
    });

    const { renameChannel, fetchChannel } = createChannelMocks("🟢-main");
    const monitor = new AgentChannelActivityMonitor({
      workspaceDir,
      cfg: makeCfg(workspaceDir),
      renameChannel,
      fetchChannel,
      reconcileIntervalMs: 60_000,
      now: () => Date.now(),
    });
    await monitor.start();
    renameChannel.mockClear();

    const childHandle = makeEmbeddedHandle();
    setActiveEmbeddedRun("sess-child", childHandle, "agent:main:subagent:child", {
      runId: "child-run",
    });
    await flushMonitorQueue(monitor);

    clearActiveEmbeddedRun("sess-child", childHandle, "agent:main:subagent:child");
    await flushMonitorQueue(monitor);
    await monitor.stop();

    expect(renameChannel.mock.calls.map((call) => call[0])).toEqual([
      { channelId: "123", name: "⚙️-main" },
      { channelId: "123", name: "🟢-main" },
    ]);
  });

  it("treats announce runs on the root session as running until the announce completes", async () => {
    const workspaceDir = await makeWorkspace();
    await writeMap(workspaceDir, {
      version: 2,
      channels: {
        "agent:main:discord:channel:123": {
          channelId: "123",
          baseName: "main",
        },
      },
    });

    const { renameChannel, fetchChannel } = createChannelMocks("🟢-main");
    const monitor = new AgentChannelActivityMonitor({
      workspaceDir,
      renameChannel,
      fetchChannel,
      reconcileIntervalMs: 60_000,
      now: () => Date.now(),
    });
    await monitor.start();
    renameChannel.mockClear();

    const announceHandle = makeEmbeddedHandle();
    setActiveEmbeddedRun("sess-root", announceHandle, "agent:main:discord:channel:123", {
      runId: "announce:v1:agent:main:subagent:child:child-run",
    });
    await flushMonitorQueue(monitor);

    clearActiveEmbeddedRun("sess-root", announceHandle, "agent:main:discord:channel:123");
    await flushMonitorQueue(monitor);
    await monitor.stop();

    expect(renameChannel.mock.calls.map((call) => call[0])).toEqual([
      { channelId: "123", name: "⚙️-main" },
      { channelId: "123", name: "🟢-main" },
    ]);
  });

  it("keeps the root channel running while descendant cleanup or announce work is still pending", async () => {
    vi.useFakeTimers();
    const workspaceDir = await makeWorkspace();
    await writeMap(workspaceDir, {
      version: 2,
      channels: {
        "agent:main:discord:channel:123": {
          channelId: "123",
          baseName: "main",
        },
      },
    });

    addSubagentRunForTests({
      runId: "child-run",
      childSessionKey: "agent:main:subagent:child",
      requesterSessionKey: "agent:main:discord:channel:123",
      requesterDisplayKey: "agent:main:discord:channel:123",
      task: "prepare completion",
      cleanup: "keep",
      expectsCompletionMessage: true,
      spawnMode: "run",
      createdAt: 1_000,
      startedAt: 1_000,
      endedAt: 2_000,
      cleanupHandled: false,
    });

    const { renameChannel, fetchChannel } = createChannelMocks("🟢-main");
    const monitor = new AgentChannelActivityMonitor({
      workspaceDir,
      renameChannel,
      fetchChannel,
      reconcileIntervalMs: 5_000,
      now: () => Date.now(),
    });
    await monitor.start();
    await flushMonitorQueue(monitor);

    releaseSubagentRun("child-run");
    await vi.advanceTimersByTimeAsync(5_000);
    await flushMonitorQueue(monitor);
    await monitor.stop();

    expect(renameChannel.mock.calls.map((call) => call[0])).toEqual([
      { channelId: "123", name: "⚙️-main" },
      { channelId: "123", name: "🟢-main" },
    ]);
  });

  it("keeps the root channel running across nested descendant pending cleanup", async () => {
    vi.useFakeTimers();
    const workspaceDir = await makeWorkspace();
    await writeMap(workspaceDir, {
      version: 2,
      channels: {
        "agent:main:discord:channel:123": {
          channelId: "123",
          baseName: "main",
        },
      },
    });

    addSubagentRunForTests({
      runId: "orch-run",
      childSessionKey: "agent:main:subagent:orch",
      requesterSessionKey: "agent:main:discord:channel:123",
      requesterDisplayKey: "agent:main:discord:channel:123",
      task: "orchestrate nested work",
      cleanup: "keep",
      expectsCompletionMessage: true,
      spawnMode: "run",
      createdAt: 1_000,
      startedAt: 1_000,
      endedAt: 2_000,
      cleanupHandled: false,
    });
    addSubagentRunForTests({
      runId: "leaf-run",
      childSessionKey: "agent:main:subagent:orch:subagent:leaf",
      requesterSessionKey: "agent:main:subagent:orch",
      requesterDisplayKey: "agent:main:subagent:orch",
      task: "leaf work",
      cleanup: "keep",
      expectsCompletionMessage: true,
      spawnMode: "run",
      createdAt: 1_500,
      startedAt: 1_500,
      endedAt: 2_500,
      cleanupHandled: false,
    });

    const { renameChannel, fetchChannel } = createChannelMocks("🟢-main");
    const monitor = new AgentChannelActivityMonitor({
      workspaceDir,
      renameChannel,
      fetchChannel,
      reconcileIntervalMs: 5_000,
      now: () => Date.now(),
    });
    await monitor.start();
    await flushMonitorQueue(monitor);

    releaseSubagentRun("leaf-run");
    await vi.advanceTimersByTimeAsync(5_000);
    await flushMonitorQueue(monitor);

    expect(renameChannel.mock.calls.map((call) => call[0])).toEqual([
      { channelId: "123", name: "⚙️-main" },
    ]);

    releaseSubagentRun("orch-run");
    await vi.advanceTimersByTimeAsync(5_000);
    await flushMonitorQueue(monitor);
    await monitor.stop();

    expect(renameChannel.mock.calls.map((call) => call[0])).toEqual([
      { channelId: "123", name: "⚙️-main" },
      { channelId: "123", name: "🟢-main" },
    ]);
  });

  it("reconciles one mapped channel without mutating another healthy channel", async () => {
    const workspaceDir = await makeWorkspace();
    await writeMap(workspaceDir, {
      version: 2,
      channels: {
        "agent:main:discord:channel:123": {
          channelId: "123",
          baseName: "main",
        },
        "agent:main:discord:channel:456": {
          channelId: "456",
          baseName: "hometax",
        },
      },
    });

    const statePath = path.join(workspaceDir, "ops", "state", "agent-activity.json");
    await fs.mkdir(path.dirname(statePath), { recursive: true });
    await fs.writeFile(
      statePath,
      `${JSON.stringify({
        version: 2,
        staleRunningMs: 21600000,
        channels: {
          "agent:main:discord:channel:123": {
            state: "running",
            updatedAt: 1000,
            runs: {
              "run-1": {
                sessionKey: "agent:main:discord:channel:123",
                status: "running",
                startedAt: 1000,
                updatedAt: 1000,
              },
            },
            lastTargetName: "⚙️-main",
          },
          "agent:main:discord:channel:456": {
            state: "idle",
            updatedAt: 1000,
            runs: {},
            lastTargetName: "🟢-hometax",
          },
        },
      }, null, 2)}\n`,
      "utf8",
    );

    const currentNames = new Map([
      ["123", "⚙️-main"],
      ["456", "🟢-hometax"],
    ]);
    const renameChannel = vi.fn(async ({ channelId, name }: { channelId: string; name: string; accountId?: string }) => {
      currentNames.set(channelId, name);
    });
    const fetchChannel = vi.fn(async ({ channelId }: { channelId: string; accountId?: string }) => ({
      name: currentNames.get(channelId),
    }));
    const monitor = new AgentChannelActivityMonitor({
      workspaceDir,
      renameChannel,
      fetchChannel,
      now: () => 12_000,
      reconcileIntervalMs: 5_000,
    });
    await monitor.start();
    await monitor.stop();

    expect(renameChannel.mock.calls.map((call) => call[0])).toEqual([
      { channelId: "123", name: "🟢-main" },
    ]);
  });

  it("preserves desired state and degrades transport on definitive rename failures", async () => {
    const workspaceDir = await makeWorkspace();
    await writeMap(workspaceDir, {
      version: 2,
      channels: {
        "agent:main:discord:channel:123": {
          channelId: "123",
          baseName: "main",
        },
      },
    });

    const renameChannel = vi.fn(async () => {
      throw new DiscordApiError("Forbidden", 403, undefined, {
        code: "http",
        method: "PATCH",
        path: "/channels/123",
      });
    });
    const fetchChannel = vi.fn(async () => ({ name: "🟢-main" }));
    const monitor = new AgentChannelActivityMonitor({
      workspaceDir,
      renameChannel,
      fetchChannel,
      now: () => Date.now(),
      staleSweepIntervalMs: 60_000,
    });
    await monitor.start();

    emitAgentEvent({
      runId: "run-1",
      stream: "lifecycle",
      sessionKey: "agent:main:discord:channel:123",
      data: { phase: "start" },
    });
    await flushMonitorQueue(monitor);
    await monitor.stop();

    const state = await readState(workspaceDir);
    expect(state.channels["agent:main:discord:channel:123"]?.desiredState).toBe("running");
    expect(state.channels["agent:main:discord:channel:123"]?.transportState).toBe("degraded");
    expect(state.channels["agent:main:discord:channel:123"]?.lastRenameStatus).toBe("degraded");
  });

  it("preserves desired state and schedules retry_wait on transient rename failures", async () => {
    const workspaceDir = await makeWorkspace();
    await writeMap(workspaceDir, {
      version: 2,
      channels: {
        "agent:main:discord:channel:123": {
          channelId: "123",
          baseName: "main",
        },
      },
    });

    const renameChannel = vi.fn(async () => {
      throw new DiscordApiError("Rate limited", 429, 1, {
        code: "http",
        method: "PATCH",
        path: "/channels/123",
      });
    });
    const fetchChannel = vi.fn(async () => ({ name: "🟢-main" }));
    const monitor = new AgentChannelActivityMonitor({
      workspaceDir,
      renameChannel,
      fetchChannel,
      now: () => 10_000,
      staleSweepIntervalMs: 60_000,
    });
    await monitor.start();

    emitAgentEvent({
      runId: "run-1",
      stream: "lifecycle",
      sessionKey: "agent:main:discord:channel:123",
      data: { phase: "start" },
    });
    await flushMonitorQueue(monitor);
    await monitor.stop();

    const state = await readState(workspaceDir);
    expect(state.channels["agent:main:discord:channel:123"]?.desiredState).toBe("running");
    expect(state.channels["agent:main:discord:channel:123"]?.transportState).toBe("retry_wait");
    expect(state.channels["agent:main:discord:channel:123"]?.lastRenameStatus).toBe("http");
    expect(typeof state.channels["agent:main:discord:channel:123"]?.nextRetryAt).toBe("number");
  });

  it("moves a category-mapped channel to running immediately and back to idle after quiet grace", async () => {
    const workspaceDir = await makeWorkspace();
    await writeMap(workspaceDir, {
      version: 2,
      channels: {
        "agent:main:discord:channel:123": {
          accountId: "default",
          categories: {
            blocked: "cat-blocked",
            idle: "cat-idle",
            running: "cat-running",
          },
          channelId: "123",
          guildId: "guild-1",
          sortKey: 10,
        },
      },
    });

    let now = 1_000;
    const { fetchChannel, getParentId, moveChannels } = createCategoryChannelMocks({
      initialName: "main",
      initialParentId: "cat-idle",
      position: 10,
    });
    const monitor = new AgentChannelActivityMonitor({
      workspaceDir,
      fetchChannel,
      moveChannels,
      now: () => now,
      reconcileIntervalMs: 5_000,
    });
    await monitor.start();

    emitAgentEvent({
      runId: "run-1",
      stream: "lifecycle",
      sessionKey: "agent:main:discord:channel:123",
      data: { phase: "start" },
    });
    await flushMonitorQueue(monitor);
    expect(getParentId()).toBe("cat-running");

    emitAgentEvent({
      runId: "run-1",
      stream: "lifecycle",
      sessionKey: "agent:main:discord:channel:123",
      data: { phase: "end" },
    });
    await flushMonitorQueue(monitor);
    expect(moveChannels).toHaveBeenCalledTimes(1);
    expect(getParentId()).toBe("cat-running");

    now += 61_000;
    await (monitor as unknown as { reconcileChannels: () => Promise<void> }).reconcileChannels();
    await flushMonitorQueue(monitor);
    await monitor.stop();

    expect(moveChannels).toHaveBeenCalledTimes(2);
    expect(moveChannels.mock.calls.at(0)?.[0]).toMatchObject({
      guildId: "guild-1",
      moves: [{ channelId: "123", parentId: "cat-running", position: 10 }],
    });
    expect(moveChannels.mock.calls.at(1)?.[0]).toMatchObject({
      guildId: "guild-1",
      moves: [{ channelId: "123", parentId: "cat-idle", position: 10 }],
    });
  });

  it("keeps a category-mapped channel in running while descendant cleanup is pending", async () => {
    const workspaceDir = await makeWorkspace();
    await writeMap(workspaceDir, {
      version: 2,
      channels: {
        "agent:main:discord:channel:123": {
          categories: {
            blocked: "cat-blocked",
            idle: "cat-idle",
            running: "cat-running",
          },
          channelId: "123",
          guildId: "guild-1",
          sortKey: 10,
        },
      },
    });
    await writeSessionStore(workspaceDir, {
      "agent:main:discord:channel:123": {
        sessionId: "root",
        updatedAt: 1,
      },
      "agent:main:subagent:child": {
        sessionId: "child",
        updatedAt: 2,
        spawnedBy: "agent:main:discord:channel:123",
      },
    });

    let now = 5_000;
    const { fetchChannel, getParentId, moveChannels } = createCategoryChannelMocks({
      initialName: "main",
      initialParentId: "cat-idle",
      position: 10,
    });
    const monitor = new AgentChannelActivityMonitor({
      workspaceDir,
      cfg: makeCfg(workspaceDir),
      fetchChannel,
      moveChannels,
      now: () => now,
      reconcileIntervalMs: 5_000,
    });
    await monitor.start();

    emitAgentEvent({
      runId: "root-run",
      stream: "lifecycle",
      sessionKey: "agent:main:discord:channel:123",
      data: { phase: "start" },
    });
    await flushMonitorQueue(monitor);

    addSubagentRunForTests({
      runId: "child-run",
      childSessionKey: "agent:main:subagent:child",
      requesterSessionKey: "agent:main:discord:channel:123",
      requesterDisplayKey: "agent:main:discord:channel:123",
      task: "finish child cleanup",
      cleanup: "keep",
      expectsCompletionMessage: true,
      spawnMode: "run",
      createdAt: now,
      startedAt: now,
      endedAt: now + 1_000,
      cleanupHandled: false,
    });
    await flushMonitorQueue(monitor);

    emitAgentEvent({
      runId: "root-run",
      stream: "lifecycle",
      sessionKey: "agent:main:discord:channel:123",
      data: { phase: "end" },
    });
    await flushMonitorQueue(monitor);

    expect(getParentId()).toBe("cat-running");
    expect(moveChannels).toHaveBeenCalledTimes(1);

    releaseSubagentRun("child-run");
    now += 61_000;
    await (monitor as unknown as { reconcileChannels: () => Promise<void> }).reconcileChannels();
    await flushMonitorQueue(monitor);
    await monitor.stop();

    expect(moveChannels).toHaveBeenCalledTimes(2);
    expect(getParentId()).toBe("cat-idle");
  });

  it("respects manual blocked category override until the operator moves the channel back out", async () => {
    const workspaceDir = await makeWorkspace();
    await writeMap(workspaceDir, {
      version: 2,
      channels: {
        "agent:main:discord:channel:123": {
          categories: {
            blocked: "cat-blocked",
            idle: "cat-idle",
            running: "cat-running",
          },
          channelId: "123",
          guildId: "guild-1",
          sortKey: 10,
        },
      },
    });

    let now = 9_000;
    const { fetchChannel, moveChannels, setParentId } = createCategoryChannelMocks({
      initialName: "main",
      initialParentId: "cat-blocked",
      position: 10,
    });
    const monitor = new AgentChannelActivityMonitor({
      workspaceDir,
      fetchChannel,
      moveChannels,
      now: () => now,
      reconcileIntervalMs: 5_000,
    });
    await monitor.start();

    emitAgentEvent({
      runId: "run-1",
      stream: "lifecycle",
      sessionKey: "agent:main:discord:channel:123",
      data: { phase: "start" },
    });
    await flushMonitorQueue(monitor);
    expect(moveChannels).toHaveBeenCalledTimes(0);

    setParentId("cat-idle");
    now += 5_000;
    await (monitor as unknown as { reconcileChannels: () => Promise<void> }).reconcileChannels();
    await flushMonitorQueue(monitor);
    await monitor.stop();

    expect(moveChannels).toHaveBeenCalledTimes(1);
    expect(moveChannels.mock.calls.at(0)?.[0]).toMatchObject({
      guildId: "guild-1",
      moves: [{ channelId: "123", parentId: "cat-running", position: 10 }],
    });
  });

  it("moves same-guild category parent changes sequentially one channel at a time", async () => {
    const workspaceDir = await makeWorkspace();
    await writeMap(workspaceDir, {
      version: 2,
      channels: {
        "agent:main:discord:channel:123": {
          categories: {
            blocked: "cat-blocked",
            idle: "cat-idle",
            running: "cat-running",
          },
          channelId: "123",
          guildId: "guild-1",
          sortKey: 1,
        },
        "agent:main:discord:channel:456": {
          categories: {
            blocked: "cat-blocked",
            idle: "cat-idle",
            running: "cat-running",
          },
          channelId: "456",
          guildId: "guild-1",
          sortKey: 2,
        },
      },
    });

    const parentByChannel = new Map<string, string>([
      ["123", "old-parent"],
      ["456", "old-parent"],
    ]);
    const moveChannels = vi.fn(
      async ({
        moves,
      }: {
        accountId?: string;
        guildId: string;
        moves: Array<{ channelId: string; parentId: string; position?: number }>;
      }) => {
        expect(moves).toHaveLength(1);
        for (const move of moves) {
          parentByChannel.set(move.channelId, move.parentId);
        }
        return { httpStatus: 200 };
      },
    );
    const fetchChannel = vi.fn(async ({ channelId }: { channelId: string; accountId?: string }) => ({
      guildId: "guild-1",
      name: channelId === "123" ? "main" : "hometax",
      parentId: parentByChannel.get(channelId),
      position: channelId === "123" ? 1 : 2,
    }));

    const monitor = new AgentChannelActivityMonitor({
      workspaceDir,
      fetchChannel,
      moveChannels,
      now: () => 20_000,
      reconcileIntervalMs: 5_000,
    });
    await monitor.start();
    await flushMonitorQueue(monitor);
    await monitor.stop();

    expect(moveChannels).toHaveBeenCalledTimes(2);
    expect(moveChannels.mock.calls.every((call) => call[0].moves.length === 1)).toBe(true);
    expect(parentByChannel.get("123")).toBe("cat-idle");
    expect(parentByChannel.get("456")).toBe("cat-idle");
  });

  it("honors retry_after for category moves and retries only after the cooldown", async () => {
    const workspaceDir = await makeWorkspace();
    await writeMap(workspaceDir, {
      version: 2,
      channels: {
        "agent:main:discord:channel:123": {
          categories: {
            blocked: "cat-blocked",
            idle: "cat-idle",
            running: "cat-running",
          },
          channelId: "123",
          guildId: "guild-1",
          sortKey: 10,
        },
      },
    });

    let now = 10_000;
    let attempt = 0;
    const { fetchChannel, getParentId, moveChannels } = createCategoryChannelMocks({
      initialName: "main",
      initialParentId: "cat-idle",
      position: 10,
      moveImpl: async ({ moves, setParentId, setPosition }) => {
        attempt += 1;
        if (attempt === 1) {
          throw new DiscordApiError("Rate limited", 429, 2, {
            code: "http",
            method: "PATCH",
            path: "/guilds/guild-1/channels",
          });
        }
        for (const move of moves) {
          setParentId(move.parentId);
          if (move.position !== undefined) {
            setPosition(move.position);
          }
        }
        return { httpStatus: 200 };
      },
    });
    const monitor = new AgentChannelActivityMonitor({
      workspaceDir,
      fetchChannel,
      moveChannels,
      now: () => now,
      reconcileIntervalMs: 5_000,
    });
    await monitor.start();

    emitAgentEvent({
      runId: "run-1",
      stream: "lifecycle",
      sessionKey: "agent:main:discord:channel:123",
      data: { phase: "start" },
    });
    await flushMonitorQueue(monitor);
    expect(moveChannels).toHaveBeenCalledTimes(1);
    expect(getParentId()).toBe("cat-idle");

    now += 1_000;
    await (monitor as unknown as { reconcileChannels: () => Promise<void> }).reconcileChannels();
    await flushMonitorQueue(monitor);
    expect(moveChannels).toHaveBeenCalledTimes(1);

    now += 1_500;
    await (monitor as unknown as { reconcileChannels: () => Promise<void> }).reconcileChannels();
    await flushMonitorQueue(monitor);
    await monitor.stop();

    expect(moveChannels).toHaveBeenCalledTimes(2);
    expect(getParentId()).toBe("cat-running");

    const state = await readState(workspaceDir);
    expect(state.channels["agent:main:discord:channel:123"]?.transportState).toBe("idle");
  });
});

