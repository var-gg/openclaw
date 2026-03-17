import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import {
  emitAgentEvent,
  registerAgentRunContext,
  resetAgentRunContextForTest,
} from "../../infra/agent-events.js";
import type { OpenClawConfig } from "../../config/config.js";
import {
  AgentChannelActivityMonitor,
  __test__,
} from "./agent-channel-activity.js";

async function makeWorkspace() {
  return await fs.mkdtemp(path.join(os.tmpdir(), "openclaw-agent-channel-activity-"));
}

async function writeMap(
  workspaceDir: string,
  value: Record<string, unknown>,
) {
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

describe("agent channel activity monitor", () => {
  beforeEach(() => {
    resetAgentRunContextForTest();
  });

  afterEach(() => {
    vi.useRealTimers();
    resetAgentRunContextForTest();
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
    const renameChannel = vi.fn(
      async (_params: { channelId: string; name: string; accountId?: string }) => {},
    );
    let currentName = "🟢-vargg-growth";
    const fetchChannel = vi.fn(async () => ({ name: currentName }));
    renameChannel.mockImplementation(async ({ name }) => {
      currentName = name;
    });
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
    const renameChannel = vi.fn(
      async (_params: { channelId: string; name: string; accountId?: string }) => {},
    );
    let currentName = "🟢-ops-room";
    const fetchChannel = vi.fn(async () => ({ name: currentName }));
    renameChannel.mockImplementation(async ({ name }) => {
      currentName = name;
    });
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
    const renameChannel = vi.fn(
      async (_params: { channelId: string; name: string; accountId?: string }) => {},
    );
    let currentName = "🟢-chief-of-staff";
    const fetchChannel = vi.fn(async () => ({ name: currentName }));
    renameChannel.mockImplementation(async ({ name }) => {
      currentName = name;
    });
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

    const renameChannel = vi.fn(
      async (_params: { channelId: string; name: string; accountId?: string }) => {},
    );
    const monitor = new AgentChannelActivityMonitor({
      workspaceDir,
      cfg: makeCfg(workspaceDir),
      renameChannel,
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

    let currentName = "🟢-main";
    const renameChannel = vi.fn(async ({ name }: { channelId: string; name: string; accountId?: string }) => {
      currentName = name;
    });
    const fetchChannel = vi.fn(async () => ({ name: currentName }));
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
    const renameChannel = vi.fn(
      async (_params: { channelId: string; name: string; accountId?: string }) => {},
    );
    let currentName = "🟢-main";
    const fetchChannel = vi.fn(async () => ({ name: currentName }));
    renameChannel.mockImplementation(async ({ name }) => {
      currentName = name;
    });
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

    let currentName = "🟢-korean-tax-agent";
    const renameChannel = vi.fn(async ({ name }: { channelId: string; name: string; accountId?: string }) => {
      currentName = name;
    });
    const fetchChannel = vi.fn(async () => ({ name: currentName }));
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

    let currentName = "🟢-main";
    const renameChannel = vi.fn(async ({ name }: { channelId: string; name: string; accountId?: string }) => {
      currentName = name;
    });
    const fetchChannel = vi.fn(async () => ({ name: currentName }));
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
});
