import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { emitAgentEvent } from "../../infra/agent-events.js";
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
    agents: Record<string, Record<string, unknown>>;
  };
}

describe("agent channel activity monitor", () => {
  afterEach(() => {
    vi.useRealTimers();
  });

  it("synthesizes canonical emoji-prefixed channel names", () => {
    expect(__test__.synthesizeChannelName("main", "idle")).toBe("🟢-main");
    expect(__test__.synthesizeChannelName("main", "running")).toBe("⚙️-main");
    expect(__test__.synthesizeChannelName("main", "error")).toBe("🔴-main");
  });

  it("renames mapped channels on start/end/error and ignores stale run completions", async () => {
    const workspaceDir = await makeWorkspace();
    await writeMap(workspaceDir, {
      version: 1,
      agents: {
        main: {
          channelId: "123",
          baseName: "main",
        },
      },
    });

    let now = 1_000;
    const renameChannel = vi.fn(
      async (_params: { channelId: string; name: string; accountId?: string }) => {},
    );
    const monitor = new AgentChannelActivityMonitor({
      workspaceDir,
      renameChannel,
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

    expect(renameChannel).toHaveBeenCalledTimes(1);
    expect(renameChannel).toHaveBeenLastCalledWith({ channelId: "123", name: "⚙️-main" });

    const monitor2 = new AgentChannelActivityMonitor({
      workspaceDir,
      renameChannel,
      now: () => (now += 100),
      staleSweepIntervalMs: 60_000,
    });
    await monitor2.start();

    emitAgentEvent({
      runId: "run-0",
      stream: "lifecycle",
      sessionKey: "agent:main:discord:channel:123",
      data: { phase: "end" },
    });
    emitAgentEvent({
      runId: "run-1",
      stream: "lifecycle",
      sessionKey: "agent:main:discord:channel:123",
      data: { phase: "error", error: "boom" },
    });
    await monitor2.stop();

    expect(renameChannel).toHaveBeenCalledTimes(2);
    expect(renameChannel).toHaveBeenLastCalledWith({ channelId: "123", name: "🔴-main" });

    const state = await readState(workspaceDir);
    expect(state.agents.main?.state).toBe("error");
    expect(state.agents.main?.activeRunId).toBeUndefined();
    expect(state.agents.main?.lastError).toBe("boom");
  });

  it("avoids duplicate Discord writes when the target name is unchanged", async () => {
    const workspaceDir = await makeWorkspace();
    await writeMap(workspaceDir, {
      agents: {
        main: {
          channelId: "123",
          baseName: "main",
        },
      },
    });

    const renameChannel = vi.fn(
      async (_params: { channelId: string; name: string; accountId?: string }) => {},
    );
    const monitor = new AgentChannelActivityMonitor({
      workspaceDir,
      renameChannel,
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
    emitAgentEvent({
      runId: "run-1",
      stream: "lifecycle",
      sessionKey: "agent:main:discord:channel:123",
      data: { phase: "start" },
    });
    await monitor.stop();

    expect(renameChannel).toHaveBeenCalledTimes(1);
  });

  it("recovers stale running agents back to idle conservatively", async () => {
    const workspaceDir = await makeWorkspace();
    await writeMap(workspaceDir, {
      staleRunningMs: 1_000,
      agents: {
        main: {
          channelId: "123",
          baseName: "main",
        },
      },
    });

    let now = 10_000;
    const renameChannel = vi.fn(
      async (_params: { channelId: string; name: string; accountId?: string }) => {},
    );
    const monitor = new AgentChannelActivityMonitor({
      workspaceDir,
      renameChannel,
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
    expect(state.agents.main?.state).toBe("idle");
    expect(state.agents.main?.activeRunId).toBeUndefined();
  });
});
