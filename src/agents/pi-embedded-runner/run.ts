import { randomBytes } from "node:crypto";
import fs from "node:fs/promises";
import type { ThinkLevel } from "../../auto-reply/thinking.js";
import {
  ensureContextEnginesInitialized,
  resolveContextEngine,
} from "../../context-engine/index.js";
import { emitAgentPlanEvent } from "../../infra/agent-events.js";
import { sleepWithAbort } from "../../infra/backoff.js";
import { getGlobalHookRunner } from "../../plugins/hook-runner-global.js";
import { enqueueCommandInLane } from "../../process/command-queue.js";
import { sanitizeForLog } from "../../terminal/ansi.js";
import { isMarkdownCapableMessageChannel } from "../../utils/message-channel.js";
import { resolveOpenClawAgentDir } from "../agent-paths.js";
import { hasConfiguredModelFallbacks } from "../agent-scope.js";
import {
  type AuthProfileFailureReason,
  markAuthProfileFailure,
  markAuthProfileGood,
  markAuthProfileUsed,
} from "../auth-profiles.js";
import { DEFAULT_MODEL, DEFAULT_PROVIDER } from "../defaults.js";
import {
  coerceToFailoverError,
  describeFailoverError,
  FailoverError,
  resolveFailoverStatus,
} from "../failover-error.js";
import { LiveSessionModelSwitchError } from "../live-model-switch-error.js";
import { shouldSwitchToLiveModel, clearLiveModelSwitchPending } from "../live-model-switch.js";
import {
  applyAuthHeaderOverride,
  applyLocalNoAuthHeaderOverride,
  ensureAuthProfileStore,
  type ResolvedProviderAuth,
  resolveAuthProfileOrder,
  shouldPreferExplicitConfigApiKeyAuth,
} from "../model-auth.js";
import { normalizeProviderId } from "../model-selection.js";
import { ensureOpenClawModelsJson } from "../models-config.js";
import { disposeSessionMcpRuntime } from "../pi-bundle-mcp-tools.js";
import {
  classifyFailoverReason,
  extractObservedOverflowTokenCount,
  type FailoverReason,
  formatAssistantErrorText,
  isAuthAssistantError,
  isBillingAssistantError,
  isCompactionFailureError,
  isFailoverAssistantError,
  isFailoverErrorMessage,
  isLikelyContextOverflowError,
  isRateLimitAssistantError,
  parseImageDimensionError,
  parseImageSizeError,
  pickFallbackThinkingLevel,
} from "../pi-embedded-helpers.js";
import { ensureRuntimePluginsLoaded } from "../runtime-plugins.js";
import { derivePromptTokens, normalizeUsage, type UsageLike } from "../usage.js";
import { redactRunIdentifier, resolveRunWorkspaceDir } from "../workspace-run.js";
import { runPostCompactionSideEffects } from "./compact.js";
import { buildEmbeddedCompactionRuntimeContext } from "./compaction-runtime-context.js";
import { runContextEngineMaintenance } from "./context-engine-maintenance.js";
import { resolveGlobalLane, resolveSessionLane } from "./lanes.js";
import { log } from "./logger.js";
import { resolveModelAsync } from "./model.js";
import { handleAssistantFailover } from "./run/assistant-failover.js";
import { runEmbeddedAttempt } from "./run/attempt.js";
import { createEmbeddedRunAuthController } from "./run/auth-controller.js";
import { createFailoverDecisionLogger } from "./run/failover-observation.js";
import { mergeRetryFailoverReason, resolveRunFailoverDecision } from "./run/failover-policy.js";
import {
  buildErrorAgentMeta,
  buildUsageAgentMetaFields,
  createCompactionDiagId,
  resolveActiveErrorContext,
  resolveMaxRunRetryIterations,
  resolveOverloadFailoverBackoffMs,
  resolveOverloadProfileRotationLimit,
  resolveRateLimitProfileRotationLimit,
  type RuntimeAuthState,
  scrubAnthropicRefusalMagic,
} from "./run/helpers.js";
import {
  resolveAckExecutionFastPathInstruction,
  resolveIncompleteTurnPayloadText,
  extractPlanningOnlyPlanDetails,
  resolvePlanningOnlyRetryInstruction,
} from "./run/incomplete-turn.js";
import type { RunEmbeddedPiAgentParams } from "./run/params.js";
import { buildEmbeddedRunPayloads } from "./run/payloads.js";
import { handleRetryLimitExhaustion } from "./run/retry-limit.js";
import { resolveEffectiveRuntimeModel, resolveHookModelSelection } from "./run/setup.js";
import {
  sessionLikelyHasOversizedToolResults,
  truncateOversizedToolResultsInSession,
} from "./tool-result-truncation.js";
import type { EmbeddedPiAgentMeta, EmbeddedPiRunResult } from "./types.js";
import { createUsageAccumulator, mergeUsageIntoAccumulator } from "./usage-accumulator.js";
import { describeUnknownError } from "./utils.js";

type ApiKeyInfo = ResolvedProviderAuth;

type RuntimeAuthState = {
  sourceApiKey: string;
  authMode: string;
  profileId?: string;
  expiresAt?: number;
  refreshTimer?: ReturnType<typeof setTimeout>;
  refreshInFlight?: Promise<void>;
};

const RUNTIME_AUTH_REFRESH_MARGIN_MS = 5 * 60 * 1000;
const RUNTIME_AUTH_REFRESH_RETRY_MS = 60 * 1000;
const RUNTIME_AUTH_REFRESH_MIN_DELAY_MS = 5 * 1000;
// Keep overload pacing noticeable enough to avoid tight retry bursts, but short
// enough that fallback still feels responsive within a single turn.
const OVERLOAD_FAILOVER_BACKOFF_POLICY: BackoffPolicy = {
  initialMs: 250,
  maxMs: 1_500,
  factor: 2,
  jitter: 0.2,
};

// Avoid Anthropic's refusal test token poisoning session transcripts.
const ANTHROPIC_MAGIC_STRING_TRIGGER_REFUSAL = "ANTHROPIC_MAGIC_STRING_TRIGGER_REFUSAL";
const ANTHROPIC_MAGIC_STRING_REPLACEMENT = "ANTHROPIC MAGIC STRING TRIGGER REFUSAL (redacted)";

function scrubAnthropicRefusalMagic(prompt: string): string {
  if (!prompt.includes(ANTHROPIC_MAGIC_STRING_TRIGGER_REFUSAL)) {
    return prompt;
  }
  return prompt.replaceAll(
    ANTHROPIC_MAGIC_STRING_TRIGGER_REFUSAL,
    ANTHROPIC_MAGIC_STRING_REPLACEMENT,
  );
}
function createCompactionDiagId(): string {
  return `ovf-${Date.now().toString(36)}-${generateSecureToken(4)}`;
}

// Defensive guard for the outer run loop across all retry branches.
const BASE_RUN_RETRY_ITERATIONS = 24;
const RUN_RETRY_ITERATIONS_PER_PROFILE = 8;
const MIN_RUN_RETRY_ITERATIONS = 32;
const MAX_RUN_RETRY_ITERATIONS = 160;

function resolveMaxRunRetryIterations(profileCandidateCount: number): number {
  const scaled =
    BASE_RUN_RETRY_ITERATIONS +
    Math.max(1, profileCandidateCount) * RUN_RETRY_ITERATIONS_PER_PROFILE;
  return Math.min(MAX_RUN_RETRY_ITERATIONS, Math.max(MIN_RUN_RETRY_ITERATIONS, scaled));
}

function resolveActiveErrorContext(params: {
  lastAssistant: { provider?: string; model?: string } | undefined;
  provider: string;
  model: string;
}): { provider: string; model: string } {
  return {
    provider: params.lastAssistant?.provider ?? params.provider,
    model: params.lastAssistant?.model ?? params.model,
  };
}

type EmbeddedAbortReasonLabel =
  | "timeout"
  | "superseded_by_new_message"
  | "external_abort_signal"
  | "gateway_stop"
  | "unknown";

const ABORT_REASON_KEYS = ["source", "reason", "stopReason", "code", "name", "type"] as const;

function normalizeAbortReasonLabel(value: string): EmbeddedAbortReasonLabel | undefined {
  const normalized = value.trim().toLowerCase().replace(/\s+/g, "_");
  if (!normalized) {
    return undefined;
  }
  if (normalized.includes("timeout")) {
    return "timeout";
  }
  if (normalized.includes("superseded_by_new_message") || normalized.includes("superseded")) {
    return "superseded_by_new_message";
  }
  if (
    normalized.includes("gateway_stop") ||
    normalized.includes("gateway_shutdown") ||
    normalized.includes("gatewaystop")
  ) {
    return "gateway_stop";
  }
  if (
    normalized.includes("external_abort_signal") ||
    normalized.includes("manual_abort") ||
    normalized.includes("stop_command") ||
    normalized.includes("abort_command") ||
    normalized.includes("manual") ||
    (normalized.includes("stop") && !normalized.includes("gateway"))
  ) {
    return "external_abort_signal";
  }
  return undefined;
}

function resolveAbortReasonLabelFromUnknown(reason: unknown): EmbeddedAbortReasonLabel | undefined {
  const candidates: string[] = [];
  if (typeof reason === "string") {
    candidates.push(reason);
  }
  if (reason instanceof Error) {
    candidates.push(reason.name, reason.message);
    if ("cause" in reason && typeof reason.cause === "string") {
      candidates.push(reason.cause);
    }
  }
  if (reason && typeof reason === "object") {
    const record = reason as Record<string, unknown>;
    for (const key of ABORT_REASON_KEYS) {
      const value = record[key];
      if (typeof value === "string") {
        candidates.push(value);
      }
    }
  }
  for (const candidate of candidates) {
    const normalized = normalizeAbortReasonLabel(candidate);
    if (normalized) {
      return normalized;
    }
  }
  return candidates.length > 0 ? "unknown" : undefined;
}

function resolveEmbeddedAbortReasonLabel(params: {
  aborted: boolean;
  timedOut: boolean;
  abortReason?: unknown;
}): EmbeddedAbortReasonLabel | undefined {
  if (!params.aborted) {
    return undefined;
  }
  if (params.timedOut) {
    return "timeout";
  }
  return resolveAbortReasonLabelFromUnknown(params.abortReason) ?? "unknown";
}

function buildUsageAgentMetaFields(params: {
  usageAccumulator: UsageAccumulator;
  lastAssistantUsage?: UsageLike | null;
  lastRunPromptUsage: ReturnType<typeof normalizeUsage> | undefined;
  /** API-reported total from the most recent call, mirroring the success path correction. */
  lastTurnTotal?: number;
}): Pick<EmbeddedPiAgentMeta, "usage" | "lastCallUsage" | "promptTokens"> {
  const usage = toNormalizedUsage(params.usageAccumulator);
  // Keep `usage.total` aligned with the API-reported latest-call total when
  // available; accumulated totals are for billing, not context display.
  if (usage && params.lastTurnTotal && params.lastTurnTotal > 0) {
    usage.total = params.lastTurnTotal;
  }
  const lastCallUsage = resolveLastCallUsage(params.lastAssistantUsage, params.usageAccumulator);
  const promptTokens = derivePromptTokens(params.lastRunPromptUsage);
  return {
    usage,
    lastCallUsage,
    promptTokens,
  };
}

/**
 * Build agentMeta for error return paths, preserving accumulated usage so that
 * session totalTokens reflects the actual context size rather than going stale.
 * Without this, error returns omit usage and the session keeps whatever
 * totalTokens was set by the previous successful run.
 */
function buildErrorAgentMeta(params: {
  sessionId: string;
  provider: string;
  model: string;
  usageAccumulator: UsageAccumulator;
  lastRunPromptUsage: ReturnType<typeof normalizeUsage> | undefined;
  lastAssistant?: { usage?: unknown } | null;
  /** API-reported total from the most recent call, mirroring the success path correction. */
  lastTurnTotal?: number;
}): EmbeddedPiAgentMeta {
  const usageMeta = buildUsageAgentMetaFields({
    usageAccumulator: params.usageAccumulator,
    lastAssistantUsage: params.lastAssistant?.usage as UsageLike | undefined,
    lastRunPromptUsage: params.lastRunPromptUsage,
    lastTurnTotal: params.lastTurnTotal,
  });
  return {
    sessionId: params.sessionId,
    provider: params.provider,
    model: params.model,
    // Only include usage fields when we have actual data from prior API calls.
    ...(usageMeta.usage ? { usage: usageMeta.usage } : {}),
    ...(usageMeta.lastCallUsage ? { lastCallUsage: usageMeta.lastCallUsage } : {}),
    ...(usageMeta.promptTokens ? { promptTokens: usageMeta.promptTokens } : {}),
  };
}

export async function runEmbeddedPiAgent(
  params: RunEmbeddedPiAgentParams,
): Promise<EmbeddedPiRunResult> {
  const sessionLane = resolveSessionLane(params.sessionKey?.trim() || params.sessionId);
  const globalLane = resolveGlobalLane(params.lane);
  const enqueueGlobal =
    params.enqueue ?? ((task, opts) => enqueueCommandInLane(globalLane, task, opts));
  const enqueueSession =
    params.enqueue ?? ((task, opts) => enqueueCommandInLane(sessionLane, task, opts));
  const channelHint = params.messageChannel ?? params.messageProvider;
  const resolvedToolResultFormat =
    params.toolResultFormat ??
    (channelHint
      ? isMarkdownCapableMessageChannel(channelHint)
        ? "markdown"
        : "plain"
      : "markdown");
  const isProbeSession = params.sessionId?.startsWith("probe-") ?? false;

  const throwIfAborted = () => {
    if (!params.abortSignal?.aborted) {
      return;
    }
    const reason = params.abortSignal.reason;
    if (reason instanceof Error) {
      throw reason;
    }
    const abortErr =
      reason !== undefined
        ? new Error("Operation aborted", { cause: reason })
        : new Error("Operation aborted");
    abortErr.name = "AbortError";
    throw abortErr;
  };

  throwIfAborted();

  return enqueueSession(() => {
    throwIfAborted();
    return enqueueGlobal(async () => {
      throwIfAborted();
      const started = Date.now();
      const workspaceResolution = resolveRunWorkspaceDir({
        workspaceDir: params.workspaceDir,
        sessionKey: params.sessionKey,
        agentId: params.agentId,
        config: params.config,
      });
      const resolvedWorkspace = workspaceResolution.workspaceDir;
      const redactedSessionId = redactRunIdentifier(params.sessionId);
      const redactedSessionKey = redactRunIdentifier(params.sessionKey);
      const redactedWorkspace = redactRunIdentifier(resolvedWorkspace);
      if (workspaceResolution.usedFallback) {
        log.warn(
          `[workspace-fallback] caller=runEmbeddedPiAgent reason=${workspaceResolution.fallbackReason} run=${params.runId} session=${redactedSessionId} sessionKey=${redactedSessionKey} agent=${workspaceResolution.agentId} workspace=${redactedWorkspace}`,
        );
      }
      ensureRuntimePluginsLoaded({
        config: params.config,
        workspaceDir: resolvedWorkspace,
        allowGatewaySubagentBinding: params.allowGatewaySubagentBinding,
      });

      let provider = (params.provider ?? DEFAULT_PROVIDER).trim() || DEFAULT_PROVIDER;
      let modelId = (params.model ?? DEFAULT_MODEL).trim() || DEFAULT_MODEL;
      const agentDir = params.agentDir ?? resolveOpenClawAgentDir();
      const fallbackConfigured = hasConfiguredModelFallbacks({
        cfg: params.config,
        agentId: params.agentId,
        sessionKey: params.sessionKey,
      });
      await ensureOpenClawModelsJson(params.config, agentDir);
      const hookRunner = getGlobalHookRunner();
      const hookCtx = {
        runId: params.runId,
        agentId: workspaceResolution.agentId,
        sessionKey: params.sessionKey,
        sessionId: params.sessionId,
        workspaceDir: resolvedWorkspace,
        modelProviderId: provider,
        modelId,
        messageProvider: params.messageProvider ?? undefined,
        trigger: params.trigger,
        channelId: params.messageChannel ?? params.messageProvider ?? undefined,
      };

      const hookSelection = await resolveHookModelSelection({
        prompt: params.prompt,
        provider,
        modelId,
        hookRunner,
        hookContext: hookCtx,
      });
      provider = hookSelection.provider;
      modelId = hookSelection.modelId;
      const legacyBeforeAgentStartResult = hookSelection.legacyBeforeAgentStartResult;

      const { model, error, authStorage, modelRegistry } = await resolveModelAsync(
        provider,
        modelId,
        agentDir,
        params.config,
      );
      if (!model) {
        throw new FailoverError(error ?? `Unknown model: ${provider}/${modelId}`, {
          reason: "model_not_found",
          provider,
          model: modelId,
        });
      }
      let runtimeModel = model;

      const resolvedRuntimeModel = resolveEffectiveRuntimeModel({
        cfg: params.config,
        provider,
        modelId,
        runtimeModel,
      });
      const ctxInfo = resolvedRuntimeModel.ctxInfo;
      let effectiveModel = resolvedRuntimeModel.effectiveModel;

      const authStore = ensureAuthProfileStore(agentDir, {
        allowKeychainPrompt: false,
      });
      const preferredProfileId = params.authProfileId?.trim();
      let lockedProfileId = params.authProfileIdSource === "user" ? preferredProfileId : undefined;
      if (lockedProfileId) {
        const lockedProfile = authStore.profiles[lockedProfileId];
        if (
          !lockedProfile ||
          normalizeProviderId(lockedProfile.provider) !== normalizeProviderId(provider)
        ) {
          lockedProfileId = undefined;
        }
      }
      const profileOrder = shouldPreferExplicitConfigApiKeyAuth(params.config, provider)
        ? []
        : resolveAuthProfileOrder({
            cfg: params.config,
            store: authStore,
            provider,
            preferredProfile: preferredProfileId,
          });
      if (lockedProfileId && !profileOrder.includes(lockedProfileId)) {
        throw new Error(`Auth profile "${lockedProfileId}" is not configured for ${provider}.`);
      }
      const profileCandidates = lockedProfileId
        ? [lockedProfileId]
        : profileOrder.length > 0
          ? profileOrder
          : [undefined];
      let profileIndex = 0;

      const initialThinkLevel = params.thinkLevel ?? "off";
      let thinkLevel = initialThinkLevel;
      const attemptedThinking = new Set<ThinkLevel>();
      let apiKeyInfo: ApiKeyInfo | null = null;
      let lastProfileId: string | undefined;
      let runtimeAuthState: RuntimeAuthState | null = null;
      let runtimeAuthRefreshCancelled = false;
      const {
        advanceAuthProfile,
        initializeAuthProfile,
        maybeRefreshRuntimeAuthForAuthError,
        stopRuntimeAuthRefreshTimer,
      } = createEmbeddedRunAuthController({
        config: params.config,
        agentDir,
        workspaceDir: resolvedWorkspace,
        authStore,
        authStorage,
        profileCandidates,
        lockedProfileId,
        initialThinkLevel,
        attemptedThinking,
        fallbackConfigured,
        allowTransientCooldownProbe: params.allowTransientCooldownProbe === true,
        getProvider: () => provider,
        getModelId: () => modelId,
        getRuntimeModel: () => runtimeModel,
        setRuntimeModel: (next) => {
          runtimeModel = next;
        },
        getEffectiveModel: () => effectiveModel,
        setEffectiveModel: (next) => {
          effectiveModel = next;
        },
        getApiKeyInfo: () => apiKeyInfo,
        setApiKeyInfo: (next) => {
          apiKeyInfo = next;
        },
        getLastProfileId: () => lastProfileId,
        setLastProfileId: (next) => {
          lastProfileId = next;
        },
        getRuntimeAuthState: () => runtimeAuthState,
        setRuntimeAuthState: (next) => {
          runtimeAuthState = next;
        },
        getRuntimeAuthRefreshCancelled: () => runtimeAuthRefreshCancelled,
        setRuntimeAuthRefreshCancelled: (next) => {
          runtimeAuthRefreshCancelled = next;
        },
        getProfileIndex: () => profileIndex,
        setProfileIndex: (next) => {
          profileIndex = next;
        },
        setThinkLevel: (next) => {
          thinkLevel = next;
        },
        log,
      });

      await initializeAuthProfile();

      const MAX_TIMEOUT_COMPACTION_ATTEMPTS = 2;
      const MAX_OVERFLOW_COMPACTION_ATTEMPTS = 3;
      const MAX_RUN_LOOP_ITERATIONS = resolveMaxRunRetryIterations(profileCandidates.length);
      let overflowCompactionAttempts = 0;
      let toolResultTruncationAttempted = false;
      let bootstrapPromptWarningSignaturesSeen =
        params.bootstrapPromptWarningSignaturesSeen ??
        (params.bootstrapPromptWarningSignature ? [params.bootstrapPromptWarningSignature] : []);
      const usageAccumulator = createUsageAccumulator();
      let lastRunPromptUsage: ReturnType<typeof normalizeUsage> | undefined;
      let autoCompactionCount = 0;
      let runLoopIterations = 0;
      let overloadProfileRotations = 0;
      let planningOnlyRetryAttempts = 0;
      let lastRetryFailoverReason: FailoverReason | null = null;
      let planningOnlyRetryInstruction: string | null = null;
      const ackExecutionFastPathInstruction = resolveAckExecutionFastPathInstruction({
        provider,
        modelId,
        prompt: params.prompt,
      });
      let rateLimitProfileRotations = 0;
      let timeoutCompactionAttempts = 0;
      const overloadFailoverBackoffMs = resolveOverloadFailoverBackoffMs(params.config);
      const overloadProfileRotationLimit = resolveOverloadProfileRotationLimit(params.config);
      const rateLimitProfileRotationLimit = resolveRateLimitProfileRotationLimit(params.config);
      const maybeEscalateRateLimitProfileFallback = (params: {
        failoverProvider: string;
        failoverModel: string;
        logFallbackDecision: (decision: "fallback_model", extra?: { status?: number }) => void;
      }) => {
        rateLimitProfileRotations += 1;
        if (rateLimitProfileRotations <= rateLimitProfileRotationLimit || !fallbackConfigured) {
          return;
        }
        const status = resolveFailoverStatus("rate_limit");
        log.warn(
          `rate-limit profile rotation cap reached for ${sanitizeForLog(provider)}/${sanitizeForLog(modelId)} after ${rateLimitProfileRotations} rotations; escalating to model fallback`,
        );
        params.logFallbackDecision("fallback_model", { status });
        throw new FailoverError(
          "The AI service is temporarily rate-limited. Please try again in a moment.",
          {
            reason: "rate_limit",
            provider: params.failoverProvider,
            model: params.failoverModel,
            profileId: lastProfileId,
            status,
          },
        );
      };
      const maybeMarkAuthProfileFailure = async (failure: {
        profileId?: string;
        reason?: AuthProfileFailureReason | null;
        config?: RunEmbeddedPiAgentParams["config"];
        agentDir?: RunEmbeddedPiAgentParams["agentDir"];
        modelId?: string;
      }) => {
        const { profileId, reason } = failure;
        if (!profileId || !reason || reason === "timeout") {
          return;
        }
        await markAuthProfileFailure({
          store: authStore,
          profileId,
          reason,
          cfg: params.config,
          agentDir,
          runId: params.runId,
          modelId: failure.modelId,
        });
      };
      const resolveAuthProfileFailureReason = (
        failoverReason: FailoverReason | null,
      ): AuthProfileFailureReason | null => {
        // Timeouts are transport/model-path failures, not auth health signals,
        // so they should not persist auth-profile failure state.
        if (!failoverReason || failoverReason === "timeout") {
          return null;
        }
        return failoverReason;
      };
      const maybeBackoffBeforeOverloadFailover = async (reason: FailoverReason | null) => {
        if (reason !== "overloaded" || overloadFailoverBackoffMs <= 0) {
          return;
        }
        log.warn(
          `overload backoff before failover for ${provider}/${modelId}: delayMs=${overloadFailoverBackoffMs}`,
        );
        try {
          await sleepWithAbort(overloadFailoverBackoffMs, params.abortSignal);
        } catch (err) {
          if (params.abortSignal?.aborted) {
            const abortErr = new Error("Operation aborted", { cause: err });
            abortErr.name = "AbortError";
            throw abortErr;
          }
          throw err;
        }
      };
      // Resolve the context engine once and reuse across retries to avoid
      // repeated initialization/connection overhead per attempt.
      ensureContextEnginesInitialized();
      const contextEngine = await resolveContextEngine(params.config);
      try {
        // When the engine owns compaction, compactEmbeddedPiSessionDirect is
        // bypassed. Fire lifecycle hooks here so recovery paths still notify
        // subscribers like memory extensions and usage trackers.
        const runOwnsCompactionBeforeHook = async (reason: string) => {
          if (
            contextEngine.info.ownsCompaction !== true ||
            !hookRunner?.hasHooks("before_compaction")
          ) {
            return;
          }
          try {
            await hookRunner.runBeforeCompaction(
              { messageCount: -1, sessionFile: params.sessionFile },
              hookCtx,
            );
          } catch (hookErr) {
            log.warn(`before_compaction hook failed during ${reason}: ${String(hookErr)}`);
          }
        };
        const runOwnsCompactionAfterHook = async (
          reason: string,
          compactResult: Awaited<ReturnType<typeof contextEngine.compact>>,
        ) => {
          if (
            contextEngine.info.ownsCompaction !== true ||
            !compactResult.ok ||
            !compactResult.compacted ||
            !hookRunner?.hasHooks("after_compaction")
          ) {
            return;
          }
          try {
            await hookRunner.runAfterCompaction(
              {
                messageCount: -1,
                compactedCount: -1,
                tokenCount: compactResult.result?.tokensAfter,
                sessionFile: params.sessionFile,
              },
              hookCtx,
            );
          } catch (hookErr) {
            log.warn(`after_compaction hook failed during ${reason}: ${String(hookErr)}`);
          }
        };
        let authRetryPending = false;
        // Hoisted so the retry-limit error path can use the most recent API total.
        let lastTurnTotal: number | undefined;
        while (true) {
          if (runLoopIterations >= MAX_RUN_LOOP_ITERATIONS) {
            const message =
              `Exceeded retry limit after ${runLoopIterations} attempts ` +
              `(max=${MAX_RUN_LOOP_ITERATIONS}).`;
            log.error(
              `[run-retry-limit] sessionKey=${params.sessionKey ?? params.sessionId} ` +
                `provider=${provider}/${modelId} attempts=${runLoopIterations} ` +
                `maxAttempts=${MAX_RUN_LOOP_ITERATIONS}`,
            );
            const retryLimitDecision = resolveRunFailoverDecision({
              stage: "retry_limit",
              fallbackConfigured,
              failoverReason: lastRetryFailoverReason,
            });
            return handleRetryLimitExhaustion({
              message,
              decision: retryLimitDecision,
              provider,
              model: modelId,
              profileId: lastProfileId,
              durationMs: Date.now() - started,
              agentMeta: buildErrorAgentMeta({
                sessionId: params.sessionId,
                provider,
                model: model.id,
                usageAccumulator,
                lastRunPromptUsage,
                lastTurnTotal,
              }),
            });
          }
          runLoopIterations += 1;
          const runtimeAuthRetry = authRetryPending;
          authRetryPending = false;
          attemptedThinking.add(thinkLevel);
          await fs.mkdir(resolvedWorkspace, { recursive: true });

          const basePrompt =
            provider === "anthropic" ? scrubAnthropicRefusalMagic(params.prompt) : params.prompt;
          const promptAdditions = [
            ackExecutionFastPathInstruction,
            planningOnlyRetryInstruction,
          ].filter(
            (value): value is string => typeof value === "string" && value.trim().length > 0,
          );
          const prompt =
            promptAdditions.length > 0
              ? `${basePrompt}\n\n${promptAdditions.join("\n\n")}`
              : basePrompt;
          let resolvedStreamApiKey: string | undefined;
          if (!runtimeAuthState && apiKeyInfo) {
            resolvedStreamApiKey = (apiKeyInfo as ApiKeyInfo).apiKey;
          }

          const attempt = await runEmbeddedAttempt({
            sessionId: params.sessionId,
            sessionKey: params.sessionKey,
            trigger: params.trigger,
            memoryFlushWritePath: params.memoryFlushWritePath,
            messageChannel: params.messageChannel,
            messageProvider: params.messageProvider,
            agentAccountId: params.agentAccountId,
            messageTo: params.messageTo,
            messageThreadId: params.messageThreadId,
            groupId: params.groupId,
            groupChannel: params.groupChannel,
            groupSpace: params.groupSpace,
            spawnedBy: params.spawnedBy,
            senderId: params.senderId,
            senderName: params.senderName,
            senderUsername: params.senderUsername,
            senderE164: params.senderE164,
            senderIsOwner: params.senderIsOwner,
            currentChannelId: params.currentChannelId,
            currentThreadTs: params.currentThreadTs,
            currentMessageId: params.currentMessageId,
            replyToMode: params.replyToMode,
            hasRepliedRef: params.hasRepliedRef,
            sessionFile: params.sessionFile,
            workspaceDir: resolvedWorkspace,
            agentDir,
            config: params.config,
            allowGatewaySubagentBinding: params.allowGatewaySubagentBinding,
            contextEngine,
            contextTokenBudget: ctxInfo.tokens,
            skillsSnapshot: params.skillsSnapshot,
            prompt,
            images: params.images,
            imageOrder: params.imageOrder,
            clientTools: params.clientTools,
            disableTools: params.disableTools,
            provider,
            modelId,
            model: applyAuthHeaderOverride(
              applyLocalNoAuthHeaderOverride(effectiveModel, apiKeyInfo),
              // When runtime auth exchange produced a different credential
              // (runtimeAuthState is set), the exchanged token lives in
              // authStorage and the SDK will pick it up automatically.
              // Skip header injection to avoid leaking the pre-exchange key.
              runtimeAuthState ? null : apiKeyInfo,
              params.config,
            ),
            resolvedApiKey: resolvedStreamApiKey,
            authProfileId: lastProfileId,
            authProfileIdSource: lockedProfileId ? "user" : "auto",
            authStorage,
            modelRegistry,
            agentId: workspaceResolution.agentId,
            legacyBeforeAgentStartResult,
            thinkLevel,
            fastMode: params.fastMode,
            verboseLevel: params.verboseLevel,
            reasoningLevel: params.reasoningLevel,
            toolResultFormat: resolvedToolResultFormat,
            execOverrides: params.execOverrides,
            bashElevated: params.bashElevated,
            timeoutMs: params.timeoutMs,
            runId: params.runId,
            abortSignal: params.abortSignal,
            replyOperation: params.replyOperation,
            shouldEmitToolResult: params.shouldEmitToolResult,
            shouldEmitToolOutput: params.shouldEmitToolOutput,
            onPartialReply: params.onPartialReply,
            onAssistantMessageStart: params.onAssistantMessageStart,
            onBlockReply: params.onBlockReply,
            onBlockReplyFlush: params.onBlockReplyFlush,
            blockReplyBreak: params.blockReplyBreak,
            blockReplyChunking: params.blockReplyChunking,
            onReasoningStream: params.onReasoningStream,
            onReasoningEnd: params.onReasoningEnd,
            onToolResult: params.onToolResult,
            onAgentEvent: params.onAgentEvent,
            extraSystemPrompt: params.extraSystemPrompt,
            inputProvenance: params.inputProvenance,
            streamParams: params.streamParams,
            ownerNumbers: params.ownerNumbers,
            enforceFinalTag: params.enforceFinalTag,
            silentExpected: params.silentExpected,
            bootstrapPromptWarningSignaturesSeen,
            bootstrapPromptWarningSignature:
              bootstrapPromptWarningSignaturesSeen[bootstrapPromptWarningSignaturesSeen.length - 1],
          });

          const {
            aborted,
            abortReason,
            promptError,
            timedOut,
            timedOutDuringCompaction,
            sessionIdUsed,
            lastAssistant,
          } = attempt;
          const resolvedAbortReason = resolveEmbeddedAbortReasonLabel({
            aborted,
            timedOut,
            abortReason,
          });
          bootstrapPromptWarningSignaturesSeen =
            attempt.bootstrapPromptWarningSignaturesSeen ??
            (attempt.bootstrapPromptWarningSignature
              ? Array.from(
                  new Set([
                    ...bootstrapPromptWarningSignaturesSeen,
                    attempt.bootstrapPromptWarningSignature,
                  ]),
                )
              : bootstrapPromptWarningSignaturesSeen);
          const lastAssistantUsage = normalizeUsage(lastAssistant?.usage as UsageLike);
          const attemptUsage = attempt.attemptUsage ?? lastAssistantUsage;
          mergeUsageIntoAccumulator(usageAccumulator, attemptUsage);
          // Keep prompt size from the latest model call so session totalTokens
          // reflects current context usage, not accumulated tool-loop usage.
          lastRunPromptUsage = lastAssistantUsage ?? attemptUsage;
          lastTurnTotal = lastAssistantUsage?.total ?? attemptUsage?.total;
          const attemptCompactionCount = Math.max(0, attempt.compactionCount ?? 0);
          autoCompactionCount += attemptCompactionCount;
          const activeErrorContext = resolveActiveErrorContext({
            lastAssistant,
            provider,
            model: modelId,
          });
          const formattedAssistantErrorText = lastAssistant
            ? formatAssistantErrorText(lastAssistant, {
                cfg: params.config,
                sessionKey: params.sessionKey ?? params.sessionId,
                provider: activeErrorContext.provider,
                model: activeErrorContext.model,
              })
            : undefined;
          const assistantErrorText =
            lastAssistant?.stopReason === "error"
              ? lastAssistant.errorMessage?.trim() || formattedAssistantErrorText
              : undefined;
          const canRestartForLiveSwitch =
            !attempt.didSendViaMessagingTool &&
            !attempt.didSendDeterministicApprovalPrompt &&
            !attempt.lastToolError &&
            attempt.toolMetas.length === 0 &&
            attempt.assistantTexts.length === 0;
          const requestedSelection = shouldSwitchToLiveModel({
            cfg: params.config,
            sessionKey: params.sessionKey,
            agentId: params.agentId,
            defaultProvider: DEFAULT_PROVIDER,
            defaultModel: DEFAULT_MODEL,
            currentProvider: provider,
            currentModel: modelId,
            currentAuthProfileId: preferredProfileId,
            currentAuthProfileIdSource: params.authProfileIdSource,
          });
          if (requestedSelection && canRestartForLiveSwitch) {
            await clearLiveModelSwitchPending({
              cfg: params.config,
              sessionKey: params.sessionKey,
              agentId: params.agentId,
            });
            log.info(
              `live session model switch requested during active attempt for ${params.sessionId}: ${provider}/${modelId} -> ${requestedSelection.provider}/${requestedSelection.model}`,
            );
            throw new LiveSessionModelSwitchError(requestedSelection);
          }
          // ── Timeout-triggered compaction ──────────────────────────────────
          // When the LLM times out with high context usage, compact before
          // retrying to break the death spiral of repeated timeouts.
          if (timedOut && !timedOutDuringCompaction) {
            // Only consider prompt-side tokens here. API totals include output
            // tokens, which can make a long generation look like high context
            // pressure even when the prompt itself was small.
            const lastTurnPromptTokens = derivePromptTokens(lastRunPromptUsage);
            const tokenUsedRatio =
              lastTurnPromptTokens != null && ctxInfo.tokens > 0
                ? lastTurnPromptTokens / ctxInfo.tokens
                : 0;
            if (timeoutCompactionAttempts >= MAX_TIMEOUT_COMPACTION_ATTEMPTS) {
              log.warn(
                `[timeout-compaction] already attempted timeout compaction ${timeoutCompactionAttempts} time(s); falling through to failover rotation`,
              );
            } else if (tokenUsedRatio > 0.65) {
              const timeoutDiagId = createCompactionDiagId();
              timeoutCompactionAttempts++;
              log.warn(
                `[timeout-compaction] LLM timed out with high prompt token usage (${Math.round(tokenUsedRatio * 100)}%); ` +
                  `attempting compaction before retry (attempt ${timeoutCompactionAttempts}/${MAX_TIMEOUT_COMPACTION_ATTEMPTS}) diagId=${timeoutDiagId}`,
              );
              let timeoutCompactResult: Awaited<ReturnType<typeof contextEngine.compact>>;
              await runOwnsCompactionBeforeHook("timeout recovery");
              try {
                const timeoutCompactionRuntimeContext = {
                  ...buildEmbeddedCompactionRuntimeContext({
                    sessionKey: params.sessionKey,
                    messageChannel: params.messageChannel,
                    messageProvider: params.messageProvider,
                    agentAccountId: params.agentAccountId,
                    currentChannelId: params.currentChannelId,
                    currentThreadTs: params.currentThreadTs,
                    currentMessageId: params.currentMessageId,
                    authProfileId: lastProfileId,
                    workspaceDir: resolvedWorkspace,
                    agentDir,
                    config: params.config,
                    skillsSnapshot: params.skillsSnapshot,
                    senderIsOwner: params.senderIsOwner,
                    senderId: params.senderId,
                    provider,
                    modelId,
                    thinkLevel,
                    reasoningLevel: params.reasoningLevel,
                    bashElevated: params.bashElevated,
                    extraSystemPrompt: params.extraSystemPrompt,
                    ownerNumbers: params.ownerNumbers,
                  }),
                  runId: params.runId,
                  trigger: "timeout_recovery",
                  diagId: timeoutDiagId,
                  attempt: timeoutCompactionAttempts,
                  maxAttempts: MAX_TIMEOUT_COMPACTION_ATTEMPTS,
                };
                timeoutCompactResult = await contextEngine.compact({
                  sessionId: params.sessionId,
                  sessionKey: params.sessionKey,
                  sessionFile: params.sessionFile,
                  tokenBudget: ctxInfo.tokens,
                  force: true,
                  compactionTarget: "budget",
                  runtimeContext: timeoutCompactionRuntimeContext,
                });
              } catch (compactErr) {
                log.warn(
                  `[timeout-compaction] contextEngine.compact() threw during timeout recovery for ${provider}/${modelId}: ${String(compactErr)}`,
                );
                timeoutCompactResult = {
                  ok: false,
                  compacted: false,
                  reason: String(compactErr),
                };
              }
              await runOwnsCompactionAfterHook("timeout recovery", timeoutCompactResult);
              if (timeoutCompactResult.compacted) {
                autoCompactionCount += 1;
                if (contextEngine.info.ownsCompaction === true) {
                  await runPostCompactionSideEffects({
                    config: params.config,
                    sessionKey: params.sessionKey,
                    sessionFile: params.sessionFile,
                  });
                }
                log.info(
                  `[timeout-compaction] compaction succeeded for ${provider}/${modelId}; retrying prompt`,
                );
                continue;
              } else {
                log.warn(
                  `[timeout-compaction] compaction did not reduce context for ${provider}/${modelId}; falling through to normal handling`,
                );
              }
            }
          }

          const contextOverflowError = !aborted
            ? (() => {
                if (promptError) {
                  const errorText = describeUnknownError(promptError);
                  if (isLikelyContextOverflowError(errorText)) {
                    return { text: errorText, source: "promptError" as const };
                  }
                  // Prompt submission failed with a non-overflow error. Do not
                  // inspect prior assistant errors from history for this attempt.
                  return null;
                }
                if (assistantErrorText && isLikelyContextOverflowError(assistantErrorText)) {
                  return {
                    text: assistantErrorText,
                    source: "assistantError" as const,
                  };
                }
                return null;
              })()
            : null;

          if (contextOverflowError) {
            const overflowDiagId = createCompactionDiagId();
            const errorText = contextOverflowError.text;
            const msgCount = attempt.messagesSnapshot?.length ?? 0;
            const observedOverflowTokens = extractObservedOverflowTokenCount(errorText);
            log.warn(
              `[context-overflow-diag] sessionKey=${params.sessionKey ?? params.sessionId} ` +
                `provider=${provider}/${modelId} source=${contextOverflowError.source} ` +
                `messages=${msgCount} sessionFile=${params.sessionFile} ` +
                `diagId=${overflowDiagId} compactionAttempts=${overflowCompactionAttempts} ` +
                `observedTokens=${observedOverflowTokens ?? "unknown"} ` +
                `error=${errorText.slice(0, 200)}`,
            );
            const isCompactionFailure = isCompactionFailureError(errorText);
            const hadAttemptLevelCompaction = attemptCompactionCount > 0;
            // If this attempt already compacted (SDK auto-compaction), avoid immediately
            // running another explicit compaction for the same overflow trigger.
            if (
              !isCompactionFailure &&
              hadAttemptLevelCompaction &&
              overflowCompactionAttempts < MAX_OVERFLOW_COMPACTION_ATTEMPTS
            ) {
              overflowCompactionAttempts++;
              log.warn(
                `context overflow persisted after in-attempt compaction (attempt ${overflowCompactionAttempts}/${MAX_OVERFLOW_COMPACTION_ATTEMPTS}); retrying prompt without additional compaction for ${provider}/${modelId}`,
              );
              continue;
            }
            // Attempt explicit overflow compaction only when this attempt did not
            // already auto-compact.
            if (
              !isCompactionFailure &&
              !hadAttemptLevelCompaction &&
              overflowCompactionAttempts < MAX_OVERFLOW_COMPACTION_ATTEMPTS
            ) {
              if (log.isEnabled("debug")) {
                log.debug(
                  `[compaction-diag] decision diagId=${overflowDiagId} branch=compact ` +
                    `isCompactionFailure=${isCompactionFailure} hasOversizedToolResults=unknown ` +
                    `attempt=${overflowCompactionAttempts + 1} maxAttempts=${MAX_OVERFLOW_COMPACTION_ATTEMPTS}`,
                );
              }
              overflowCompactionAttempts++;
              log.warn(
                `context overflow detected (attempt ${overflowCompactionAttempts}/${MAX_OVERFLOW_COMPACTION_ATTEMPTS}); attempting auto-compaction for ${provider}/${modelId}`,
              );
              let compactResult: Awaited<ReturnType<typeof contextEngine.compact>>;
              await runOwnsCompactionBeforeHook("overflow recovery");
              try {
                const overflowCompactionRuntimeContext = {
                  ...buildEmbeddedCompactionRuntimeContext({
                    sessionKey: params.sessionKey,
                    messageChannel: params.messageChannel,
                    messageProvider: params.messageProvider,
                    agentAccountId: params.agentAccountId,
                    currentChannelId: params.currentChannelId,
                    currentThreadTs: params.currentThreadTs,
                    currentMessageId: params.currentMessageId,
                    authProfileId: lastProfileId,
                    workspaceDir: resolvedWorkspace,
                    agentDir,
                    config: params.config,
                    skillsSnapshot: params.skillsSnapshot,
                    senderIsOwner: params.senderIsOwner,
                    senderId: params.senderId,
                    provider,
                    modelId,
                    thinkLevel,
                    reasoningLevel: params.reasoningLevel,
                    bashElevated: params.bashElevated,
                    extraSystemPrompt: params.extraSystemPrompt,
                    ownerNumbers: params.ownerNumbers,
                  }),
                  runId: params.runId,
                  trigger: "overflow",
                  ...(observedOverflowTokens !== undefined
                    ? { currentTokenCount: observedOverflowTokens }
                    : {}),
                  diagId: overflowDiagId,
                  attempt: overflowCompactionAttempts,
                  maxAttempts: MAX_OVERFLOW_COMPACTION_ATTEMPTS,
                };
                compactResult = await contextEngine.compact({
                  sessionId: params.sessionId,
                  sessionKey: params.sessionKey,
                  sessionFile: params.sessionFile,
                  tokenBudget: ctxInfo.tokens,
                  ...(observedOverflowTokens !== undefined
                    ? { currentTokenCount: observedOverflowTokens }
                    : {}),
                  force: true,
                  compactionTarget: "budget",
                  runtimeContext: overflowCompactionRuntimeContext,
                });
                if (compactResult.ok && compactResult.compacted) {
                  await runContextEngineMaintenance({
                    contextEngine,
                    sessionId: params.sessionId,
                    sessionKey: params.sessionKey,
                    sessionFile: params.sessionFile,
                    reason: "compaction",
                    runtimeContext: overflowCompactionRuntimeContext,
                  });
                }
              } catch (compactErr) {
                log.warn(
                  `contextEngine.compact() threw during overflow recovery for ${provider}/${modelId}: ${String(compactErr)}`,
                );
                compactResult = {
                  ok: false,
                  compacted: false,
                  reason: String(compactErr),
                };
              }
              await runOwnsCompactionAfterHook("overflow recovery", compactResult);
              if (compactResult.compacted) {
                autoCompactionCount += 1;
                log.info(`auto-compaction succeeded for ${provider}/${modelId}; retrying prompt`);
                continue;
              }
              log.warn(
                `auto-compaction failed for ${provider}/${modelId}: ${compactResult.reason ?? "nothing to compact"}`,
              );
            }
            // Fallback: try truncating oversized tool results in the session.
            // This handles the case where a single tool result exceeds the
            // context window and compaction cannot reduce it further.
            if (!toolResultTruncationAttempted) {
              const contextWindowTokens = ctxInfo.tokens;
              const hasOversized = attempt.messagesSnapshot
                ? sessionLikelyHasOversizedToolResults({
                    messages: attempt.messagesSnapshot,
                    contextWindowTokens,
                  })
                : false;

              if (hasOversized) {
                if (log.isEnabled("debug")) {
                  log.debug(
                    `[compaction-diag] decision diagId=${overflowDiagId} branch=truncate_tool_results ` +
                      `isCompactionFailure=${isCompactionFailure} hasOversizedToolResults=${hasOversized} ` +
                      `attempt=${overflowCompactionAttempts} maxAttempts=${MAX_OVERFLOW_COMPACTION_ATTEMPTS}`,
                  );
                }
                toolResultTruncationAttempted = true;
                log.warn(
                  `[context-overflow-recovery] Attempting tool result truncation for ${provider}/${modelId} ` +
                    `(contextWindow=${contextWindowTokens} tokens)`,
                );
                const truncResult = await truncateOversizedToolResultsInSession({
                  sessionFile: params.sessionFile,
                  contextWindowTokens,
                  sessionId: params.sessionId,
                  sessionKey: params.sessionKey,
                });
                if (truncResult.truncated) {
                  log.info(
                    `[context-overflow-recovery] Truncated ${truncResult.truncatedCount} tool result(s); retrying prompt`,
                  );
                  // Do NOT reset overflowCompactionAttempts here — the global cap must remain
                  // enforced across all iterations to prevent unbounded compaction cycles (OC-65).
                  continue;
                }
                log.warn(
                  `[context-overflow-recovery] Tool result truncation did not help: ${truncResult.reason ?? "unknown"}`,
                );
              } else if (log.isEnabled("debug")) {
                log.debug(
                  `[compaction-diag] decision diagId=${overflowDiagId} branch=give_up ` +
                    `isCompactionFailure=${isCompactionFailure} hasOversizedToolResults=${hasOversized} ` +
                    `attempt=${overflowCompactionAttempts} maxAttempts=${MAX_OVERFLOW_COMPACTION_ATTEMPTS}`,
                );
              }
            }
            if (
              (isCompactionFailure ||
                overflowCompactionAttempts >= MAX_OVERFLOW_COMPACTION_ATTEMPTS ||
                toolResultTruncationAttempted) &&
              log.isEnabled("debug")
            ) {
              log.debug(
                `[compaction-diag] decision diagId=${overflowDiagId} branch=give_up ` +
                  `isCompactionFailure=${isCompactionFailure} hasOversizedToolResults=unknown ` +
                  `attempt=${overflowCompactionAttempts} maxAttempts=${MAX_OVERFLOW_COMPACTION_ATTEMPTS}`,
              );
            }
            const kind = isCompactionFailure ? "compaction_failure" : "context_overflow";
            return {
              payloads: [
                {
                  text:
                    "Context overflow: prompt too large for the model. " +
                    "Try /reset (or /new) to start a fresh session, or use a larger-context model.",
                  isError: true,
                },
              ],
              meta: {
                durationMs: Date.now() - started,
                agentMeta: buildErrorAgentMeta({
                  sessionId: sessionIdUsed,
                  provider,
                  model: model.id,
                  usageAccumulator,
                  lastRunPromptUsage,
                  lastAssistant,
                  lastTurnTotal,
                }),
                systemPromptReport: attempt.systemPromptReport,
                error: { kind, message: errorText },
              },
            };
          }

          if (promptError && !aborted) {
            // Normalize wrapped errors (e.g. abort-wrapped RESOURCE_EXHAUSTED) into
            // FailoverError so rate-limit classification works even for nested shapes.
            const normalizedPromptFailover = coerceToFailoverError(promptError, {
              provider: activeErrorContext.provider,
              model: activeErrorContext.model,
              profileId: lastProfileId,
            });
            const promptErrorDetails = normalizedPromptFailover
              ? describeFailoverError(normalizedPromptFailover)
              : describeFailoverError(promptError);
            const errorText = promptErrorDetails.message || describeUnknownError(promptError);
            if (await maybeRefreshRuntimeAuthForAuthError(errorText, runtimeAuthRetry)) {
              authRetryPending = true;
              continue;
            }
            // Handle role ordering errors with a user-friendly message
            if (/incorrect role information|roles must alternate/i.test(errorText)) {
              return {
                payloads: [
                  {
                    text:
                      "Message ordering conflict - please try again. " +
                      "If this persists, use /new to start a fresh session.",
                    isError: true,
                  },
                ],
                meta: {
                  durationMs: Date.now() - started,
                  agentMeta: buildErrorAgentMeta({
                    sessionId: sessionIdUsed,
                    provider,
                    model: model.id,
                    usageAccumulator,
                    lastRunPromptUsage,
                    lastAssistant,
                    lastTurnTotal,
                  }),
                  systemPromptReport: attempt.systemPromptReport,
                  error: { kind: "role_ordering", message: errorText },
                },
              };
            }
            // Handle image size errors with a user-friendly message (no retry needed)
            const imageSizeError = parseImageSizeError(errorText);
            if (imageSizeError) {
              const maxMb = imageSizeError.maxMb;
              const maxMbLabel =
                typeof maxMb === "number" && Number.isFinite(maxMb) ? `${maxMb}` : null;
              const maxBytesHint = maxMbLabel ? ` (max ${maxMbLabel}MB)` : "";
              return {
                payloads: [
                  {
                    text:
                      `Image too large for the model${maxBytesHint}. ` +
                      "Please compress or resize the image and try again.",
                    isError: true,
                  },
                ],
                meta: {
                  durationMs: Date.now() - started,
                  agentMeta: buildErrorAgentMeta({
                    sessionId: sessionIdUsed,
                    provider,
                    model: model.id,
                    usageAccumulator,
                    lastRunPromptUsage,
                    lastAssistant,
                    lastTurnTotal,
                  }),
                  systemPromptReport: attempt.systemPromptReport,
                  error: { kind: "image_size", message: errorText },
                },
              };
            }
            const promptFailoverReason =
              promptErrorDetails.reason ?? classifyFailoverReason(errorText, { provider });
            const promptProfileFailureReason =
              resolveAuthProfileFailureReason(promptFailoverReason);
            await maybeMarkAuthProfileFailure({
              profileId: lastProfileId,
              reason: promptProfileFailureReason,
              modelId,
            });
            const promptFailoverFailure =
              promptFailoverReason !== null || isFailoverErrorMessage(errorText, { provider });
            // Capture the failing profile before auth-profile rotation mutates `lastProfileId`.
            const failedPromptProfileId = lastProfileId;
            const logPromptFailoverDecision = createFailoverDecisionLogger({
              stage: "prompt",
              runId: params.runId,
              rawError: errorText,
              failoverReason: promptFailoverReason,
              profileFailureReason: promptProfileFailureReason,
              provider,
              model: modelId,
              profileId: failedPromptProfileId,
              fallbackConfigured,
              aborted,
            });
            if (promptFailoverReason === "rate_limit") {
              maybeEscalateRateLimitProfileFallback({
                failoverProvider: provider,
                failoverModel: modelId,
                logFallbackDecision: logPromptFailoverDecision,
              });
            }
            let promptFailoverDecision = resolveRunFailoverDecision({
              stage: "prompt",
              aborted,
              fallbackConfigured,
              failoverFailure: promptFailoverFailure,
              failoverReason: promptFailoverReason,
              profileRotated: false,
            });
            if (
              promptFailoverDecision.action === "rotate_profile" &&
              (await advanceAuthProfile())
            ) {
              lastRetryFailoverReason = mergeRetryFailoverReason({
                previous: lastRetryFailoverReason,
                failoverReason: promptFailoverReason,
              });
              logPromptFailoverDecision("rotate_profile");
              await maybeBackoffBeforeOverloadFailover(promptFailoverReason);
              continue;
            }
            if (promptFailoverDecision.action === "rotate_profile") {
              promptFailoverDecision = resolveRunFailoverDecision({
                stage: "prompt",
                aborted,
                fallbackConfigured,
                failoverFailure: promptFailoverFailure,
                failoverReason: promptFailoverReason,
                profileRotated: true,
              });
            }
            const fallbackThinking = pickFallbackThinkingLevel({
              message: errorText,
              attempted: attemptedThinking,
            });
            if (fallbackThinking) {
              log.warn(
                `unsupported thinking level for ${provider}/${modelId}; retrying with ${fallbackThinking}`,
              );
              thinkLevel = fallbackThinking;
              continue;
            }
            // Throw FailoverError for prompt-side failover reasons when fallbacks
            // are configured so outer model fallback can continue on overload,
            // rate-limit, auth, or billing failures.
            if (promptFailoverDecision.action === "fallback_model") {
              const fallbackReason = promptFailoverDecision.reason ?? "unknown";
              const status = resolveFailoverStatus(fallbackReason);
              logPromptFailoverDecision("fallback_model", { status });
              await maybeBackoffBeforeOverloadFailover(promptFailoverReason);
              throw (
                normalizedPromptFailover ??
                new FailoverError(errorText, {
                  reason: fallbackReason,
                  provider,
                  model: modelId,
                  profileId: lastProfileId,
                  status,
                })
              );
            }
            if (promptFailoverDecision.action === "surface_error") {
              logPromptFailoverDecision("surface_error");
            }
            throw promptError;
          }

          const fallbackThinking = pickFallbackThinkingLevel({
            message: lastAssistant?.errorMessage,
            attempted: attemptedThinking,
          });
          if (fallbackThinking && !aborted) {
            log.warn(
              `unsupported thinking level for ${provider}/${modelId}; retrying with ${fallbackThinking}`,
            );
            thinkLevel = fallbackThinking;
            continue;
          }

          const authFailure = isAuthAssistantError(lastAssistant);
          const rateLimitFailure = isRateLimitAssistantError(lastAssistant);
          const billingFailure = isBillingAssistantError(lastAssistant);
          const failoverFailure = isFailoverAssistantError(lastAssistant);
          const assistantFailoverReason = classifyFailoverReason(
            lastAssistant?.errorMessage ?? "",
            {
              provider: lastAssistant?.provider,
            },
          );
          const assistantProfileFailureReason =
            resolveAuthProfileFailureReason(assistantFailoverReason);
          const cloudCodeAssistFormatError = attempt.cloudCodeAssistFormatError;
          const imageDimensionError = parseImageDimensionError(lastAssistant?.errorMessage ?? "");
          // Capture the failing profile before auth-profile rotation mutates `lastProfileId`.
          const failedAssistantProfileId = lastProfileId;
          const logAssistantFailoverDecision = createFailoverDecisionLogger({
            stage: "assistant",
            runId: params.runId,
            rawError: lastAssistant?.errorMessage?.trim(),
            failoverReason: assistantFailoverReason,
            profileFailureReason: assistantProfileFailureReason,
            provider: activeErrorContext.provider,
            model: activeErrorContext.model,
            profileId: failedAssistantProfileId,
            fallbackConfigured,
            timedOut,
            aborted,
          });

          if (
            authFailure &&
            (await maybeRefreshRuntimeAuthForAuthError(
              lastAssistant?.errorMessage ?? "",
              runtimeAuthRetry,
            ))
          ) {
            authRetryPending = true;
            continue;
          }
          if (imageDimensionError && lastProfileId) {
            const details = [
              imageDimensionError.messageIndex !== undefined
                ? `message=${imageDimensionError.messageIndex}`
                : null,
              imageDimensionError.contentIndex !== undefined
                ? `content=${imageDimensionError.contentIndex}`
                : null,
              imageDimensionError.maxDimensionPx !== undefined
                ? `limit=${imageDimensionError.maxDimensionPx}px`
                : null,
            ]
              .filter(Boolean)
              .join(" ");
            log.warn(
              `Profile ${lastProfileId} rejected image payload${details ? ` (${details})` : ""}.`,
            );
          }

          const assistantFailoverDecision = resolveRunFailoverDecision({
            stage: "assistant",
            aborted,
            fallbackConfigured,
            failoverFailure,
            failoverReason: assistantFailoverReason,
            timedOut,
            timedOutDuringCompaction,
            profileRotated: false,
          });
          const assistantFailoverOutcome = await handleAssistantFailover({
            initialDecision: assistantFailoverDecision,
            aborted,
            fallbackConfigured,
            failoverFailure,
            failoverReason: assistantFailoverReason,
            timedOut,
            timedOutDuringCompaction,
            assistantProfileFailureReason,
            lastProfileId,
            modelId,
            provider,
            activeErrorContext,
            lastAssistant,
            config: params.config,
            sessionKey: params.sessionKey ?? params.sessionId,
            authFailure,
            rateLimitFailure,
            billingFailure,
            cloudCodeAssistFormatError,
            isProbeSession,
            overloadProfileRotations,
            overloadProfileRotationLimit,
            previousRetryFailoverReason: lastRetryFailoverReason,
            logAssistantFailoverDecision,
            warn: (message) => log.warn(message),
            maybeMarkAuthProfileFailure,
            maybeEscalateRateLimitProfileFallback,
            maybeBackoffBeforeOverloadFailover,
            advanceAuthProfile,
          });
          overloadProfileRotations = assistantFailoverOutcome.overloadProfileRotations;
          if (assistantFailoverOutcome.action === "retry") {
            lastRetryFailoverReason = assistantFailoverOutcome.lastRetryFailoverReason;
            continue;
          }
          if (assistantFailoverOutcome.action === "throw") {
            throw assistantFailoverOutcome.error;
          }

          const usageMeta = buildUsageAgentMetaFields({
            usageAccumulator,
            lastAssistantUsage: lastAssistant?.usage as UsageLike | undefined,
            lastRunPromptUsage,
            lastTurnTotal,
          });
          const agentMeta: EmbeddedPiAgentMeta = {
            sessionId: sessionIdUsed,
            provider: lastAssistant?.provider ?? provider,
            model: lastAssistant?.model ?? model.id,
            usage: usageMeta.usage,
            lastCallUsage: usageMeta.lastCallUsage,
            promptTokens: usageMeta.promptTokens,
            compactionCount: autoCompactionCount > 0 ? autoCompactionCount : undefined,
          };

          const payloads = buildEmbeddedRunPayloads({
            assistantTexts: attempt.assistantTexts,
            toolMetas: attempt.toolMetas,
            lastAssistant: attempt.lastAssistant,
            lastToolError: attempt.lastToolError,
            config: params.config,
            isCronTrigger: params.trigger === "cron",
            sessionKey: params.sessionKey ?? params.sessionId,
            provider: activeErrorContext.provider,
            model: activeErrorContext.model,
            verboseLevel: params.verboseLevel,
            reasoningLevel: params.reasoningLevel,
            toolResultFormat: resolvedToolResultFormat,
            suppressToolErrorWarnings: params.suppressToolErrorWarnings,
            inlineToolResultsAllowed: false,
            didSendViaMessagingTool: attempt.didSendViaMessagingTool,
            didSendDeterministicApprovalPrompt: attempt.didSendDeterministicApprovalPrompt,
          });

          // Timeout aborts can leave the run without any assistant payloads.
          // Emit an explicit timeout error instead of silently completing, so
          // callers do not lose the turn as an orphaned user message.
          if (timedOut && !timedOutDuringCompaction && payloads.length === 0) {
            return {
              payloads: [
                {
                  text:
                    "Request timed out before a response was generated. " +
                    "Please try again, or increase `agents.defaults.timeoutSeconds` in your config.",
                  isError: true,
                },
              ],
              meta: {
                durationMs: Date.now() - started,
                agentMeta,
                aborted,
                abortSource: resolvedAbortReason,
                abortReason: resolvedAbortReason,
                systemPromptReport: attempt.systemPromptReport,
              },
              didSendViaMessagingTool: attempt.didSendViaMessagingTool,
              didSendDeterministicApprovalPrompt: attempt.didSendDeterministicApprovalPrompt,
              messagingToolSentTexts: attempt.messagingToolSentTexts,
              messagingToolSentMediaUrls: attempt.messagingToolSentMediaUrls,
              messagingToolSentTargets: attempt.messagingToolSentTargets,
              successfulCronAdds: attempt.successfulCronAdds,
            };
          }

          // Detect incomplete turns where prompt() resolved prematurely and the
          // runner would otherwise drop an empty reply.
          const incompleteTurnText = resolveIncompleteTurnPayloadText({
            payloadCount: payloads.length,
            aborted,
            timedOut,
            attempt,
          });
          const nextPlanningOnlyRetryInstruction = resolvePlanningOnlyRetryInstruction({
            provider,
            modelId,
            aborted,
            timedOut,
            attempt,
          });
          if (
            !incompleteTurnText &&
            nextPlanningOnlyRetryInstruction &&
            planningOnlyRetryAttempts < 1
          ) {
            const planningOnlyText = attempt.assistantTexts.join("\n\n").trim();
            const planDetails = extractPlanningOnlyPlanDetails(planningOnlyText);
            if (planDetails) {
              emitAgentPlanEvent({
                runId: params.runId,
                ...(params.sessionKey ? { sessionKey: params.sessionKey } : {}),
                data: {
                  phase: "update",
                  title: "Assistant proposed a plan",
                  explanation: planDetails.explanation,
                  steps: planDetails.steps,
                  source: "planning_only_retry",
                },
              });
              void params.onAgentEvent?.({
                stream: "plan",
                data: {
                  phase: "update",
                  title: "Assistant proposed a plan",
                  explanation: planDetails.explanation,
                  steps: planDetails.steps,
                  source: "planning_only_retry",
                },
              });
            }
            planningOnlyRetryAttempts += 1;
            planningOnlyRetryInstruction = nextPlanningOnlyRetryInstruction;
            log.warn(
              `planning-only turn detected: runId=${params.runId} sessionId=${params.sessionId} ` +
                `provider=${provider}/${modelId} — retrying once with act-now steer`,
            );
            continue;
          }
          if (incompleteTurnText) {
            const incompleteStopReason = attempt.lastAssistant?.stopReason;
            log.warn(
              `incomplete turn detected: runId=${params.runId} sessionId=${params.sessionId} ` +
                `stopReason=${incompleteStopReason} payloads=0 — surfacing error to user`,
            );

            // Mark the failing profile for cooldown so multi-profile setups
            // rotate away from the exhausted credential on the next turn.
            if (lastProfileId) {
              await maybeMarkAuthProfileFailure({
                profileId: lastProfileId,
                reason: resolveAuthProfileFailureReason(assistantFailoverReason),
              });
            }

            return {
              payloads: [
                {
                  text: incompleteTurnText,
                  isError: true,
                },
              ],
              meta: {
                durationMs: Date.now() - started,
                agentMeta,
                aborted,
                systemPromptReport: attempt.systemPromptReport,
              },
              didSendViaMessagingTool: attempt.didSendViaMessagingTool,
              didSendDeterministicApprovalPrompt: attempt.didSendDeterministicApprovalPrompt,
              messagingToolSentTexts: attempt.messagingToolSentTexts,
              messagingToolSentMediaUrls: attempt.messagingToolSentMediaUrls,
              messagingToolSentTargets: attempt.messagingToolSentTargets,
              successfulCronAdds: attempt.successfulCronAdds,
            };
          }

          log.debug(
            `embedded run done: runId=${params.runId} sessionId=${params.sessionId} durationMs=${Date.now() - started} aborted=${aborted}`,
          );
          if (lastProfileId) {
            await markAuthProfileGood({
              store: authStore,
              provider,
              profileId: lastProfileId,
              agentDir: params.agentDir,
            });
            await markAuthProfileUsed({
              store: authStore,
              profileId: lastProfileId,
              agentDir: params.agentDir,
            });
          }
          return {
            payloads: payloads.length ? payloads : undefined,
            meta: {
              durationMs: Date.now() - started,
              agentMeta,
              aborted,
              abortSource: resolvedAbortReason,
              abortReason: resolvedAbortReason,
              systemPromptReport: attempt.systemPromptReport,
              // Handle client tool calls (OpenResponses hosted tools)
              // Propagate the LLM stop reason so callers (lifecycle events,
              // ACP bridge) can distinguish end_turn from max_tokens.
              stopReason: attempt.clientToolCall
                ? "tool_calls"
                : attempt.yieldDetected
                  ? "end_turn"
                  : (lastAssistant?.stopReason as string | undefined),
              pendingToolCalls: attempt.clientToolCall
                ? [
                    {
                      id: randomBytes(5).toString("hex").slice(0, 9),
                      name: attempt.clientToolCall.name,
                      arguments: JSON.stringify(attempt.clientToolCall.params),
                    },
                  ]
                : undefined,
            },
            didSendViaMessagingTool: attempt.didSendViaMessagingTool,
            didSendDeterministicApprovalPrompt: attempt.didSendDeterministicApprovalPrompt,
            messagingToolSentTexts: attempt.messagingToolSentTexts,
            messagingToolSentMediaUrls: attempt.messagingToolSentMediaUrls,
            messagingToolSentTargets: attempt.messagingToolSentTargets,
            successfulCronAdds: attempt.successfulCronAdds,
          };
        }
      } finally {
        await contextEngine.dispose?.();
        stopRuntimeAuthRefreshTimer();
        if (params.cleanupBundleMcpOnRunEnd === true) {
          await disposeSessionMcpRuntime(params.sessionId).catch((error) => {
            log.warn(
              `bundle-mcp cleanup failed after run for ${params.sessionId}: ${describeUnknownError(error)}`,
            );
          });
        }
      }
    });
  });
}
