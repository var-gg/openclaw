import { ChannelType } from "@buape/carbon";
import { resolveAckReaction, resolveHumanDelayConfig } from "../../agents/identity.js";
import { EmbeddedBlockChunker } from "../../agents/pi-embedded-block-chunker.js";
import { resolveChunkMode } from "../../auto-reply/chunk.js";
import { dispatchInboundMessage } from "../../auto-reply/dispatch.js";
import { formatInboundEnvelope, resolveEnvelopeFormatOptions } from "../../auto-reply/envelope.js";
import {
  buildPendingHistoryContextFromMap,
  clearHistoryEntriesIfEnabled,
} from "../../auto-reply/reply/history.js";
import { finalizeInboundContext } from "../../auto-reply/reply/inbound-context.js";
import { createReplyDispatcherWithTyping } from "../../auto-reply/reply/reply-dispatcher.js";
import type { AgentRunAbortContext, ReplyPayload } from "../../auto-reply/types.js";
import { shouldAckReaction as shouldAckReactionGate } from "../../channels/ack-reactions.js";
import { logTypingFailure, logAckFailure } from "../../channels/logging.js";
import { createReplyPrefixOptions } from "../../channels/reply-prefix.js";
import { recordInboundSession } from "../../channels/session.js";
import {
  createStatusReactionController,
  DEFAULT_TIMING,
  type StatusReactionAdapter,
} from "../../channels/status-reactions.js";
import { createTypingCallbacks } from "../../channels/typing.js";
import { isDangerousNameMatchingEnabled } from "../../config/dangerous-name-matching.js";
import { resolveDiscordPreviewStreamMode } from "../../config/discord-preview-streaming.js";
import { resolveMarkdownTableMode } from "../../config/markdown-tables.js";
import { readSessionUpdatedAt, resolveStorePath } from "../../config/sessions.js";
import { emitAgentEvent } from "../../infra/agent-events.js";
import { danger, logVerbose, shouldLogVerbose } from "../../globals.js";
import { convertMarkdownTables } from "../../markdown/tables.js";
import { buildAgentSessionKey } from "../../routing/resolve-route.js";
import { resolveThreadSessionKeys } from "../../routing/session-key.js";
import { stripReasoningTagsFromText } from "../../shared/text/reasoning-tags.js";
import { truncateUtf16Safe } from "../../utils.js";
import { chunkDiscordTextWithMode } from "../chunk.js";
import { resolveDiscordDraftStreamingChunking } from "../draft-chunking.js";
import { createDiscordDraftStream } from "../draft-stream.js";
import { reactMessageDiscord, removeReactionDiscord } from "../send.js";
import { editMessageDiscord } from "../send.messages.js";
import { normalizeDiscordSlug } from "./allow-list.js";
import { resolveTimestampMs } from "./format.js";
import { buildDiscordInboundAccessContext } from "./inbound-context.js";
import type { DiscordMessagePreflightContext } from "./message-handler.preflight.js";
import {
  buildDiscordMediaPayload,
  resolveDiscordMessageText,
  resolveForwardedMediaList,
  resolveMediaList,
} from "./message-utils.js";
import { buildDirectLabel, buildGuildLabel, resolveReplyContext } from "./reply-context.js";
import { deliverDiscordReply } from "./reply-delivery.js";
import { resolveDiscordAutoThreadReplyPlan, resolveDiscordThreadStarter } from "./threading.js";
import { sendTyping } from "./typing.js";

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}

type DiscordLifecycleEvent =
  | "run_start"
  | "run_end"
  | "run_abort"
  | "send_start"
  | "send_end"
  | "send_fail";

function emitDiscordLifecycleLog(params: {
  runtime: { log?: (...args: unknown[]) => void };
  event: DiscordLifecycleEvent;
  fields: {
    runId?: string;
    sessionKey?: string;
    channelId?: string;
    messageId?: string;
    threadId?: string | number;
    accountId?: string;
    source?: string;
    reason?: string;
    explicit?: boolean;
    kind?: string;
    queuedFinal?: boolean;
    finalQueuedCount?: number;
    finalSendAttempts?: number;
    finalSendSucceeded?: number;
    finalSendFailed?: number;
    error?: string;
  };
}): void {
  params.runtime.log?.(
    `discord.lifecycle ${JSON.stringify({
      event: params.event,
      ...params.fields,
    })}`,
  );
}

export async function processDiscordMessage(ctx: DiscordMessagePreflightContext) {
  const {
    cfg,
    discordConfig,
    accountId,
    token,
    runtime,
    guildHistories,
    historyLimit,
    mediaMaxBytes,
    textLimit,
    replyToMode,
    ackReactionScope,
    message,
    author,
    sender,
    data,
    client,
    channelInfo,
    channelName,
    messageChannelId,
    isGuildMessage,
    isDirectMessage,
    isGroupDm,
    baseText,
    messageText,
    shouldRequireMention,
    canDetectMention,
    effectiveWasMentioned,
    shouldBypassMention,
    threadChannel,
    threadParentId,
    threadParentName,
    threadParentType,
    threadName,
    displayChannelSlug,
    guildInfo,
    guildSlug,
    channelConfig,
    baseSessionKey,
    boundSessionKey,
    threadBindings,
    route,
    commandAuthorized,
  } = ctx;

  const mediaList = await resolveMediaList(message, mediaMaxBytes);
  const forwardedMediaList = await resolveForwardedMediaList(message, mediaMaxBytes);
  mediaList.push(...forwardedMediaList);
  const text = messageText;
  if (!text) {
    logVerbose(`discord: drop message ${message.id} (empty content)`);
    return;
  }
  const ackReaction = resolveAckReaction(cfg, route.agentId, {
    channel: "discord",
    accountId,
  });
  const removeAckAfterReply = cfg.messages?.removeAckAfterReply ?? false;
  const shouldAckReaction = () =>
    Boolean(
      ackReaction &&
      shouldAckReactionGate({
        scope: ackReactionScope,
        isDirect: isDirectMessage,
        isGroup: isGuildMessage || isGroupDm,
        isMentionableGroup: isGuildMessage,
        requireMention: Boolean(shouldRequireMention),
        canDetectMention,
        effectiveWasMentioned,
        shouldBypassMention,
      }),
    );
  const statusReactionsEnabled = shouldAckReaction();
  const discordAdapter: StatusReactionAdapter = {
    setReaction: async (emoji) => {
      await reactMessageDiscord(messageChannelId, message.id, emoji, {
        rest: client.rest as never,
      });
    },
    removeReaction: async (emoji) => {
      await removeReactionDiscord(messageChannelId, message.id, emoji, {
        rest: client.rest as never,
      });
    },
  };
  const statusReactions = createStatusReactionController({
    enabled: statusReactionsEnabled,
    adapter: discordAdapter,
    initialEmoji: ackReaction,
    onError: (err) => {
      logAckFailure({
        log: logVerbose,
        channel: "discord",
        target: `${messageChannelId}/${message.id}`,
        error: err,
      });
    },
  });
  if (statusReactionsEnabled) {
    void statusReactions.setQueued();
  }

  const fromLabel = isDirectMessage
    ? buildDirectLabel(author)
    : buildGuildLabel({
        guild: data.guild ?? undefined,
        channelName: channelName ?? messageChannelId,
        channelId: messageChannelId,
      });
  const senderLabel = sender.label;
  const isForumParent =
    threadParentType === ChannelType.GuildForum || threadParentType === ChannelType.GuildMedia;
  const forumParentSlug =
    isForumParent && threadParentName ? normalizeDiscordSlug(threadParentName) : "";
  const threadChannelId = threadChannel?.id;
  const isForumStarter =
    Boolean(threadChannelId && isForumParent && forumParentSlug) && message.id === threadChannelId;
  const forumContextLine = isForumStarter ? `[Forum parent: #${forumParentSlug}]` : null;
  const groupChannel = isGuildMessage && displayChannelSlug ? `#${displayChannelSlug}` : undefined;
  const groupSubject = isDirectMessage ? undefined : groupChannel;
  const senderName = sender.isPluralKit
    ? (sender.name ?? author.username)
    : (data.member?.nickname ?? author.globalName ?? author.username);
  const senderUsername = sender.isPluralKit
    ? (sender.tag ?? sender.name ?? author.username)
    : author.username;
  const senderTag = sender.tag;
  const { groupSystemPrompt, ownerAllowFrom, untrustedContext } = buildDiscordInboundAccessContext({
    channelConfig,
    guildInfo,
    sender: { id: sender.id, name: sender.name, tag: sender.tag },
    allowNameMatching: isDangerousNameMatchingEnabled(discordConfig),
    isGuild: isGuildMessage,
    channelTopic: channelInfo?.topic,
  });
  const storePath = resolveStorePath(cfg.session?.store, {
    agentId: route.agentId,
  });
  const envelopeOptions = resolveEnvelopeFormatOptions(cfg);
  const previousTimestamp = readSessionUpdatedAt({
    storePath,
    sessionKey: route.sessionKey,
  });
  let combinedBody = formatInboundEnvelope({
    channel: "Discord",
    from: fromLabel,
    timestamp: resolveTimestampMs(message.timestamp),
    body: text,
    chatType: isDirectMessage ? "direct" : "channel",
    senderLabel,
    previousTimestamp,
    envelope: envelopeOptions,
  });
  const shouldIncludeChannelHistory =
    !isDirectMessage && !(isGuildMessage && channelConfig?.autoThread && !threadChannel);
  if (shouldIncludeChannelHistory) {
    combinedBody = buildPendingHistoryContextFromMap({
      historyMap: guildHistories,
      historyKey: messageChannelId,
      limit: historyLimit,
      currentMessage: combinedBody,
      formatEntry: (entry) =>
        formatInboundEnvelope({
          channel: "Discord",
          from: fromLabel,
          timestamp: entry.timestamp,
          body: `${entry.body} [id:${entry.messageId ?? "unknown"} channel:${messageChannelId}]`,
          chatType: "channel",
          senderLabel: entry.sender,
          envelope: envelopeOptions,
        }),
    });
  }
  const replyContext = resolveReplyContext(message, resolveDiscordMessageText);
  if (forumContextLine) {
    combinedBody = `${combinedBody}\n${forumContextLine}`;
  }

  let threadStarterBody: string | undefined;
  let threadLabel: string | undefined;
  let parentSessionKey: string | undefined;
  if (threadChannel) {
    const includeThreadStarter = channelConfig?.includeThreadStarter !== false;
    if (includeThreadStarter) {
      const starter = await resolveDiscordThreadStarter({
        channel: threadChannel,
        client,
        parentId: threadParentId,
        parentType: threadParentType,
        resolveTimestampMs,
      });
      if (starter?.text) {
        // Keep thread starter as raw text; metadata is provided out-of-band in the system prompt.
        threadStarterBody = starter.text;
      }
    }
    const parentName = threadParentName ?? "parent";
    threadLabel = threadName
      ? `Discord thread #${normalizeDiscordSlug(parentName)} › ${threadName}`
      : `Discord thread #${normalizeDiscordSlug(parentName)}`;
    if (threadParentId) {
      parentSessionKey = buildAgentSessionKey({
        agentId: route.agentId,
        channel: route.channel,
        peer: { kind: "channel", id: threadParentId },
      });
    }
  }
  const mediaPayload = buildDiscordMediaPayload(mediaList);
  const threadKeys = resolveThreadSessionKeys({
    baseSessionKey,
    threadId: threadChannel ? messageChannelId : undefined,
    parentSessionKey,
    useSuffix: false,
  });
  const replyPlan = await resolveDiscordAutoThreadReplyPlan({
    client,
    message,
    messageChannelId,
    isGuildMessage,
    channelConfig,
    threadChannel,
    channelType: channelInfo?.type,
    baseText: baseText ?? "",
    combinedBody,
    replyToMode,
    agentId: route.agentId,
    channel: route.channel,
  });
  const deliverTarget = replyPlan.deliverTarget;
  const replyTarget = replyPlan.replyTarget;
  const replyReference = replyPlan.replyReference;
  const autoThreadContext = replyPlan.autoThreadContext;

  const effectiveFrom = isDirectMessage
    ? `discord:${author.id}`
    : (autoThreadContext?.From ?? `discord:channel:${messageChannelId}`);
  const effectiveTo = autoThreadContext?.To ?? replyTarget;
  if (!effectiveTo) {
    runtime.error?.(danger("discord: missing reply target"));
    return;
  }
  // Keep DM routes user-addressed so follow-up sends resolve direct session keys.
  const lastRouteTo = isDirectMessage ? `user:${author.id}` : effectiveTo;

  const inboundHistory =
    shouldIncludeChannelHistory && historyLimit > 0
      ? (guildHistories.get(messageChannelId) ?? []).map((entry) => ({
          sender: entry.sender,
          body: entry.body,
          timestamp: entry.timestamp,
        }))
      : undefined;

  const ctxPayload = finalizeInboundContext({
    Body: combinedBody,
    BodyForAgent: baseText ?? text,
    InboundHistory: inboundHistory,
    RawBody: baseText,
    CommandBody: baseText,
    From: effectiveFrom,
    To: effectiveTo,
    SessionKey: boundSessionKey ?? autoThreadContext?.SessionKey ?? threadKeys.sessionKey,
    AccountId: route.accountId,
    ChatType: isDirectMessage ? "direct" : "channel",
    ConversationLabel: fromLabel,
    SenderName: senderName,
    SenderId: sender.id,
    SenderUsername: senderUsername,
    SenderTag: senderTag,
    GroupSubject: groupSubject,
    GroupChannel: groupChannel,
    UntrustedContext: untrustedContext,
    GroupSystemPrompt: isGuildMessage ? groupSystemPrompt : undefined,
    GroupSpace: isGuildMessage ? (guildInfo?.id ?? guildSlug) || undefined : undefined,
    OwnerAllowFrom: ownerAllowFrom,
    Provider: "discord" as const,
    Surface: "discord" as const,
    WasMentioned: effectiveWasMentioned,
    MessageSid: message.id,
    ReplyToId: replyContext?.id,
    ReplyToBody: replyContext?.body,
    ReplyToSender: replyContext?.sender,
    ParentSessionKey: autoThreadContext?.ParentSessionKey ?? threadKeys.parentSessionKey,
    MessageThreadId: threadChannel?.id ?? autoThreadContext?.createdThreadId ?? undefined,
    ThreadStarterBody: threadStarterBody,
    ThreadLabel: threadLabel,
    Timestamp: resolveTimestampMs(message.timestamp),
    ...mediaPayload,
    CommandAuthorized: commandAuthorized,
    CommandSource: "text" as const,
    // Originating channel for reply routing.
    OriginatingChannel: "discord" as const,
    OriginatingTo: autoThreadContext?.OriginatingTo ?? replyTarget,
  });
  const persistedSessionKey = ctxPayload.SessionKey ?? route.sessionKey;
  let lifecycleRunId: string | undefined;
  let finalSendAttempts = 0;
  let finalSendSucceeded = 0;
  let finalSendFailed = 0;
  let fallbackFinalSent = false;
  const lifecycleCorrelation = {
    sessionKey: persistedSessionKey,
    channelId: messageChannelId,
    messageId: message.id,
    threadId: ctxPayload.MessageThreadId,
    accountId,
  };
  const emitLifecycle = (
    event: DiscordLifecycleEvent,
    fields: {
      runId?: string;
      source?: string;
      reason?: string;
      explicit?: boolean;
      kind?: string;
      queuedFinal?: boolean;
      finalQueuedCount?: number;
      finalSendAttempts?: number;
      finalSendSucceeded?: number;
      finalSendFailed?: number;
      error?: string;
    } = {},
  ) => {
    emitDiscordLifecycleLog({
      runtime,
      event,
      fields: {
        ...lifecycleCorrelation,
        runId: fields.runId ?? lifecycleRunId,
        ...fields,
      },
    });
  };

  await recordInboundSession({
    storePath,
    sessionKey: persistedSessionKey,
    ctx: ctxPayload,
    updateLastRoute: {
      sessionKey: persistedSessionKey,
      channel: "discord",
      to: lastRouteTo,
      accountId: route.accountId,
    },
    onRecordError: (err) => {
      logVerbose(`discord: failed updating session meta: ${String(err)}`);
    },
  });

  if (shouldLogVerbose()) {
    const preview = truncateUtf16Safe(combinedBody, 200).replace(/\n/g, "\\n");
    logVerbose(
      `discord inbound: channel=${messageChannelId} deliver=${deliverTarget} from=${ctxPayload.From} preview="${preview}"`,
    );
  }

  const typingChannelId = deliverTarget.startsWith("channel:")
    ? deliverTarget.slice("channel:".length)
    : messageChannelId;

  const { onModelSelected, ...prefixOptions } = createReplyPrefixOptions({
    cfg,
    agentId: route.agentId,
    channel: "discord",
    accountId: route.accountId,
  });
  const tableMode = resolveMarkdownTableMode({
    cfg,
    channel: "discord",
    accountId,
  });
  const chunkMode = resolveChunkMode(cfg, "discord", accountId);

  const typingCallbacks = createTypingCallbacks({
    start: () => sendTyping({ client, channelId: typingChannelId }),
    onStartError: (err) => {
      logTypingFailure({
        log: logVerbose,
        channel: "discord",
        target: typingChannelId,
        error: err,
      });
    },
  });

  // --- Discord draft stream (edit-based preview streaming) ---
  const discordStreamMode = resolveDiscordPreviewStreamMode(discordConfig);
  const draftMaxChars = Math.min(textLimit, 2000);
  const accountBlockStreamingEnabled =
    typeof discordConfig?.blockStreaming === "boolean"
      ? discordConfig.blockStreaming
      : cfg.agents?.defaults?.blockStreamingDefault === "on";
  const canStreamDraft = discordStreamMode !== "off" && !accountBlockStreamingEnabled;
  const draftReplyToMessageId = () => replyReference.use();
  const deliverChannelId = deliverTarget.startsWith("channel:")
    ? deliverTarget.slice("channel:".length)
    : messageChannelId;
  const draftStream = canStreamDraft
    ? createDiscordDraftStream({
        rest: client.rest,
        channelId: deliverChannelId,
        maxChars: draftMaxChars,
        replyToMessageId: draftReplyToMessageId,
        minInitialChars: 30,
        throttleMs: 1200,
        log: logVerbose,
        warn: logVerbose,
      })
    : undefined;
  const draftChunking =
    draftStream && discordStreamMode === "block"
      ? resolveDiscordDraftStreamingChunking(cfg, accountId)
      : undefined;
  const shouldSplitPreviewMessages = discordStreamMode === "block";
  const draftChunker = draftChunking ? new EmbeddedBlockChunker(draftChunking) : undefined;
  let lastPartialText = "";
  let draftText = "";
  let hasStreamedMessage = false;
  let finalizedViaPreviewMessage = false;

  const resolvePreviewFinalText = (text?: string) => {
    if (typeof text !== "string") {
      return undefined;
    }
    const formatted = convertMarkdownTables(text, tableMode);
    const chunks = chunkDiscordTextWithMode(formatted, {
      maxChars: draftMaxChars,
      maxLines: discordConfig?.maxLinesPerMessage,
      chunkMode,
    });
    if (!chunks.length && formatted) {
      chunks.push(formatted);
    }
    if (chunks.length !== 1) {
      return undefined;
    }
    const trimmed = chunks[0].trim();
    if (!trimmed) {
      return undefined;
    }
    const currentPreviewText = discordStreamMode === "block" ? draftText : lastPartialText;
    if (
      currentPreviewText &&
      currentPreviewText.startsWith(trimmed) &&
      trimmed.length < currentPreviewText.length
    ) {
      return undefined;
    }
    return trimmed;
  };

  const updateDraftFromPartial = (text?: string) => {
    if (!draftStream || !text) {
      return;
    }
    // Strip reasoning/thinking tags that may leak through the stream.
    const cleaned = stripReasoningTagsFromText(text, { mode: "strict", trim: "both" });
    // Skip pure-reasoning messages (e.g. "Reasoning:\n…") that contain no answer text.
    if (!cleaned || cleaned.startsWith("Reasoning:\n")) {
      return;
    }
    if (cleaned === lastPartialText) {
      return;
    }
    hasStreamedMessage = true;
    if (discordStreamMode === "partial") {
      // Keep the longer preview to avoid visible punctuation flicker.
      if (
        lastPartialText &&
        lastPartialText.startsWith(cleaned) &&
        cleaned.length < lastPartialText.length
      ) {
        return;
      }
      lastPartialText = cleaned;
      draftStream.update(cleaned);
      return;
    }

    let delta = cleaned;
    if (cleaned.startsWith(lastPartialText)) {
      delta = cleaned.slice(lastPartialText.length);
    } else {
      // Streaming buffer reset (or non-monotonic stream). Start fresh.
      draftChunker?.reset();
      draftText = "";
    }
    lastPartialText = cleaned;
    if (!delta) {
      return;
    }
    if (!draftChunker) {
      draftText = cleaned;
      draftStream.update(draftText);
      return;
    }
    draftChunker.append(delta);
    draftChunker.drain({
      force: false,
      emit: (chunk) => {
        draftText += chunk;
        draftStream.update(draftText);
      },
    });
  };

  const flushDraft = async () => {
    if (!draftStream) {
      return;
    }
    if (draftChunker?.hasBuffered()) {
      draftChunker.drain({
        force: true,
        emit: (chunk) => {
          draftText += chunk;
        },
      });
      draftChunker.reset();
      if (draftText) {
        draftStream.update(draftText);
      }
    }
    await draftStream.flush();
  };

  // When draft streaming is active, suppress block streaming to avoid double-streaming.
  const disableBlockStreamingForDraft = draftStream ? true : undefined;
  const sendDeliveryFailureFallback = async (trigger: "dispatch_error" | "send_fail") => {
    if (fallbackFinalSent || replyReference.hasReplied()) {
      return;
    }
    fallbackFinalSent = true;
    const fallbackPayload: ReplyPayload = {
      text:
        "I couldn't deliver the previous response due to a Discord send error. " +
        "Please resend your message.",
      isError: true,
    };
    const replyToId = replyReference.use();
    finalSendAttempts += 1;
    emitLifecycle("send_start", { kind: "fallback", reason: trigger });
    try {
      await deliverDiscordReply({
        replies: [fallbackPayload],
        target: deliverTarget,
        token,
        accountId,
        rest: client.rest,
        runtime,
        replyToId,
        replyToMode,
        textLimit,
        maxLinesPerMessage: discordConfig?.maxLinesPerMessage,
        tableMode,
        chunkMode,
        sessionKey: ctxPayload.SessionKey,
        threadBindings,
      });
      replyReference.markSent();
      finalSendSucceeded += 1;
      emitLifecycle("send_end", { kind: "fallback", reason: trigger });
    } catch (fallbackErr) {
      finalSendFailed += 1;
      emitLifecycle("send_fail", {
        kind: "fallback",
        reason: trigger,
        error: String(fallbackErr),
      });
      runtime.error?.(danger(`discord fallback reply failed: ${String(fallbackErr)}`));
    }
  };

  const { dispatcher, replyOptions, markDispatchIdle } = createReplyDispatcherWithTyping({
    ...prefixOptions,
    humanDelay: resolveHumanDelayConfig(cfg, route.agentId),
    deliver: async (payload: ReplyPayload, info) => {
      const isFinal = info.kind === "final";
      if (isFinal) {
        finalSendAttempts += 1;
        emitLifecycle("send_start", { kind: info.kind });
      }
      if (info.kind === "block") {
        // Block payloads carry reasoning/thinking content that should not be
        // delivered to external channels. Skip them regardless of streamMode.
        return;
      }
      try {
        if (draftStream && isFinal) {
          await flushDraft();
          const hasMedia = Boolean(payload.mediaUrl) || (payload.mediaUrls?.length ?? 0) > 0;
          const finalText = payload.text;
          const previewFinalText = resolvePreviewFinalText(finalText);
          const previewMessageId = draftStream.messageId();

          // Try to finalize via preview edit (text-only, fits in 2000 chars, not an error)
          const canFinalizeViaPreviewEdit =
            !finalizedViaPreviewMessage &&
            !hasMedia &&
            typeof previewFinalText === "string" &&
            typeof previewMessageId === "string" &&
            !payload.isError;

          if (canFinalizeViaPreviewEdit) {
            await draftStream.stop();
            try {
              await editMessageDiscord(
                deliverChannelId,
                previewMessageId,
                { content: previewFinalText },
                { rest: client.rest },
              );
              finalizedViaPreviewMessage = true;
              replyReference.markSent();
              if (isFinal) {
                finalSendSucceeded += 1;
                emitLifecycle("send_end", { kind: info.kind });
              }
              return;
            } catch (err) {
              logVerbose(
                `discord: preview final edit failed; falling back to standard send (${String(err)})`,
              );
            }
          }

          // Check if stop() flushed a message we can edit
          if (!finalizedViaPreviewMessage) {
            await draftStream.stop();
            const messageIdAfterStop = draftStream.messageId();
            if (
              typeof messageIdAfterStop === "string" &&
              typeof previewFinalText === "string" &&
              !hasMedia &&
              !payload.isError
            ) {
              try {
                await editMessageDiscord(
                  deliverChannelId,
                  messageIdAfterStop,
                  { content: previewFinalText },
                  { rest: client.rest },
                );
                finalizedViaPreviewMessage = true;
                replyReference.markSent();
                if (isFinal) {
                  finalSendSucceeded += 1;
                  emitLifecycle("send_end", { kind: info.kind });
                }
                return;
              } catch (err) {
                logVerbose(
                  `discord: post-stop preview edit failed; falling back to standard send (${String(err)})`,
                );
              }
            }
          }

          // Clear the preview and fall through to standard delivery
          if (!finalizedViaPreviewMessage) {
            await draftStream.clear();
          }
        }

        const replyToId = replyReference.use();
        await deliverDiscordReply({
          replies: [payload],
          target: deliverTarget,
          token,
          accountId,
          rest: client.rest,
          runtime,
          replyToId,
          replyToMode,
          textLimit,
          maxLinesPerMessage: discordConfig?.maxLinesPerMessage,
          tableMode,
          chunkMode,
          sessionKey: ctxPayload.SessionKey,
          threadBindings,
        });
        replyReference.markSent();
        if (isFinal) {
          finalSendSucceeded += 1;
          emitLifecycle("send_end", { kind: info.kind });
        }
      } catch (err) {
        if (isFinal) {
          finalSendFailed += 1;
          emitLifecycle("send_fail", { kind: info.kind, error: String(err) });
        }
        throw err;
      }
    },
    onError: (err, info) => {
      runtime.error?.(danger(`discord ${info.kind} reply failed: ${String(err)}`));
    },
    onReplyStart: async () => {
      await typingCallbacks.onReplyStart();
      await statusReactions.setThinking();
    },
  });

  let dispatchResult: Awaited<ReturnType<typeof dispatchInboundMessage>> | null = null;
  let dispatchError = false;
  let dispatchThrown: unknown;
  let didEmitRunStart = false;
  let didEmitTerminalLifecycleBackstop = false;
  const emitRunStart = (runId?: string) => {
    if (didEmitRunStart) {
      return;
    }
    didEmitRunStart = true;
    emitLifecycle("run_start", { runId });
  };
  const emitTerminalLifecycleBackstop = (phase: "end" | "error") => {
    if (didEmitTerminalLifecycleBackstop || !lifecycleRunId) {
      return;
    }
    didEmitTerminalLifecycleBackstop = true;
    emitAgentEvent({
      runId: lifecycleRunId,
      stream: "lifecycle",
      sessionKey: persistedSessionKey,
      data: {
        phase,
        ...(phase === "error" || dispatchThrown ? { error: String(dispatchThrown ?? "discord dispatch error") } : {}),
      },
    });
  };
  try {
    dispatchResult = await dispatchInboundMessage({
      ctx: ctxPayload,
      cfg,
      dispatcher,
      replyOptions: {
        ...replyOptions,
        onAgentRunStart: (runId) => {
          lifecycleRunId = runId;
          emitRunStart(runId);
        },
        onAgentRunAbort: (abortCtx: AgentRunAbortContext) => {
          lifecycleRunId = abortCtx.runId;
          emitLifecycle("run_abort", {
            runId: abortCtx.runId,
            source: abortCtx.source,
            reason: abortCtx.reason,
            explicit: abortCtx.explicit,
          });
        },
        skillFilter: channelConfig?.skills,
        disableBlockStreaming:
          disableBlockStreamingForDraft ??
          (typeof discordConfig?.blockStreaming === "boolean"
            ? !discordConfig.blockStreaming
            : undefined),
        onPartialReply: draftStream ? (payload) => updateDraftFromPartial(payload.text) : undefined,
        onAssistantMessageStart: draftStream
          ? () => {
              if (shouldSplitPreviewMessages && hasStreamedMessage) {
                logVerbose("discord: calling forceNewMessage() for draft stream");
                draftStream.forceNewMessage();
              }
              lastPartialText = "";
              draftText = "";
              draftChunker?.reset();
            }
          : undefined,
        onReasoningEnd: draftStream
          ? () => {
              if (shouldSplitPreviewMessages && hasStreamedMessage) {
                logVerbose("discord: calling forceNewMessage() for draft stream");
                draftStream.forceNewMessage();
              }
              lastPartialText = "";
              draftText = "";
              draftChunker?.reset();
            }
          : undefined,
        onModelSelected,
        onReasoningStream: async () => {
          await statusReactions.setThinking();
        },
        onToolStart: async (payload) => {
          await statusReactions.setTool(payload.name);
        },
      },
    });
  } catch (err) {
    dispatchError = true;
    dispatchThrown = err;
  } finally {
    // Must stop() first to flush debounced content before clear() wipes state
    await draftStream?.stop();
    if (!finalizedViaPreviewMessage) {
      await draftStream?.clear();
    }
    markDispatchIdle();
    if (statusReactionsEnabled) {
      if (dispatchError) {
        await statusReactions.setError();
      } else {
        await statusReactions.setDone();
      }
      if (removeAckAfterReply) {
        void (async () => {
          await sleep(dispatchError ? DEFAULT_TIMING.errorHoldMs : DEFAULT_TIMING.doneHoldMs);
          await statusReactions.clear();
        })();
      } else {
        void statusReactions.restoreInitial();
      }
    }
  }
  if (dispatchError && finalSendSucceeded === 0) {
    await sendDeliveryFailureFallback("dispatch_error");
  } else if (finalSendFailed > 0 && finalSendSucceeded === 0) {
    await sendDeliveryFailureFallback("send_fail");
  }
  emitRunStart(lifecycleRunId);
  emitLifecycle("run_end", {
    queuedFinal: dispatchResult?.queuedFinal ?? false,
    finalQueuedCount: dispatchResult?.counts.final ?? 0,
    finalSendAttempts,
    finalSendSucceeded,
    finalSendFailed,
    ...(dispatchThrown ? { error: String(dispatchThrown) } : {}),
  });
  emitTerminalLifecycleBackstop(dispatchThrown ? "error" : "end");
  if (dispatchThrown) {
    throw dispatchThrown;
  }

  if (!dispatchResult?.queuedFinal) {
    if (isGuildMessage) {
      clearHistoryEntriesIfEnabled({
        historyMap: guildHistories,
        historyKey: messageChannelId,
        limit: historyLimit,
      });
    }
    return;
  }
  if (shouldLogVerbose()) {
    const finalCount = dispatchResult.counts.final;
    logVerbose(
      `discord: delivered ${finalCount} reply${finalCount === 1 ? "" : "ies"} to ${replyTarget}`,
    );
  }
  if (isGuildMessage) {
    clearHistoryEntriesIfEnabled({
      historyMap: guildHistories,
      historyKey: messageChannelId,
      limit: historyLimit,
    });
  }
}
