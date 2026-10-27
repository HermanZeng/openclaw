import { normalizeOptionalString } from "@openclaw/normalization-core/string-coerce";
import { appendCurrentInboundContext } from "../../agents/embedded-agent-runner/run/runtime-context-prompt.js";
import type { CurrentInboundPromptContext } from "../../agents/internal-runtime-context.js";
import { normalizeChatType } from "../../channels/chat-type.js";
import { isAgentEventLifecycleGenerationCurrent } from "../../infra/agent-events.js";
import { channelRouteDedupeKey } from "../../plugin-sdk/channel-route.js";
import type {
  ReplyOperation,
  SessionControllerInput,
  SessionControllerMailbox,
} from "../../sessions/session-controller.contracts.js";
import { isSessionControllerSourceQueued } from "../../sessions/session-controller.mailbox-source.js";
import { getSessionControllerEntryForOperation } from "../../sessions/session-controller.state.js";
import { resolveFollowupReplyAnchor } from "./queue/delivery-context.js";
import type { FollowupRun } from "./queue/types.js";

/** Immutable process-local fact minted by the owner that completed a source reply. */
type ReplyCompletionHandoff = Readonly<{
  kind: "completed_source_reply";
  ownerKey: string;
  ownerSessionId: string;
  ownerLifecycleGeneration: string;
  deliveryTargetKey: string;
}>;

type OperationCompletion =
  | { kind: "recorded"; handoff: ReplyCompletionHandoff }
  | { kind: "published" };

type QueuedPredecessor = {
  handoff: ReplyCompletionHandoff;
  recipients: WeakSet<SessionControllerInput>;
};

const completionsByOperation = new WeakMap<ReplyOperation, OperationCompletion>();
const predecessorsByMailbox = new WeakMap<SessionControllerMailbox, QueuedPredecessor>();

/** Fields that can change where or how a queued reply is delivered. */
function resolveFollowupReplyDeliveryTargetKey(run: FollowupRun): string {
  return JSON.stringify([
    channelRouteDedupeKey({
      channel: run.originatingChannel ?? run.run.messageProvider,
      to: run.originatingTo,
      accountId: run.originatingAccountId ?? run.run.agentAccountId,
      threadId: run.originatingThreadId,
    }),
    normalizeOptionalString(run.originatingChatId) ?? "",
    resolveFollowupReplyAnchor(run) ?? "",
    run.originatingReplyToMode ?? "",
    normalizeChatType(run.originatingChatType ?? run.run.chatType) ?? "",
  ]);
}

const COMPLETED_SOURCE_REPLY_HANDOFF_DIRECTIVE =
  "The immediately preceding turn already delivered its final user-visible reply via the `message` tool. Do not replay it merely to finish that preceding turn. Treat the current queued content as a fresh request, answer it normally, and send a new reply whenever it warrants one—including when it explicitly asks to repeat or discuss the prior reply.";

/** Records settled delivery evidence, not transcript guesses or planned sends. */
export function recordReplyOperationCompletedSourceReply(
  operation: ReplyOperation,
  source: FollowupRun,
): void {
  const lifecycleGeneration = operation.lifecycleGeneration;
  if (
    operation.result ||
    !lifecycleGeneration ||
    !isAgentEventLifecycleGenerationCurrent(lifecycleGeneration)
  ) {
    return;
  }
  completionsByOperation.set(operation, {
    kind: "recorded",
    handoff: Object.freeze({
      kind: "completed_source_reply",
      ownerKey: operation.key,
      ownerSessionId: operation.sessionId,
      ownerLifecycleGeneration: lifecycleGeneration,
      deliveryTargetKey: resolveFollowupReplyDeliveryTargetKey(source),
    }),
  });
}

/** Claims one operation's update; duplicate clear callbacks cannot erase its successor. */
function takeReplyOperationCompletionHandoff(params: {
  operation: ReplyOperation;
  queueKey: string;
  admissionSessionId: string;
}): { kind: "claimed"; handoff?: ReplyCompletionHandoff } | { kind: "ignored" } {
  const completion = completionsByOperation.get(params.operation);
  if (
    completion?.kind === "published" ||
    params.operation.key !== params.queueKey ||
    params.operation.sessionId !== params.admissionSessionId
  ) {
    return { kind: "ignored" };
  }
  completionsByOperation.set(params.operation, { kind: "published" });
  const handoff = completion?.handoff;
  if (
    params.operation.result?.kind !== "completed" ||
    !handoff ||
    params.operation.key !== handoff.ownerKey ||
    params.operation.sessionId !== handoff.ownerSessionId ||
    !isAgentEventLifecycleGenerationCurrent(handoff.ownerLifecycleGeneration)
  ) {
    return { kind: "claimed" };
  }
  return { kind: "claimed", handoff };
}

/** Publish only an adopted source's predecessor update to its exact physical mailbox. */
export function publishReplyOperationCompletionHandoff(params: {
  operation: ReplyOperation;
  source: FollowupRun;
  queueKey: string;
  admissionSessionId: string;
}): void {
  const input = params.source.controllerInput;
  const custody = params.source.controllerClaim?.custody ?? input?.custody;
  // Source custody is not inferred from terminal disposition: preparation can
  // finish as consumed without ever adopting input or executing its request.
  if (!input || !custody?.adopted) {
    return;
  }
  const mailbox = getSessionControllerEntryForOperation(params.operation).mailbox;
  if (!mailbox || input.mailbox !== mailbox) {
    return;
  }
  const completion = takeReplyOperationCompletionHandoff(params);
  if (completion.kind === "ignored") {
    return;
  }
  predecessorsByMailbox.delete(mailbox);
  if (!completion.handoff || mailbox.abortController.signal.aborted) {
    return;
  }
  // An empty mailbox can survive for redelivery dedupe. Capture existing input
  // identities so an ordinary later turn cannot inherit this queued-only fact.
  const recipients = mailbox.entries.filter(
    (candidate) =>
      isSessionControllerSourceQueued(candidate) &&
      !candidate.abortSignal.aborted &&
      !candidate.retirementRequested,
  );
  if (recipients.length > 0) {
    predecessorsByMailbox.set(mailbox, {
      handoff: completion.handoff,
      recipients: new WeakSet(recipients),
    });
  }
}

/** Consume after actual source adoption, never during retryable preparation. */
export function consumeQueuedReplyCompletionHandoff(
  queued: FollowupRun,
  operation: ReplyOperation,
): ReplyCompletionHandoff | undefined {
  const claim = queued.controllerClaim ?? queued.controllerInput?.claim;
  const custody = queued.controllerClaim?.custody ?? queued.controllerInput?.custody;
  if (!claim || claim.operation !== operation || !custody?.adopted || operation.result) {
    return undefined;
  }
  const mailbox = getSessionControllerEntryForOperation(operation).mailbox;
  if (!mailbox || claim.mailbox !== mailbox) {
    return undefined;
  }
  const predecessor = predecessorsByMailbox.get(mailbox);
  if (!predecessor) {
    return undefined;
  }
  predecessorsByMailbox.delete(mailbox);
  const handoff = predecessor.handoff;
  return !mailbox.abortController.signal.aborted &&
    claim.inputs.some((input) => predecessor.recipients.has(input)) &&
    operation.key === handoff.ownerKey &&
    operation.sessionId === handoff.ownerSessionId &&
    operation.lifecycleGeneration === handoff.ownerLifecycleGeneration &&
    isAgentEventLifecycleGenerationCurrent(handoff.ownerLifecycleGeneration) &&
    queued.run.sessionKey === handoff.ownerKey &&
    (queued.admissionSessionId ?? queued.run.sessionId) === handoff.ownerSessionId &&
    queued.currentInboundEventKind !== "room_event" &&
    handoff.deliveryTargetKey === resolveFollowupReplyDeliveryTargetKey(queued)
    ? handoff
    : undefined;
}

/** Runtime fragments preserve prompt layout and never change user-authored transcript bytes. */
export function appendCompletedSourceReplyHandoffDirective(
  context: CurrentInboundPromptContext | undefined,
): CurrentInboundPromptContext {
  return appendCurrentInboundContext(context, [
    { kind: "runtime-instruction", text: COMPLETED_SOURCE_REPLY_HANDOFF_DIRECTIVE },
  ]);
}
