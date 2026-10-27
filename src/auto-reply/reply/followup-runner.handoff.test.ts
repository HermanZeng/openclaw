import { randomUUID } from "node:crypto";
import os from "node:os";
import path from "node:path";
import { afterEach, assert, describe, expect, it } from "vitest";
import type {
  ReplyOperation,
  SessionControllerMailboxClaim,
} from "../../sessions/session-controller.contracts.js";
import { deferSessionControllerClaimBeforeExecution } from "../../sessions/session-controller.mailbox-claim.js";
import {
  claimSessionControllerInput,
  releaseSessionControllerClaim,
  retireSessionControllerInput,
} from "../../sessions/session-controller.mailbox.js";
import { createReplyOperation } from "../../sessions/session-controller.operation.js";
import { runAfterReplyOperationClear } from "../../sessions/session-controller.state.js";
import { createDeferredCore } from "../../shared/deferred.js";
import { createQueueTestRun } from "./queue.test-helpers.js";
import { scheduleFollowupDrain } from "./queue/drain.js";
import { enqueueFollowupRun } from "./queue/enqueue.js";
import { admitFollowupRunLifecycle, completeFollowupRunLifecycle } from "./queue/lifecycle.js";
import type { FollowupRun } from "./queue/types.js";
import {
  appendCompletedSourceReplyHandoffDirective,
  consumeQueuedReplyCompletionHandoff,
  publishReplyOperationCompletionHandoff,
  recordReplyOperationCompletedSourceReply,
} from "./reply-completion-handoff.js";

const MARKER = "immediately preceding turn already delivered";
const operations: ReplyOperation[] = [];
const claims: SessionControllerMailboxClaim[] = [];
const runs: FollowupRun[] = [];

function fixture(
  key = `agent:main:handoff-${randomUUID()}`,
  mode: "followup" | "collect" = "followup",
) {
  const storePath = path.join(os.tmpdir(), "openclaw-handoff", randomUUID(), "sessions.json");
  const queue = (prompt: string, overrides: Partial<FollowupRun> = {}) => {
    const source = createQueueTestRun(
      {
        prompt,
        transcriptPrompt: prompt,
        messageId: randomUUID(),
        currentInboundEventKind: "user_request",
        currentInboundContext: { text: `Context for ${prompt}` },
        originatingChannel: "telegram",
        originatingTo: "chat-1",
        originatingAccountId: "primary",
        originatingThreadId: 7,
        ...overrides,
      },
      {
        agentId: "main",
        sessionKey: key,
        sessionId: "session-1",
        messageProvider: "telegram",
        agentAccountId: "primary",
        sourceReplyDeliveryMode: "message_tool_only",
      },
    );
    runs.push(source);
    // The real enqueue owner records source identity and redelivery custody.
    // No dispatch callback is installed: tests select through the controller below.
    expect(
      enqueueFollowupRun(key, source, { mode, debounceMs: 0 }, "message-id", undefined, false, {
        storePath,
      }),
    ).toBe(true);
    return source;
  };
  return { key, queue };
}

async function own(source: FollowupRun, pending = claimSessionControllerInput(source)) {
  const claim = await pending;
  claims.push(claim);
  const sessionKey = source.run.sessionKey;
  assert(sessionKey);
  const operation = createReplyOperation({
    sessionKey,
    sessionId: source.run.sessionId,
    agentId: source.run.agentId,
    resetTriggered: false,
    mailboxClaim: claim,
  });
  operations.push(operation);
  return { source, claim, operation };
}

async function finish(
  turn: Awaited<ReturnType<typeof own>>,
  options: { delivered?: boolean; retry?: boolean } = {},
) {
  if (options.delivered) {
    recordReplyOperationCompletedSourceReply(turn.operation, turn.source);
  }
  runAfterReplyOperationClear(turn.operation, (admissionSessionId) =>
    publishReplyOperationCompletionHandoff({
      operation: turn.operation,
      source: turn.source,
      queueKey: turn.operation.key,
      admissionSessionId,
    }),
  );
  if (!options.retry) {
    completeFollowupRunLifecycle(turn.source, "consumed");
  }
  turn.operation.complete();
  await turn.operation.ownerSettlement;
  releaseSessionControllerClaim(turn.claim);
  await turn.claim.settlement.promise;
}

afterEach(async () => {
  const ownedOperations = operations.splice(0);
  const ownedClaims = claims.splice(0);
  for (const operation of ownedOperations) {
    operation.complete();
  }
  for (const claim of ownedClaims) {
    releaseSessionControllerClaim(claim);
  }
  for (const run of runs) {
    if (run.controllerInput) {
      retireSessionControllerInput(run.controllerInput);
    }
  }
  await Promise.all([
    ...ownedOperations.map((operation) => operation.ownerSettlement),
    ...ownedClaims.map((claim) => claim.settlement.promise),
    ...runs.splice(0).map((run) => run.controllerInput?.settlement.promise),
  ]);
});

describe("completed-reply facts through real controller input custody", () => {
  it("preserves inline and resumable context without mutating the source projection", () => {
    const original = {
      text: "Authenticated current request",
      resumableText: "Resumable current request",
      promptJoiner: " " as const,
    };
    const appended = appendCompletedSourceReplyHandoffDirective(original);
    expect(original).toEqual({
      text: "Authenticated current request",
      resumableText: "Resumable current request",
      promptJoiner: " ",
    });
    expect(appended).not.toBe(original);
    expect(appended.promptJoiner).toBe(" ");
    expect(appended.text.endsWith(original.text)).toBe(true);
    expect(appended.resumableText?.endsWith(original.resumableText)).toBe(true);
    expect(appended.text.split(MARKER)).toHaveLength(2);
    expect(appended.resumableText?.split(MARKER)).toHaveLength(2);
    expect(appended.fragments?.at(-1)).toMatchObject({ kind: "runtime-instruction" });
  });

  it("hands A to adopted B and B to prequeued C exactly once without rewriting requests", async () => {
    const f = fixture();
    const a = await own(f.queue("A"));
    await admitFollowupRunLifecycle(a.source);
    const bSource = f.queue("B");
    const cSource = f.queue("C");
    const bPending = claimSessionControllerInput(bSource);
    const cPending = claimSessionControllerInput(cSource);
    await finish(a, { delivered: true });
    const b = await own(bSource, bPending);
    await admitFollowupRunLifecycle(b.source);
    const predecessorA = consumeQueuedReplyCompletionHandoff(b.source, b.operation);
    assert(predecessorA);
    const contextB = appendCompletedSourceReplyHandoffDirective(b.source.currentInboundContext);
    expect(contextB.text.split(MARKER)).toHaveLength(2);
    expect(b.source.prompt).toBe("B");
    expect(b.source.transcriptPrompt).toBe("B");
    expect(b.source.currentInboundContext?.text).toBe("Context for B");
    expect(consumeQueuedReplyCompletionHandoff(b.source, b.operation)).toBeUndefined();
    await finish(b, { delivered: true });
    const c = await own(cSource, cPending);
    await admitFollowupRunLifecycle(c.source);
    const predecessorB = consumeQueuedReplyCompletionHandoff(c.source, c.operation);
    assert(predecessorB);
    expect(predecessorB).not.toBe(predecessorA);
    expect(
      appendCompletedSourceReplyHandoffDirective(c.source.currentInboundContext).text.split(MARKER),
    ).toHaveLength(2);
    expect(c.source.transcriptPrompt).toBe("C");
    await finish(c);
  });

  it("preserves A across a real pre-execution refusal and failed operation clear before B retries", async () => {
    const f = fixture();
    const a = await own(f.queue("A"));
    await admitFollowupRunLifecycle(a.source);
    const source = f.queue("B");
    const pending = claimSessionControllerInput(source);
    await finish(a, { delivered: true });
    const refused = await own(source, pending);
    expect(consumeQueuedReplyCompletionHandoff(source, refused.operation)).toBeUndefined();
    expect(deferSessionControllerClaimBeforeExecution(refused.claim)).toBe(true);
    refused.operation.fail("run_failed", new Error("preparation refused before source adoption"));
    await finish(refused, { retry: true });
    const retried = await own(source);
    expect(retried.claim).not.toBe(refused.claim);
    await admitFollowupRunLifecycle(source);
    expect(consumeQueuedReplyCompletionHandoff(source, retried.operation)).toBeDefined();
    await finish(retried);
  });

  it("uses the real collected claim's custody and publishes to its pending successor", async () => {
    const f = fixture(undefined, "collect");
    const a = await own(f.queue("A"));
    await admitFollowupRunLifecycle(a.source);
    const b = f.queue("B");
    const c = f.queue("C");
    const collected = createDeferredCore<{
      turn: Awaited<ReturnType<typeof own>>;
      successor: FollowupRun;
      successorClaim: ReturnType<typeof claimSessionControllerInput>;
    }>();
    scheduleFollowupDrain(
      f.key,
      async (source) => {
        try {
          const claim = source.controllerClaim;
          assert(claim);
          expect(claim.sources).toEqual([b, c]);
          expect(claim.inputs).toHaveLength(2);
          const turn = await own(source, Promise.resolve(claim));
          await admitFollowupRunLifecycle(source);
          expect(claim.custody.adopted).toBe(true);
          expect(consumeQueuedReplyCompletionHandoff(source, turn.operation)).toBeDefined();
          const successor = f.queue("D");
          const successorClaim = claimSessionControllerInput(successor);
          recordReplyOperationCompletedSourceReply(turn.operation, source);
          runAfterReplyOperationClear(turn.operation, (admissionSessionId) =>
            publishReplyOperationCompletionHandoff({
              operation: turn.operation,
              source,
              queueKey: f.key,
              admissionSessionId,
            }),
          );
          completeFollowupRunLifecycle(source, "consumed");
          turn.operation.complete();
          await turn.operation.ownerSettlement;
          // The real drain releases its selected claim after this callback returns.
          // Waiting for claim settlement here would await this callback's own cleanup.
          collected.resolve({ turn, successor, successorClaim });
        } catch (error) {
          collected.reject(error);
          throw error;
        }
      },
      a.claim.mailbox.owner,
    );
    await finish(a, { delivered: true });
    const { turn, successor, successorClaim } = await collected.promise;
    await turn.claim.settlement.promise;
    const d = await own(successor, successorClaim);
    await admitFollowupRunLifecycle(successor);
    expect(consumeQueuedReplyCompletionHandoff(successor, d.operation)).toBeDefined();
    await finish(d);
  });

  it("does not replace A when main consumes an unadopted preparation failure", async () => {
    const f = fixture();
    const a = await own(f.queue("A"));
    await admitFollowupRunLifecycle(a.source);
    const bSource = f.queue("B");
    const cSource = f.queue("C");
    const bPending = claimSessionControllerInput(bSource);
    const cPending = claimSessionControllerInput(cSource);
    await finish(a, { delivered: true });
    const b = await own(bSource, bPending);
    b.operation.fail("run_failed", new Error("unformatted preflight failure"));
    await finish(b);
    const c = await own(cSource, cPending);
    await admitFollowupRunLifecycle(c.source);
    expect(consumeQueuedReplyCompletionHandoff(c.source, c.operation)).toBeDefined();
    await finish(c);
  });

  it("does replace A after an adopted source genuinely fails without delivery", async () => {
    const f = fixture();
    const a = await own(f.queue("A"));
    await admitFollowupRunLifecycle(a.source);
    const bSource = f.queue("B");
    const cSource = f.queue("C");
    const bPending = claimSessionControllerInput(bSource);
    const cPending = claimSessionControllerInput(cSource);
    await finish(a, { delivered: true });
    const b = await own(bSource, bPending);
    await admitFollowupRunLifecycle(b.source);
    b.operation.fail("run_failed", new Error("terminal failure after source adoption"));
    await finish(b);
    const c = await own(cSource, cPending);
    await admitFollowupRunLifecycle(c.source);
    expect(consumeQueuedReplyCompletionHandoff(c.source, c.operation)).toBeUndefined();
    await finish(c);
  });

  it("does not give an ordinary later input A's fact when redelivery dedupe retains the same mailbox", async () => {
    const f = fixture();
    const a = await own(f.queue("A"));
    await admitFollowupRunLifecycle(a.source);
    const abandoned = f.queue("queued then canceled");
    const mailbox = a.claim.mailbox;
    await finish(a, { delivered: true });
    assert(abandoned.controllerInput);
    retireSessionControllerInput(abandoned.controllerInput);
    await abandoned.controllerInput.settlement.promise;
    expect(mailbox.recentSources.size).toBeGreaterThan(0);
    const ordinary = f.queue("later ordinary request");
    expect(ordinary.controllerInput?.mailbox).toBe(mailbox);
    const next = await own(ordinary);
    await admitFollowupRunLifecycle(ordinary);
    expect(consumeQueuedReplyCompletionHandoff(ordinary, next.operation)).toBeUndefined();
    await finish(next);
  });

  it.each([
    { originatingAccountId: "other" },
    { originatingTo: "chat-2" },
    { originatingThreadId: 8 },
    { currentInboundEventKind: "room_event" as const },
  ])("does not hand a source reply to a different recipient: %j", async (mismatch) => {
    const f = fixture();
    const a = await own(f.queue("A"));
    await admitFollowupRunLifecycle(a.source);
    const source = f.queue("B", mismatch);
    const pending = claimSessionControllerInput(source);
    await finish(a, { delivered: true });
    const b = await own(source, pending);
    await admitFollowupRunLifecycle(source);
    expect(consumeQueuedReplyCompletionHandoff(source, b.operation)).toBeUndefined();
    await finish(b);
  });
});
