import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import "../agents/prepared-model-runtime.js";
import "./server-start.js";
import { expect, it } from "vitest";
import { withinTest } from "../../test/helpers/promise.js";
import { runQaGatewayFixture } from "../../test/helpers/qa-gateway-cleanup.js";
import { withFullRuntimeReplyConfig } from "../auto-reply/reply/get-reply-fast-path.js";
import { dispatchReplyWithBufferedBlockDispatcherCore } from "../auto-reply/reply/provider-dispatcher.js";
import { buildTestCtx } from "../auto-reply/reply/test-ctx.js";
import {
  bindIngressLifecycleToReplyOptions,
  createChannelIngressDrain,
} from "../channels/message/ingress-drain.js";
import { createTestIngressQueue } from "../channels/message/ingress-drain.test-helpers.js";
import type { OpenClawConfig } from "../config/types.openclaw.js";
import { waitForGatewayActiveWork } from "../infra/gateway-active-work.js";
import { createDeferredCore } from "../shared/deferred.js";
import { withOpenClawTestState } from "../test-utils/openclaw-test-state.js";
import { createChatFixture } from "./gateway-chat-fixture.test-support.js";
import { writeToolCall } from "./gateway-provider-fixture.test-support.js";
import { KERNEL_TEST_ENV } from "./test-helpers.env.js";

const HANDOFF = "immediately preceding turn already delivered";

type Message = { role?: string; content?: string | Array<{ type?: string; text?: string }> };
function messageText(message: Message): string {
  return typeof message.content === "string"
    ? message.content
    : (message.content ?? [])
        .map((part) =>
          part.type === "text" || part.type === "input_text" ? (part.text ?? "") : "",
        )
        .join("\n");
}

// Real channel dispatcher, durable ingress, HTTP provider and internal source-reply
// persistence. This is not public chat.send message-tool mode or Telegram live proof.
it(
  "passes one completed-predecessor fragment through queued provider requests and never replays input",
  { timeout: 120_000 },
  async ({ signal }) => {
    const scenarios = [true, false].map((sourceDelivered) => {
      const nonce = randomUUID();
      return {
        sourceDelivered,
        sessionKey: "",
        executionOrder: [] as string[],
        firstReached: createDeferredCore(),
        releaseFirst: createDeferredCore(),
        turns: ["A", "B", "C"].map((label, index) => ({
          label,
          id: randomUUID(),
          prompt: `FRESH_${label}_${nonce}: Answer this current request only.`,
          reply: `VISIBLE_${label}_${nonce}`,
          quiet: index === 0 && !sourceDelivered,
          directiveCount: index === 0 ? 0 : index === 1 ? Number(sourceDelivered) : 1,
          deferred: createDeferredCore(),
          providerRequests: [] as string[],
        })),
      };
    });
    const failed = createDeferredCore<never>();
    void failed.promise.catch(() => {});
    const wait = <T>(work: Promise<T>) => withinTest(Promise.race([work, failed.promise]), signal);
    await withOpenClawTestState(
      {
        label: "completed-reply-handoff",
        env: {
          ...KERNEL_TEST_ENV,
          VITEST: process.env.VITEST,
          OPENCLAW_TEST_MINIMAL_GATEWAY: undefined,
          OPENCLAW_DISABLE_BUNDLED_PLUGINS: "1",
        },
      },
      async (state) => {
        let config: OpenClawConfig | undefined;
        const automaticPayloads: unknown[] = [];
        const fixture = createChatFixture("completed-reply-handoff", {
          configure: (base) => {
            config = withFullRuntimeReplyConfig({
              ...base,
              agents: { ...base.agents, ownership: "explicit", entries: { main: {} } },
              plugins: { enabled: false, slots: { memory: "none" } },
              tools: { allow: ["message"], toolSearch: false, codeMode: { enabled: false } },
              messages: { queue: { mode: "followup", debounceMsByChannel: { webchat: 0 } } },
            });
            return config;
          },
          respond: async (serialized, _sessionId, response) => {
            try {
              if (serialized.includes("conversationLabelSource")) {
                return "Synthetic handoff";
              }
              const body = JSON.parse(serialized) as { input?: Message[] };
              const newest = (body.input ?? [])
                .filter((message) => message.role === "user")
                .map(messageText)
                .findLast((text) =>
                  scenarios.some((scenario) =>
                    scenario.turns.some((turn) => text.includes(turn.prompt)),
                  ),
                );
              const scenario = scenarios.find((candidate) =>
                candidate.turns.some((turn) => newest?.includes(turn.prompt)),
              );
              const turn = scenario?.turns.find((candidate) => newest?.includes(candidate.prompt));
              assert(
                scenario && turn && newest,
                "the provider must receive the actual current source request",
              );
              turn.providerRequests.push(serialized);
              scenario.executionOrder.push(turn.label);
              expect(turn.providerRequests).toHaveLength(1);
              expect(newest).toContain(turn.prompt);
              // This custom Responses fixture uses transient carrier replay. Append-only
              // providers must retain their historical carriers and need separately scoped counts.
              expect(serialized.split(HANDOFF).length - 1).toBe(turn.directiveCount);
              if (turn.label === "A") {
                scenario.firstReached.resolve();
                await scenario.releaseFirst.promise;
              }
              if (turn.quiet) {
                return "NO_REPLY";
              }
              // The real message tool and host delivery owner must establish completion.
              // A synthetic completedSourceDelivery result would bypass the defect.
              writeToolCall(response, "message", {
                action: "send",
                message: turn.reply,
                final: true,
              });
              return undefined;
            } catch (error) {
              failed.reject(error);
              throw error;
            }
          },
        });
        const queue = createTestIngressQueue(state.stateDir);
        const createDrain = () =>
          createChannelIngressDrain({
            queue,
            // Several accepted transport messages may wait in the one session mailbox.
            deferredLaneOccupancy: "release",
            dispatchClaimedEvent: async (event, lifecycle) => {
              const scenario = scenarios.find((candidate) =>
                candidate.turns.some((turn) => turn.id === event.id),
              );
              const turn = scenario?.turns.find((candidate) => candidate.id === event.id);
              assert(config && scenario && turn);
              const result = await dispatchReplyWithBufferedBlockDispatcherCore({
                cfg: config,
                ctx: buildTestCtx({
                  AgentId: "main",
                  Provider: "webchat",
                  Surface: "webchat",
                  From: "webchat:synthetic-user",
                  To: "webchat:synthetic-conversation",
                  OriginatingChannel: "webchat",
                  OriginatingTo: "webchat:synthetic-conversation",
                  SessionKey: scenario.sessionKey,
                  MessageSid: event.id,
                  Body: event.payload.text,
                  RawBody: event.payload.text,
                  CommandBody: event.payload.text,
                  CommandAuthorized: true,
                  ChatType: "direct",
                  InboundEventKind: turn.quiet ? "room_event" : "user_request",
                }),
                dispatcherOptions: {
                  deliver: async (payload) => {
                    automaticPayloads.push(payload);
                  },
                },
                replyOptions: {
                  ...bindIngressLifecycleToReplyOptions(lifecycle),
                  sourceReplyDeliveryMode: "message_tool_only",
                },
              });
              if (result.deferredToActiveRun) {
                expect(result.deferredToActiveRun).toBe("followup");
                turn.deferred.resolve();
                return { kind: "deferred" };
              }
              return { kind: "completed" };
            },
          });
        let drain: ReturnType<typeof createDrain> | undefined;
        await runQaGatewayFixture(
          async () => {
            await fixture.start();
            const activeDrain = createDrain();
            drain = activeDrain;
            for (const scenario of scenarios) {
              const created = await fixture.client.request<{ key: string }>("sessions.create", {
                agentId: "main",
                label: "Queued source handoff",
              });
              scenario.sessionKey = created.key;
              const first = scenario.turns[0]!;
              await queue.enqueue(
                first.id,
                { text: first.prompt },
                { laneKey: scenario.sessionKey },
              );
              expect(await activeDrain.drainOnce()).toEqual({ started: 1 });
              await wait(scenario.firstReached.promise);
              for (const turn of scenario.turns.slice(1)) {
                await queue.enqueue(
                  turn.id,
                  { text: turn.prompt },
                  { laneKey: scenario.sessionKey },
                );
                expect(await activeDrain.drainOnce()).toEqual({ started: 1 });
                await wait(turn.deferred.promise);
              }
              expect(scenario.turns.map((turn) => turn.providerRequests.length)).toEqual([1, 0, 0]);
              scenario.releaseFirst.resolve();
              await wait(activeDrain.waitForIdle());
              const drained = await wait(waitForGatewayActiveWork(30_000));
              expect(drained.drained, JSON.stringify(drained.snapshot.blockers)).toBe(true);
              expect(await queue.listClaims()).toEqual([]);
              expect(scenario.turns.map((turn) => turn.providerRequests.length)).toEqual([1, 1, 1]);
              expect(scenario.executionOrder).toEqual(["A", "B", "C"]);
              const history = await fixture.client.request<{ messages: Message[] }>(
                "chat.history",
                { sessionKey: scenario.sessionKey, limit: 100 },
              );
              const visible = history.messages
                .filter((message) => message.role === "assistant")
                .map(messageText);
              const user = history.messages
                .filter((message) => message.role === "user")
                .map(messageText);
              for (const turn of scenario.turns) {
                expect(visible.filter((text) => text.includes(turn.reply))).toHaveLength(
                  Number(!turn.quiet),
                );
                expect(user.filter((text) => text.includes(turn.prompt))).toHaveLength(1);
              }
              expect(visible.filter((text) => text && text !== "NO_REPLY")).toEqual(
                scenario.turns.filter((turn) => !turn.quiet).map((turn) => turn.reply),
              );
              expect(user.join("\n")).not.toContain(HANDOFF);
              expect(automaticPayloads).toEqual([]);
              expect(fixture.errors).toEqual([]);
            }
          },
          () => {
            for (const scenario of scenarios) {
              scenario.releaseFirst.resolve();
            }
            drain?.dispose();
          },
          () => fixture.close(),
          () => drain?.dispose({ waitForSettlements: true }),
        );
      },
    );
  },
);
