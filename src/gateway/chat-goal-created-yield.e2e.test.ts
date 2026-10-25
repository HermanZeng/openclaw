import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { asOptionalRecord } from "@openclaw/normalization-core/record-coerce";
import { expect, it } from "vitest";
import { withinTest } from "../../test/helpers/promise.js";
import { runQaGatewayFixture } from "../../test/helpers/qa-gateway-cleanup.js";
import { subscribeSubagentRunChanges } from "../agents/subagents/registry/subagent-registry-publication.js";
import { getSubagentRunByRunId } from "../agents/subagents/registry/subagent-registry.js";
import { loadSessionEntry } from "../config/sessions/session-accessor.js";
import { waitForGatewayActiveWork } from "../infra/gateway-active-work.js";
import { createDeferredCore } from "../shared/deferred.js";
import { withOpenClawTestState } from "../test-utils/openclaw-test-state.js";
import { createChatFixture } from "./gateway-chat-fixture.test-support.js";
import { writeToolCall } from "./gateway-provider-fixture.test-support.js";
import { KERNEL_TEST_ENV } from "./test-helpers.env.js";

function records(value: unknown): Record<string, unknown>[] {
  if (typeof value === "string") {
    try {
      return records(JSON.parse(value) as unknown);
    } catch {
      return [];
    }
  }
  if (Array.isArray(value)) {
    return value.flatMap(records);
  }
  const record = asOptionalRecord(value);
  return record ? [record, ...Object.values(record).flatMap(records)] : [];
}

// Real Gateway, tools and committed child return. Only provider decisions are scripted.
it(
  "continues a model-created Goal once after a delegated or ordinary final",
  {
    timeout: 120_000,
  },
  async ({ signal }) => {
    const scenarios = [true, false].map((delegated) => {
      const nonce = randomUUID();
      return {
        delegated,
        prompt: `GOAL_CREATE_${nonce}: Create the requested Goal, finish the first item, then continue the remaining item.`,
        childTask: `CHILD_ITEM_${nonce}: Return the first-item finding only.`,
        childResult: `CHILD_DONE_${nonce}`,
        parentCalls: 0,
        childCalls: 0,
        sessionKey: "",
        childRunId: undefined as string | undefined,
        releaseChild: createDeferredCore(),
        childStarted: createDeferredCore(),
        yielded: createDeferredCore(),
        committed: createDeferredCore(),
        failed: createDeferredCore<never>(),
      };
    });
    for (const scenario of scenarios) {
      void scenario.failed.promise.catch(() => {});
    }
    await withOpenClawTestState(
      {
        label: "goal-created-yield",
        env: {
          ...KERNEL_TEST_ENV,
          VITEST: process.env.VITEST,
          OPENCLAW_TEST_MINIMAL_GATEWAY: undefined,
          OPENCLAW_DISABLE_BUNDLED_PLUGINS: "1",
        },
      },
      async () => {
        const fixture = createChatFixture("goal-created-yield", {
          configure: (config) => ({
            ...config,
            agents: { ...config.agents, ownership: "explicit", entries: { main: {} } },
            plugins: { slots: { memory: "none" } },
            tools: { profile: "coding", toolSearch: false },
            messages: { queue: { mode: "followup", debounceMsByChannel: { webchat: 0 } } },
          }),
          respond: async (body, _sessionId, response) => {
            if (body.includes("conversationLabelSource")) {
              return "Goal test";
            }
            const scenario =
              scenarios.find((candidate) => body.includes(candidate.prompt)) ??
              scenarios.find((candidate) => body.includes(candidate.childTask));
            assert(scenario, "only fixture conversations may call this provider");
            try {
              if (!body.includes(scenario.prompt) && body.includes(scenario.childTask)) {
                scenario.childCalls++;
                expect(scenario.childCalls).toBe(1);
                scenario.childStarted.resolve();
                await scenario.releaseChild.promise;
                return scenario.childResult;
              }
              scenario.parentCalls++;
              const output = records(JSON.parse(body) as unknown).findLast(
                (record) => record.type === "function_call_output",
              );
              const toolOutput = records(output?.output);
              if (scenario.parentCalls === 1) {
                writeToolCall(response, "create_goal", { objective: scenario.prompt });
              } else if (scenario.parentCalls === 2) {
                expect(toolOutput).toContainEqual(expect.objectContaining({ status: "created" }));
                if (!scenario.delegated) {
                  return "First item done; the remaining item is unfinished.";
                }
                writeToolCall(response, "sessions_spawn", {
                  task: scenario.childTask,
                  agentId: "main",
                  runtime: "subagent",
                  context: "isolated",
                  cleanup: "keep",
                  completionTarget: "parent",
                });
              } else if (scenario.delegated && scenario.parentCalls === 3) {
                const receipt = toolOutput.find(
                  (record) =>
                    typeof record.runId === "string" && typeof record.childSessionKey === "string",
                );
                assert(receipt && typeof receipt.runId === "string", "real spawn receipt required");
                scenario.childRunId = receipt.runId;
                writeToolCall(response, "sessions_yield", {});
              } else if (scenario.delegated && scenario.parentCalls === 4) {
                expect(body).toContain(scenario.childResult);
                expect(body).toContain("Active goal:");
                return "Delegated item done; the remaining item is unfinished.";
              } else if (scenario.parentCalls === (scenario.delegated ? 5 : 3)) {
                expect(body).toContain("Advance the active goal");
                writeToolCall(response, "update_goal", { status: "complete" });
              } else if (scenario.parentCalls === (scenario.delegated ? 6 : 4)) {
                expect(toolOutput).toContainEqual(expect.objectContaining({ status: "updated" }));
                return "Both synthetic items are complete.";
              } else {
                throw new Error(`Unexpected Goal request ${scenario.parentCalls}`);
              }
              return undefined;
            } catch (error) {
              scenario.failed.reject(error);
              return "Fixture assertion failed; stop this turn.";
            }
          },
        });
        await runQaGatewayFixture(
          async () => {
            await fixture.start();
            for (const scenario of scenarios) {
              const wait = <T>(promise: Promise<T>) =>
                withinTest(Promise.race([promise, scenario.failed.promise]), signal);
              const created = await fixture.client.request<{ key: string }>("sessions.create", {
                agentId: "main",
                label: scenario.delegated ? "Delegated Goal" : "Direct Goal",
              });
              scenario.sessionKey = created.key;
              const unsubscribe = subscribeSubagentRunChanges("persistence", () => {
                const child = scenario.childRunId && getSubagentRunByRunId(scenario.childRunId);
                if (!child) {
                  return;
                }
                if (child.requesterSettleWake?.requesterYieldBatch && !child.requesterTurnRunId) {
                  scenario.yielded.resolve();
                }
                if (child.cleanupCompletedAt !== undefined && !child.requesterSettleWake) {
                  scenario.committed.resolve();
                }
                if (child.delivery?.status === "suspended") {
                  scenario.failed.reject(
                    new Error("Child delivery suspended: " + child.delivery.lastError),
                  );
                }
              });
              try {
                const accepted = await fixture.client.request<{ status: string; runId: string }>(
                  "chat.send",
                  {
                    sessionKey: scenario.sessionKey,
                    message: scenario.prompt,
                    idempotencyKey: randomUUID(),
                  },
                );
                expect(accepted.status).toBe("started");
                if (scenario.delegated) {
                  await wait(
                    Promise.all([scenario.childStarted.promise, scenario.yielded.promise]),
                  );
                  expect(scenario.parentCalls).toBe(3);
                  scenario.releaseChild.resolve();
                  await wait(scenario.committed.promise);
                } else {
                  expect(
                    await wait(
                      fixture.client.request("agent.wait", {
                        runId: accepted.runId,
                        timeoutMs: 30_000,
                      }),
                    ),
                  ).toMatchObject({ status: "ok" });
                }
                const drained = await wait(waitForGatewayActiveWork(30_000));
                expect(drained.drained, JSON.stringify(drained.snapshot.blockers)).toBe(true);
                expect(
                  loadSessionEntry({ agentId: "main", sessionKey: scenario.sessionKey })?.goal,
                ).toMatchObject({
                  status: "complete",
                  continuationTurns: 1,
                });
                expect(scenario.parentCalls).toBe(scenario.delegated ? 6 : 4);
                expect(scenario.childCalls).toBe(Number(scenario.delegated));
                expect(fixture.errors).toEqual([]);
              } finally {
                scenario.releaseChild.resolve();
                unsubscribe();
              }
            }
          },
          async () => {
            for (const scenario of scenarios) {
              scenario.releaseChild.resolve();
            }
            await fixture.close();
          },
        );
      },
    );
  },
);
