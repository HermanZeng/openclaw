import path from "node:path";
import { Worker } from "node:worker_threads";
import { describe, expect, it, vi } from "vitest";
import { createDeferred } from "../../../test/helpers/promise.js";
import { resolveOpenClawStateSqlitePath } from "../../state/openclaw-state-db.paths.js";
import {
  resolveSqliteSessionReclamationWorkerDatabaseOptions,
  resolveSqliteSessionReclamationWorkerExit,
  resolveSqliteSessionReclamationSourceWorkerExecArgv,
  runExclusiveSqliteSessionReclamation,
  observeSqliteSessionReclamationWorker,
  type SqliteSessionEntryReclamationWorkerMessage,
} from "./session-accessor.sqlite-reclamation.js";
import { settleSqliteSessionReclamationWorkerDatabase } from "./session-accessor.sqlite-reclamation.worker.js";

describe("SQLite session entry reclamation queue", () => {
  it("holds later materialization work until the active reclamation phase settles", async () => {
    const events: string[] = [];
    const firstGate = createDeferred();
    const firstStarted = createDeferred();

    const first = runExclusiveSqliteSessionReclamation(async () => {
      events.push("first:materialize");
      firstStarted.resolve();
      await firstGate.promise;
      events.push("first:reclaimed");
    });
    await firstStarted.promise;
    const second = runExclusiveSqliteSessionReclamation(async () => {
      events.push("second:materialize");
      events.push("second:reclaimed");
    });

    await Promise.resolve();
    expect(events).toEqual(["first:materialize"]);

    firstGate.resolve();
    await Promise.all([first, second]);
    expect(events).toEqual([
      "first:materialize",
      "first:reclaimed",
      "second:materialize",
      "second:reclaimed",
    ]);
  });

  it("pins Worker state ownership when Windows environment keys use noncanonical casing", () => {
    const stateDir = path.resolve("tmp", "openclaw-reclamation-mixed-case-state");
    const rawEnv = {
      openclaw_state_dir: stateDir,
      VITEST: "1",
    } as NodeJS.ProcessEnv;
    const caseInsensitiveEnv = new Proxy(rawEnv, {
      get(target, property, receiver) {
        if (typeof property !== "string") {
          return Reflect.get(target, property, receiver);
        }
        const key = Object.keys(target).find(
          (candidate) => candidate.toLowerCase() === property.toLowerCase(),
        );
        return key === undefined ? undefined : target[key];
      },
    });
    const expectedStatePath = resolveOpenClawStateSqlitePath(caseInsensitiveEnv);

    const workerOptions = resolveSqliteSessionReclamationWorkerDatabaseOptions({
      agentId: "main",
      env: caseInsensitiveEnv,
      path: path.join(stateDir, "agents", "main", "state.sqlite"),
    });

    expect(workerOptions.env?.OPENCLAW_STATE_DIR).toBe(stateDir);
    expect(resolveOpenClawStateSqlitePath(workerOptions.env)).toBe(expectedStatePath);
  });

  it("retries Worker handle and lease cleanup without losing the committed result", async () => {
    const close = vi
      .fn()
      .mockReturnValueOnce({ errors: [new Error("state lease remained busy")], settled: false })
      .mockReturnValueOnce({ errors: [], settled: true });
    const delay = vi.fn(async () => undefined);

    await expect(
      settleSqliteSessionReclamationWorkerDatabase("worker.sqlite", { close, delay }),
    ).resolves.toEqual({
      attempts: 2,
      cleanupWarnings: ["state lease remained busy"],
      settled: true,
    });
    expect(close).toHaveBeenCalledTimes(2);
    expect(delay).toHaveBeenCalledOnce();
  });

  it("lets the global queue advance after persistent cleanup failure in a real Worker", async () => {
    const events: string[] = [];
    const messages: SqliteSessionEntryReclamationWorkerMessage[] = [];
    const result = {
      kind: "history-eviction",
      value: { archivedTranscripts: [], deleted: true },
    } as const;

    const first = runExclusiveSqliteSessionReclamation(async () => {
      events.push("first:start");
      const worker = new Worker(
        new URL(
          "../../../test/fixtures/sqlite-session-reclamation-cleanup-failure.worker.mjs",
          import.meta.url,
        ),
        {
          execArgv: resolveSqliteSessionReclamationSourceWorkerExecArgv(),
          workerData: {
            reclamationData: {
              plan: {
                databaseOptions: { agentId: "main", env: {}, path: "worker.sqlite" },
                kind: "history-eviction",
                materializedPlans: [],
                protectedSessionIds: [],
                sessionId: "session-1",
              },
              type: "sqlite-session-reclamation-v2",
            },
            result,
            workerModuleUrl: new URL(
              "./session-accessor.sqlite-reclamation.worker.ts",
              import.meta.url,
            ).href,
          },
        },
      );
      worker.on("message", (message: SqliteSessionEntryReclamationWorkerMessage) => {
        messages.push(message);
      });
      const workerResult = await observeSqliteSessionReclamationWorker({
        databasePath: "worker.sqlite",
        worker,
      });
      events.push("first:exited");
      return workerResult;
    });
    const second = runExclusiveSqliteSessionReclamation(async () => {
      events.push("second:ran");
    });

    await expect(first).resolves.toEqual(result);
    await expect(second).resolves.toBeUndefined();
    expect(events).toEqual(["first:start", "first:exited", "second:ran"]);
    expect(messages).toEqual([
      {
        cleanupIncomplete: true,
        cleanupWarnings: [
          "state database unavailable",
          "SQLite session reclamation worker database cleanup remained incomplete after 3 attempts",
        ],
        result,
        type: "done",
      },
    ]);
  });

  it("treats a committed Worker message as authoritative over a later exit error", () => {
    const message = {
      result: {
        kind: "history-eviction",
        value: { archivedTranscripts: [], deleted: true },
      },
      type: "done",
    } satisfies SqliteSessionEntryReclamationWorkerMessage;

    expect(
      resolveSqliteSessionReclamationWorkerExit({
        code: 1,
        message,
        workerError: new Error("post-commit cleanup error"),
      }),
    ).toEqual(message.result);
  });

  it("preserves an explicit pre-commit Worker failure", () => {
    expect(() =>
      resolveSqliteSessionReclamationWorkerExit({
        code: 0,
        message: { error: "reclamation rolled back", type: "failed" },
      }),
    ).toThrow("reclamation rolled back");
  });
});
