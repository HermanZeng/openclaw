import path from "node:path";
import { describe, expect, it } from "vitest";
import { createDeferred } from "../../../test/helpers/promise.js";
import { resolveOpenClawStateSqlitePath } from "../../state/openclaw-state-db.paths.js";
import {
  resolveSqliteSessionReclamationWorkerDatabaseOptions,
  runExclusiveSqliteSessionReclamation,
} from "./session-accessor.sqlite-reclamation.js";

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
});
