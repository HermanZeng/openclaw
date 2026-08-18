import { describe, expect, it } from "vitest";
import { createDeferred } from "../../../test/helpers/promise.js";
import { runExclusiveSqliteSessionEntryReclamation } from "./session-accessor.sqlite-reclamation.js";

describe("SQLite session entry reclamation queue", () => {
  it("holds later materialization work until the active reclamation phase settles", async () => {
    const events: string[] = [];
    const firstGate = createDeferred();
    const firstStarted = createDeferred();

    const first = runExclusiveSqliteSessionEntryReclamation(async () => {
      events.push("first:materialize");
      firstStarted.resolve();
      await firstGate.promise;
      events.push("first:reclaimed");
    });
    await firstStarted.promise;
    const second = runExclusiveSqliteSessionEntryReclamation(async () => {
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
});
