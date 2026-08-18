import fs from "node:fs";
import { performance } from "node:perf_hooks";
import { afterEach, expect, test } from "vitest";
import { resolveSqliteTargetFromSessionStorePath } from "../config/sessions/session-sqlite-target.js";
import {
  closeOpenClawAgentDatabasesForTest,
  openOpenClawAgentDatabase,
} from "../state/openclaw-agent-db.js";
import { closeOpenClawStateDatabaseForTest } from "../state/openclaw-state-db.js";
import { rpcReq, writeSessionStore } from "./test-helpers.js";
import {
  sessionStoreEntry,
  setupGatewaySessionsTestHarness,
} from "./test/server-sessions.test-helpers.js";

const SESSION_ID = "phase3-reclamation-e2e";
const SESSION_KEY = "discord:group:phase3-reclamation-e2e";
const CANONICAL_SESSION_KEY = `agent:main:${SESSION_KEY}`;
const UNRELATED_SESSION_ID = "phase3-reclamation-unrelated";
const UNRELATED_SESSION_KEY = "discord:group:phase3-reclamation-unrelated";
const ROWS = 200_000;

const { createSessionStoreDir, openClient } = setupGatewaySessionsTestHarness();

afterEach(() => {
  closeOpenClawAgentDatabasesForTest();
  closeOpenClawStateDatabaseForTest();
});

function countRows(
  database: ReturnType<typeof openOpenClawAgentDatabase>,
  table: string,
  sessionId: string,
): number {
  const row = database.db
    .prepare(`SELECT count(*) AS count FROM ${table} WHERE session_id = ?`)
    .get(sessionId) as { count: number | bigint };
  return Number(row.count);
}

function seedTranscriptState(storePath: string): void {
  const target = resolveSqliteTargetFromSessionStorePath(storePath, { agentId: "main" });
  if (!target.path) {
    throw new Error("expected SQLite database path");
  }
  const database = openOpenClawAgentDatabase({ agentId: "main", path: target.path });
  const now = Date.now();
  const eventJson = JSON.stringify({
    type: "message",
    message: { content: "phase3 e2e transcript message", role: "user" },
  });
  const insertEvent = database.db.prepare(
    "INSERT INTO transcript_events (session_id, seq, event_json, created_at) VALUES (?, ?, ?, ?)",
  );
  const insertActive = database.db.prepare(
    `INSERT INTO session_transcript_active_events
       (session_id, active_position, event_seq, message_position)
     VALUES (?, ?, ?, ?)`,
  );
  const insertFts = database.db.prepare(
    `INSERT INTO session_transcript_fts (text, session_id, message_id, role, timestamp)
     VALUES ('phase3 e2e transcript message', ?, ?, 'user', ?)`,
  );
  database.db.exec("BEGIN IMMEDIATE");
  try {
    for (let index = 0; index < ROWS; index += 1) {
      insertEvent.run(SESSION_ID, index, eventJson, now + index);
      insertActive.run(SESSION_ID, index, index, index);
      insertFts.run(SESSION_ID, `${SESSION_ID}-message-${index}`, now);
    }
    database.db
      .prepare(
        `INSERT INTO session_transcript_index_state (
           session_id, indexed_seq, needs_rebuild, active_event_count,
           active_message_count, updated_at
         ) VALUES (?, ?, 0, ?, ?, ?)`,
      )
      .run(SESSION_ID, ROWS - 1, ROWS, ROWS, now);
    database.db
      .prepare(
        `INSERT INTO transcript_rewrite_watermarks (session_id, generation, updated_at)
         VALUES (?, 'phase3-e2e-generation', ?)`,
      )
      .run(SESSION_ID, now);
    insertEvent.run(
      UNRELATED_SESSION_ID,
      0,
      JSON.stringify({
        type: "message",
        message: { content: "unrelated transcript message", role: "assistant" },
      }),
      now,
    );
    insertActive.run(UNRELATED_SESSION_ID, 0, 0, 0);
    insertFts.run(UNRELATED_SESSION_ID, `${UNRELATED_SESSION_ID}-message-0`, now);
    database.db
      .prepare(
        `INSERT INTO session_transcript_index_state (
           session_id, indexed_seq, needs_rebuild, active_event_count,
           active_message_count, updated_at
         ) VALUES (?, 0, 0, 1, 1, ?)`,
      )
      .run(UNRELATED_SESSION_ID, now);
    database.db
      .prepare(
        `INSERT INTO transcript_rewrite_watermarks (session_id, generation, updated_at)
         VALUES (?, 'phase3-unrelated-generation', ?)`,
      )
      .run(UNRELATED_SESSION_ID, now);
    database.db.exec("COMMIT");
  } catch (error) {
    database.db.exec("ROLLBACK");
    throw error;
  }
}

test("local proof: sessions.delete stalls the gateway during final transcript reclamation", async () => {
  const { storePath } = await createSessionStoreDir();
  await writeSessionStore({
    entries: {
      [SESSION_KEY]: sessionStoreEntry(SESSION_ID),
      [UNRELATED_SESSION_KEY]: sessionStoreEntry(UNRELATED_SESSION_ID),
    },
    storePath,
  });
  seedTranscriptState(storePath);

  const samples: number[] = [];
  let previous = performance.now();
  const heartbeat = setInterval(() => {
    const current = performance.now();
    samples.push(current - previous);
    previous = current;
  }, 10);

  const { ws } = await openClient();
  await new Promise<void>((resolve) => {
    setTimeout(resolve, 25);
  });
  const deleteStartedAt = performance.now();
  const deleted = await rpcReq<{ archived: string[]; deleted: boolean; key: string; ok: true }>(
    ws,
    "sessions.delete",
    { key: SESSION_KEY },
  );
  const deleteMs = performance.now() - deleteStartedAt;
  clearInterval(heartbeat);
  const maxGatewayGapMs = Math.max(...samples);

  const target = resolveSqliteTargetFromSessionStorePath(storePath, { agentId: "main" });
  if (!target.path) {
    throw new Error("expected SQLite database path after deletion");
  }
  const database = openOpenClawAgentDatabase({ agentId: "main", path: target.path });
  const targetCounts = {
    active: countRows(database, "session_transcript_active_events", SESSION_ID),
    fts: countRows(database, "session_transcript_fts", SESSION_ID),
    indexState: countRows(database, "session_transcript_index_state", SESSION_ID),
    transcriptEvents: countRows(database, "transcript_events", SESSION_ID),
    rewriteWatermarks: countRows(database, "transcript_rewrite_watermarks", SESSION_ID),
    windows: countRows(database, "session_windows", SESSION_ID),
  };
  const unrelatedCounts = {
    active: countRows(database, "session_transcript_active_events", UNRELATED_SESSION_ID),
    fts: countRows(database, "session_transcript_fts", UNRELATED_SESSION_ID),
    indexState: countRows(database, "session_transcript_index_state", UNRELATED_SESSION_ID),
    transcriptEvents: countRows(database, "transcript_events", UNRELATED_SESSION_ID),
    rewriteWatermarks: countRows(database, "transcript_rewrite_watermarks", UNRELATED_SESSION_ID),
    windows: countRows(database, "session_windows", UNRELATED_SESSION_ID),
  };
  const targetNodeCount = Number(
    (
      database.db
        .prepare("SELECT count(*) AS count FROM session_nodes WHERE current_session_id = ?")
        .get(SESSION_ID) as { count: number | bigint }
    ).count,
  );
  const unrelatedNodeCount = Number(
    (
      database.db
        .prepare("SELECT count(*) AS count FROM session_nodes WHERE current_session_id = ?")
        .get(UNRELATED_SESSION_ID) as { count: number | bigint }
    ).count,
  );
  const archive = database.db
    .prepare(
      `SELECT archive_sha256, length(archive_blob) AS archive_bytes, published_at
       FROM session_transcript_archives WHERE session_id = ?`,
    )
    .get(SESSION_ID) as
    | { archive_bytes: number | bigint; archive_sha256: string; published_at: number | null }
    | undefined;
  process.stdout.write(
    `${JSON.stringify({
      archive,
      deleteMs,
      deleted,
      maxGatewayGapMs,
      rows: ROWS,
      samples: samples.length,
      targetCounts,
      targetNodeCount,
      unrelatedCounts,
      unrelatedNodeCount,
    })}\n`,
  );

  expect(deleted.ok).toBe(true);
  expect(deleted.payload).toMatchObject({
    archived: [expect.any(String)],
    deleted: true,
    key: CANONICAL_SESSION_KEY,
    ok: true,
  });
  expect(fs.existsSync(deleted.payload?.archived[0] ?? "")).toBe(true);
  expect(targetCounts).toEqual({
    active: 0,
    fts: 0,
    indexState: 0,
    transcriptEvents: 0,
    rewriteWatermarks: 0,
    windows: 0,
  });
  expect(targetNodeCount).toBe(0);
  expect(unrelatedCounts).toEqual({
    active: 1,
    fts: 1,
    indexState: 1,
    transcriptEvents: 1,
    rewriteWatermarks: 1,
    windows: 1,
  });
  expect(unrelatedNodeCount).toBe(1);
  expect(archive).toMatchObject({
    archive_sha256: expect.stringMatching(/^[a-f0-9]{64}$/u),
    published_at: expect.any(Number),
  });
  expect(Number(archive?.archive_bytes ?? 0)).toBeGreaterThan(0);
  expect(maxGatewayGapMs).toBeGreaterThan(500);
  ws.close();
}, 120_000);
