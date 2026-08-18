// Measures the synchronous SQLite transaction that reclaims one transcript generation.
import fs from "node:fs";
import path from "node:path";
import { performance } from "node:perf_hooks";
import type { DatabaseSync } from "node:sqlite";
import { materializeSessionStateDeletePlans } from "../src/config/sessions/session-accessor.sqlite-archive.js";
import { readSessionStateDeleteSnapshot } from "../src/config/sessions/session-accessor.sqlite-delete-snapshot.js";
import { deleteMaterializedSessionStatePlans } from "../src/config/sessions/session-accessor.sqlite-lifecycle-state.js";
import { deleteSessionTranscriptIndexInTransaction } from "../src/config/sessions/session-transcript-index.js";
import {
  closeOpenClawAgentDatabasesForTest,
  openOpenClawAgentDatabase,
  runOpenClawAgentWriteTransaction,
} from "../src/state/openclaw-agent-db.js";
import { closeOpenClawStateDatabaseForTest } from "../src/state/openclaw-state-db.js";

const AGENT_ID = "main";
const SESSION_KEY = "agent:main:reclamation-benchmark";
const ANCHOR_SESSION_ID = "reclamation-anchor";
const TARGET_SESSION_ID = "reclamation-target";
const FILLER_SESSION_ID = "reclamation-filler";
const DEFAULT_FILLER_ROWS = [0, 10_000, 50_000, 200_000];

type ScenarioReport = {
  componentMs?: {
    activeProjection: number;
    commit: number;
    fts: number;
    indexState: number;
    ownerCascade: number;
  };
  databaseBytes: number;
  eventLoopDelayMs: number;
  fillerRows: number;
  preclearMs?: number;
  seedMs: number;
  targetRows: number;
  transactionMs: number;
  verification: {
    fillerFtsRows: number;
    targetFtsRows: number;
    targetTranscriptRows: number;
    targetWindowRows: number;
  };
};

type BenchmarkMode = "components" | "precleared" | "production";

function parseMode(argv: readonly string[]): BenchmarkMode {
  if (argv.includes("--components")) {
    return "components";
  }
  return argv.includes("--precleared") ? "precleared" : "production";
}

function parseFillerRows(argv: readonly string[]): number[] {
  const raw = argv.find((value) => value.startsWith("--rows="))?.slice("--rows=".length);
  if (!raw) {
    return DEFAULT_FILLER_ROWS;
  }
  const values = raw.split(",").map((value) => Number(value.trim()));
  if (values.length === 0 || values.some((value) => !Number.isSafeInteger(value) || value < 0)) {
    throw new Error("--rows must be a comma-separated list of non-negative safe integers");
  }
  return values;
}

function parseTargetRows(argv: readonly string[]): number {
  const raw = argv
    .find((value) => value.startsWith("--target-rows="))
    ?.slice("--target-rows=".length);
  if (!raw) {
    return 1;
  }
  const value = Number(raw);
  if (!Number.isSafeInteger(value) || value < 1) {
    throw new Error("--target-rows must be a positive safe integer");
  }
  return value;
}

function runSeedTransaction(database: DatabaseSync, operation: () => void): void {
  database.exec("BEGIN IMMEDIATE");
  try {
    operation();
    database.exec("COMMIT");
  } catch (error) {
    database.exec("ROLLBACK");
    throw error;
  }
}

function seedWindow(database: DatabaseSync, sessionId: string, now: number): void {
  database
    .prepare(
      `INSERT INTO session_windows (
         session_id, session_key, created_at, updated_at, transcript_updated_at, status
       ) VALUES (?, ?, ?, ?, ?, 'done')`,
    )
    .run(sessionId, SESSION_KEY, now, now, now);
}

function seedTranscriptProjection(params: {
  database: DatabaseSync;
  rows: number;
  sessionId: string;
  text: string;
  now: number;
}): void {
  if (params.rows === 0) {
    return;
  }
  const eventJson = JSON.stringify({
    type: "message",
    message: { content: params.text, role: "user" },
  });
  const insertEvent = params.database.prepare(
    "INSERT INTO transcript_events (session_id, seq, event_json, created_at) VALUES (?, ?, ?, ?)",
  );
  const insertActive = params.database.prepare(
    `INSERT INTO session_transcript_active_events
       (session_id, active_position, event_seq, message_position)
     VALUES (?, ?, ?, ?)`,
  );
  const insertFts = params.database.prepare(
    `INSERT INTO session_transcript_fts (text, session_id, message_id, role, timestamp)
     VALUES (?, ?, ?, 'user', ?)`,
  );
  for (let index = 0; index < params.rows; index += 1) {
    insertEvent.run(params.sessionId, index, eventJson, params.now + index);
    insertActive.run(params.sessionId, index, index, index);
    insertFts.run(
      params.text,
      params.sessionId,
      `${params.sessionId}-message-${index}`,
      params.now,
    );
  }
  params.database
    .prepare(
      `INSERT INTO session_transcript_index_state (
         session_id, indexed_seq, needs_rebuild, active_event_count,
         active_message_count, updated_at
       ) VALUES (?, ?, 0, ?, ?, ?)`,
    )
    .run(params.sessionId, params.rows - 1, params.rows, params.rows, params.now);
}

function countRows(database: DatabaseSync, table: string, sessionId: string): number {
  const row = database
    .prepare(`SELECT count(*) AS count FROM ${table} WHERE session_id = ?`)
    .get(sessionId) as { count: number | bigint };
  return Number(row.count);
}

function databaseBytes(database: DatabaseSync): number {
  const pageCount = database.prepare("PRAGMA page_count").get() as { page_count: number | bigint };
  const pageSize = database.prepare("PRAGMA page_size").get() as { page_size: number | bigint };
  return Number(pageCount.page_count) * Number(pageSize.page_size);
}

function runComponentDelete(database: DatabaseSync): NonNullable<ScenarioReport["componentMs"]> {
  database.exec("BEGIN IMMEDIATE");
  try {
    let startedAt = performance.now();
    database
      .prepare("DELETE FROM session_transcript_fts WHERE session_id = ?")
      .run(TARGET_SESSION_ID);
    const fts = performance.now() - startedAt;

    startedAt = performance.now();
    database
      .prepare("DELETE FROM session_transcript_active_events WHERE session_id = ?")
      .run(TARGET_SESSION_ID);
    const activeProjection = performance.now() - startedAt;

    startedAt = performance.now();
    database
      .prepare("DELETE FROM session_transcript_index_state WHERE session_id = ?")
      .run(TARGET_SESSION_ID);
    const indexState = performance.now() - startedAt;

    startedAt = performance.now();
    database.prepare("DELETE FROM session_windows WHERE session_id = ?").run(TARGET_SESSION_ID);
    const ownerCascade = performance.now() - startedAt;

    startedAt = performance.now();
    database.exec("COMMIT");
    const commit = performance.now() - startedAt;
    return { activeProjection, commit, fts, indexState, ownerCascade };
  } catch (error) {
    database.exec("ROLLBACK");
    throw error;
  }
}

async function runScenario(
  fillerRows: number,
  targetRows: number,
  mode: BenchmarkMode,
): Promise<ScenarioReport> {
  const benchmarkRoot = path.join(path.dirname(process.cwd()), ".openclaw-reclamation-bench");
  fs.mkdirSync(benchmarkRoot, { recursive: true });
  const root = fs.mkdtempSync(path.join(benchmarkRoot, `scenario-${fillerRows}-`));
  const databasePath = path.join(root, "agent.sqlite");
  const env = { ...process.env, OPENCLAW_STATE_DIR: path.join(root, "state") };
  const options = { agentId: AGENT_ID, env, path: databasePath };
  const database = openOpenClawAgentDatabase(options);
  try {
    const seedStartedAt = performance.now();
    const now = Date.now();
    runSeedTransaction(database.db, () => {
      database.db
        .prepare(
          `INSERT INTO session_nodes (session_key, current_session_id, entry_json, updated_at)
           VALUES (?, ?, '{}', ?)`,
        )
        .run(SESSION_KEY, ANCHOR_SESSION_ID, now);
      seedWindow(database.db, ANCHOR_SESSION_ID, now);
      seedWindow(database.db, TARGET_SESSION_ID, now);
      seedWindow(database.db, FILLER_SESSION_ID, now);
      seedTranscriptProjection({
        database: database.db,
        rows: targetRows,
        sessionId: TARGET_SESSION_ID,
        text: "target transcript message",
        now,
      });
      seedTranscriptProjection({
        database: database.db,
        rows: fillerRows,
        sessionId: FILLER_SESSION_ID,
        text: "unrelated filler transcript message",
        now,
      });
    });
    const seedMs = performance.now() - seedStartedAt;

    const plans =
      mode !== "components"
        ? await materializeSessionStateDeletePlans([
            {
              agentId: AGENT_ID,
              archiveDirectory: root,
              archiveTranscript: false,
              databasePath,
              reason: "deleted",
              sessionId: TARGET_SESSION_ID,
              snapshot: readSessionStateDeleteSnapshot(database.db, TARGET_SESSION_ID),
            },
          ])
        : [];

    let preclearMs: number | undefined;
    if (mode === "precleared") {
      const preclearStartedAt = performance.now();
      runOpenClawAgentWriteTransaction(
        (writeDatabase) =>
          deleteSessionTranscriptIndexInTransaction(writeDatabase.db, TARGET_SESSION_ID),
        options,
        { operationLabel: "benchmark.session-reclamation-preclear" },
      );
      preclearMs = performance.now() - preclearStartedAt;
    }

    const timerStartedAt = performance.now();
    const timerDelay = new Promise<number>((resolve) => {
      setTimeout(() => resolve(performance.now() - timerStartedAt), 0);
    });
    const transactionStartedAt = performance.now();
    let componentMs: ScenarioReport["componentMs"];
    if (mode === "components") {
      componentMs = runComponentDelete(database.db);
    } else {
      runOpenClawAgentWriteTransaction(
        (writeDatabase) => deleteMaterializedSessionStatePlans(writeDatabase, plans),
        options,
        { operationLabel: "benchmark.session-reclamation" },
      );
    }
    const transactionMs = performance.now() - transactionStartedAt;
    const eventLoopDelayMs = await timerDelay;

    const verification = {
      fillerFtsRows: countRows(database.db, "session_transcript_fts", FILLER_SESSION_ID),
      targetFtsRows: countRows(database.db, "session_transcript_fts", TARGET_SESSION_ID),
      targetTranscriptRows: countRows(database.db, "transcript_events", TARGET_SESSION_ID),
      targetWindowRows: countRows(database.db, "session_windows", TARGET_SESSION_ID),
    };
    if (
      verification.fillerFtsRows !== fillerRows ||
      verification.targetFtsRows !== 0 ||
      verification.targetTranscriptRows !== 0 ||
      verification.targetWindowRows !== 0
    ) {
      throw new Error(`reclamation verification failed: ${JSON.stringify(verification)}`);
    }
    return {
      ...(componentMs ? { componentMs } : {}),
      databaseBytes: databaseBytes(database.db),
      eventLoopDelayMs,
      fillerRows,
      ...(preclearMs === undefined ? {} : { preclearMs }),
      seedMs,
      targetRows,
      transactionMs,
      verification,
    };
  } finally {
    closeOpenClawAgentDatabasesForTest();
    closeOpenClawStateDatabaseForTest();
    fs.rmSync(root, { force: true, maxRetries: 5, recursive: true, retryDelay: 100 });
  }
}

async function main(): Promise<void> {
  const reports: ScenarioReport[] = [];
  const mode = parseMode(process.argv.slice(2));
  const targetRows = parseTargetRows(process.argv.slice(2));
  for (const fillerRows of parseFillerRows(process.argv.slice(2))) {
    const report = await runScenario(fillerRows, targetRows, mode);
    reports.push(report);
    process.stdout.write(`${JSON.stringify(report)}\n`);
  }
  process.stdout.write(
    `${JSON.stringify({ node: process.version, platform: process.platform, reports }, null, 2)}\n`,
  );
}

await main();
