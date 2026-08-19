import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { Worker } from "node:worker_threads";
import { toStringifiedError } from "@openclaw/normalization-core/error-coercion";
import { isRecord } from "@openclaw/normalization-core/record-coerce";
import { executeSqliteQuerySync, getNodeSqliteKysely } from "../../infra/kysely-sync.js";
import { createSubsystemLogger } from "../../logging/subsystem.js";
import { KeyedAsyncQueue } from "../../plugin-sdk/keyed-async-queue.js";
import type { DB as OpenClawAgentKyselyDatabase } from "../../state/openclaw-agent-db.generated.js";
import {
  isIncognitoOpenClawAgentSqlitePath,
  openOpenClawAgentDatabase,
  resolveOpenClawAgentSqlitePath,
  runOpenClawAgentWriteTransaction,
  type OpenClawAgentDatabase,
  type OpenClawAgentDatabaseOptions,
} from "../../state/openclaw-agent-db.js";
import {
  resolveOpenClawStateDirForDatabasePath,
  resolveOpenClawStateSqlitePath,
} from "../../state/openclaw-state-db.paths.js";
import type { MaterializedSessionStateDeletePlan } from "./session-accessor.sqlite-archive.js";
import type {
  DeleteSessionEntryLifecycleParams,
  DeleteSessionEntryLifecycleResult,
  SessionLifecycleArchivedTranscript,
} from "./session-accessor.sqlite-contract.js";
import {
  sqliteLifecycleTargetSnapshotsEqual,
  sqliteSessionEntriesEqual,
} from "./session-accessor.sqlite-entry-equality.js";
import {
  deleteLifecycleTargetRows,
  readLifecycleTargetSnapshot,
  type SqliteLifecycleTargetSnapshot,
} from "./session-accessor.sqlite-entry-store.js";
import {
  assertPlannedLifecycleArtifactEntriesUnchanged,
  deleteMaterializedSessionStatePlans,
  deletePlannedLifecycleArtifactEntries,
} from "./session-accessor.sqlite-lifecycle-state.js";
import type { SessionEntryRemovalPlan } from "./session-accessor.sqlite-lifecycle-types.js";
import { deleteSessionDeliveryArtifacts } from "./session-accessor.sqlite-node-artifacts.js";
import { cloneSessionEntry, getSessionKysely } from "./session-accessor.sqlite-scope.js";
import type { InternalSessionEntry as SessionEntry } from "./types.js";

type SessionBoardCleanupDatabase = Pick<
  OpenClawAgentKyselyDatabase,
  "board_tabs" | "board_widgets"
> & {
  sqlite_schema: {
    name: string | null;
    type: string;
  };
};

type SqliteSessionReclamationPlanBase = {
  databaseOptions: OpenClawAgentDatabaseOptions & { path: string };
  materializedPlans: MaterializedSessionStateDeletePlan[];
};

type SqliteSessionEntryReclamationPlan = SqliteSessionReclamationPlanBase & {
  kind: "entry";
  params: DeleteSessionEntryLifecycleParams;
  preparedTargetSnapshot: SqliteLifecycleTargetSnapshot;
};

type SqliteLifecycleArtifactReclamationPlan = SqliteSessionReclamationPlanBase & {
  entries: SessionEntryRemovalPlan[];
  kind: "lifecycle-artifacts";
};

type SqliteHistoryEvictionReclamationPlan = SqliteSessionReclamationPlanBase & {
  kind: "history-eviction";
  protectedSessionIds: string[];
  sessionId: string;
};

type SqliteHistoricalGenerationReclamationPlan = SqliteSessionReclamationPlanBase & {
  kind: "historical-generation";
  params: DeleteSessionEntryLifecycleParams;
  preparedTargetSnapshot: SqliteLifecycleTargetSnapshot;
  protectedSessionIds: string[];
  sessionId: string;
};

export type SqliteSessionReclamationPlan =
  | SqliteSessionEntryReclamationPlan
  | SqliteLifecycleArtifactReclamationPlan
  | SqliteHistoryEvictionReclamationPlan
  | SqliteHistoricalGenerationReclamationPlan;

type SqliteLifecycleArtifactReclamationResult = {
  archivedTranscripts: SessionLifecycleArchivedTranscript[];
  removedEntries: number;
};

type SqliteHistoryEvictionReclamationResult = {
  archivedTranscripts: SessionLifecycleArchivedTranscript[];
  deleted: boolean;
};

type SqliteHistoricalGenerationReclamationResult = SqliteHistoryEvictionReclamationResult & {
  expectedEntryMismatch?: true;
};

type SqliteSessionReclamationResult =
  | { kind: "entry"; value: DeleteSessionEntryLifecycleResult }
  | { kind: "lifecycle-artifacts"; value: SqliteLifecycleArtifactReclamationResult }
  | { kind: "history-eviction"; value: SqliteHistoryEvictionReclamationResult }
  | { kind: "historical-generation"; value: SqliteHistoricalGenerationReclamationResult };

export type SqliteSessionEntryReclamationWorkerMessage =
  | {
      cleanupWarnings?: string[];
      result: SqliteSessionReclamationResult;
      type: "done";
    }
  | {
      cleanupWarnings?: string[];
      error: string;
      type: "failed";
    };

// Bound the whole materialize-and-reclaim phase, not only Worker execution. This
// prevents separate stores from retaining several maximum-size archive payloads.
const sqliteSessionReclamationQueue = new KeyedAsyncQueue();
const SQLITE_SESSION_RECLAMATION_QUEUE_KEY = "session-reclamation";
const reclamationLog = createSubsystemLogger("sessions/reclamation");

export function runExclusiveSqliteSessionReclamation<T>(task: () => Promise<T>): Promise<T> {
  return sqliteSessionReclamationQueue.enqueue(SQLITE_SESSION_RECLAMATION_QUEUE_KEY, task);
}

function deleteSessionBoardRows(
  database: OpenClawAgentDatabase,
  sessionKeys: readonly string[],
): void {
  const keys = [...new Set(sessionKeys)];
  if (keys.length === 0) {
    return;
  }
  const db = getNodeSqliteKysely<SessionBoardCleanupDatabase>(database.db);
  const tableRows = executeSqliteQuerySync(
    database.db,
    db
      .selectFrom("sqlite_schema")
      .select("name")
      .where("type", "=", "table")
      .where("name", "in", ["board_tabs", "board_widgets"]),
  ).rows;
  const tables = new Set(tableRows.map((row) => row.name));
  if (!tables.has("board_tabs") || !tables.has("board_widgets")) {
    return;
  }
  executeSqliteQuerySync(
    database.db,
    db.deleteFrom("board_widgets").where("session_key", "in", keys),
  );
  executeSqliteQuerySync(database.db, db.deleteFrom("board_tabs").where("session_key", "in", keys));
}

export function shouldDeleteSqliteSessionEntryLifecycle(
  database: OpenClawAgentDatabase,
  entry: SessionEntry | undefined,
  params: DeleteSessionEntryLifecycleParams,
): entry is SessionEntry {
  if (!entry) {
    return false;
  }
  if (
    params.expectedEntry !== undefined &&
    !sqliteSessionEntriesEqual(entry, params.expectedEntry)
  ) {
    return false;
  }
  if (
    params.expectedSessionId !== undefined &&
    (params.expectedSessionId === null
      ? entry.sessionId !== undefined
      : entry.sessionId !== params.expectedSessionId)
  ) {
    return false;
  }
  if (
    params.expectedLifecycleRevision !== undefined &&
    entry.lifecycleRevision !== params.expectedLifecycleRevision
  ) {
    return false;
  }
  if (params.expectedUpdatedAt !== undefined && entry.updatedAt !== params.expectedUpdatedAt) {
    return false;
  }
  if (params.expectedTranscript) {
    const expectedTranscript = params.expectedTranscript;
    const rows = executeSqliteQuerySync(
      database.db,
      getSessionKysely(database.db)
        .selectFrom("transcript_events")
        .select("event_json")
        .where("session_id", "=", expectedTranscript.sessionId)
        .orderBy("seq", "asc"),
    ).rows;
    if (
      entry.sessionId !== expectedTranscript.sessionId ||
      rows.length !== expectedTranscript.eventJson.length ||
      rows.some((row, index) => row.event_json !== expectedTranscript.eventJson[index])
    ) {
      return false;
    }
  }
  return true;
}

function expectedEntryMismatchResult(): DeleteSessionEntryLifecycleResult {
  return { archivedTranscripts: [], deleted: false, expectedEntryMismatch: true };
}

/** Executes the complete validated archive-before-delete transaction on one connection. */
function reclaimSqliteSessionEntryInTransaction(
  plan: SqliteSessionEntryReclamationPlan,
): DeleteSessionEntryLifecycleResult {
  return runOpenClawAgentWriteTransaction<DeleteSessionEntryLifecycleResult>((transactionDb) => {
    const transactionSnapshot = readLifecycleTargetSnapshot(transactionDb, plan.params.target);
    const transactionEntry = transactionSnapshot.primary?.entry;
    if (
      !sqliteLifecycleTargetSnapshotsEqual(plan.preparedTargetSnapshot, transactionSnapshot) ||
      !shouldDeleteSqliteSessionEntryLifecycle(transactionDb, transactionEntry, plan.params)
    ) {
      return expectedEntryMismatchResult();
    }
    const sessionKeys = [
      plan.params.target.canonicalKey,
      ...plan.params.target.storeKeys,
      ...transactionSnapshot.rows.map((row) => row.sessionKey),
    ];
    const archivedTranscripts = deleteMaterializedSessionStatePlans(
      transactionDb,
      plan.materializedPlans,
      undefined,
      new Set(sessionKeys),
    );
    deleteLifecycleTargetRows(transactionDb, plan.params.target);
    if (plan.params.deleteDeliveryArtifacts === true) {
      deleteSessionDeliveryArtifacts(
        transactionDb,
        plan.params.target.canonicalKey,
        sessionKeys.filter((key) => key !== plan.params.target.canonicalKey),
      );
    }
    deleteSessionBoardRows(transactionDb, sessionKeys);
    const deletedEntry = plan.preparedTargetSnapshot.primary?.entry;
    if (!deletedEntry) {
      throw new Error("SQLite reclamation plan is missing its prepared entry");
    }
    return {
      archivedTranscripts,
      deleted: true,
      deletedEntry: cloneSessionEntry(deletedEntry),
      ...(deletedEntry.sessionId ? { deletedSessionId: deletedEntry.sessionId } : {}),
    };
  }, plan.databaseOptions);
}

/** Commits lifecycle-artifact cleanup only if every planned entry is unchanged. */
function reclaimSqliteLifecycleArtifactsInTransaction(
  plan: SqliteLifecycleArtifactReclamationPlan,
): SqliteLifecycleArtifactReclamationResult {
  return runOpenClawAgentWriteTransaction<SqliteLifecycleArtifactReclamationResult>(
    (transactionDb) => {
      assertPlannedLifecycleArtifactEntriesUnchanged(transactionDb, plan.entries);
      const archivedTranscripts = deleteMaterializedSessionStatePlans(
        transactionDb,
        plan.materializedPlans,
        undefined,
        new Set(plan.entries.map((entry) => entry.sessionKey)),
      );
      const removedEntries = deletePlannedLifecycleArtifactEntries(transactionDb, plan.entries);
      return { archivedTranscripts, removedEntries };
    },
    plan.databaseOptions,
  );
}

function reclaimSqliteFreePagesBestEffort(
  databaseOptions: OpenClawAgentDatabaseOptions & { path: string },
): void {
  try {
    const database = openOpenClawAgentDatabase(databaseOptions);
    database.walMaintenance.checkpoint();
    const row: unknown =
      database.db /* sqlite-allow-raw: page accounting is exposed only via PRAGMA */
        .prepare("PRAGMA freelist_count")
        .get();
    const freePages = Number(isRecord(row) ? row.freelist_count : 0);
    if (Number.isSafeInteger(freePages) && freePages > 0) {
      // sqlite-allow-raw -- incremental vacuum is a maintenance PRAGMA, not a data query.
      database.db.exec(`PRAGMA incremental_vacuum(${freePages});`);
    }
    database.walMaintenance.checkpoint();
  } catch {
    // The row deletion is already committed. A later budget pass can reclaim pages.
  }
}

/** Revalidates one disk-budget candidate and reclaims its state atomically. */
function reclaimSqliteHistoryEvictionInTransaction(
  plan: SqliteHistoryEvictionReclamationPlan,
): SqliteHistoryEvictionReclamationResult {
  const result = runOpenClawAgentWriteTransaction<SqliteHistoryEvictionReclamationResult>(
    (transactionDb) => {
      const archivedTranscripts = deleteMaterializedSessionStatePlans(
        transactionDb,
        plan.materializedPlans,
        new Set(plan.protectedSessionIds),
      );
      const db = getSessionKysely(transactionDb.db);
      const deleted =
        executeSqliteQuerySync(
          transactionDb.db,
          db
            .selectFrom("session_windows")
            .select("session_id")
            .where("session_id", "=", plan.sessionId),
        ).rows.length === 0;
      return {
        archivedTranscripts: deleted ? archivedTranscripts : [],
        deleted,
      };
    },
    plan.databaseOptions,
  );
  if (result.deleted) {
    reclaimSqliteFreePagesBestEffort(plan.databaseOptions);
  }
  return result;
}

/** Revalidates and reclaims one direct-delete historical generation atomically. */
function reclaimSqliteHistoricalGenerationInTransaction(
  plan: SqliteHistoricalGenerationReclamationPlan,
): SqliteHistoricalGenerationReclamationResult {
  return runOpenClawAgentWriteTransaction<SqliteHistoricalGenerationReclamationResult>(
    (transactionDb) => {
      const transactionSnapshot = readLifecycleTargetSnapshot(transactionDb, plan.params.target);
      if (
        !sqliteLifecycleTargetSnapshotsEqual(plan.preparedTargetSnapshot, transactionSnapshot) ||
        !shouldDeleteSqliteSessionEntryLifecycle(
          transactionDb,
          transactionSnapshot.primary?.entry,
          plan.params,
        )
      ) {
        return { archivedTranscripts: [], deleted: false, expectedEntryMismatch: true };
      }
      const archivedTranscripts = deleteMaterializedSessionStatePlans(
        transactionDb,
        plan.materializedPlans,
        new Set(plan.protectedSessionIds),
      );
      const db = getSessionKysely(transactionDb.db);
      const deleted =
        executeSqliteQuerySync(
          transactionDb.db,
          db
            .selectFrom("session_windows")
            .select("session_id")
            .where("session_id", "=", plan.sessionId),
        ).rows.length === 0;
      return {
        archivedTranscripts: deleted ? archivedTranscripts : [],
        deleted,
      };
    },
    plan.databaseOptions,
  );
}

export function reclaimSqliteSessionInTransaction(
  plan: SqliteSessionReclamationPlan,
): SqliteSessionReclamationResult {
  switch (plan.kind) {
    case "entry":
      return { kind: plan.kind, value: reclaimSqliteSessionEntryInTransaction(plan) };
    case "lifecycle-artifacts":
      return { kind: plan.kind, value: reclaimSqliteLifecycleArtifactsInTransaction(plan) };
    case "history-eviction":
      return { kind: plan.kind, value: reclaimSqliteHistoryEvictionInTransaction(plan) };
    case "historical-generation":
      return { kind: plan.kind, value: reclaimSqliteHistoricalGenerationInTransaction(plan) };
  }
  throw new Error("Unsupported SQLite session reclamation plan");
}

function resolveSqliteSessionEntryReclamationWorkerUrl(currentModuleUrl = import.meta.url): URL {
  const currentPath = fileURLToPath(currentModuleUrl);
  const normalized = currentPath.replaceAll(path.sep, "/");
  const distMarker = "/dist/";
  const distIndex = normalized.lastIndexOf(distMarker);
  if (distIndex >= 0) {
    const distRoot = currentPath.slice(0, distIndex + distMarker.length);
    return pathToFileURL(
      path.join(distRoot, "config", "sessions", "session-accessor.sqlite-reclamation.worker.js"),
    );
  }
  const extension = path.extname(currentPath) || ".js";
  return new URL(`./session-accessor.sqlite-reclamation.worker${extension}`, currentModuleUrl);
}

function resolveSourceWorkerExecArgv(): string[] {
  const tsxApiUrl = import.meta.resolve("tsx/esm/api");
  const registerTsx = `import { register } from ${JSON.stringify(tsxApiUrl)}; register();`;
  return ["--import", `data:text/javascript,${encodeURIComponent(registerTsx)}`];
}

function prepareReclamationWorkerTransferList(plan: SqliteSessionReclamationPlan): ArrayBuffer[] {
  const buffers = new Set<ArrayBuffer>();
  for (const materializedPlan of plan.materializedPlans) {
    const archive = materializedPlan.archive;
    if (!archive) {
      continue;
    }
    const bytes = archive.bytes;
    let buffer: ArrayBuffer;
    if (
      bytes.buffer instanceof ArrayBuffer &&
      bytes.byteOffset === 0 &&
      bytes.byteLength === bytes.buffer.byteLength
    ) {
      buffer = bytes.buffer;
    } else {
      const ownedBytes = Uint8Array.from(bytes);
      archive.bytes = ownedBytes;
      buffer = ownedBytes.buffer;
    }
    buffers.add(buffer);
  }
  return [...buffers];
}

function spawnSqliteSessionReclamationWorker(
  plan: SqliteSessionReclamationPlan,
): Promise<SqliteSessionReclamationResult> {
  const workerUrl = resolveSqliteSessionEntryReclamationWorkerUrl();
  const transferList = prepareReclamationWorkerTransferList(plan);
  let worker: Worker;
  try {
    worker = new Worker(workerUrl, {
      execArgv: workerUrl.pathname.endsWith(".ts") ? resolveSourceWorkerExecArgv() : undefined,
      transferList,
      workerData: { plan, type: "sqlite-session-reclamation-v2" },
    });
  } catch (error) {
    return Promise.reject(toStringifiedError(error));
  }

  return new Promise((resolve, reject) => {
    let message: SqliteSessionEntryReclamationWorkerMessage | undefined;
    let workerError: Error | undefined;
    worker.on("message", (nextMessage: SqliteSessionEntryReclamationWorkerMessage) => {
      message = nextMessage;
    });
    worker.once("error", (error) => {
      // Wait for exit so the caller never races the Worker's SQLite handles on Windows.
      workerError = toStringifiedError(error);
    });
    worker.once("exit", (code) => {
      worker.removeAllListeners();
      if (message?.cleanupWarnings?.length) {
        reclamationLog.warn("SQLite session reclamation worker recovered cleanup failures", {
          errors: message.cleanupWarnings,
          path: plan.databaseOptions.path,
        });
      }
      try {
        resolve(resolveSqliteSessionReclamationWorkerExit({ code, message, workerError }));
      } catch (error) {
        reject(error);
      }
    });
  });
}

export function resolveSqliteSessionReclamationWorkerExit(params: {
  code: number;
  message?: SqliteSessionEntryReclamationWorkerMessage;
  workerError?: Error;
}): SqliteSessionReclamationResult {
  // A structured message is emitted only after database-handle and lease cleanup
  // settles. Once present, it is authoritative over a later Worker exit error.
  if (params.message?.type === "done") {
    return params.message.result;
  }
  if (params.message?.type === "failed") {
    throw new Error(params.message.error);
  }
  if (params.workerError) {
    throw params.workerError;
  }
  if (params.code !== 0) {
    throw new Error(`SQLite session reclamation worker exited with code ${params.code}`);
  }
  throw new Error("SQLite session reclamation worker exited without a result");
}

function resolveSqliteSessionReclamationPlan(
  plan: SqliteSessionReclamationPlan,
): Promise<SqliteSessionReclamationResult> {
  const databasePath = plan.databaseOptions.path;
  if (
    isIncognitoOpenClawAgentSqlitePath(databasePath, {
      agentId: plan.databaseOptions.agentId,
      env: plan.databaseOptions.env,
    })
  ) {
    return Promise.resolve(reclaimSqliteSessionInTransaction(plan));
  }
  return spawnSqliteSessionReclamationWorker(plan);
}

/** Pins Worker lease and registry writes to the shared-state root resolved by the parent. */
export function resolveSqliteSessionReclamationWorkerDatabaseOptions(
  options: OpenClawAgentDatabaseOptions,
): OpenClawAgentDatabaseOptions & { path: string } {
  const sourceEnv = options.env ?? process.env;
  const sharedStatePath = options.database?.path ?? resolveOpenClawStateSqlitePath(sourceEnv);
  const authoritativeStateDir = resolveOpenClawStateDirForDatabasePath(sharedStatePath);
  return {
    agentId: options.agentId,
    env: {
      ...sourceEnv,
      // Worker process.env and structured-cloned objects are case-sensitive on Windows.
      // Supplying the canonical key also preserves Vitest's parent-thread state root.
      OPENCLAW_STATE_DIR: authoritativeStateDir,
    },
    path: resolveOpenClawAgentSqlitePath(options),
  };
}

/** Keeps one atomic live-entry reclamation transaction off the Gateway event loop. */
export async function runSqliteSessionEntryReclamation(params: {
  databaseOptions: OpenClawAgentDatabaseOptions;
  materializedPlans: MaterializedSessionStateDeletePlan[];
  deleteParams: DeleteSessionEntryLifecycleParams;
  preparedTargetSnapshot: SqliteLifecycleTargetSnapshot;
}): Promise<DeleteSessionEntryLifecycleResult> {
  const plan: SqliteSessionEntryReclamationPlan = {
    databaseOptions: resolveSqliteSessionReclamationWorkerDatabaseOptions(params.databaseOptions),
    kind: "entry",
    materializedPlans: params.materializedPlans,
    params: params.deleteParams,
    preparedTargetSnapshot: params.preparedTargetSnapshot,
  };
  const result = await resolveSqliteSessionReclamationPlan(plan);
  if (result.kind !== plan.kind) {
    throw new Error(`SQLite session reclamation worker returned ${result.kind} for ${plan.kind}`);
  }
  return result.value;
}

/** Keeps one atomic lifecycle-artifact cleanup transaction off the Gateway event loop. */
export async function runSqliteLifecycleArtifactReclamation(params: {
  databaseOptions: OpenClawAgentDatabaseOptions;
  entries: SessionEntryRemovalPlan[];
  materializedPlans: MaterializedSessionStateDeletePlan[];
}): Promise<SqliteLifecycleArtifactReclamationResult> {
  const plan: SqliteLifecycleArtifactReclamationPlan = {
    databaseOptions: resolveSqliteSessionReclamationWorkerDatabaseOptions(params.databaseOptions),
    entries: params.entries,
    kind: "lifecycle-artifacts",
    materializedPlans: params.materializedPlans,
  };
  const result = await resolveSqliteSessionReclamationPlan(plan);
  if (result.kind !== plan.kind) {
    throw new Error(`SQLite session reclamation worker returned ${result.kind} for ${plan.kind}`);
  }
  return result.value;
}

/** Keeps one atomic historical-session eviction transaction off the Gateway event loop. */
export async function runSqliteHistoryEvictionReclamation(params: {
  databaseOptions: OpenClawAgentDatabaseOptions;
  materializedPlans: MaterializedSessionStateDeletePlan[];
  protectedSessionIds: ReadonlySet<string>;
  sessionId: string;
}): Promise<SqliteHistoryEvictionReclamationResult> {
  const plan: SqliteHistoryEvictionReclamationPlan = {
    databaseOptions: resolveSqliteSessionReclamationWorkerDatabaseOptions(params.databaseOptions),
    kind: "history-eviction",
    materializedPlans: params.materializedPlans,
    protectedSessionIds: [...params.protectedSessionIds],
    sessionId: params.sessionId,
  };
  const result = await resolveSqliteSessionReclamationPlan(plan);
  if (result.kind !== plan.kind) {
    throw new Error(`SQLite session reclamation worker returned ${result.kind} for ${plan.kind}`);
  }
  return result.value;
}

/** Keeps one direct-delete historical-generation transaction off the Gateway event loop. */
export async function runSqliteHistoricalGenerationReclamation(params: {
  databaseOptions: OpenClawAgentDatabaseOptions;
  deleteParams: DeleteSessionEntryLifecycleParams;
  materializedPlans: MaterializedSessionStateDeletePlan[];
  preparedTargetSnapshot: SqliteLifecycleTargetSnapshot;
  protectedSessionIds: ReadonlySet<string>;
  sessionId: string;
}): Promise<SqliteHistoricalGenerationReclamationResult> {
  const plan: SqliteHistoricalGenerationReclamationPlan = {
    databaseOptions: resolveSqliteSessionReclamationWorkerDatabaseOptions(params.databaseOptions),
    kind: "historical-generation",
    materializedPlans: params.materializedPlans,
    params: params.deleteParams,
    preparedTargetSnapshot: params.preparedTargetSnapshot,
    protectedSessionIds: [...params.protectedSessionIds],
    sessionId: params.sessionId,
  };
  const result = await resolveSqliteSessionReclamationPlan(plan);
  if (result.kind !== plan.kind) {
    throw new Error(`SQLite session reclamation worker returned ${result.kind} for ${plan.kind}`);
  }
  return result.value;
}
