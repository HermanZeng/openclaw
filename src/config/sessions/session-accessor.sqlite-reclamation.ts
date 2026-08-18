import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { Worker } from "node:worker_threads";
import { toStringifiedError } from "@openclaw/normalization-core/error-coercion";
import { executeSqliteQuerySync, getNodeSqliteKysely } from "../../infra/kysely-sync.js";
import { KeyedAsyncQueue } from "../../plugin-sdk/keyed-async-queue.js";
import type { DB as OpenClawAgentKyselyDatabase } from "../../state/openclaw-agent-db.generated.js";
import {
  isIncognitoOpenClawAgentSqlitePath,
  resolveOpenClawAgentSqlitePath,
  runOpenClawAgentWriteTransaction,
  type OpenClawAgentDatabase,
  type OpenClawAgentDatabaseOptions,
} from "../../state/openclaw-agent-db.js";
import type { MaterializedSessionStateDeletePlan } from "./session-accessor.sqlite-archive.js";
import type {
  DeleteSessionEntryLifecycleParams,
  DeleteSessionEntryLifecycleResult,
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
import { deleteMaterializedSessionStatePlans } from "./session-accessor.sqlite-lifecycle-state.js";
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

export type SqliteSessionEntryReclamationPlan = {
  databaseOptions: OpenClawAgentDatabaseOptions & { path: string };
  materializedPlans: MaterializedSessionStateDeletePlan[];
  params: DeleteSessionEntryLifecycleParams;
  preparedTargetSnapshot: SqliteLifecycleTargetSnapshot;
};

export type SqliteSessionEntryReclamationWorkerMessage = {
  result: DeleteSessionEntryLifecycleResult;
  type: "done";
};

// Bound the whole materialize-and-reclaim phase, not only Worker execution. This
// prevents separate stores from retaining several maximum-size archive payloads.
const sqliteSessionEntryReclamationQueue = new KeyedAsyncQueue();
const SQLITE_SESSION_ENTRY_RECLAMATION_QUEUE_KEY = "session-entry-reclamation";

export function runExclusiveSqliteSessionEntryReclamation<T>(task: () => Promise<T>): Promise<T> {
  return sqliteSessionEntryReclamationQueue.enqueue(
    SQLITE_SESSION_ENTRY_RECLAMATION_QUEUE_KEY,
    task,
  );
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
export function reclaimSqliteSessionEntryInTransaction(
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

function prepareReclamationWorkerTransferList(
  plan: SqliteSessionEntryReclamationPlan,
): ArrayBuffer[] {
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

function spawnSqliteSessionEntryReclamationWorker(
  plan: SqliteSessionEntryReclamationPlan,
): Promise<DeleteSessionEntryLifecycleResult> {
  const workerUrl = resolveSqliteSessionEntryReclamationWorkerUrl();
  const transferList = prepareReclamationWorkerTransferList(plan);
  let worker: Worker;
  try {
    worker = new Worker(workerUrl, {
      execArgv: workerUrl.pathname.endsWith(".ts") ? resolveSourceWorkerExecArgv() : undefined,
      transferList,
      workerData: { plan, type: "sqlite-session-entry-reclamation-v1" },
    });
  } catch (error) {
    return Promise.reject(toStringifiedError(error));
  }

  return new Promise((resolve, reject) => {
    let result: DeleteSessionEntryLifecycleResult | undefined;
    let workerError: Error | undefined;
    worker.on("message", (message: SqliteSessionEntryReclamationWorkerMessage) => {
      if (message.type === "done") {
        result = message.result;
      }
    });
    worker.once("error", (error) => {
      // Wait for exit so the caller never races the Worker's SQLite handles on Windows.
      workerError = toStringifiedError(error);
    });
    worker.once("exit", (code) => {
      worker.removeAllListeners();
      if (workerError) {
        reject(workerError);
        return;
      }
      if (code !== 0) {
        reject(new Error(`SQLite session reclamation worker exited with code ${code}`));
        return;
      }
      if (!result) {
        reject(new Error("SQLite session reclamation worker exited without a result"));
        return;
      }
      resolve(result);
    });
  });
}

/** Keeps one atomic live-entry reclamation transaction off the Gateway event loop. */
export function runSqliteSessionEntryReclamation(params: {
  databaseOptions: OpenClawAgentDatabaseOptions;
  materializedPlans: MaterializedSessionStateDeletePlan[];
  deleteParams: DeleteSessionEntryLifecycleParams;
  preparedTargetSnapshot: SqliteLifecycleTargetSnapshot;
}): Promise<DeleteSessionEntryLifecycleResult> {
  const databasePath = resolveOpenClawAgentSqlitePath(params.databaseOptions);
  const plan: SqliteSessionEntryReclamationPlan = {
    databaseOptions: { ...params.databaseOptions, path: databasePath },
    materializedPlans: params.materializedPlans,
    params: params.deleteParams,
    preparedTargetSnapshot: params.preparedTargetSnapshot,
  };
  if (
    isIncognitoOpenClawAgentSqlitePath(databasePath, {
      agentId: params.databaseOptions.agentId,
      env: params.databaseOptions.env,
    })
  ) {
    return Promise.resolve(reclaimSqliteSessionEntryInTransaction(plan));
  }
  return spawnSqliteSessionEntryReclamationWorker(plan);
}
