/** Worker entrypoint for the final SQLite session reclamation transaction. */
import { parentPort, workerData } from "node:worker_threads";
import { toStringifiedError } from "@openclaw/normalization-core/error-coercion";
import { isRecord } from "@openclaw/normalization-core/record-coerce";
import {
  settleOpenClawAgentDatabaseWorkerClose,
  type OpenClawAgentDatabaseWorkerCloseResult,
  type OpenClawAgentDatabaseOptions,
} from "../../state/openclaw-agent-db.js";
import type { MaterializedSessionStateDeletePlan } from "./session-accessor.sqlite-archive.js";
import type {
  DeleteSessionEntryLifecycleParams,
  SessionLifecycleArchivedTranscript,
} from "./session-accessor.sqlite-contract.js";
import type { SessionStateDeleteSnapshot } from "./session-accessor.sqlite-delete-snapshot.types.js";
import type { SqliteLifecycleTargetSnapshot } from "./session-accessor.sqlite-entry-store.js";
import type { SessionEntryRemovalPlan } from "./session-accessor.sqlite-lifecycle-types.js";
import {
  reclaimSqliteSessionInTransaction,
  type SqliteSessionEntryReclamationWorkerMessage,
  type SqliteSessionReclamationPlan,
} from "./session-accessor.sqlite-reclamation.js";
import type { InternalSessionEntry as SessionEntry } from "./types.js";

function parseEnvironment(value: unknown): NodeJS.ProcessEnv | undefined | null {
  if (value === undefined) {
    return undefined;
  }
  if (!isRecord(value)) {
    return null;
  }
  const env: NodeJS.ProcessEnv = {};
  for (const [key, entry] of Object.entries(value)) {
    if (entry !== undefined && typeof entry !== "string") {
      return null;
    }
    env[key] = entry;
  }
  return env;
}

const WORKER_CLOSE_RETRY_INITIAL_MS = 25;
const WORKER_CLOSE_RETRY_MAX_MS = 500;
const WORKER_CLOSE_MAX_ATTEMPTS = 3;

export type SqliteSessionReclamationWorkerDatabaseSettlement = {
  attempts: number;
  cleanupWarnings: string[];
  settled: boolean;
};

export async function settleSqliteSessionReclamationWorkerDatabase(
  pathname: string,
  dependencies: {
    close?: (targetPath: string) => OpenClawAgentDatabaseWorkerCloseResult;
    delay?: (delayMs: number) => Promise<void>;
  } = {},
): Promise<SqliteSessionReclamationWorkerDatabaseSettlement> {
  const close = dependencies.close ?? settleOpenClawAgentDatabaseWorkerClose;
  const delay =
    dependencies.delay ??
    ((delayMs: number) =>
      new Promise<void>((resolve) => {
        setTimeout(resolve, delayMs);
      }));
  const warnings = new Set<string>();
  let retryDelayMs = WORKER_CLOSE_RETRY_INITIAL_MS;
  for (let attempt = 1; attempt <= WORKER_CLOSE_MAX_ATTEMPTS; attempt += 1) {
    const outcome = close(pathname);
    for (const error of outcome.errors) {
      warnings.add(error.message);
    }
    if (outcome.settled) {
      return { attempts: attempt, cleanupWarnings: [...warnings], settled: true };
    }
    if (attempt < WORKER_CLOSE_MAX_ATTEMPTS) {
      await delay(retryDelayMs);
      retryDelayMs = Math.min(retryDelayMs * 2, WORKER_CLOSE_RETRY_MAX_MS);
    }
  }
  warnings.add(
    `SQLite session reclamation worker database cleanup remained incomplete after ${WORKER_CLOSE_MAX_ATTEMPTS} attempts`,
  );
  return {
    attempts: WORKER_CLOSE_MAX_ATTEMPTS,
    cleanupWarnings: [...warnings],
    settled: false,
  };
}

function parseStringArray(value: unknown): string[] | undefined {
  if (!Array.isArray(value)) {
    return undefined;
  }
  const strings: string[] = [];
  for (const entry of value) {
    if (typeof entry !== "string") {
      return undefined;
    }
    strings.push(entry);
  }
  return strings;
}

function isStructuredCloneSessionEntry(value: unknown): value is SessionEntry {
  return (
    isRecord(value) &&
    typeof value.sessionId === "string" &&
    typeof value.updatedAt === "number" &&
    Number.isFinite(value.updatedAt)
  );
}

function parseSessionEntry(value: unknown): SessionEntry | undefined {
  // The parent created this payload with structured clone. Preserve projection-only
  // fields such as owner because they participate in the exact snapshot fence.
  return isStructuredCloneSessionEntry(value) ? value : undefined;
}

function parseLifecycleTargetSnapshot(value: unknown): SqliteLifecycleTargetSnapshot | undefined {
  if (!isRecord(value) || !Array.isArray(value.rows)) {
    return undefined;
  }
  const rows: SqliteLifecycleTargetSnapshot["rows"] = [];
  for (const rowValue of value.rows) {
    if (!isRecord(rowValue) || typeof rowValue.sessionKey !== "string") {
      return undefined;
    }
    const entry = parseSessionEntry(rowValue.entry);
    if (!entry) {
      return undefined;
    }
    rows.push({ entry, sessionKey: rowValue.sessionKey });
  }
  let primary: SqliteLifecycleTargetSnapshot["primary"];
  if (value.primary !== undefined) {
    if (!isRecord(value.primary) || typeof value.primary.key !== "string") {
      return undefined;
    }
    const entry = parseSessionEntry(value.primary.entry);
    if (!entry) {
      return undefined;
    }
    primary = { entry, key: value.primary.key };
  }
  return { primary, rows };
}

function parseDeleteParams(value: unknown): DeleteSessionEntryLifecycleParams | undefined {
  if (!isRecord(value) || !isRecord(value.target)) {
    return undefined;
  }
  const target = value.target;
  const storeKeys = parseStringArray(target.storeKeys);
  const archiveTranscript = value.archiveTranscript;
  const storePath = value.storePath;
  const canonicalKey = target.canonicalKey;
  const agentId = value.agentId;
  const deleteTranscriptWithoutArchive = value.deleteTranscriptWithoutArchive;
  const deleteDeliveryArtifacts = value.deleteDeliveryArtifacts;
  const expectedSessionId = value.expectedSessionId;
  const expectedLifecycleRevision = value.expectedLifecycleRevision;
  const expectedUpdatedAt = value.expectedUpdatedAt;
  const requireWriteSuccess = value.requireWriteSuccess;
  if (
    typeof archiveTranscript !== "boolean" ||
    typeof storePath !== "string" ||
    typeof canonicalKey !== "string" ||
    !storeKeys ||
    (agentId !== undefined && typeof agentId !== "string") ||
    (deleteTranscriptWithoutArchive !== undefined &&
      typeof deleteTranscriptWithoutArchive !== "boolean") ||
    (deleteDeliveryArtifacts !== undefined && typeof deleteDeliveryArtifacts !== "boolean") ||
    (expectedSessionId !== undefined &&
      expectedSessionId !== null &&
      typeof expectedSessionId !== "string") ||
    (expectedLifecycleRevision !== undefined && typeof expectedLifecycleRevision !== "string") ||
    (expectedUpdatedAt !== undefined && typeof expectedUpdatedAt !== "number") ||
    (requireWriteSuccess !== undefined && typeof requireWriteSuccess !== "boolean")
  ) {
    return undefined;
  }
  const expectedEntry =
    value.expectedEntry === undefined ? undefined : parseSessionEntry(value.expectedEntry);
  if (value.expectedEntry !== undefined && !expectedEntry) {
    return undefined;
  }
  let expectedTranscript: DeleteSessionEntryLifecycleParams["expectedTranscript"];
  if (value.expectedTranscript !== undefined) {
    if (!isRecord(value.expectedTranscript)) {
      return undefined;
    }
    const eventJson = parseStringArray(value.expectedTranscript.eventJson);
    const sessionId = value.expectedTranscript.sessionId;
    if (!eventJson || typeof sessionId !== "string") {
      return undefined;
    }
    expectedTranscript = { eventJson, sessionId };
  }
  return {
    ...(agentId !== undefined ? { agentId } : {}),
    archiveTranscript,
    ...(deleteTranscriptWithoutArchive !== undefined ? { deleteTranscriptWithoutArchive } : {}),
    ...(deleteDeliveryArtifacts !== undefined ? { deleteDeliveryArtifacts } : {}),
    ...(expectedEntry ? { expectedEntry } : {}),
    ...(expectedTranscript ? { expectedTranscript } : {}),
    ...(expectedSessionId !== undefined ? { expectedSessionId } : {}),
    ...(expectedLifecycleRevision !== undefined ? { expectedLifecycleRevision } : {}),
    ...(expectedUpdatedAt !== undefined ? { expectedUpdatedAt } : {}),
    ...(requireWriteSuccess !== undefined ? { requireWriteSuccess } : {}),
    storePath,
    target: { canonicalKey, storeKeys },
  };
}

function parseDeleteSnapshot(value: unknown): SessionStateDeleteSnapshot | undefined {
  if (!isRecord(value)) {
    return undefined;
  }
  const acpParentStreamEventCount = value.acpParentStreamEventCount;
  const generation = value.generation;
  const lastSeq = value.lastSeq;
  const sessionKey = value.sessionKey;
  const sessionUpdatedAt = value.sessionUpdatedAt;
  const trajectoryLastSeq = value.trajectoryLastSeq;
  const transcriptUpdatedAt = value.transcriptUpdatedAt;
  if (
    typeof acpParentStreamEventCount !== "number" ||
    (generation !== null && typeof generation !== "string") ||
    (lastSeq !== null && typeof lastSeq !== "number") ||
    (sessionKey !== null && typeof sessionKey !== "string") ||
    (sessionUpdatedAt !== null && typeof sessionUpdatedAt !== "number") ||
    (trajectoryLastSeq !== null && typeof trajectoryLastSeq !== "number") ||
    (transcriptUpdatedAt !== null && typeof transcriptUpdatedAt !== "number")
  ) {
    return undefined;
  }
  return {
    acpParentStreamEventCount,
    generation,
    lastSeq,
    sessionKey,
    sessionUpdatedAt,
    trajectoryLastSeq,
    transcriptUpdatedAt,
  };
}

function parseArchivedTranscript(
  value: unknown,
): SessionLifecycleArchivedTranscript | null | undefined {
  if (value === null) {
    return null;
  }
  if (!isRecord(value)) {
    return undefined;
  }
  const generation = value.generation;
  const sessionId = value.sessionId;
  const sourcePath = value.sourcePath;
  const archivedPath = value.archivedPath;
  if (
    typeof generation !== "string" ||
    typeof sessionId !== "string" ||
    typeof sourcePath !== "string" ||
    typeof archivedPath !== "string"
  ) {
    return undefined;
  }
  return { archivedPath, generation, sessionId, sourcePath };
}

function parseMaterializedPlans(value: unknown): MaterializedSessionStateDeletePlan[] | undefined {
  if (!Array.isArray(value)) {
    return undefined;
  }
  const plans: MaterializedSessionStateDeletePlan[] = [];
  for (const planValue of value) {
    if (!isRecord(planValue)) {
      return undefined;
    }
    const agentId = planValue.agentId;
    const archiveDirectory = planValue.archiveDirectory;
    const archiveTranscript = planValue.archiveTranscript;
    const databasePath = planValue.databasePath;
    const reason = planValue.reason;
    const sessionId = planValue.sessionId;
    const snapshot = parseDeleteSnapshot(planValue.snapshot);
    const archivedTranscript = parseArchivedTranscript(planValue.archivedTranscript);
    if (
      typeof agentId !== "string" ||
      typeof archiveDirectory !== "string" ||
      typeof archiveTranscript !== "boolean" ||
      typeof databasePath !== "string" ||
      (reason !== "deleted" && reason !== "reset") ||
      typeof sessionId !== "string" ||
      !snapshot ||
      archivedTranscript === undefined
    ) {
      return undefined;
    }
    let archive: MaterializedSessionStateDeletePlan["archive"];
    if (planValue.archive === null) {
      archive = null;
    } else if (isRecord(planValue.archive)) {
      const archiveName = planValue.archive.archiveName;
      const bytes = planValue.archive.bytes;
      const createdAt = planValue.archive.createdAt;
      const encoding = planValue.archive.encoding;
      const sha256 = planValue.archive.sha256;
      if (
        typeof archiveName !== "string" ||
        !(bytes instanceof Uint8Array) ||
        typeof createdAt !== "number" ||
        (encoding !== "identity" && encoding !== "zstd") ||
        typeof sha256 !== "string"
      ) {
        return undefined;
      }
      archive = { archiveName, bytes, createdAt, encoding, sha256 };
    } else {
      return undefined;
    }
    plans.push({
      agentId,
      archive,
      archiveDirectory,
      archivedTranscript,
      archiveTranscript,
      databasePath,
      reason,
      sessionId,
      snapshot,
    });
  }
  return plans;
}

function parseEntryRemovalPlans(value: unknown): SessionEntryRemovalPlan[] | undefined {
  if (!Array.isArray(value)) {
    return undefined;
  }
  const entries: SessionEntryRemovalPlan[] = [];
  for (const entryValue of value) {
    if (!isRecord(entryValue) || typeof entryValue.sessionKey !== "string") {
      return undefined;
    }
    const expectedEntry =
      entryValue.expectedEntry === undefined
        ? undefined
        : parseSessionEntry(entryValue.expectedEntry);
    if (entryValue.expectedEntry !== undefined && !expectedEntry) {
      return undefined;
    }
    entries.push({ expectedEntry, sessionKey: entryValue.sessionKey });
  }
  return entries;
}

function parseWorkerPlan(value: unknown): SqliteSessionReclamationPlan | undefined {
  if (!isRecord(value) || !isRecord(value.databaseOptions)) {
    return undefined;
  }
  const databaseOptions = value.databaseOptions;
  const agentId = databaseOptions.agentId;
  const databasePath = databaseOptions.path;
  const env = parseEnvironment(databaseOptions.env);
  const materializedPlans = parseMaterializedPlans(value.materializedPlans);
  if (
    typeof agentId !== "string" ||
    typeof databasePath !== "string" ||
    env === null ||
    !materializedPlans
  ) {
    return undefined;
  }
  const parsedDatabaseOptions = {
    agentId,
    ...(env !== undefined ? { env } : {}),
    path: databasePath,
  } satisfies OpenClawAgentDatabaseOptions & { path: string };
  if (value.kind === "entry") {
    const params = parseDeleteParams(value.params);
    const preparedTargetSnapshot = parseLifecycleTargetSnapshot(value.preparedTargetSnapshot);
    if (!params || !preparedTargetSnapshot?.primary) {
      return undefined;
    }
    return {
      databaseOptions: parsedDatabaseOptions,
      kind: value.kind,
      materializedPlans,
      params,
      preparedTargetSnapshot,
    };
  }
  if (value.kind === "lifecycle-artifacts") {
    const entries = parseEntryRemovalPlans(value.entries);
    if (!entries) {
      return undefined;
    }
    return {
      databaseOptions: parsedDatabaseOptions,
      entries,
      kind: value.kind,
      materializedPlans,
    };
  }
  if (value.kind === "history-eviction") {
    const protectedSessionIds = parseStringArray(value.protectedSessionIds);
    if (!protectedSessionIds || typeof value.sessionId !== "string") {
      return undefined;
    }
    return {
      databaseOptions: parsedDatabaseOptions,
      kind: value.kind,
      materializedPlans,
      protectedSessionIds,
      sessionId: value.sessionId,
    };
  }
  if (value.kind === "historical-generation") {
    const params = parseDeleteParams(value.params);
    const preparedTargetSnapshot = parseLifecycleTargetSnapshot(value.preparedTargetSnapshot);
    const protectedSessionIds = parseStringArray(value.protectedSessionIds);
    if (
      !params ||
      !preparedTargetSnapshot?.primary ||
      !protectedSessionIds ||
      typeof value.sessionId !== "string"
    ) {
      return undefined;
    }
    return {
      databaseOptions: parsedDatabaseOptions,
      kind: value.kind,
      materializedPlans,
      params,
      preparedTargetSnapshot,
      protectedSessionIds,
      sessionId: value.sessionId,
    };
  }
  return undefined;
}

type SqliteSessionReclamationWorkerPort = Pick<
  NonNullable<typeof parentPort>,
  "close" | "postMessage"
>;

export async function runSqliteSessionReclamationWorker(
  data: unknown,
  port: SqliteSessionReclamationWorkerPort | null,
  dependencies: {
    reclaim?: typeof reclaimSqliteSessionInTransaction;
    settle?: typeof settleSqliteSessionReclamationWorkerDatabase;
  } = {},
): Promise<void> {
  if (!isRecord(data) || data.type !== "sqlite-session-reclamation-v2") {
    return;
  }
  if (!port) {
    throw new Error("SQLite session reclamation worker requires a parent port");
  }
  const plan = parseWorkerPlan(data.plan);
  if (!plan) {
    throw new Error("SQLite session reclamation worker requires valid plan data");
  }
  let message:
    | Omit<Extract<SqliteSessionEntryReclamationWorkerMessage, { type: "done" }>, "cleanupWarnings">
    | Omit<
        Extract<SqliteSessionEntryReclamationWorkerMessage, { type: "failed" }>,
        "cleanupWarnings"
      >;
  try {
    const result = (dependencies.reclaim ?? reclaimSqliteSessionInTransaction)(plan);
    message = { result, type: "done" };
  } catch (error) {
    message = { error: toStringifiedError(error).message, type: "failed" };
  }
  const cleanup = await (dependencies.settle ?? settleSqliteSessionReclamationWorkerDatabase)(
    plan.databaseOptions.path,
  );
  try {
    const postMessage = port.postMessage.bind(port);
    postMessage({
      ...message,
      ...(cleanup.cleanupWarnings.length > 0 ? { cleanupWarnings: cleanup.cleanupWarnings } : {}),
      ...(!cleanup.settled ? { cleanupIncomplete: true as const } : {}),
    } satisfies SqliteSessionEntryReclamationWorkerMessage);
  } finally {
    port.close();
  }
}

void runSqliteSessionReclamationWorker(workerData, parentPort);
