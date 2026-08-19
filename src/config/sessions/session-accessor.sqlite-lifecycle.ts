import { normalizeOptionalString } from "@openclaw/normalization-core/string-coerce";
import { parseAgentSessionKey } from "../../routing/session-key.js";
import {
  isAgentHarnessSessionKey,
  isValidAgentHarnessSessionStoreEntry,
  MODEL_SELECTION_LOCK_REMOVAL_MESSAGE,
  resolveAgentHarnessSessionStoreEntryError,
} from "../../sessions/agent-harness-session-key.js";
import { runExclusiveSessionLifecycleMutation } from "../../sessions/session-lifecycle-admission.js";
import { emitSessionIdentityMutation } from "../../sessions/session-lifecycle-events.js";
import { withOpenClawAgentDatabaseReadOnly } from "../../state/openclaw-agent-db-readonly.js";
import {
  openOpenClawAgentDatabase,
  runOpenClawAgentWriteTransaction,
} from "../../state/openclaw-agent-db.js";
import type { ResetSessionEntryLifecycleMutation } from "./session-accessor.lifecycle-types.js";
import { publishSessionStateArchives } from "./session-accessor.sqlite-archive-store.js";
import { materializeSessionStateDeletePlans } from "./session-accessor.sqlite-archive.js";
import type {
  SessionLifecycleArchivedTranscript,
  DeleteSessionEntryLifecycleParams,
  DeleteSessionEntryLifecycleResult,
  ResetSessionEntryLifecycleParams,
  ResetSessionEntryLifecycleResult,
  SessionLifecycleArtifactCleanupParams,
  SessionLifecycleArtifactCleanupResult,
} from "./session-accessor.sqlite-contract.js";
import {
  sqliteLifecycleTargetSnapshotsEqual,
  sqliteSessionEntriesEqual,
} from "./session-accessor.sqlite-entry-equality.js";
import {
  assertLifecycleTargetUnchanged,
  deleteLegacySessionEntryRows,
  readLifecycleTargetSnapshot,
  rehomeSessionWindows,
  writeSessionEntry,
} from "./session-accessor.sqlite-entry-store.js";
import { emitArchivedTranscriptUpdates } from "./session-accessor.sqlite-events.js";
import { emitCommittedSessionEntryRemovals } from "./session-accessor.sqlite-identity.js";
import {
  planSessionLifecycleArtifactCleanup,
  planSessionStateDeleteIfUnreferenced,
  readSessionGenerationIdsForKeys,
  planSessionStateAfterEntryRemoval,
  readReferencedSessionIdsAfterTargetMutation,
} from "./session-accessor.sqlite-lifecycle-state.js";
import { loadTranscriptEventsFromDatabase } from "./session-accessor.sqlite-read.js";
import {
  runExclusiveSqliteSessionReclamation,
  runSqliteHistoricalGenerationReclamation,
  runSqliteLifecycleArtifactReclamation,
  runSqliteSessionEntryReclamation,
  shouldDeleteSqliteSessionEntryLifecycle,
} from "./session-accessor.sqlite-reclamation.js";
import {
  cloneSessionEntry,
  resolveSqliteReadScope,
  resolveSqliteStoreScope,
  resolveSqliteTranscriptArchiveDirectory,
  runExclusiveSqliteSessionWrite,
  toDatabaseOptions,
} from "./session-accessor.sqlite-scope.js";
import { appendTranscriptEventsInTransaction } from "./session-accessor.sqlite-transcript-store.js";
import {
  collectAdmissionProtectedSessionIds,
  kickSessionHistoryDiskBudgetMaintenance,
} from "./session-history-eviction.js";
import { buildSessionResetBoundaryPlan } from "./session-reset-boundary-event.js";
import type { InternalSessionEntry as SessionEntry } from "./types.js";

// Single-target lifecycle owner: cleanup, reset, guarded delete, and trusted rollback.

export async function cleanupSessionLifecycleArtifactsCore(
  params: SessionLifecycleArtifactCleanupParams,
): Promise<SessionLifecycleArtifactCleanupResult> {
  const sessionKeySegmentPrefix = params.sessionKeySegmentPrefix.trim();
  const transcriptContentMarker = params.transcriptContentMarker;
  const pluginOwnerId = params.pluginOwnerId?.trim();
  if (!sessionKeySegmentPrefix || !transcriptContentMarker) {
    return { removedEntries: 0, archivedTranscriptArtifacts: 0 };
  }

  const resolved = resolveSqliteReadScope({
    ...(params.agentId ? { agentId: params.agentId } : {}),
    storePath: params.storePath,
  });
  const databaseOptions = toDatabaseOptions(resolved);
  // Maintenance must not turn a read-only startup probe into a newly materialized agent store.
  if (!withOpenClawAgentDatabaseReadOnly(() => true, databaseOptions).found) {
    return { removedEntries: 0, archivedTranscriptArtifacts: 0 };
  }
  const cleanupPlan = await runExclusiveSqliteSessionWrite(resolved, async () => {
    const database = openOpenClawAgentDatabase(databaseOptions);
    return planSessionLifecycleArtifactCleanup(database, {
      ...(params.agentId !== undefined ? { agentId: resolved.agentId } : {}),
      archiveRemovedEntryTranscripts: params.archiveRemovedEntryTranscripts !== false,
      archiveDirectory: resolveSqliteTranscriptArchiveDirectory(resolved),
      ...(pluginOwnerId ? { pluginOwnerId } : {}),
      sessionKeySegmentPrefix,
      transcriptContentMarker,
      orphanTranscriptMinAgeMs: params.orphanTranscriptMinAgeMs,
      nowMs: params.nowMs ?? Date.now(),
    });
  });
  const committed = await runExclusiveSqliteSessionReclamation(async () => {
    const materializedPlans = await materializeSessionStateDeletePlans(cleanupPlan.deletePlans);
    return await runExclusiveSqliteSessionWrite(resolved, async () =>
      runSqliteLifecycleArtifactReclamation({
        databaseOptions,
        entries: cleanupPlan.entries,
        materializedPlans,
      }),
    );
  });
  emitCommittedSessionEntryRemovals(cleanupPlan.entries);
  const archivedTranscripts = await publishSessionStateArchives(
    resolved,
    committed.archivedTranscripts,
  );
  return {
    removedEntries: committed.removedEntries,
    archivedTranscriptArtifacts: archivedTranscripts.length,
  };
}

/** Resets one persisted session entry using SQLite session rows. */
export async function resetSessionEntryLifecycle(
  params: ResetSessionEntryLifecycleParams,
): Promise<ResetSessionEntryLifecycleResult> {
  const agentId = params.agentId ?? parseAgentSessionKey(params.target.canonicalKey)?.agentId;
  const resolved = resolveSqliteStoreScope(params.storePath, { agentId });
  // Retained reset history is the store's growth event; give the throttled
  // budget pass a chance to extract-and-evict once we finish.
  try {
    return await runExclusiveSqliteSessionWrite(resolved, async () => {
      const database = openOpenClawAgentDatabase(toDatabaseOptions(resolved));
      const targetSnapshot = readLifecycleTargetSnapshot(database, params.target);
      const current = targetSnapshot.primary;
      const nextEntry = await params.buildNextEntry({
        currentEntry: current ? cloneSessionEntry(current.entry) : undefined,
        primaryKey: params.target.canonicalKey,
      });
      const resetBoundaryPlan =
        params.resetBoundaryReason &&
        current?.entry.sessionId &&
        !sqliteSessionEntriesEqual(current.entry, nextEntry)
          ? await buildSessionResetBoundaryPlan({
              events: loadTranscriptEventsFromDatabase(database, current.entry.sessionId),
              reason: params.resetBoundaryReason,
            })
          : undefined;
      const mutation: ResetSessionEntryLifecycleMutation = {
        nextEntry: cloneSessionEntry(nextEntry),
        ...(current ? { previousEntry: cloneSessionEntry(current.entry) } : {}),
        ...(current?.entry.sessionId ? { previousSessionId: current.entry.sessionId } : {}),
      };
      runOpenClawAgentWriteTransaction((transactionDb) => {
        assertLifecycleTargetUnchanged(transactionDb, params.target, current?.entry, "reset");
        if (resetBoundaryPlan && current?.entry.sessionId) {
          const events = [...resetBoundaryPlan.seedEvents, resetBoundaryPlan.event];
          const appended = appendTranscriptEventsInTransaction(
            transactionDb,
            {
              ...resolved,
              sessionId: current.entry.sessionId,
              sessionKey: current.key,
            },
            events,
          );
          if (appended !== events.length) {
            throw new Error(`Failed to append reset boundary for ${current.key}`);
          }
        }
        writeSessionEntry(transactionDb, params.target.canonicalKey, nextEntry, {
          previousEntry: current?.entry ?? null,
        });
        rehomeSessionWindows(transactionDb, params.target.canonicalKey, params.target.storeKeys);
        deleteLegacySessionEntryRows(
          transactionDb,
          params.target.storeKeys,
          params.target.canonicalKey,
          { rehomeMembers: current?.entry.sessionId === nextEntry.sessionId },
        );
        // Reset only advances the live entry and route. Historical rows stay searchable;
        // disk-budget cleanup owns durable extraction before reclaiming them.
      }, toDatabaseOptions(resolved));
      if (current) {
        emitSessionIdentityMutation({
          kind: "reset",
          previous: {
            ...(current.entry.sessionId ? { sessionId: current.entry.sessionId } : {}),
            sessionKeys: targetSnapshot.rows.map((row) => row.sessionKey),
          },
          current: {
            ...(nextEntry.sessionId ? { sessionId: nextEntry.sessionId } : {}),
            sessionKeys: [params.target.canonicalKey],
          },
        });
      } else {
        emitSessionIdentityMutation({
          kind: "create",
          previous: { sessionKeys: [] },
          current: {
            ...(nextEntry.sessionId ? { sessionId: nextEntry.sessionId } : {}),
            sessionKeys: [params.target.canonicalKey],
          },
        });
      }
      await params.afterEntryMutation?.(mutation);
      return {
        ...mutation,
        archivedTranscripts: [],
      };
    });
  } finally {
    // Reset is what turns the old generation into an eviction candidate; a
    // throttled kick could be suppressed by a recent pre-reset pass and never
    // retried if the agent idles, leaving an over-budget store unreclaimed.
    kickSessionHistoryDiskBudgetMaintenance({
      ...(resolved.agentId ? { agentId: resolved.agentId } : {}),
      storePath: params.storePath,
      force: true,
    });
  }
}

async function deleteSqliteSessionEntryLifecycleInternal(
  params: DeleteSessionEntryLifecycleParams,
  allowLockedEntryRemoval: boolean,
  expectedPluginOwnerId?: string,
): Promise<DeleteSessionEntryLifecycleResult> {
  const agentId = params.agentId ?? parseAgentSessionKey(params.target.canonicalKey)?.agentId;
  const resolved = resolveSqliteStoreScope(params.storePath, { agentId });
  try {
    return await deleteSqliteSessionEntryLifecycleLocked(
      resolved,
      params,
      allowLockedEntryRemoval,
      expectedPluginOwnerId,
    );
  } finally {
    // Deletion writes an archive per retained generation before reclaiming
    // rows, so usage can spike well past the budget; force a pass instead of
    // waiting up to the throttle interval (or forever, if the agent idles).
    kickSessionHistoryDiskBudgetMaintenance({
      ...(params.agentId ? { agentId: params.agentId } : {}),
      storePath: params.storePath,
      force: true,
    });
  }
}

const DELETE_EXPECTED_ENTRY_MISMATCH = Symbol("delete-expected-entry-mismatch");

async function deleteSqliteSessionEntryLifecycleLocked(
  resolved: ReturnType<typeof resolveSqliteStoreScope>,
  params: DeleteSessionEntryLifecycleParams,
  allowLockedEntryRemoval: boolean,
  expectedPluginOwnerId?: string,
): Promise<DeleteSessionEntryLifecycleResult> {
  const prepared = await runExclusiveSqliteSessionWrite(resolved, async () => {
    const database = openOpenClawAgentDatabase(toDatabaseOptions(resolved));
    const targetSnapshot = readLifecycleTargetSnapshot(database, params.target);
    const current = targetSnapshot.primary;
    if (!current) {
      return null;
    }
    if (!shouldDeleteSqliteSessionEntryLifecycle(database, current.entry, params)) {
      return DELETE_EXPECTED_ENTRY_MISMATCH;
    }
    if (current.entry.modelSelectionLocked === true && !allowLockedEntryRemoval) {
      throw new Error(MODEL_SELECTION_LOCK_REMOVAL_MESSAGE);
    }
    if (
      expectedPluginOwnerId &&
      targetSnapshot.rows.some(
        ({ entry, sessionKey }) =>
          isAgentHarnessSessionKey(sessionKey) ||
          entry.agentHarnessId !== undefined ||
          entry.modelSelectionLocked !== true ||
          normalizeOptionalString(entry.pluginOwnerId) !== expectedPluginOwnerId,
      )
    ) {
      throw new Error(MODEL_SELECTION_LOCK_REMOVAL_MESSAGE);
    }
    const referencedAfterDelete = readReferencedSessionIdsAfterTargetMutation(
      database,
      params.target,
    );
    const deleteTranscriptState =
      params.archiveTranscript || params.deleteTranscriptWithoutArchive === true;
    // SQLite transcript state is keyed by session id; sessionFile is only its
    // marker. Materialization dedupes aliases that share the same state owner.
    const archiveDirectory = resolveSqliteTranscriptArchiveDirectory(resolved);
    const entryPlans = deleteTranscriptState
      ? targetSnapshot.rows.flatMap(({ entry }) =>
          planSessionStateAfterEntryRemoval({
            archiveDirectory,
            archiveTranscript: params.archiveTranscript,
            database,
            entry,
            reason: "deleted",
            referencedSessionIds: referencedAfterDelete,
          }),
        )
      : [];
    const entryPlanIds = new Set(entryPlans.map((plan) => plan.sessionId));
    // Ids only — archive extraction happens lazily one generation at a time
    // outside the SQLite write transaction.
    const historicalGenerationIds = deleteTranscriptState
      ? readSessionGenerationIdsForKeys(database, [
          params.target.canonicalKey,
          ...params.target.storeKeys,
          ...targetSnapshot.rows.map((row) => row.sessionKey),
        ]).filter((sessionId) => !entryPlanIds.has(sessionId))
      : [];
    // Historical generations are reclaimed BEFORE the entry-removing
    // transaction, one generation per transaction: an archive or delete
    // failure aborts the whole deletion while the live entry still exists,
    // so a retry rediscovers the remaining history. Acknowledging deletion
    // first would let surviving generations become unreachable via delete.
    // Preflight the admission fence over every generation BEFORE deleting
    // anything, so an in-flight run rejects the whole deletion instead of
    // aborting it midway through committed removals.
    const preflightFence = collectAdmissionProtectedSessionIds({
      database,
      storePath: params.storePath,
    });
    for (const sessionId of historicalGenerationIds) {
      if (preflightFence.has(sessionId) && !referencedAfterDelete.has(sessionId)) {
        throw new Error(
          `cannot delete session history while work is in flight for ${sessionId}; retry after the run completes`,
        );
      }
    }
    return { archiveDirectory, current, entryPlans, historicalGenerationIds, targetSnapshot };
  });
  if (!prepared) {
    await publishSessionStateArchives(resolved, []);
    return { archivedTranscripts: [], deleted: false };
  }
  if (prepared === DELETE_EXPECTED_ENTRY_MISMATCH) {
    await publishSessionStateArchives(resolved, []);
    return expectedEntryMismatchResult([]);
  }

  const historicalArchivedTranscripts: SessionLifecycleArchivedTranscript[] = [];
  for (const sessionId of prepared.historicalGenerationIds) {
    const reclamation = await runExclusiveSessionLifecycleMutation({
      scope: params.storePath,
      identities: [
        params.target.canonicalKey,
        ...params.target.storeKeys,
        ...prepared.targetSnapshot.rows.map((row) => row.sessionKey),
        sessionId,
      ],
      run: async () => {
        const plan = await runExclusiveSqliteSessionWrite(resolved, async () => {
          const database = openOpenClawAgentDatabase(toDatabaseOptions(resolved));
          const targetSnapshot = readLifecycleTargetSnapshot(database, params.target);
          if (
            !sqliteLifecycleTargetSnapshotsEqual(prepared.targetSnapshot, targetSnapshot) ||
            !shouldDeleteSqliteSessionEntryLifecycle(
              database,
              targetSnapshot.primary?.entry,
              params,
            )
          ) {
            return DELETE_EXPECTED_ENTRY_MISMATCH;
          }
          const referencedAfterDelete = readReferencedSessionIdsAfterTargetMutation(
            database,
            params.target,
          );
          if (referencedAfterDelete.has(sessionId)) {
            return null;
          }
          return planSessionStateDeleteIfUnreferenced({
            archiveDirectory: prepared.archiveDirectory,
            archiveTranscript: params.archiveTranscript,
            database,
            reason: "deleted",
            referencedSessionIds: referencedAfterDelete,
            sessionId,
          });
        });
        if (!plan || plan === DELETE_EXPECTED_ENTRY_MISMATCH) {
          return plan;
        }
        return await runExclusiveSqliteSessionReclamation(async () => {
          const materializedGeneration = await materializeSessionStateDeletePlans([plan]);
          const fenceAtDelete = await runExclusiveSqliteSessionWrite(resolved, async () => {
            const database = openOpenClawAgentDatabase(toDatabaseOptions(resolved));
            const targetSnapshot = readLifecycleTargetSnapshot(database, params.target);
            if (
              !sqliteLifecycleTargetSnapshotsEqual(prepared.targetSnapshot, targetSnapshot) ||
              !shouldDeleteSqliteSessionEntryLifecycle(
                database,
                targetSnapshot.primary?.entry,
                params,
              )
            ) {
              return DELETE_EXPECTED_ENTRY_MISMATCH;
            }
            return collectAdmissionProtectedSessionIds({
              database,
              storePath: params.storePath,
            });
          });
          if (fenceAtDelete === DELETE_EXPECTED_ENTRY_MISMATCH) {
            return fenceAtDelete;
          }
          if (fenceAtDelete.has(sessionId)) {
            throw new Error(
              `cannot delete session history while work is in flight for ${sessionId}; retry after the run completes`,
            );
          }
          return await runExclusiveSqliteSessionWrite(resolved, async () =>
            runSqliteHistoricalGenerationReclamation({
              databaseOptions: toDatabaseOptions(resolved),
              deleteParams: params,
              materializedPlans: materializedGeneration,
              preparedTargetSnapshot: prepared.targetSnapshot,
              protectedSessionIds: fenceAtDelete,
              sessionId,
            }),
          );
        });
      },
    });
    if (reclamation === DELETE_EXPECTED_ENTRY_MISMATCH) {
      return expectedEntryMismatchResult(historicalArchivedTranscripts);
    }
    if (!reclamation) {
      continue;
    }
    if (reclamation.expectedEntryMismatch) {
      return expectedEntryMismatchResult(historicalArchivedTranscripts);
    }
    if (!reclamation.deleted) {
      continue;
    }
    // Publish each committed generation immediately: a later archive or
    // transaction failure aborts the deletion, and observers must still see
    // the removals that already happened (retry completes the remainder).
    const publishedGeneration = await publishSessionStateArchives(
      resolved,
      reclamation.archivedTranscripts,
    );
    emitArchivedTranscriptUpdates(publishedGeneration);
    historicalArchivedTranscripts.push(...publishedGeneration);
  }

  // Archive materialization remains outside the store writer lane. The global
  // reclamation lane bounds whole-buffer residency until the Worker exits.
  const result = await runExclusiveSqliteSessionReclamation(async () => {
    const materializedPlans = await materializeSessionStateDeletePlans(prepared.entryPlans);
    return await runExclusiveSqliteSessionWrite(resolved, async () =>
      runSqliteSessionEntryReclamation({
        databaseOptions: toDatabaseOptions(resolved),
        deleteParams: params,
        materializedPlans,
        preparedTargetSnapshot: prepared.targetSnapshot,
      }),
    );
  });
  if (result.deleted) {
    emitSessionIdentityMutation({
      kind: "delete",
      previous: {
        ...(prepared.current.entry.sessionId
          ? { sessionId: prepared.current.entry.sessionId }
          : {}),
        sessionKeys: prepared.targetSnapshot.rows.map((row) => row.sessionKey),
      },
    });
  }
  result.archivedTranscripts = await publishSessionStateArchives(
    resolved,
    result.archivedTranscripts,
  );
  emitArchivedTranscriptUpdates(result.archivedTranscripts);
  // Historical generations were emitted per commit above; merge them into
  // the result after the final emit so callers still see every archive.
  result.archivedTranscripts.push(...historicalArchivedTranscripts);
  return result;
}

function expectedEntryMismatchResult(
  archivedTranscripts: SessionLifecycleArchivedTranscript[],
): DeleteSessionEntryLifecycleResult {
  return { archivedTranscripts, deleted: false, expectedEntryMismatch: true };
}

/** Deletes one persisted session entry using SQLite session rows. */
export async function deleteSessionEntryLifecycle(
  params: DeleteSessionEntryLifecycleParams,
): Promise<DeleteSessionEntryLifecycleResult> {
  return await deleteSqliteSessionEntryLifecycleInternal(params, false);
}

/** Rolls back one exact locked row created by failed trusted harness initialization. */
export async function rollbackAgentHarnessSessionEntryLifecycle(
  params: DeleteSessionEntryLifecycleParams & { expectedEntry: SessionEntry },
): Promise<DeleteSessionEntryLifecycleResult> {
  const hasExactTarget =
    params.target.storeKeys.length === 1 &&
    params.target.storeKeys[0] === params.target.canonicalKey;
  const expectedEntryError = resolveAgentHarnessSessionStoreEntryError(
    params.target.canonicalKey,
    params.expectedEntry,
  );
  if (
    !hasExactTarget ||
    expectedEntryError ||
    !isValidAgentHarnessSessionStoreEntry(params.target.canonicalKey, params.expectedEntry)
  ) {
    throw new Error(expectedEntryError ?? MODEL_SELECTION_LOCK_REMOVAL_MESSAGE);
  }
  return await deleteSqliteSessionEntryLifecycleInternal(params, true);
}

/** Rolls back one exact locked CLI row created by a failed plugin initializer. */
export async function rollbackPluginOwnedSessionEntryLifecycle(
  params: DeleteSessionEntryLifecycleParams & {
    expectedEntry: SessionEntry;
    expectedPluginOwnerId: string;
  },
): Promise<DeleteSessionEntryLifecycleResult> {
  const expectedEntry = params.expectedEntry;
  const validPluginOwner = normalizeOptionalString(expectedEntry.pluginOwnerId);
  const expectedPluginOwner = normalizeOptionalString(params.expectedPluginOwnerId);
  if (
    isAgentHarnessSessionKey(params.target.canonicalKey) ||
    expectedEntry.agentHarnessId !== undefined ||
    expectedEntry.modelSelectionLocked !== true ||
    !validPluginOwner ||
    validPluginOwner !== expectedPluginOwner
  ) {
    throw new Error(MODEL_SELECTION_LOCK_REMOVAL_MESSAGE);
  }
  return await deleteSqliteSessionEntryLifecycleInternal(params, true, expectedPluginOwner);
}
