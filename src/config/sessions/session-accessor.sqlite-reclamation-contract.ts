import type {
  DeleteSessionEntryLifecycleResult,
  SessionLifecycleArchivedTranscript,
} from "./session-accessor.sqlite-contract.js";

export type SqliteLifecycleArtifactReclamationResult = {
  archivedTranscripts: SessionLifecycleArchivedTranscript[];
  removedEntries: number;
};

export type SqliteHistoryEvictionReclamationResult = {
  archivedTranscripts: SessionLifecycleArchivedTranscript[];
  deleted: boolean;
};

export type SqliteHistoricalGenerationReclamationResult = SqliteHistoryEvictionReclamationResult & {
  expectedEntryMismatch?: true;
};

export type SqliteSessionReclamationResult =
  | { kind: "entry"; value: DeleteSessionEntryLifecycleResult }
  | { kind: "lifecycle-artifacts"; value: SqliteLifecycleArtifactReclamationResult }
  | { kind: "history-eviction"; value: SqliteHistoryEvictionReclamationResult }
  | { kind: "historical-generation"; value: SqliteHistoricalGenerationReclamationResult };

export type SqliteSessionEntryReclamationWorkerMessage =
  | {
      cleanupIncomplete?: true;
      cleanupWarnings?: string[];
      result: SqliteSessionReclamationResult;
      type: "done";
    }
  | {
      cleanupIncomplete?: true;
      cleanupWarnings?: string[];
      error: string;
      type: "failed";
    };
