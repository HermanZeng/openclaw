import type { Worker } from "node:worker_threads";
import { toStringifiedError } from "@openclaw/normalization-core/error-coercion";
import { createSubsystemLogger } from "../../logging/subsystem.js";
import type {
  SqliteSessionEntryReclamationWorkerMessage,
  SqliteSessionReclamationResult,
} from "./session-accessor.sqlite-reclamation-contract.js";

const reclamationLog = createSubsystemLogger("sessions/reclamation");

export function resolveSqliteSessionReclamationSourceWorkerExecArgv(): string[] {
  const tsxApiUrl = import.meta.resolve("tsx/esm/api");
  const registerTsx = `import { register } from ${JSON.stringify(tsxApiUrl)}; register();`;
  return ["--import", `data:text/javascript,${encodeURIComponent(registerTsx)}`];
}

export function observeSqliteSessionReclamationWorker(params: {
  databasePath: string;
  worker: Worker;
}): Promise<SqliteSessionReclamationResult> {
  return new Promise((resolve, reject) => {
    let message: SqliteSessionEntryReclamationWorkerMessage | undefined;
    let workerError: Error | undefined;
    params.worker.on("message", (nextMessage: SqliteSessionEntryReclamationWorkerMessage) => {
      message = nextMessage;
    });
    params.worker.once("error", (error) => {
      // Wait for exit so the caller never races the Worker's SQLite handles on Windows.
      workerError = toStringifiedError(error);
    });
    params.worker.once("exit", (code) => {
      params.worker.removeAllListeners();
      if (message?.cleanupIncomplete) {
        reclamationLog.error(
          message.type === "done"
            ? "SQLite session reclamation committed but Worker database cleanup remains incomplete"
            : "SQLite session reclamation failed and Worker database cleanup remains incomplete",
          {
            errors: message.cleanupWarnings ?? [],
            path: params.databasePath,
            recovery: "restart OpenClaw before deleting the owning agent",
          },
        );
      } else if (message?.cleanupWarnings?.length) {
        reclamationLog.warn("SQLite session reclamation worker recovered cleanup failures", {
          errors: message.cleanupWarnings,
          path: params.databasePath,
        });
      }
      try {
        resolve(resolveSqliteSessionReclamationWorkerExit({ code, message, workerError }));
      } catch (error) {
        reject(toStringifiedError(error));
      }
    });
  });
}

function resolveSqliteSessionReclamationWorkerExit(params: {
  code: number;
  message?: SqliteSessionEntryReclamationWorkerMessage;
  workerError?: Error;
}): SqliteSessionReclamationResult {
  // A structured message is emitted after bounded database-handle and lease cleanup.
  // Once present, it is authoritative over a later Worker exit error; unresolved
  // cleanup is reported separately without misreporting a committed transaction.
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
