import {
  resolveOpenClawAgentSqlitePath,
  type OpenClawAgentDatabaseOptions,
} from "../../state/openclaw-agent-db.js";
import {
  resolveOpenClawStateDirForDatabasePath,
  resolveOpenClawStateSqlitePath,
} from "../../state/openclaw-state-db.paths.js";

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
