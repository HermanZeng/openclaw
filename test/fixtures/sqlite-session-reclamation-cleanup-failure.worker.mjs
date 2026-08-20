import { parentPort, workerData } from "node:worker_threads";

if (!parentPort || typeof workerData?.workerModuleUrl !== "string") {
  throw new Error("SQLite reclamation cleanup-failure fixture requires Worker data");
}

const { runSqliteSessionReclamationWorker, settleSqliteSessionReclamationWorkerDatabase } =
  await import(workerData.workerModuleUrl);

await runSqliteSessionReclamationWorker(workerData.reclamationData, parentPort, {
  reclaim: () => workerData.result,
  settle: (pathname) =>
    settleSqliteSessionReclamationWorkerDatabase(pathname, {
      close: () => ({
        errors: [new Error("state database unavailable")],
        settled: false,
      }),
      delay: async () => undefined,
    }),
});
