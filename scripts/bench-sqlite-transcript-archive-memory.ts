import { constants as bufferConstants } from "node:buffer";
import { spawn, execFileSync } from "node:child_process";
import { createCipheriv, createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { performance } from "node:perf_hooks";
import { fileURLToPath } from "node:url";
import zlib from "node:zlib";
import { replaceSessionEntry } from "../src/config/sessions/session-accessor.js";
import {
  materializeSqliteSessionStateDeletePlans,
  type MaterializedSqliteSessionStateDeletePlan,
} from "../src/config/sessions/session-accessor.sqlite-archive.js";
import { planSqliteSessionStateDeleteIfUnreferenced } from "../src/config/sessions/session-accessor.sqlite-lifecycle-state.js";
import { appendSqliteTranscriptEvent } from "../src/config/sessions/session-accessor.sqlite.js";
import { resolveSqliteTargetFromSessionStorePath } from "../src/config/sessions/session-sqlite-target.js";
import {
  closeOpenClawAgentDatabasesForTest,
  openOpenClawAgentDatabase,
} from "../src/state/openclaw-agent-db.js";

const MIB = 1024 * 1024;
const TARGET_EVENT_LOGICAL_MIB = 1;
const CHILD_RESULT_PREFIX = "OPENCLAW_ARCHIVE_BENCH_CHILD_RESULT=";
const DEFAULT_TIMEOUT_MS = 5 * 60 * 1000;
const SCRIPT_PATH = fileURLToPath(import.meta.url);
const REPO_ROOT = path.resolve(path.dirname(SCRIPT_PATH), "..");
const WORKLOAD_PROFILE = "deterministic-high-entropy-base64";
const DETERMINISTIC_KEY = createHash("sha256")
  .update("openclaw-sqlite-transcript-archive-memory-v1", "utf8")
  .digest();

type TranscriptEvent = {
  type: "session";
  id: string;
  content: string;
};

type WorkloadIdentity = {
  eventCount: number;
  logicalBytes: number;
  logicalSha256: string;
  logicalUtf16CodeUnits: number;
  profile: typeof WORKLOAD_PROFILE;
  targetEventLogicalMiB: number;
};

type BenchChildConfig = {
  archiveDirectory: string;
  sessionId: string;
  sourceFingerprint: SourceFingerprint;
  storePath: string;
};

type ChildMeasurement = {
  archivedPath: string | null;
  baselineHeapUsedMiB: number | null;
  baselineRssMiB: number | null;
  durationMs: number | null;
  endHeapUsedMiB: number | null;
  endRssMiB: number | null;
  errorMessage: string | null;
  errorName: string | null;
  errorStack: string | null;
  operationEstablishedProcessHighWater: boolean;
  operationProcessHighWaterDeltaMiB: number | null;
  operationProcessHighWaterMiB: number | null;
  processLifetimeMaxRssAfterKiB: number | null;
  processLifetimeMaxRssBeforeKiB: number | null;
  maxSampledRssDeltaMiB: number | null;
  maxSampledRssMiB: number | null;
  nodeOptions: string | null;
  rssSampleCount: number;
  stage: "plan" | "materialize" | "complete";
  success: boolean;
};

type ChildProcessResult = {
  childExitCode: number | null;
  childSignal: NodeJS.Signals | null;
  controlledEnvironmentKeys: string[];
  measurement: ChildMeasurement | null;
  stderr: string;
  timedOut: boolean;
  watchdogTimeoutMs: number;
};

type SourceIdentity = {
  command: string[];
  execArgv: string[];
  materializerSha256: string;
  scriptSha256: string;
  sourceCommit: string;
  sourceDirty: boolean;
  workerSha256: string;
};

type SourceFingerprint = Pick<
  SourceIdentity,
  "materializerSha256" | "scriptSha256" | "sourceCommit" | "sourceDirty" | "workerSha256"
>;

function parsePositiveSafeInteger(rawValue: string, label: string): number {
  if (!/^[1-9]\d*$/u.test(rawValue)) {
    throw new Error(`${label} must be a positive integer`);
  }
  const value = Number(rawValue);
  if (!Number.isSafeInteger(value)) {
    throw new Error(`${label} must be a positive safe integer`);
  }
  return value;
}

function parsePayloadMiB(): number {
  const rawValue = process.argv[2];
  if (rawValue === undefined) {
    throw new Error("usage: bench-sqlite-transcript-archive-memory.ts <payload-mib>");
  }
  return parsePositiveSafeInteger(rawValue, "payload-mib");
}

function mib(bytes: number): number {
  return Math.round((bytes / MIB) * 100) / 100;
}

function hashFile(filePath: string): string {
  return createHash("sha256").update(fs.readFileSync(filePath)).digest("hex");
}

function git(args: string[]): string {
  return execFileSync("git", args, {
    cwd: REPO_ROOT,
    encoding: "utf8",
    windowsHide: true,
  }).trim();
}

function readSourceIdentity(): SourceIdentity {
  const materializerPath = path.join(
    REPO_ROOT,
    "src",
    "config",
    "sessions",
    "session-accessor.sqlite-archive.ts",
  );
  const workerPath = path.join(
    REPO_ROOT,
    "src",
    "config",
    "sessions",
    "session-accessor.sqlite-archive.worker.ts",
  );
  return {
    command: [process.execPath, ...process.execArgv, ...process.argv.slice(1)],
    execArgv: [...process.execArgv],
    materializerSha256: hashFile(materializerPath),
    scriptSha256: hashFile(SCRIPT_PATH),
    sourceCommit: git(["rev-parse", "HEAD"]),
    sourceDirty: git(["status", "--porcelain", "--untracked-files=all"]).length > 0,
    workerSha256: hashFile(workerPath),
  };
}

function sourceFingerprint(source: SourceIdentity): SourceFingerprint {
  return {
    materializerSha256: source.materializerSha256,
    scriptSha256: source.scriptSha256,
    sourceCommit: source.sourceCommit,
    sourceDirty: source.sourceDirty,
    workerSha256: source.workerSha256,
  };
}

function sourceFingerprintsEqual(left: SourceFingerprint, right: SourceFingerprint): boolean {
  return (
    left.materializerSha256 === right.materializerSha256 &&
    left.scriptSha256 === right.scriptSha256 &&
    left.sourceCommit === right.sourceCommit &&
    left.sourceDirty === right.sourceDirty &&
    left.workerSha256 === right.workerSha256
  );
}

function deterministicBytes(size: number, eventIndex: number): Buffer {
  const iv = Buffer.alloc(16);
  // Keep each event in a disjoint AES-CTR counter namespace. Writing the event
  // index into the low counter bits would make adjacent streams overlap.
  iv.writeUInt32BE(eventIndex, 0);
  const cipher = createCipheriv("aes-256-ctr", DETERMINISTIC_KEY, iv);
  return Buffer.concat([cipher.update(Buffer.alloc(size)), cipher.final()]);
}

async function seedTranscript(params: {
  payloadMiB: number;
  sessionId: string;
  sessionKey: string;
  storePath: string;
}): Promise<WorkloadIdentity> {
  await replaceSessionEntry(
    { sessionKey: params.sessionKey, storePath: params.storePath },
    { sessionId: params.sessionId, updatedAt: Date.now() },
  );

  const eventCount = Math.max(1, Math.ceil(params.payloadMiB / TARGET_EVENT_LOGICAL_MIB));
  const rawBytesPerEvent = Math.ceil((params.payloadMiB * MIB * 3) / 4 / eventCount);
  const logicalHash = createHash("sha256");
  let logicalBytes = 0;
  let logicalUtf16CodeUnits = 0;
  for (let index = 0; index < eventCount; index += 1) {
    const event: TranscriptEvent = {
      type: "session",
      id: `${params.sessionId}-${index}`,
      content: deterministicBytes(rawBytesPerEvent, index).toString("base64"),
    };
    const line = `${JSON.stringify(event)}\n`;
    logicalBytes += Buffer.byteLength(line, "utf8");
    logicalUtf16CodeUnits += line.length;
    logicalHash.update(line, "utf8");
    await appendSqliteTranscriptEvent(
      { sessionKey: params.sessionKey, sessionId: params.sessionId, storePath: params.storePath },
      event,
    );
  }
  return {
    eventCount,
    logicalBytes,
    logicalSha256: logicalHash.digest("hex"),
    logicalUtf16CodeUnits,
    profile: WORKLOAD_PROFILE,
    targetEventLogicalMiB: TARGET_EVENT_LOGICAL_MIB,
  };
}

async function settleAndCollect(): Promise<void> {
  globalThis.gc?.();
  await new Promise<void>((resolve) => {
    setTimeout(resolve, 100);
  });
  globalThis.gc?.();
}

async function runMeasurementChild(configPath: string): Promise<ChildMeasurement> {
  let stage: ChildMeasurement["stage"] = "plan";
  let baseline: NodeJS.MemoryUsage | undefined;
  let startedAt: number | undefined;
  let maxSampledRssBytes = 0;
  let processLifetimeMaxRssBeforeKiB: number | undefined;
  let rssSampleCount = 0;
  let sampler: NodeJS.Timeout | undefined;
  let materialized: MaterializedSqliteSessionStateDeletePlan[] | undefined;
  try {
    if (!globalThis.gc) {
      throw new Error("measurement child requires --expose-gc");
    }
    const config = JSON.parse(fs.readFileSync(configPath, "utf8")) as BenchChildConfig;
    const childSource = sourceFingerprint(readSourceIdentity());
    if (!sourceFingerprintsEqual(childSource, config.sourceFingerprint)) {
      throw new Error("measurement child source fingerprint does not match the orchestrator");
    }
    const target = resolveSqliteTargetFromSessionStorePath(config.storePath);
    if (!target.path) {
      throw new Error(`could not resolve SQLite database path for ${config.storePath}`);
    }
    const database = openOpenClawAgentDatabase({
      agentId: target.agentId ?? "main",
      path: target.path,
    });
    const plan = planSqliteSessionStateDeleteIfUnreferenced({
      archiveDirectory: config.archiveDirectory,
      database,
      referencedSessionIds: new Set(),
      sessionId: config.sessionId,
    });
    if (!plan) {
      throw new Error(`expected an archive plan for ${config.sessionId}`);
    }
    closeOpenClawAgentDatabasesForTest();
    await settleAndCollect();

    stage = "materialize";
    processLifetimeMaxRssBeforeKiB = process.resourceUsage().maxRSS;
    baseline = process.memoryUsage();
    maxSampledRssBytes = baseline.rss;
    sampler = setInterval(() => {
      rssSampleCount += 1;
      maxSampledRssBytes = Math.max(maxSampledRssBytes, process.memoryUsage().rss);
    }, 2);
    startedAt = performance.now();
    materialized = await materializeSqliteSessionStateDeletePlans([plan]);
    maxSampledRssBytes = Math.max(maxSampledRssBytes, process.memoryUsage().rss);
    stage = "complete";
    const after = process.memoryUsage();
    const processLifetimeMaxRssAfterKiB = process.resourceUsage().maxRSS;
    const operationEstablishedProcessHighWater =
      processLifetimeMaxRssAfterKiB > processLifetimeMaxRssBeforeKiB;
    const operationProcessHighWaterBytes = processLifetimeMaxRssAfterKiB * 1024;
    return {
      archivedPath: materialized[0]?.archivedTranscript?.archivedPath ?? null,
      baselineHeapUsedMiB: mib(baseline.heapUsed),
      baselineRssMiB: mib(baseline.rss),
      durationMs: Math.round((performance.now() - startedAt) * 100) / 100,
      endHeapUsedMiB: mib(after.heapUsed),
      endRssMiB: mib(after.rss),
      errorMessage: null,
      errorName: null,
      errorStack: null,
      operationEstablishedProcessHighWater,
      operationProcessHighWaterDeltaMiB: operationEstablishedProcessHighWater
        ? mib(Math.max(0, operationProcessHighWaterBytes - baseline.rss))
        : null,
      operationProcessHighWaterMiB: operationEstablishedProcessHighWater
        ? mib(operationProcessHighWaterBytes)
        : null,
      processLifetimeMaxRssAfterKiB,
      processLifetimeMaxRssBeforeKiB,
      maxSampledRssDeltaMiB: mib(Math.max(0, maxSampledRssBytes - baseline.rss)),
      maxSampledRssMiB: mib(maxSampledRssBytes),
      nodeOptions: process.env.NODE_OPTIONS ?? null,
      rssSampleCount,
      stage,
      success: true,
    };
  } catch (error) {
    const after = process.memoryUsage();
    const processLifetimeMaxRssAfterKiB = process.resourceUsage().maxRSS;
    const operationEstablishedProcessHighWater =
      processLifetimeMaxRssBeforeKiB !== undefined &&
      processLifetimeMaxRssAfterKiB > processLifetimeMaxRssBeforeKiB;
    const operationProcessHighWaterBytes = processLifetimeMaxRssAfterKiB * 1024;
    return {
      archivedPath: null,
      baselineHeapUsedMiB: baseline ? mib(baseline.heapUsed) : null,
      baselineRssMiB: baseline ? mib(baseline.rss) : null,
      durationMs:
        startedAt === undefined ? null : Math.round((performance.now() - startedAt) * 100) / 100,
      endHeapUsedMiB: mib(after.heapUsed),
      endRssMiB: mib(after.rss),
      errorMessage: error instanceof Error ? error.message : String(error),
      errorName: error instanceof Error ? error.name : "UnknownError",
      errorStack: error instanceof Error ? (error.stack ?? null) : null,
      operationEstablishedProcessHighWater,
      operationProcessHighWaterDeltaMiB:
        operationEstablishedProcessHighWater && baseline
          ? mib(Math.max(0, operationProcessHighWaterBytes - baseline.rss))
          : null,
      operationProcessHighWaterMiB: operationEstablishedProcessHighWater
        ? mib(operationProcessHighWaterBytes)
        : null,
      processLifetimeMaxRssAfterKiB,
      processLifetimeMaxRssBeforeKiB: processLifetimeMaxRssBeforeKiB ?? null,
      maxSampledRssDeltaMiB: baseline ? mib(Math.max(0, maxSampledRssBytes - baseline.rss)) : null,
      maxSampledRssMiB: maxSampledRssBytes > 0 ? mib(maxSampledRssBytes) : null,
      nodeOptions: process.env.NODE_OPTIONS ?? null,
      rssSampleCount,
      stage,
      success: false,
    };
  } finally {
    if (sampler) {
      clearInterval(sampler);
    }
    closeOpenClawAgentDatabasesForTest();
  }
}

function buildMeasurementChildEnvironment(): NodeJS.ProcessEnv {
  const environment: NodeJS.ProcessEnv = {};
  const keys =
    process.platform === "win32"
      ? [
          "COMSPEC",
          "HOME",
          "LOCALAPPDATA",
          "Path",
          "PATHEXT",
          "SystemRoot",
          "TEMP",
          "TMP",
          "USERPROFILE",
          "WINDIR",
        ]
      : ["HOME", "LANG", "LC_ALL", "PATH", "TEMP", "TMP", "TMPDIR"];
  for (const key of keys) {
    const value = process.env[key];
    if (value !== undefined) {
      environment[key] = value;
    }
  }
  return environment;
}

function spawnMeasurementChild(configPath: string): Promise<ChildProcessResult> {
  const timeoutMs = parsePositiveSafeInteger(
    process.env.OPENCLAW_ARCHIVE_BENCH_TIMEOUT_MS ?? String(DEFAULT_TIMEOUT_MS),
    "OPENCLAW_ARCHIVE_BENCH_TIMEOUT_MS",
  );
  const childEnvironment = buildMeasurementChildEnvironment();
  const child = spawn(
    process.execPath,
    ["--expose-gc", "--import", "tsx", SCRIPT_PATH, "--measure", configPath],
    {
      cwd: REPO_ROOT,
      env: childEnvironment,
      stdio: ["ignore", "pipe", "pipe"],
      windowsHide: true,
    },
  );
  child.stdout.setEncoding("utf8");
  child.stderr.setEncoding("utf8");
  let stdout = "";
  let stderr = "";
  let timedOut = false;
  child.stdout.on("data", (chunk: string) => {
    stdout += chunk;
  });
  child.stderr.on("data", (chunk: string) => {
    stderr += chunk;
  });
  const watchdog = setTimeout(() => {
    timedOut = true;
    child.kill();
  }, timeoutMs);

  return new Promise((resolve, reject) => {
    child.once("error", (error) => {
      clearTimeout(watchdog);
      reject(error);
    });
    child.once("close", (code, signal) => {
      clearTimeout(watchdog);
      const resultLine = stdout
        .split(/\r?\n/u)
        .findLast((line) => line.startsWith(CHILD_RESULT_PREFIX));
      let measurement: ChildMeasurement | null = null;
      if (resultLine) {
        try {
          measurement = JSON.parse(
            resultLine.slice(CHILD_RESULT_PREFIX.length),
          ) as ChildMeasurement;
        } catch (error) {
          reject(
            new Error("measurement child returned malformed structured output", { cause: error }),
          );
          return;
        }
      }
      resolve({
        childExitCode: code,
        childSignal: signal,
        controlledEnvironmentKeys: Object.keys(childEnvironment).toSorted(),
        measurement,
        stderr,
        timedOut,
        watchdogTimeoutMs: timeoutMs,
      });
    });
  });
}

async function digestArchive(filePath: string): Promise<{ bytes: number; sha256: string }> {
  const source = fs.createReadStream(filePath);
  const stream = filePath.endsWith(".zst") ? source.pipe(zlib.createZstdDecompress()) : source;
  const hash = createHash("sha256");
  let bytes = 0;
  for await (const chunk of stream) {
    const buffer = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
    bytes += buffer.length;
    hash.update(buffer);
  }
  return { bytes, sha256: hash.digest("hex") };
}

async function runOrchestrator(): Promise<number> {
  const payloadMiB = parsePayloadMiB();
  const source = readSourceIdentity();
  const allowDirty = process.env.OPENCLAW_ARCHIVE_BENCH_ALLOW_DIRTY === "1";
  const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "openclaw-archive-memory-bench-"));
  const storePath = path.join(tempDir, "agents", "main", "sessions", "sessions.json");
  const archiveDirectory = path.join(tempDir, "archives");
  const sessionId = `archive-memory-${payloadMiB}mib`;
  const sessionKey = `agent:main:${sessionId}`;
  let stage = "identity";
  let workload: WorkloadIdentity | null = null;
  let child: ChildProcessResult | null = null;
  let verification: {
    archiveFileBytes: number;
    archiveLogicalBytes: number;
    archiveLogicalSha256: string;
    archiveMatchesExpected: boolean;
  } | null = null;
  let error: unknown;
  let cleanupError: unknown;
  let sourceAfter: SourceIdentity | null = null;

  try {
    if (source.sourceDirty && !allowDirty) {
      throw new Error(
        "source tree is dirty; commit the benchmark or set OPENCLAW_ARCHIVE_BENCH_ALLOW_DIRTY=1 for development",
      );
    }
    stage = "seed";
    workload = await seedTranscript({ payloadMiB, sessionId, sessionKey, storePath });
    closeOpenClawAgentDatabasesForTest();
    const configPath = path.join(tempDir, "child-config.json");
    fs.writeFileSync(
      configPath,
      JSON.stringify({
        archiveDirectory,
        sessionId,
        sourceFingerprint: sourceFingerprint(source),
        storePath,
      } satisfies BenchChildConfig),
      "utf8",
    );

    stage = "measure";
    child = await spawnMeasurementChild(configPath);
    if (child.timedOut) {
      throw new Error("measurement child exceeded watchdog timeout");
    }
    if (child.childExitCode !== 0 || child.childSignal !== null) {
      throw new Error(
        `measurement child exited unexpectedly (exit=${String(child.childExitCode)}, signal=${String(child.childSignal)})`,
      );
    }
    if (!child.measurement) {
      throw new Error(
        `measurement child produced no structured result (exit=${String(child.childExitCode)}, signal=${String(child.childSignal)})`,
      );
    }
    if (!child.measurement.success) {
      const materializeError = new Error(
        child.measurement.errorMessage ?? "measurement child failed without an error message",
      );
      materializeError.name = child.measurement.errorName ?? "MeasurementChildError";
      materializeError.stack = child.measurement.errorStack ?? materializeError.stack;
      throw materializeError;
    }
    if (!child.measurement.archivedPath) {
      throw new Error("measurement child returned no archive path");
    }

    stage = "verify";
    const archivedPath = child.measurement.archivedPath;
    const archiveDigest = await digestArchive(archivedPath);
    verification = {
      archiveFileBytes: fs.statSync(archivedPath).size,
      archiveLogicalBytes: archiveDigest.bytes,
      archiveLogicalSha256: archiveDigest.sha256,
      archiveMatchesExpected:
        archiveDigest.bytes === workload.logicalBytes &&
        archiveDigest.sha256 === workload.logicalSha256,
    };
    if (!verification.archiveMatchesExpected) {
      throw new Error(`archive verification failed for ${sessionId}`);
    }
    stage = "complete";
  } catch (caught) {
    error = caught;
  } finally {
    closeOpenClawAgentDatabasesForTest();
    try {
      sourceAfter = readSourceIdentity();
      if (!sourceFingerprintsEqual(sourceFingerprint(source), sourceFingerprint(sourceAfter))) {
        error ??= new Error("source fingerprint changed during benchmark execution");
      }
    } catch (caught) {
      error ??= caught;
    }
    try {
      fs.rmSync(tempDir, { recursive: true, force: true });
    } catch (caught) {
      cleanupError = caught;
    }
  }

  const success = !error && !cleanupError && verification?.archiveMatchesExpected === true;
  const sourceStable =
    sourceAfter !== null &&
    sourceFingerprintsEqual(sourceFingerprint(source), sourceFingerprint(sourceAfter));
  const publishableEvidence =
    sourceStable &&
    !source.sourceDirty &&
    !cleanupError &&
    child?.childExitCode === 0 &&
    child.childSignal === null &&
    child.timedOut === false &&
    child.measurement !== null &&
    (child.measurement.success ? verification?.archiveMatchesExpected === true : true);
  console.log(
    JSON.stringify(
      {
        child,
        cleanup: {
          errorMessage:
            cleanupError instanceof Error
              ? cleanupError.message
              : cleanupError
                ? String(cleanupError)
                : null,
          success: !cleanupError,
        },
        error: error
          ? {
              message: error instanceof Error ? error.message : String(error),
              name: error instanceof Error ? error.name : "UnknownError",
              stack: error instanceof Error ? (error.stack ?? null) : null,
            }
          : null,
        maxStringLengthUtf16CodeUnits: bufferConstants.MAX_STRING_LENGTH,
        node: process.version,
        payloadTargetMiB: payloadMiB,
        platform: `${process.platform}-${process.arch}`,
        publishableEvidence,
        source: {
          after: sourceAfter,
          before: source,
          stable: sourceStable,
        },
        stage,
        success,
        verification,
        workload,
      },
      null,
      2,
    ),
  );
  return success ? 0 : 1;
}

if (process.argv[2] === "--measure") {
  const configPath = process.argv[3];
  if (!configPath) {
    throw new Error("measurement child requires a config path");
  }
  const measurement = await runMeasurementChild(configPath);
  process.stdout.write(`${CHILD_RESULT_PREFIX}${JSON.stringify(measurement)}\n`);
} else {
  process.exitCode = await runOrchestrator();
}
