import { pipe } from "effect/Function";
import * as NodeServices from "@effect/platform-node/NodeServices";
import type { ProviderReplayEntry, ProviderReplayTranscript } from "@t3tools/contracts";
import * as Console from "effect/Console";
import * as DateTime from "effect/DateTime";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import { TestClock } from "effect/testing";
import { ChildProcess, ChildProcessSpawner } from "effect/process";
import * as NodeOS from "node:os";

import {
  makeOmpRecordingSpawner,
  OMP_REPLAY_ANY,
  OMP_RPC_REPLAY_PROTOCOL,
  OmpOrchestratorReplayHarness,
} from "../src/orchestration-v2/Adapters/OmpAdapterV2.testkit.ts";
import * as OmpAdapterV2Testkit from "../src/orchestration-v2/Adapters/OmpAdapterV2.testkit.ts";
import { OMP_PROVIDER } from "@t3tools/provider-omp/server";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import { provideDeterministicTestRuntime } from "../src/orchestration-v2/testkit/DeterministicRuntime.ts";
import { ORCHESTRATOR_REPLAY_FIXTURES } from "../src/orchestration-v2/testkit/fixtures/index.ts";
import { materializeFixtureInput } from "../src/orchestration-v2/testkit/fixtures/shared.ts";
import { runOrchestratorV2ProviderReplayScenario } from "../src/orchestration-v2/testkit/ProviderReplayHarness.ts";
import {
  checkpointWorkspace,
  makeCheckpointWorkspace,
} from "@t3tools/provider-testing/replayWorkspace";

// Keep the user's extensions, skills, rules, context files and language servers out of the session.
const HERMETIC_LAUNCH_ARGS =
  "--thinking off --no-extensions --no-skills --no-rules --no-context-files --no-lsp";
const CLOCK_TICK = Duration.millis(20);
const MODEL_ROLES = [
  "default",
  "smol",
  "slow",
  "vision",
  "plan",
  "commit",
  "tiny",
  "memory",
  "task",
  "advisor",
  "widget",
  "judge",
] as const;
// Every discovery provider that can contribute MCP servers, skills, rules, hooks or commands.
const DISABLED_DISCOVERY_PROVIDERS = [
  "native",
  "mcp-json",
  "claude",
  "claude-plugins",
  "codex",
  "cursor",
  "windsurf",
  "vscode",
  "opencode",
  "agent-plugins",
  "gemini",
  "omp-plugins",
] as const;
// Path values T3 derives from its state dir; the values that follow these flags vary per run.
const RUN_SPECIFIC_PATH_FLAGS = new Set(["--session-dir", "--append-system-prompt", "--config"]);
const MODEL_LISTING_COMMANDS = new Set(["get_model_picker", "get_available_models"]);
const SECRET_KEY_PATTERN =
  /(?:token|secret|password|apikey|authorization|cookie|accountid|email)$/iu;
const SIGNATURE_KEYS = new Set(["signature", "thinkingSignature", "textSignature"]);
// get_state fields the adapter never reads: the system prompt carries the user's own SYSTEM.md and
// machine context, and dumpTools repeats omp's tool schemas on every call. Only their shape is kept.
const SHAPE_ONLY_ARRAY_FIELDS = new Map<string, (item: unknown) => unknown>([
  ["systemPrompt", () => "<system-prompt-section>"],
  ["dumpTools", (tool: unknown) => ({ name: stringField(tool, "name") })],
]);

function readArgValue(name: string): string | undefined {
  const index = process.argv.indexOf(name);
  return index === -1 ? undefined : process.argv[index + 1];
}

const scenario = readArgValue("--scenario");
const fixture = ORCHESTRATOR_REPLAY_FIXTURES.find((entry) => entry.name === scenario);
const variant = fixture?.providers.find((provider) => provider.driver === OMP_PROVIDER);
if (fixture === undefined || variant === undefined) {
  const names = ORCHESTRATOR_REPLAY_FIXTURES.filter((entry) =>
    entry.providers.some((provider) => provider.driver === OMP_PROVIDER),
  ).map((entry) => entry.name);
  throw new Error(
    `Pass --scenario with a fixture that registers OMP: ${names.join(", ")}. Optional: --out <file>, --state-root <dir>.`,
  );
}

const ompBinary = process.env.T3_OMP_BIN ?? "omp";
const recordingModel = variant.modelSelection.model;
const home = process.env.HOME ?? "";

/** A `--config` overlay that pins every chat role to the recording model and turns off extras that spend. */
function recordingConfig(model: string): string {
  return [
    "modelRoles:",
    ...MODEL_ROLES.map((role) => `  ${role}: ${JSON.stringify(model)}`),
    "defaultThinkingLevel: low",
    "retry:",
    "  modelFallback: false",
    "advisor:",
    "  enabled: false",
    "memory:",
    '  backend: "off"',
    "memories:",
    "  enabled: false",
    "autolearn:",
    "  enabled: false",
    "title:",
    "  refreshOnReplan: false",
    "  refreshAfterPrompts: 0",
    "providers:",
    '  cacheWarming: "off"',
    "features:",
    "  unexpectedStopDetection: mechanical",
    "disabledProviders:",
    ...DISABLED_DISCOVERY_PROVIDERS.map((provider) => `  - ${provider}`),
    "mcp:",
    "  enableProjectConfig: false",
    "  shared: false",
    "dev:",
    "  autoqa: false",
    "update:",
    "  auto: false",
    "startup:",
    "  checkUpdate: false",
    "",
  ].join("\n");
}

const UUID_PATTERN = /[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/giu;
const ISO_TIMESTAMP_PATTERN = /\b\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z\b/gu;
const EMAIL_PATTERN = /[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}/gu;
const NORMALIZED_EPOCH_MS = DateTime.toEpochMillis(DateTime.makeUnsafe("2026-01-01T00:00:00.000Z"));

function escapeRegExp(value: string): string {
  return value.replace(/[.*+?^${}()|[\]\\]/gu, "\\$&");
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function stringField(value: unknown, key: string): string | undefined {
  const field = isRecord(value) ? value[key] : undefined;
  return typeof field === "string" ? field : undefined;
}

/** Replaces a machine name only as a whole word, so short names cannot rewrite ordinary text. */
function wholeWordReplacer(word: string, replacement: string): (value: string) => string {
  if (word.length < 3) return (value) => value;
  const pattern = new RegExp(`\\b${escapeRegExp(word)}\\b`, "gu");
  return (value) => value.replace(pattern, replacement);
}

/** `provider/model` pairs an omp frame reports as in use, outside model listings. */
function reportedModels(value: unknown): ReadonlyArray<string> {
  if (Array.isArray(value)) return value.flatMap(reportedModels);
  if (!isRecord(value)) return [];
  const provider = stringField(value, "provider");
  const model = stringField(value, "model");
  const nestedProvider = stringField(value.model, "provider");
  const nestedId = stringField(value.model, "id");
  return [
    ...(provider !== undefined && model !== undefined ? [`${provider}/${model}`] : []),
    ...(nestedProvider !== undefined && nestedId !== undefined
      ? [`${nestedProvider}/${nestedId}`]
      : []),
    ...Object.values(value).flatMap(reportedModels),
  ];
}

function isModelListing(frame: unknown): boolean {
  return MODEL_LISTING_COMMANDS.has(stringField(frame, "command") ?? "");
}

/** Why a live frame must not be recorded: a chunked frame escapes normalization, and only one model may run. */
function inboundRefusal(frame: unknown): string | undefined {
  if (stringField(frame, "type") === "rpc_chunk") {
    return "omp chunked a frame over 1 MiB; its base64 payload cannot be normalized";
  }
  if (isModelListing(frame)) return undefined;
  const other = reportedModels(frame).find((model) => model !== recordingModel);
  return other === undefined
    ? undefined
    : `omp reported model ${other}; recordings use ${recordingModel} only`;
}

interface NormalizationInput {
  /** Every spelling of each directory, longest first. */
  readonly workspaces: ReadonlyArray<string>;
  readonly stateDirs: ReadonlyArray<string>;
}

const PRIVATE_PREFIX = /^\/private(?=\/)/u;

/** macOS reports `/private/tmp` paths as `/tmp` too, so a recorded path can take either form. */
function pathSpellings(paths: ReadonlyArray<string>): ReadonlyArray<string> {
  const spellings = new Set(paths.flatMap((path) => [path, path.replace(PRIVATE_PREFIX, "")]));
  return [...spellings].toSorted((left, right) => right.length - left.length);
}

function anyOf(paths: ReadonlyArray<string>): string {
  return `(?:${paths.map(escapeRegExp).join("|")})`;
}

// Normalize both directions consistently so echoed paths, UUIDs and times still match on replay.
function makeNormalizer(input: NormalizationInput) {
  const sessionFiles = new Map<string, string>();
  const uuids = new Map<string, string>();
  let timeAnchorMs: number | undefined;
  const shiftTime = (ms: number): number => {
    timeAnchorMs ??= ms;
    return NORMALIZED_EPOCH_MS + (ms - timeAnchorMs);
  };
  const sessionsRoot = `${anyOf(input.stateDirs)}/omp-sessions/[^/\\s"']+`;
  const sessionFilePattern = new RegExp(`${sessionsRoot}/[^\\s"']+?\\.jsonl`, "gu");
  const sessionDirPattern = new RegExp(sessionsRoot, "gu");
  const stateDirPattern = new RegExp(anyOf(input.stateDirs), "gu");
  const workspacePattern = new RegExp(anyOf(input.workspaces), "gu");
  const sessionFileName = (file: string): string => {
    const key = file.replace(PRIVATE_PREFIX, "");
    const existing = sessionFiles.get(key);
    if (existing !== undefined) return existing;
    const next = `/omp-sessions/session-${sessionFiles.size + 1}.jsonl`;
    sessionFiles.set(key, next);
    return next;
  };
  const uuidName = (uuid: string): string => {
    const existing = uuids.get(uuid.toLowerCase());
    if (existing !== undefined) return existing;
    const next = `00000000-0000-4000-8000-${String(uuids.size + 1).padStart(12, "0")}`;
    uuids.set(uuid.toLowerCase(), next);
    return next;
  };
  const shiftIso = (iso: string): string =>
    pipe(
      DateTime.makeUnsafe(iso),
      DateTime.toEpochMillis,
      shiftTime,
      DateTime.makeUnsafe,
      DateTime.formatIso,
    );
  const replacements: ReadonlyArray<(value: string) => string> = [
    (value) => value.replace(sessionFilePattern, sessionFileName),
    (value) => value.replace(sessionDirPattern, "/omp-sessions/workspace"),
    (value) => value.replace(stateDirPattern, "/omp-state"),
    (value) => value.replace(workspacePattern, "<workspace>"),
    (value) => (home.length > 1 ? value.replaceAll(home, "~") : value),
    wholeWordReplacer(NodeOS.hostname(), "<hostname>"),
    wholeWordReplacer(NodeOS.userInfo().username, "<user>"),
    (value) => value.replace(EMAIL_PATTERN, "<email>"),
    (value) => value.replace(ISO_TIMESTAMP_PATTERN, shiftIso),
    (value) => value.replace(UUID_PATTERN, uuidName),
  ];
  const normalizeString = (value: string): string =>
    replacements.reduce((current, replace) => replace(current), value);
  const normalizeField = (key: string, entry: unknown): unknown => {
    const shapeOnly = SHAPE_ONLY_ARRAY_FIELDS.get(key);
    if (shapeOnly !== undefined && Array.isArray(entry)) return entry.map(shapeOnly);
    if (SIGNATURE_KEYS.has(key) && typeof entry === "string") return "<signature>";
    if (SECRET_KEY_PATTERN.test(key) && typeof entry === "string") return "<redacted>";
    if (key === "timestamp" && typeof entry === "number") return shiftTime(entry);
    return normalizeValue(entry);
  };
  const normalizeValue = (value: unknown): unknown => {
    if (typeof value === "string") return normalizeString(value);
    if (Array.isArray(value)) return value.map(normalizeValue);
    if (!isRecord(value)) return value;
    return Object.fromEntries(
      Object.entries(value).map(([key, entry]) => [key, normalizeField(key, entry)]),
    );
  };
  return { normalizeString, normalizeValue };
}

function withRunSpecificPathsMatched(frame: Record<string, unknown>): Record<string, unknown> {
  const args = frame.args;
  if (frame.type !== "process_start" || !Array.isArray(args)) return frame;
  return {
    ...frame,
    args: args.map((arg, index) =>
      RUN_SPECIFIC_PATH_FLAGS.has(String(args[index - 1])) ? OMP_REPLAY_ANY : arg,
    ),
  };
}

/** The adapter only needs the selected model's picker row; the rest describe the recording machine. */
function withSelectedModelRows(frame: Record<string, unknown>): Record<string, unknown> {
  const data = frame.data;
  if (frame.command !== "get_model_picker" || !isRecord(data) || !Array.isArray(data.rows)) {
    return frame;
  }
  const rows = data.rows.filter(
    (row) => `${stringField(row, "provider")}/${stringField(row, "id")}` === recordingModel,
  );
  return { ...frame, data: { ...data, rows } };
}

/** omp's built-in commands only; extension and file commands name the recording user's setup. */
function withBuiltInCommandsOnly(frame: Record<string, unknown>): Record<string, unknown> {
  if (frame.type !== "available_commands_update" || !Array.isArray(frame.commands)) return frame;
  return {
    ...frame,
    commands: frame.commands.filter((command) => stringField(command, "source") === "builtin"),
  };
}

function normalizeEntries(input: {
  readonly entries: ReadonlyArray<ProviderReplayEntry>;
  readonly normalizeValue: (value: unknown) => unknown;
}): ReadonlyArray<ProviderReplayEntry> {
  const normalizeFrame = (frame: unknown): unknown =>
    input.normalizeValue(
      isRecord(frame)
        ? withBuiltInCommandsOnly(withSelectedModelRows(withRunSpecificPathsMatched(frame)))
        : frame,
    );
  return input.entries.map((entry) =>
    entry.type === "runtime_exit" ? entry : { ...entry, frame: normalizeFrame(entry.frame) },
  );
}

const encodeJson = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));

function encodeTranscriptNdjson(transcript: ProviderReplayTranscript): string {
  const { entries, ...metadata } = transcript;
  return [
    encodeJson({ type: "transcript_start", ...metadata }),
    ...entries.map((entry) => encodeJson(entry)),
    "",
  ].join("\n");
}

// Follow wall time for live omp timers while retaining replay's TestClock steps.
const followWallClock = Effect.gen(function* () {
  while (true) {
    yield* TestClock.withLive(Effect.sleep(CLOCK_TICK));
    yield* TestClock.adjust(CLOCK_TICK);
  }
});

const readOmpVersion = Effect.gen(function* () {
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  return (yield* spawner.string(ChildProcess.make(ompBinary, ["--version"]))).trim();
});

const makeStateDir = Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem;
  const stateRoot = readArgValue("--state-root");
  return yield* fs.makeTempDirectory(
    stateRoot === undefined
      ? { prefix: "t3-omp-record-state-" }
      : { directory: stateRoot, prefix: `${fixture.name}-` },
  );
});

const record = Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const ompVersion = yield* readOmpVersion;
  const outputPath = readArgValue("--out") ?? (yield* path.fromFileUrl(variant.transcriptFile));
  const fixtureInput = fixture.buildInput();
  const workspace = yield* Effect.promise(() =>
    makeCheckpointWorkspace(`omp-rpc-record-${fixture.name}`, fixtureInput.workspaceFiles),
  );
  const stateDir = yield* makeStateDir;
  yield* Effect.addFinalizer(() =>
    Effect.all([
      fs.remove(workspace, { recursive: true, force: true }),
      fs.remove(stateDir, { recursive: true, force: true }),
    ]).pipe(Effect.ignore),
  );
  const configPath = path.join(stateDir, "recording-config.yml");
  yield* fs.writeFileString(configPath, recordingConfig(recordingModel));
  const launchArgs = `--model ${recordingModel} ${HERMETIC_LAUNCH_ARGS} --config ${configPath}`;

  const entries: Array<ProviderReplayEntry> = [];
  const refusals: Array<string> = [];
  const refuseInbound = (frame: unknown): boolean => {
    const refusal = inboundRefusal(frame);
    if (refusal !== undefined) refusals.push(refusal);
    return refusal !== undefined;
  };
  const placeholder: ProviderReplayTranscript = {
    provider: OMP_PROVIDER,
    protocol: OMP_RPC_REPLAY_PROTOCOL,
    version: ompVersion,
    scenario: fixture.name,
    entries: [],
  };
  const layerRecordingSpawner = Layer.effect(
    ChildProcessSpawner.ChildProcessSpawner,
    Effect.map(Effect.service(ChildProcessSpawner.ChildProcessSpawner), (live) =>
      makeOmpRecordingSpawner(live, entries, refuseInbound),
    ),
  ).pipe(Layer.provide(NodeServices.layer));

  const materializeInput = materializeFixtureInput({
    scenario: fixture.name,
    fixtureInput,
    driver: variant.driver,
    modelSelection: variant.modelSelection,
  }).pipe(Effect.provide(IdAllocator.layer));
  const continuationOptions =
    variant.runContinuationWorker === true ? { runContinuationWorker: true } : {};

  const liveRun = Effect.gen(function* () {
    yield* followWallClock.pipe(Effect.forkScoped);
    const materialized = yield* materializeInput;
    yield* runOrchestratorV2ProviderReplayScenario(
      {
        name: `${fixture.name}/omp:record`,
        transcript: placeholder,
        commands: materialized.commands,
        steps: materialized.steps,
        projectionThreadIds: materialized.projectionThreadIds,
        runtimePolicyOverride: { ...variant.runtimePolicyOverride, cwd: workspace },
      },
      {
        driver: OMP_PROVIDER,
        decodeTranscript: Effect.succeed,
        makeProviderAdapterRegistryLayer: () =>
          OmpAdapterV2Testkit.layer({
            binaryPath: ompBinary,
            launchArgs,
            stateDir,
            spawner: layerRecordingSpawner,
          }),
      },
      continuationOptions,
    );
  }).pipe(Effect.scoped, provideDeterministicTestRuntime);
  // A refused frame kills omp, so report the refusal rather than the turn failure it causes.
  const liveExit = yield* Effect.exit(liveRun);
  if (refusals.length > 0) {
    return yield* Effect.die(new Error(`Recording stopped: ${[...new Set(refusals)].join("; ")}`));
  }
  yield* liveExit;

  const { normalizeString, normalizeValue } = makeNormalizer({
    workspaces: pathSpellings([workspace, yield* fs.realPath(workspace)]),
    stateDirs: pathSpellings([stateDir, yield* fs.realPath(stateDir)]),
  });
  const transcript = {
    ...placeholder,
    metadata: {
      generatedBy: "live-omp-recorder",
      ompVersion,
      model: recordingModel,
      launchArgs: normalizeString(launchArgs),
    },
    entries: normalizeEntries({ entries, normalizeValue }),
  } satisfies ProviderReplayTranscript;

  // Assert on normalized replay; the live projection has raw paths. Failed assertions preserve the fixture.
  yield* Effect.gen(function* () {
    const replayWorkspace = yield* checkpointWorkspace(fixture.name, fixtureInput.workspaceFiles);
    const materialized = yield* materializeInput;
    const replayResult = yield* runOrchestratorV2ProviderReplayScenario(
      {
        name: `${fixture.name}/omp:verify`,
        transcript: yield* OmpOrchestratorReplayHarness.decodeTranscript(transcript),
        commands: materialized.commands,
        steps: materialized.steps,
        projectionThreadIds: materialized.projectionThreadIds,
        runtimePolicyOverride: { ...variant.runtimePolicyOverride, cwd: replayWorkspace },
      },
      OmpOrchestratorReplayHarness,
      continuationOptions,
    );
    variant.assertOutput(replayResult, transcript);
  }).pipe(Effect.scoped, provideDeterministicTestRuntime);

  yield* fs.makeDirectory(path.dirname(outputPath), { recursive: true });
  yield* fs.writeFileString(outputPath, encodeTranscriptNdjson(transcript));
  yield* Console.log(`Wrote ${transcript.entries.length} OMP RPC replay entries to ${outputPath}`);
});

await Effect.runPromise(record.pipe(Effect.scoped, Effect.provide(NodeServices.layer)));
