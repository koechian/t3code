/**
 * Provider snapshot: version, readiness, model catalog and per-workspace commands, all from
 * passive probes (`--no-session --no-extensions --no-tools --offline`, no model call).
 * Copy-and-adapt of packages/provider-pi/src/server/status.ts.
 */
import { ProviderDriverKind, type ServerProvider } from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Option from "effect/Option";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import type * as HttpClient from "effect/http/HttpClient";
import * as ChildProcess from "effect/process/ChildProcess";
import { causeErrorTag } from "@t3tools/shared/observability";
import { resolveSpawnCommand } from "@t3tools/shared/shell";
import {
  brokenVersionDetail,
  type ProviderCompatibilityLookup,
} from "@t3tools/provider-core/server/compatibility";
import {
  enrichProviderSnapshotWithVersionAdvisory,
  type ProviderMaintenanceCapabilities,
} from "@t3tools/provider-core/server/maintenanceResolver";
import type * as ProviderLatestVersions from "@t3tools/provider-core/server/ProviderLatestVersions";
import {
  buildServerProvider,
  isCommandMissingCause,
  parseCliVersionResult,
  providerProbeEnvironment,
  resolveProviderProbeTimeoutMs,
  spawnAndCollect,
  type CommandResult,
  type ProviderProbeResult,
  type ServerProviderDraft,
} from "@t3tools/provider-core/server/snapshotProbe";

import type { OmpSettings } from "../settings.ts";
import { parseOmpAvailableCommands, type OmpDiscoveredCommands } from "./commands.ts";
import {
  buildOmpProbeLaunch,
  OMP_SUPPORTED_RUNTIME_MODES,
  resolveOmpLaunchArgs,
  type OmpUnattendedOverlay,
} from "./launch.ts";
import { OMP_MODEL_PICKER_LIMIT, ompServerModels } from "./modelCapabilities.ts";
import {
  makeOmpRpcConnection,
  OmpRpcError,
  OmpRpcTimeoutError,
  type OmpRpcConnection,
  type OmpRpcHandshakeError,
} from "./rpc.ts";
import { OmpAvailableSlashCommand, OmpModelPickerResult, ompRecordField } from "./wire.ts";

const OMP_PRESENTATION = {
  displayName: "OMP",
  showInteractionModeToggle: false,
  supportedRuntimeModes: OMP_SUPPORTED_RUNTIME_MODES,
  reportsContextWindow: true,
} as const;

const OMP_DRIVER_KIND = ProviderDriverKind.make("omp");
const VERSION_PROBE_TIMEOUT_MS = 10_000;
const OMP_RPC_DISCOVERY_TIMEOUT_MS = 30_000;
/** omp's startup errors when no model is usable (it exits 1 before `ready`), with T3's advice. */
const OMP_NO_MODEL_STARTUP_ERRORS: ReadonlyArray<{
  readonly stderr: string;
  readonly message: string;
}> = [
  {
    stderr: "No default model selected",
    message:
      "OMP has no default model. Run `omp` in a terminal and use /login, set a provider API key environment variable, or pass --model in the OMP launch arguments.",
  },
  {
    stderr: "No model available matching enabledModels",
    message:
      "OMP has no signed-in model that its enabledModels setting allows. Run `omp` in a terminal and use /login for an allowed provider, or adjust enabledModels in omp's config.",
  },
  {
    stderr: "No models available.",
    message:
      "OMP has no usable models. Run `omp` in a terminal to see why, then use /login or set a provider API key environment variable.",
  },
];

const decodeModelPickerResult = Schema.decodeUnknownOption(OmpModelPickerResult);
const decodeAvailableSlashCommand = Schema.decodeUnknownOption(OmpAvailableSlashCommand);

type OmpProbeError = OmpRpcError | OmpRpcTimeoutError | OmpRpcHandshakeError;

// A failed response echoes omp's own `command` field as the operation, so log only names we sent.
const OMP_LOGGED_RPC_OPERATIONS: ReadonlySet<string> = new Set([
  "spawn",
  "read",
  "write",
  "exit",
  "terminate",
  "launch",
  "get_model_picker",
  "get_available_commands",
]);

// Transport failures that a process exiting before `ready` produces; worth asking omp why.
const OMP_TRANSPORT_DOWN_OPERATIONS: ReadonlySet<string> = new Set(["read", "exit"]);

function ompBinary(settings: OmpSettings): string {
  return settings.binaryPath || "omp";
}

function ompFallbackModels(settings: OmpSettings) {
  return ompServerModels([], settings.customModels);
}

function errorProbe(version: string | null, message: string): ProviderProbeResult {
  return { installed: true, version, status: "error", auth: { status: "unknown" }, message };
}

function unauthenticatedProbe(version: string, message: string): ProviderProbeResult {
  return {
    installed: true,
    version,
    status: "warning",
    auth: { status: "unauthenticated", type: "omp" },
    message,
  };
}

function ompSnapshot(
  settings: OmpSettings,
  checkedAt: string,
  probe: ProviderProbeResult,
): ServerProviderDraft {
  return buildServerProvider({
    presentation: OMP_PRESENTATION,
    enabled: settings.enabled,
    checkedAt,
    models: ompFallbackModels(settings),
    probe,
  });
}

const nowIso = Effect.map(DateTime.now, DateTime.formatIso);

export const buildInitialOmpProviderSnapshot = Effect.fn(
  "OmpProviderStatus.buildInitialOmpProviderSnapshot",
)(function* (settings: OmpSettings) {
  const checkedAt = yield* nowIso;
  return ompSnapshot(settings, checkedAt, {
    installed: settings.enabled,
    version: null,
    status: "warning",
    auth: { status: "unknown" },
    message: settings.enabled
      ? "Checking OMP CLI availability..."
      : "OMP is disabled in T3 Code settings.",
  });
});

export const runOmpVersionCommand = Effect.fn("OmpProviderStatus.runOmpVersionCommand")(function* (
  settings: OmpSettings,
  environment: NodeJS.ProcessEnv,
  cwd: string | undefined,
) {
  const command = ompBinary(settings);
  const spawnCommand = yield* resolveSpawnCommand(command, ["--version"], { env: environment });
  return yield* spawnAndCollect(
    command,
    ChildProcess.make(spawnCommand.command, spawnCommand.args, {
      env: environment,
      shell: spawnCommand.shell,
      ...(cwd === undefined ? {} : { cwd }),
    }),
  );
});

type OmpVersionCheck =
  | { readonly ok: true; readonly version: string }
  | { readonly ok: false; readonly probe: ProviderProbeResult };

const rejectedVersion = (probe: ProviderProbeResult): OmpVersionCheck => ({ ok: false, probe });

const rateOmpVersion = Effect.fn("OmpProviderStatus.rateOmpVersion")(function* (
  output: CommandResult,
  compatibility: ProviderCompatibilityLookup,
) {
  if (output.code !== 0) {
    return rejectedVersion(errorProbe(null, "OMP CLI is installed but failed to run."));
  }
  const version = parseCliVersionResult(output);
  if (version === null) {
    return rejectedVersion(
      errorProbe(
        null,
        "T3 Code could not determine the OMP version. Update OMP, then refresh its status.",
      ),
    );
  }
  // Stop before the RPC probe: a release the policy rates broken cannot drive T3 sessions.
  const broken = brokenVersionDetail({
    name: "OMP",
    version,
    advisory: yield* compatibility(OMP_DRIVER_KIND, version),
  });
  return broken === undefined
    ? ({ ok: true, version } satisfies OmpVersionCheck)
    : rejectedVersion(errorProbe(version, broken));
});

const checkOmpVersion = Effect.fn("OmpProviderStatus.checkOmpVersion")(function* (
  settings: OmpSettings,
  environment: NodeJS.ProcessEnv,
  compatibility: ProviderCompatibilityLookup,
) {
  const result = yield* runOmpVersionCommand(
    settings,
    providerProbeEnvironment(environment),
    undefined,
  ).pipe(
    Effect.timeoutOption(
      resolveProviderProbeTimeoutMs("omp", VERSION_PROBE_TIMEOUT_MS, environment),
    ),
    Effect.result,
  );
  if (Result.isFailure(result)) {
    const error = result.failure;
    yield* Effect.logWarning("OMP CLI health check failed.", { errorTag: error._tag });
    const missing = isCommandMissingCause(error);
    return rejectedVersion({
      ...errorProbe(
        null,
        missing
          ? "OMP CLI (`omp`) is not installed or not on PATH. Install it from https://omp.sh, or set its binary path in T3 Code settings."
          : "Failed to execute OMP CLI health check.",
      ),
      installed: !missing,
    });
  }
  if (Option.isNone(result.success)) {
    return rejectedVersion(
      errorProbe(null, "OMP CLI is installed but timed out while running `omp --version`."),
    );
  }
  return yield* rateOmpVersion(result.success.value, compatibility);
});

interface OmpProbeInput {
  readonly settings: OmpSettings;
  readonly environment: NodeJS.ProcessEnv;
  readonly launchArgs: ReadonlyArray<string>;
  readonly cwd: string;
  readonly unattendedOverlay: OmpUnattendedOverlay;
}

const writeUnattendedOverlay = (overlay: OmpUnattendedOverlay) =>
  overlay.ensure.pipe(
    Effect.mapError(
      (cause) =>
        new OmpRpcError({
          operation: "launch",
          detail: "Could not write the unattended settings overlay",
          cause,
        }),
    ),
  );

const openOmpProbe = Effect.fnUntraced(function* (input: OmpProbeInput) {
  yield* writeUnattendedOverlay(input.unattendedOverlay);
  const launch = buildOmpProbeLaunch({
    launchArgs: input.launchArgs,
    environment: input.environment,
    unattendedOverlayPath: input.unattendedOverlay.path,
  });
  const connection = yield* makeOmpRpcConnection({
    command: ompBinary(input.settings),
    args: launch.args,
    cwd: input.cwd,
    env: launch.env,
  });
  yield* Stream.fromQueue(connection.events).pipe(
    Stream.runDrain,
    Effect.ignore,
    Effect.forkScoped,
  );
  return connection;
});

const resolveProbeLaunchArgs = Effect.fnUntraced(function* (settings: OmpSettings) {
  const resolved = resolveOmpLaunchArgs(settings.launchArgs);
  if (!resolved.ok) {
    return yield* new OmpRpcError({ operation: "launch", detail: resolved.message });
  }
  return resolved.args;
});

export interface OmpModelPickerQuery {
  readonly query?: string;
  readonly limit: number;
}

const readOmpModelPicker = Effect.fnUntraced(function* (
  connection: OmpRpcConnection,
  query: OmpModelPickerQuery,
  timeoutMs: number,
) {
  const response = yield* connection.request(
    {
      type: "get_model_picker",
      ...(query.query === undefined ? {} : { query: query.query }),
      limit: query.limit,
    },
    { timeoutMs },
  );
  const picker = decodeModelPickerResult(response.data);
  if (Option.isNone(picker)) {
    return yield* new OmpRpcError({
      operation: "get_model_picker",
      detail: "Invalid model picker result",
    });
  }
  return picker.value;
});

function decodeAvailableCommands(data: unknown): ReadonlyArray<OmpAvailableSlashCommand> | null {
  const commands = ompRecordField(data, "commands");
  if (!Array.isArray(commands)) return null;
  return commands.flatMap((command) => Option.toArray(decodeAvailableSlashCommand(command)));
}

const readOmpCommands = Effect.fnUntraced(function* (
  connection: OmpRpcConnection,
  timeoutMs: number,
) {
  const response = yield* connection.request({ type: "get_available_commands" }, { timeoutMs });
  const commands = decodeAvailableCommands(response.data);
  if (commands === null) {
    return yield* new OmpRpcError({
      operation: "get_available_commands",
      detail: "Invalid command catalog",
    });
  }
  return parseOmpAvailableCommands(commands);
});

const probeTimeoutMs = (environment: NodeJS.ProcessEnv) =>
  resolveProviderProbeTimeoutMs("omp", OMP_RPC_DISCOVERY_TIMEOUT_MS, environment);

/**
 * `get_model_picker` in a passive probe process started in `cwd` (`buildOmpProbeLaunch`). Text
 * generation resolves custom slugs through it, so probe launches stay owned by this module.
 */
export const probeOmpModelPicker = Effect.fn("OmpProviderStatus.probeOmpModelPicker")(function* (
  settings: OmpSettings,
  environment: NodeJS.ProcessEnv,
  cwd: string,
  query: OmpModelPickerQuery,
  unattendedOverlay: OmpUnattendedOverlay,
) {
  const launchArgs = yield* resolveProbeLaunchArgs(settings);
  const connection = yield* openOmpProbe({
    settings,
    environment,
    launchArgs,
    cwd,
    unattendedOverlay,
  });
  return yield* readOmpModelPicker(connection, query, probeTimeoutMs(environment));
}, Effect.scoped);

/**
 * `get_available_commands` in a probe process started in `cwd`. The probe runs `--no-tools`, so
 * the catalog lacks MCP prompt and extension commands until a live session's
 * `available_commands_update` arrives. A failure fails, so it never replaces a usable catalog.
 */
export const discoverOmpCommandsForCwd = Effect.fn("OmpProviderStatus.discoverOmpCommandsForCwd")(
  function* (
    settings: OmpSettings,
    environment: NodeJS.ProcessEnv,
    cwd: string,
    unattendedOverlay: OmpUnattendedOverlay,
  ) {
    const launchArgs = yield* resolveProbeLaunchArgs(settings);
    const connection = yield* openOmpProbe({
      settings,
      environment: providerProbeEnvironment(environment),
      launchArgs,
      cwd,
      unattendedOverlay,
    });
    return yield* readOmpCommands(connection, probeTimeoutMs(environment));
  },
  Effect.scoped,
  (effect, _settings, environment) => {
    const timeoutMs = probeTimeoutMs(environment);
    return effect.pipe(
      Effect.timeoutOrElse({
        duration: timeoutMs,
        orElse: () => Effect.fail(new OmpRpcTimeoutError({ operation: "discovery", timeoutMs })),
      }),
    );
  },
);

interface OmpDiscovery {
  readonly picker: OmpModelPickerResult;
  readonly commands: OmpDiscoveredCommands | undefined;
}

const discoverOmpViaRpc = Effect.fn("OmpProviderStatus.discoverOmpViaRpc")(function* (
  input: OmpProbeInput,
) {
  const timeoutMs = probeTimeoutMs(input.environment);
  const connection = yield* openOmpProbe(input);
  const picker = yield* readOmpModelPicker(
    connection,
    { limit: OMP_MODEL_PICKER_LIMIT },
    timeoutMs,
  );
  // Commands are optional: the picker alone decides readiness.
  const commands = yield* readOmpCommands(connection, timeoutMs).pipe(
    Effect.option,
    Effect.map(Option.getOrUndefined),
  );
  return { picker, commands } satisfies OmpDiscovery;
}, Effect.scoped);

function exitedBeforeReady(error: OmpProbeError): boolean {
  switch (error._tag) {
    case "OmpRpcHandshakeError":
      return error.reason === "no_ready_frame";
    case "OmpRpcError":
      return OMP_TRANSPORT_DOWN_OPERATIONS.has(error.operation);
    default:
      return false;
  }
}

interface OmpDiscoveryContext extends OmpProbeInput {
  readonly version: string;
}

/**
 * The RPC transport logs omp's stderr by length only, so a probe that exited before `ready` is
 * run once more with stdin closed to read why. Same passive argv as the probe.
 */
const noModelStartupMessage = Effect.fn("OmpProviderStatus.noModelStartupMessage")(function* (
  context: OmpDiscoveryContext,
) {
  const launch = buildOmpProbeLaunch({
    launchArgs: context.launchArgs,
    environment: context.environment,
    unattendedOverlayPath: context.unattendedOverlay.path,
  });
  const command = ompBinary(context.settings);
  const spawnCommand = yield* resolveSpawnCommand(command, launch.args, { env: launch.env });
  const output = yield* spawnAndCollect(
    command,
    ChildProcess.make(spawnCommand.command, spawnCommand.args, {
      env: launch.env,
      shell: spawnCommand.shell,
      cwd: context.cwd,
      stdin: { stream: Stream.empty },
    }),
  ).pipe(
    Effect.timeoutOption(
      resolveProviderProbeTimeoutMs("omp", VERSION_PROBE_TIMEOUT_MS, context.environment),
    ),
    Effect.option,
    Effect.map(Option.flatten),
  );
  if (Option.isNone(output) || output.value.code === 0) return undefined;
  const stderr = output.value.stderr;
  return OMP_NO_MODEL_STARTUP_ERRORS.find((error) => stderr.includes(error.stderr))?.message;
});

function operationOf(error: OmpProbeError): string {
  return error._tag === "OmpRpcHandshakeError" ? "handshake" : error.operation;
}

function isProtocolUnsupported(error: OmpProbeError): boolean {
  return error._tag === "OmpRpcHandshakeError" && error.reason === "protocol_unsupported";
}

const logDiscoveryFailure = (
  cause: Cause.Cause<OmpProbeError>,
  failure: OmpProbeError | undefined,
) => {
  const operation = failure === undefined ? undefined : operationOf(failure);
  return Effect.logWarning("OMP RPC discovery failed.", {
    errorTag: causeErrorTag(cause),
    ...(operation !== undefined && OMP_LOGGED_RPC_OPERATIONS.has(operation) ? { operation } : {}),
  });
};

const failedDiscoveryProbe = Effect.fn("OmpProviderStatus.failedDiscoveryProbe")(function* (
  context: OmpDiscoveryContext,
  cause: Cause.Cause<OmpProbeError>,
) {
  const failure = Option.getOrUndefined(Cause.findErrorOption(cause));
  yield* logDiscoveryFailure(cause, failure);
  if (failure !== undefined && isProtocolUnsupported(failure)) {
    return errorProbe(context.version, failure.message);
  }
  if (failure !== undefined && exitedBeforeReady(failure)) {
    const message = yield* noModelStartupMessage(context);
    if (message !== undefined) return unauthenticatedProbe(context.version, message);
  }
  return {
    installed: true,
    version: context.version,
    status: "ready",
    auth: { status: "unknown" },
    message:
      "OMP is available, but T3 Code could not refresh its models and commands. The live session will retry startup.",
  } satisfies ProviderProbeResult;
});

function discoveredSnapshot(
  context: OmpDiscoveryContext,
  checkedAt: string,
  discovery: OmpDiscovery,
): ServerProviderDraft {
  const authenticated = discovery.picker.total > 0;
  return buildServerProvider({
    presentation: OMP_PRESENTATION,
    enabled: context.settings.enabled,
    checkedAt,
    models: ompServerModels(discovery.picker.rows, context.settings.customModels),
    slashCommands: discovery.commands?.slashCommands ?? [],
    skills: discovery.commands?.skills ?? [],
    probe: authenticated
      ? {
          installed: true,
          version: context.version,
          status: "ready",
          auth: { status: "authenticated", type: "omp" },
        }
      : unauthenticatedProbe(
          context.version,
          "OMP has no usable models. Run `omp` in a terminal and use /login, or set a provider API key environment variable.",
        ),
  });
}

const discoverOmpSnapshot = Effect.fn("OmpProviderStatus.discoverOmpSnapshot")(function* (
  context: OmpDiscoveryContext,
  checkedAt: string,
) {
  const exit = yield* discoverOmpViaRpc(context).pipe(
    Effect.timeoutOption(probeTimeoutMs(context.environment)),
    Effect.exit,
  );
  if (Exit.isFailure(exit)) {
    const probe = yield* failedDiscoveryProbe(context, exit.cause);
    return ompSnapshot(context.settings, checkedAt, probe);
  }
  if (Option.isNone(exit.value)) {
    return ompSnapshot(context.settings, checkedAt, {
      installed: true,
      version: context.version,
      status: "ready",
      auth: { status: "unknown" },
      message:
        "OMP is available, but listing its models timed out. The live session will retry startup.",
    });
  }
  return discoveredSnapshot(context, checkedAt, exit.value.value);
});

/**
 * Disabled → warning; `omp --version` (→ `omp/<semver>`); broken-version advisory; launch-arg
 * validation; probe `get_model_picker {limit}`: ready when `total > 0`.
 */
export const checkOmpProviderStatus = Effect.fn("checkOmpProviderStatus")(function* (
  settings: OmpSettings,
  environment: NodeJS.ProcessEnv,
  cwd: string,
  compatibility: ProviderCompatibilityLookup,
  unattendedOverlay: OmpUnattendedOverlay,
) {
  const checkedAt = yield* nowIso;
  if (!settings.enabled) {
    return ompSnapshot(settings, checkedAt, {
      installed: false,
      version: null,
      status: "warning",
      auth: { status: "unknown" },
      message: "OMP is disabled in T3 Code settings.",
    });
  }
  const versionCheck = yield* checkOmpVersion(settings, environment, compatibility);
  if (!versionCheck.ok) return ompSnapshot(settings, checkedAt, versionCheck.probe);
  const launchArgs = resolveOmpLaunchArgs(settings.launchArgs);
  if (!launchArgs.ok) {
    return ompSnapshot(settings, checkedAt, errorProbe(versionCheck.version, launchArgs.message));
  }
  return yield* discoverOmpSnapshot(
    {
      settings,
      environment: providerProbeEnvironment(environment),
      launchArgs: launchArgs.args,
      cwd,
      unattendedOverlay,
      version: versionCheck.version,
    },
    checkedAt,
  );
});

export const enrichOmpSnapshot = (input: {
  readonly snapshot: ServerProvider;
  readonly maintenanceCapabilities: ProviderMaintenanceCapabilities;
  readonly enableProviderUpdateChecks?: boolean;
  readonly publishSnapshot: (snapshot: ServerProvider) => Effect.Effect<void>;
}): Effect.Effect<
  void,
  never,
  HttpClient.HttpClient | ProviderLatestVersions.ProviderLatestVersions
> =>
  enrichProviderSnapshotWithVersionAdvisory(input.snapshot, input.maintenanceCapabilities, {
    enableProviderUpdateChecks: input.enableProviderUpdateChecks,
  }).pipe(
    Effect.flatMap(input.publishSnapshot),
    Effect.catchCause((cause) =>
      Effect.logWarning("OMP version advisory enrichment failed", {
        errorTag: causeErrorTag(cause),
      }),
    ),
    Effect.asVoid,
  );
