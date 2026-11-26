/**
 * Process launch policy: user launch-arg validation, the argv for each kind of omp process,
 * the T3-owned `--config` overlay, and the child environment.
 * Copy-and-adapt of packages/provider-pi/src/server/mcpInjection.ts (launch-arg half only).
 */
import type { RuntimeMode } from "@t3tools/contracts";
import { tokenizeCliArgs } from "@t3tools/shared/cliArgs";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import type * as PlatformError from "effect/PlatformError";
// @effect-diagnostics-next-line nodeBuiltinImport:off -- overlay paths are derived synchronously, so the hash must be too.
import * as NodeCrypto from "node:crypto";

/** Flags T3 owns. `--profile` stays allowed (DECISIONS: Process and transport). */
const OMP_RESERVED_LAUNCH_ARGUMENTS: ReadonlySet<string> = new Set([
  "--mode",
  "--session-dir",
  "--session",
  "--resume",
  "-r",
  "--continue",
  "-c",
  "--restore",
  "--fork",
  "--no-session",
  "--print",
  "-p",
  "--cwd",
  "--no-ui",
  "--approval-mode",
  "--auto-approve",
  "--yolo",
  "--goal",
  "--plan",
  "--plan-yolo",
  "--plan-yolo-into",
  "--from-claude",
  "--from-codex",
  "--export",
  "--append-system-prompt",
  "--help",
  "-h",
  "--version",
  "-v",
]);

/** Built-in omp flags that consume the next token (upstream cli/flag-tables.ts STRING_SETTERS). */
const OMP_ARGUMENTS_WITH_VALUES: ReadonlySet<string> = new Set([
  "--config",
  "--add-dir",
  "--provider",
  "--model",
  "--smol",
  "--slow",
  "--prewalk-into",
  "--max-time",
  "--warn-cost",
  "--max-cost",
  "--service-tier",
  "--spinner-interval",
  "--max-fps",
  "--motion",
  "--api-key",
  "--system-prompt",
  "--system-prompt-template",
  "--provider-session-id",
  "--prompt-cache-key",
  "--models",
  "--tools",
  "--thinking",
  "--hook",
  "--extension",
  "-e",
  "--trusted-extension",
  "--plugin-dir",
  "--skills",
  "--profile",
  "--alias",
]);

/** Unreserved built-in booleans (upstream cli/flag-tables.ts VALUELESS_FLAGS); they never take a value. */
const OMP_ARGUMENTS_WITHOUT_VALUES: ReadonlySet<string> = new Set([
  "--allow-home",
  "--no-tools",
  "--no-judge",
  "--no-spotlight",
  "--offline",
  "--no-lsp",
  "--no-pty",
  "--hide-thinking",
  "--advisor",
  "--wait-async",
  "--external-thinking",
  "--prewalk",
  "--no-prewalk",
  "--print-thoughts",
  "--no-extensions",
  "--no-skills",
  "--no-rules",
  "--no-context-files",
  "--no-title",
]);

/**
 * Dropped from user args in probe, fork and print launches, with their values and `=` forms.
 * omp rejects them next to `--no-tools` (upstream cli/args.ts:478-483), an explicit extension
 * loads even under `--no-extensions` (main.ts:2060-2067), and `--tools` re-enables tools.
 */
const OMP_TOOL_LOADING_ARGUMENTS: ReadonlySet<string> = new Set([
  "--extension",
  "-e",
  "--hook",
  "--trusted-extension",
  "--tools",
]);

/**
 * Also dropped in print launches: `--system-prompt-template` cannot be combined with T3's
 * `--system-prompt` (cli/args.ts:485-487), and `--advisor` overrides the overlay (main.ts:2222).
 */
const OMP_PRINT_CONFLICTING_ARGUMENTS: ReadonlySet<string> = new Set([
  "--system-prompt-template",
  "--advisor",
]);

const OMP_PRINT_DROPPED_ARGUMENTS: ReadonlySet<string> = new Set([
  ...OMP_TOOL_LOADING_ARGUMENTS,
  ...OMP_PRINT_CONFLICTING_ARGUMENTS,
]);

/** Restrictions follow the user args so the user cannot re-enable tools or extensions. */
const OMP_UNATTENDED_RESTRICTIONS = ["--no-extensions", "--no-tools", "--offline"] as const;

export type OmpLaunchArgsResolution =
  | { readonly ok: true; readonly args: ReadonlyArray<string> }
  | { readonly ok: false; readonly message: string };

type OmpLaunchArgumentStep = { readonly next: number } | { readonly error: string };

/** The `--flag` of a `--flag=value` token; undefined for every other token. */
function inlineOptionName(arg: string): string | undefined {
  const equalsIndex = arg.indexOf("=");
  return arg.startsWith("-") && equalsIndex > 0 ? arg.slice(0, equalsIndex) : undefined;
}

/** omp splits every `--flag=value`; T3 splits only built-in value flags, as Pi does (MI:94-101). */
function splitBuiltInInlineValues(args: ReadonlyArray<string>): ReadonlyArray<string> {
  return args.flatMap((arg) => {
    const option = arg.startsWith("--") ? inlineOptionName(arg) : undefined;
    return option !== undefined && OMP_ARGUMENTS_WITH_VALUES.has(option)
      ? [option, arg.slice(option.length + 1)]
      : [arg];
  });
}

function reservedOmpArgument(arg: string): string | undefined {
  const name = arg.startsWith("--") ? (inlineOptionName(arg) ?? arg) : arg;
  return OMP_RESERVED_LAUNCH_ARGUMENTS.has(name) ? name : undefined;
}

/** An unknown `--flag` may be an extension string flag that takes one value-like token. */
function extensionValueWidth(next: string | undefined): number {
  return next !== undefined && !next.startsWith("-") && !next.startsWith("@") ? 1 : 0;
}

function unsupportedArgumentMessage(arg: string): string {
  if (arg === "--") return "OMP launch arguments cannot include positional prompts.";
  if (arg.startsWith("-") && arg !== "-") {
    return `OMP launch argument '${arg}' is not supported by T3 Code.`;
  }
  return `OMP launch arguments cannot include positional prompt '${arg}'.`;
}

function launchArgumentStep(args: ReadonlyArray<string>, index: number): OmpLaunchArgumentStep {
  const arg = args[index] ?? "";
  const reserved = reservedOmpArgument(arg);
  if (reserved !== undefined) {
    return {
      error: `OMP launch argument '${reserved}' is controlled by T3 Code and cannot be overridden.`,
    };
  }
  if (OMP_ARGUMENTS_WITH_VALUES.has(arg)) {
    return args[index + 1] === undefined
      ? { error: `OMP launch argument '${arg}' requires a value.` }
      : { next: index + 2 };
  }
  if (OMP_ARGUMENTS_WITHOUT_VALUES.has(arg) || inlineOptionName(arg)?.startsWith("--")) {
    return { next: index + 1 };
  }
  if (arg.startsWith("--") && arg.length > 2) {
    return { next: index + 1 + extensionValueWidth(args[index + 1]) };
  }
  return { error: unsupportedArgumentMessage(arg) };
}

export function resolveOmpLaunchArgs(launchArgs: string): OmpLaunchArgsResolution {
  const args = splitBuiltInInlineValues(tokenizeCliArgs(launchArgs));
  let index = 0;
  while (index < args.length) {
    const step = launchArgumentStep(args, index);
    if ("error" in step) return { ok: false, message: step.error };
    index = step.next;
  }
  return { ok: true, args };
}

function isBuiltInOmpArgument(name: string): boolean {
  return OMP_ARGUMENTS_WITH_VALUES.has(name) || OMP_ARGUMENTS_WITHOUT_VALUES.has(name);
}

/** Tokens a validated argument spans, as `resolveOmpLaunchArgs` reads them. */
function ompArgumentWidth(args: ReadonlyArray<string>, index: number): number {
  const arg = args[index] ?? "";
  if (OMP_ARGUMENTS_WITH_VALUES.has(arg)) return 2;
  if (OMP_ARGUMENTS_WITHOUT_VALUES.has(arg) || inlineOptionName(arg) !== undefined) return 1;
  return 1 + extensionValueWidth(args[index + 1]);
}

/**
 * User args for probe, fork and print launches: built-in flags only, minus `dropped`, each with
 * its value token or `=` form. These launches load no extensions, so omp exits 2 on an extension
 * flag (upstream main.ts:2712-2717). Kept values are copied as they are, so
 * `--system-prompt --tools` keeps its literal prompt (MI:182-221).
 */
function unattendedOmpArguments(
  args: ReadonlyArray<string>,
  dropped: ReadonlySet<string>,
): ReadonlyArray<string> {
  const kept: string[] = [];
  for (let index = 0; index < args.length;) {
    const width = ompArgumentWidth(args, index);
    const arg = args[index] ?? "";
    const name = inlineOptionName(arg) ?? arg;
    if (isBuiltInOmpArgument(name) && !dropped.has(name)) {
      kept.push(...args.slice(index, index + width));
    }
    index += width;
  }
  return kept;
}

export type OmpApprovalMode = "always-ask" | "write" | "yolo";

/** Runtime modes omp enforces natively; `auto` is not offered (DECISIONS: Approvals). */
export const OMP_SUPPORTED_RUNTIME_MODES = [
  "approval-required",
  "auto-accept-edits",
  "full-access",
] as const satisfies ReadonlyArray<RuntimeMode>;

const OMP_APPROVAL_MODES: Readonly<Record<RuntimeMode, OmpApprovalMode>> = {
  "approval-required": "always-ask",
  "auto-accept-edits": "write",
  auto: "always-ask",
  "full-access": "yolo",
};

/** An unoffered stored mode (`auto`) falls back to Supervised, i.e. `always-ask`. */
export function ompApprovalModeForRuntimeMode(mode: RuntimeMode): OmpApprovalMode {
  return OMP_APPROVAL_MODES[mode];
}

export interface OmpSessionOverlay {
  readonly approvalMode: OmpApprovalMode;
  /** Host tool names written as `tools.approval.<name>: allow` (read-only T3 tools). */
  readonly allowedToolNames: ReadonlyArray<string>;
}

/** A JSON string is a YAML double-quoted scalar, so `-` in host tool names needs no escaping. */
const yamlString = (value: string) => JSON.stringify(value);

/**
 * Minimal overlay; nothing else of the user's config changes. omp refuses one that does not parse,
 * so names are deduplicated. The shell trace was ~80% of bash frame bytes. Without `rpc` in
 * `goal.continuationModes`, a goal only rides along with the next prompt.
 */
export function renderOmpSessionOverlay(overlay: OmpSessionOverlay): string {
  const allowed = [...new Set(overlay.allowedToolNames)];
  const lines = [
    "display:",
    "  shellVisualization: false",
    "goal:",
    "  continuationModes:",
    '    - "interactive"',
    '    - "rpc"',
    "tools:",
    `  approvalMode: ${yamlString(overlay.approvalMode)}`,
  ];
  if (allowed.length > 0) {
    lines.push("  approval:", ...allowed.map((name) => `    ${yamlString(name)}: "allow"`));
  }
  return `${lines.join("\n")}\n`;
}

/** Text generation only: advisor and memory off, so a title costs one model call. */
export function renderOmpTextGenerationOverlay(): string {
  return [
    "advisor:",
    "  enabled: false",
    "memory:",
    '  backend: "off"',
    "memories:",
    "  enabled: false",
    "",
  ].join("\n");
}

/**
 * Settings every launch T3 runs without a user (probe, fork, print) keeps off. omp's startup
 * marketplace refresh fetches plugin catalogs and, set to `auto`, installs plugin updates
 * (upstream main.ts `scheduleMarketplaceAutoUpdate`); `--offline` does not gate it.
 */
export function renderOmpUnattendedOverlay(): string {
  return ["marketplace:", '  autoUpdate: "off"', ""].join("\n");
}

const OMP_OVERLAYS_DIR_NAME = "omp-overlays";
const OMP_SESSIONS_DIR_NAME = "omp-sessions";
const OMP_UNATTENDED_OVERLAY_FILE = "unattended.yml";

/** The unattended overlay, passed to `--config`, and an effect that writes it on first use. */
export interface OmpUnattendedOverlay {
  readonly path: string;
  readonly ensure: Effect.Effect<void, PlatformError.PlatformError>;
}

/** `<stateDir>/omp-overlays/unattended.yml`; its content never changes, so writers cannot disagree. */
export const makeOmpUnattendedOverlay = Effect.fn("OmpLaunch.makeUnattendedOverlay")(function* (
  stateDir: string,
) {
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const overlayPath = path.join(stateDir, OMP_OVERLAYS_DIR_NAME, OMP_UNATTENDED_OVERLAY_FILE);
  let written = false;
  const ensure = Effect.suspend(() =>
    written
      ? Effect.void
      : writeOmpConfigOverlay(overlayPath, renderOmpUnattendedOverlay()).pipe(
          Effect.tap(() =>
            Effect.sync(() => {
              written = true;
            }),
          ),
          Effect.provideService(FileSystem.FileSystem, fileSystem),
          Effect.provideService(Path.Path, path),
        ),
  );
  return { path: overlayPath, ensure } satisfies OmpUnattendedOverlay;
});

/** `<stateDir>/omp-sessions`, the root of the session directories T3 passes to `--session-dir`. */
export function ompSessionsRoot(path: Path.Path, stateDir: string): string {
  return path.join(stateDir, OMP_SESSIONS_DIR_NAME);
}

/** Leaves room for the staging and `.append-system.md` names under the 255-byte file-name limit. */
const OMP_OVERLAY_STEM_MAX_LENGTH = 128;
const OMP_OVERLAY_DIGEST_LENGTH = 16;

/** A file-safe stem for `processKey`; a long key keeps a readable prefix plus a digest of the key. */
function ompOverlayFileStem(processKey: string): string {
  const safeKey = processKey.replace(/[^A-Za-z0-9._-]/g, "-");
  if (safeKey.length <= OMP_OVERLAY_STEM_MAX_LENGTH) return safeKey;
  const digest = NodeCrypto.createHash("sha256").update(processKey).digest("hex");
  const prefixLength = OMP_OVERLAY_STEM_MAX_LENGTH - OMP_OVERLAY_DIGEST_LENGTH - 1;
  return `${safeKey.slice(0, prefixLength)}-${digest.slice(0, OMP_OVERLAY_DIGEST_LENGTH)}`;
}

/** Path of a per-process overlay under `<stateDir>/omp-overlays/`. */
export function ompOverlayPath(path: Path.Path, stateDir: string, processKey: string): string {
  return path.join(stateDir, OMP_OVERLAYS_DIR_NAME, `${ompOverlayFileStem(processKey)}.yml`);
}

/**
 * The `--append-system-prompt` file beside an overlay, named after the overlay's file-safe stem;
 * a raw session id can hold ':' (an NTFS stream on Windows).
 */
export function ompAppendSystemPromptPath(path: Path.Path, overlayPath: string): string {
  return path.join(
    path.dirname(overlayPath),
    `${path.basename(overlayPath, ".yml")}.append-system.md`,
  );
}

/**
 * Atomic write (staged in the same directory, then renamed): omp stat-polls the file every
 * 500 ms and refuses an overlay that is missing or does not parse.
 */
export const writeOmpConfigOverlay = Effect.fn("OmpLaunch.writeConfigOverlay")(function* (
  filePath: string,
  contents: string,
) {
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const directory = path.dirname(filePath);
  yield* fileSystem.makeDirectory(directory, { recursive: true });
  yield* Effect.scoped(
    Effect.gen(function* () {
      const staging = yield* fileSystem.makeTempDirectoryScoped({ directory, prefix: ".staging-" });
      const staged = path.join(staging, path.basename(filePath));
      yield* fileSystem.writeFileString(staged, contents);
      // Windows refuses a rename over a file a reader has open; omp reads it on every poll.
      yield* fileSystem.rename(staged, filePath).pipe(Effect.retry({ times: 3 }));
    }),
  );
});

export interface OmpAppendSystemPromptInput {
  readonly cwd: string;
  readonly home: string;
  readonly environment: NodeJS.ProcessEnv;
  /** T3_CODE_ORCHESTRATION_INSTRUCTIONS + PULL_REQUEST_LINKING_INSTRUCTIONS. */
  readonly t3Instructions: string;
}

const APPEND_SYSTEM_FILE_NAME = "APPEND_SYSTEM.md";
const OMP_PROJECT_CONFIG_DIRS = [".omp", ".claude", ".codex", ".gemini"] as const;

function nonEmptyEnvironmentValue(value: string | undefined): string | undefined {
  const trimmed = value?.trim();
  return trimmed === undefined || trimmed.length === 0 ? undefined : trimmed;
}

/**
 * omp's user-level config dirs in priority order (upstream config.ts:17-22, 90-160). Claude's,
 * Codex's and Gemini's home dirs are opt-in through omp's `enabledProviders`, which T3 cannot
 * read; Claude's also counts when `CLAUDE_CONFIG_DIR` is set (capability/index.ts:419-428).
 * Named profiles are not resolved.
 */
function ompUserConfigDirs(path: Path.Path, input: OmpAppendSystemPromptInput): string[] {
  const agentDir = nonEmptyEnvironmentValue(input.environment["PI_CODING_AGENT_DIR"]);
  const configDirName = nonEmptyEnvironmentValue(input.environment["PI_CONFIG_DIR"]) ?? ".omp";
  const claudeDir = nonEmptyEnvironmentValue(input.environment["CLAUDE_CONFIG_DIR"]);
  return [
    agentDir === undefined
      ? path.join(input.home, configDirName, "agent")
      : path.resolve(input.cwd, agentDir),
    ...(claudeDir === undefined ? [] : [path.resolve(input.cwd, claudeDir)]),
  ];
}

/** Project dirs first, then user dirs: upstream main.ts:1527-1537. */
function appendSystemPromptCandidates(
  path: Path.Path,
  input: OmpAppendSystemPromptInput,
): ReadonlyArray<string> {
  return [
    ...OMP_PROJECT_CONFIG_DIRS.map((dir) => path.join(input.cwd, dir)),
    ...ompUserConfigDirs(path, input),
  ].map((dir) => path.join(dir, APPEND_SYSTEM_FILE_NAME));
}

/**
 * Passing `--append-system-prompt` stops omp discovering the user's APPEND_SYSTEM.md
 * (upstream main.ts:1692), so the file T3 passes carries T3's text plus that file's content.
 */
export const composeOmpAppendSystemPrompt = Effect.fn("OmpLaunch.composeAppendSystemPrompt")(
  function* (input: OmpAppendSystemPromptInput) {
    const fileSystem = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    for (const candidate of appendSystemPromptCandidates(path, input)) {
      const userAppend = yield* fileSystem.readFileString(candidate).pipe(Effect.option);
      if (Option.isSome(userAppend)) return `${input.t3Instructions}\n\n${userAppend.value}`;
    }
    return input.t3Instructions;
  },
);

export interface OmpLaunch {
  readonly args: ReadonlyArray<string>;
  readonly env: NodeJS.ProcessEnv;
}

export interface OmpRpcLaunchInput {
  /** Already validated by `resolveOmpLaunchArgs`. */
  readonly launchArgs: ReadonlyArray<string>;
  readonly environment: NodeJS.ProcessEnv;
  readonly threadId: string;
  readonly sessionDir: string;
  /** Absolute session file to reopen; omitted for a new thread. */
  readonly resumeSessionFile?: string;
  readonly overlayPath: string;
  /**
   * File holding T3's system-prompt append (see MODULES.md, launch.ts). It must exist before
   * spawn: omp reads a missing path as the literal prompt text (system-prompt.ts:316-331).
   */
  readonly appendSystemPromptPath?: string;
}

/**
 * `omp --mode rpc-ui --session-dir <dir> [--resume <file>] --no-title
 *  [--append-system-prompt <file>] <user args> --config <overlay>`; the overlay comes last so it
 *  wins over a user `--config`.
 */
export function buildOmpRpcLaunch(input: OmpRpcLaunchInput): OmpLaunch {
  return {
    args: [
      "--mode",
      "rpc-ui",
      "--session-dir",
      input.sessionDir,
      ...(input.resumeSessionFile === undefined ? [] : ["--resume", input.resumeSessionFile]),
      "--no-title",
      ...(input.appendSystemPromptPath === undefined
        ? []
        : ["--append-system-prompt", input.appendSystemPromptPath]),
      ...input.launchArgs,
      "--config",
      input.overlayPath,
    ],
    env: ompChildEnvironment(input.environment, input.threadId),
  };
}

export interface OmpProbeLaunchInput {
  readonly launchArgs: ReadonlyArray<string>;
  readonly environment: NodeJS.ProcessEnv;
  /** `OmpUnattendedOverlay.path`, written before the launch. */
  readonly unattendedOverlayPath: string;
}

/**
 * Passive health/catalog probe: `--mode rpc-ui --no-session <built-in user args without
 * OMP_TOOL_LOADING_ARGUMENTS> --no-extensions --no-tools --offline --config <unattended>`. Only
 * `--no-tools` keeps omp from starting the user's MCP servers (upstream sdk.ts:2993).
 */
export function buildOmpProbeLaunch(input: OmpProbeLaunchInput): OmpLaunch {
  return {
    args: [
      "--mode",
      "rpc-ui",
      "--no-session",
      ...unattendedOmpArguments(input.launchArgs, OMP_TOOL_LOADING_ARGUMENTS),
      ...OMP_UNATTENDED_RESTRICTIONS,
      "--config",
      input.unattendedOverlayPath,
    ],
    env: ompChildEnvironment(input.environment, undefined),
  };
}

export interface OmpForkLaunchInput {
  readonly launchArgs: ReadonlyArray<string>;
  readonly environment: NodeJS.ProcessEnv;
  /** Where the new session file lands (omp writes forks into the launch `--session-dir`). */
  readonly sessionDir: string;
  readonly sourceSessionFile: string;
  /** `OmpUnattendedOverlay.path`, written before the launch. */
  readonly unattendedOverlayPath: string;
}

/**
 * Short-lived fork process, restricted like a probe after `--fork <source> --session-dir <dir>`.
 * `--fork` copies the source without taking its lease and records the process cwd in the new
 * header; `--resume` would keep the source's cwd (session-manager.ts `forkFrom` vs `open`).
 */
export function buildOmpForkLaunch(input: OmpForkLaunchInput): OmpLaunch {
  return {
    args: [
      "--mode",
      "rpc-ui",
      "--fork",
      input.sourceSessionFile,
      "--session-dir",
      input.sessionDir,
      "--no-title",
      ...unattendedOmpArguments(input.launchArgs, OMP_TOOL_LOADING_ARGUMENTS),
      ...OMP_UNATTENDED_RESTRICTIONS,
      "--config",
      input.unattendedOverlayPath,
    ],
    env: ompChildEnvironment(input.environment, undefined),
  };
}

export interface OmpPrintLaunchInput {
  /**
   * Validated user args; only built-in flags are passed, without OMP_TOOL_LOADING_ARGUMENTS and
   * OMP_PRINT_CONFLICTING_ARGUMENTS.
   */
  readonly launchArgs: ReadonlyArray<string>;
  readonly environment: NodeJS.ProcessEnv;
  readonly model: { readonly provider: string; readonly modelId: string } | undefined;
  readonly systemPrompt: string;
  /** `OmpUnattendedOverlay.path`, written before the launch. */
  readonly unattendedOverlayPath: string;
  /** The text-generation overlay; it follows the unattended one. */
  readonly overlayPath: string;
  readonly maxTimeSeconds: number;
  readonly maxCostUsd: number;
}

/**
 * User args come before T3's flags so T3's win; they still carry the `--profile`, `--provider` or
 * `--api-key` the user's models need. Without `--no-session`, omp saves the run in the user's
 * session dir, or with `autoResume` continues their latest session (upstream main.ts:1433-1520).
 */
export function buildOmpPrintLaunch(input: OmpPrintLaunchInput): OmpLaunch {
  return {
    args: [
      "-p",
      "--mode",
      "json",
      "--no-session",
      ...unattendedOmpArguments(input.launchArgs, OMP_PRINT_DROPPED_ARGUMENTS),
      ...(input.model === undefined
        ? []
        : ["--model", `${input.model.provider}/${input.model.modelId}`]),
      "--thinking",
      "off",
      "--no-tools",
      "--no-extensions",
      "--no-skills",
      "--no-rules",
      "--no-context-files",
      "--no-lsp",
      "--no-title",
      "--max-time",
      String(input.maxTimeSeconds),
      "--max-cost",
      String(input.maxCostUsd),
      "--system-prompt",
      literalPromptArgument(input.systemPrompt),
      "--config",
      input.unattendedOverlayPath,
      "--config",
      input.overlayPath,
    ],
    env: ompChildEnvironment(input.environment, undefined),
  };
}

/** omp reads a one-line prompt argument as a file path when that file exists (system-prompt.ts:316). */
function literalPromptArgument(text: string): string {
  return text.includes("\n") ? text : `${text}\n`;
}

/** PortScanner.AGENT_THREAD_ENV, as a literal because this package cannot reach apps/server. */
const T3_THREAD_ENV = "T3CODE_THREAD_ID";

/** Inherited values that belong to another T3 session, or that make omp emit titles (R§36). */
const OMP_INHERITED_ENVIRONMENT_EXCLUSIONS = [
  "T3_MCP_URL",
  "T3_MCP_BEARER_TOKEN",
  "PI_RPC_EMIT_TITLE",
  T3_THREAD_ENV,
] as const;

/** Child environment: drop inherited T3 MCP credentials and PI_RPC_EMIT_TITLE, add T3CODE_THREAD_ID. */
export function ompChildEnvironment(
  environment: NodeJS.ProcessEnv,
  threadId: string | undefined,
): NodeJS.ProcessEnv {
  const env = { ...environment };
  for (const key of OMP_INHERITED_ENVIRONMENT_EXCLUSIONS) delete env[key];
  if (threadId !== undefined) env[T3_THREAD_ENV] = threadId;
  return env;
}
