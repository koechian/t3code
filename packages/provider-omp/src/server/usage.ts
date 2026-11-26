/**
 * Usage history from omp session files: T3's own under `<stateDir>/omp-sessions`, and the
 * terminal sessions in omp's own sessions directory.
 */
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as ProviderHost from "@t3tools/provider-core/server/ProviderHost";
import {
  parseTimestampMs,
  tokenCount,
  totalTokens,
  type ProviderUsageReader,
  type TranscriptUsageFormat,
  type UsageRecord,
} from "@t3tools/provider-core/server/usage";
import * as HostProcess from "@t3tools/shared/HostProcess";

import type { OmpSettings } from "../settings.ts";
import { ompSessionsRoot } from "./launch.ts";

const OmpUsageScanState = Schema.Struct({ sessionId: Schema.mutableKey(Schema.String) });
type OmpUsageScanState = typeof OmpUsageScanState.Type;

const Usage = Schema.Struct({
  input: Schema.optional(Schema.Unknown),
  output: Schema.optional(Schema.Unknown),
  cacheRead: Schema.optional(Schema.Unknown),
  cacheWrite: Schema.optional(Schema.Unknown),
  reasoningTokens: Schema.optional(Schema.Unknown),
  cost: Schema.optional(Schema.Struct({ total: Schema.optional(Schema.Unknown) })),
  cttl: Schema.optional(Schema.Struct({ ephemeral1h: Schema.optional(Schema.Unknown) })),
});
const Identity = { id: Schema.NonEmptyString, timestamp: Schema.String };
const Model = { provider: Schema.NonEmptyString, model: Schema.NonEmptyString };
const Entry = Schema.Union([
  Schema.Struct({ type: Schema.Literal("session"), id: Schema.NonEmptyString }),
  Schema.Struct({
    ...Identity,
    type: Schema.Literal("message"),
    message: Schema.Struct({ ...Model, role: Schema.Literal("assistant"), usage: Usage }),
  }),
  // Calls outside the transcript (compaction, titles, memory) record their spend here.
  Schema.Struct({ ...Identity, ...Model, type: Schema.Literal("model_usage"), usage: Usage }),
]);
const decodeEntry = Schema.decodeUnknownOption(Entry);

type OmpUsageEntry = Exclude<typeof Entry.Type, { readonly type: "session" }>;

function positiveCost(value: unknown): number | null {
  return typeof value === "number" && Number.isFinite(value) && value > 0 ? value : null;
}

function usageRecord(
  entry: OmpUsageEntry,
  timestampMs: number,
  sessionId: string,
): UsageRecord | undefined {
  const source = entry.type === "message" ? entry.message : entry;
  const usage = source.usage;
  const model = `${source.provider}/${source.model}`;
  const outputTokens = tokenCount(usage.output);
  const cacheCreationTokens = tokenCount(usage.cacheWrite);
  const totals = {
    // omp's input and cache counters are already disjoint.
    uncachedInputTokens: tokenCount(usage.input),
    cachedInputTokens: tokenCount(usage.cacheRead),
    cacheCreationTokens,
    outputTokens,
    reasoningTokens: Math.min(outputTokens, tokenCount(usage.reasoningTokens)),
  };
  // Omitted rates default to zero; use shared estimates without a positive reported cost.
  const reportedCostUsd = positiveCost(usage.cost?.total);
  if (totalTokens(totals) === 0 && reportedCostUsd === null) return undefined;
  const cacheCreation1hTokens = Math.min(cacheCreationTokens, tokenCount(usage.cttl?.ephemeral1h));
  return {
    provider: "omp",
    timestampMs,
    model,
    rateModel: source.model,
    sessionId,
    totals,
    ...(cacheCreation1hTokens > 0 ? { cacheCreation1hTokens } : {}),
    reportedCostUsd,
    speed: "standard",
    // Branches and forks copy entries with their ids and timestamps into new files.
    dedupeKey: JSON.stringify(["omp", entry.id, entry.timestamp, model]),
  };
}

/**
 * Line 1 is the title slot, line 2 the `{type: "session", id}` header; usage comes from
 * assistant `message` entries and `model_usage` entries (not `get_session_stats`, which covers
 * only the window since the last compaction).
 */
function parseOmpUsageEntry(parsed: unknown, state: OmpUsageScanState): readonly UsageRecord[] {
  const decoded = decodeEntry(parsed);
  if (Option.isNone(decoded)) return [];
  const entry = decoded.value;
  if (entry.type === "session") {
    state.sessionId = entry.id;
    return [];
  }
  const timestampMs = parseTimestampMs(entry.timestamp);
  if (timestampMs === null) return [];
  const record = usageRecord(entry, timestampMs, state.sessionId);
  return record === undefined ? [] : [record];
}

export const ompUsageFormat: TranscriptUsageFormat<OmpUsageScanState> = {
  selectFields: {
    type: true,
    id: true,
    timestamp: true,
    provider: true,
    model: true,
    usage: true,
    message: { role: true, provider: true, model: true, usage: true },
  },
  usageMarkers: ['"usage"', '"session"'],
  parseLine: (line, state) => {
    let parsed: unknown;
    try {
      parsed = JSON.parse(line);
    } catch {
      return [];
    }
    return parseOmpUsageEntry(parsed, state);
  },
  parseProjected: parseOmpUsageEntry,
  state: { initial: () => ({ sessionId: "" }), schema: OmpUsageScanState },
};

const OMP_CONFIG_DIR_NAME = ".omp";
const OMP_XDG_APP_NAME = "omp";
const OMP_PROFILE_NAME_PATTERN = /^[a-z0-9][a-z0-9._-]{0,63}$/;
const WINDOWS_RESERVED_BASENAME_PATTERN = /^(?:CON|PRN|AUX|NUL|COM[0-9]|LPT[0-9])(?:\..*)?$/i;

/** The home omp resolves against: the instance's HOME (USERPROFILE on Windows), else the host's. */
function ompHome(environment: NodeJS.ProcessEnv, hostHome: string, platform: NodeJS.Platform) {
  return (platform === "win32" ? environment.USERPROFILE : environment.HOME) || hostHome;
}

function isOmpProfileName(name: string): boolean {
  return (
    OMP_PROFILE_NAME_PATTERN.test(name) &&
    !name.endsWith(".") &&
    !WINDOWS_RESERVED_BASENAME_PATTERN.test(name)
  );
}

/** `OMP_PROFILE`, else `PI_PROFILE`; "default", blank and invalid names select the default profile. */
function ompProfile(environment: NodeJS.ProcessEnv): string | undefined {
  const name = (environment.OMP_PROFILE ?? environment.PI_PROFILE)?.trim();
  if (!name || name === "default") return undefined;
  return isOmpProfileName(name) ? name : undefined;
}

function ompAgentDir(
  environment: NodeJS.ProcessEnv,
  home: string,
  profile: string | undefined,
  path: Path.Path,
): string {
  const configRoot = path.join(home, environment.PI_CONFIG_DIR || OMP_CONFIG_DIR_NAME);
  return profile === undefined
    ? path.join(configRoot, "agent")
    : path.join(configRoot, "profiles", profile, "agent");
}

/**
 * Pi reads these two variables as well, so Pi's reader already scans the directory they name, and
 * UsageService parses a directory for one provider only. A named profile ignores the agent dir.
 */
function isDecidedByPiVariables(environment: NodeJS.ProcessEnv, profile: string | undefined) {
  if (environment.PI_CODING_AGENT_SESSION_DIR?.trim()) return true;
  return profile === undefined && Boolean(environment.PI_CODING_AGENT_DIR?.trim());
}

/** omp moves data under `$XDG_DATA_HOME/omp` on Linux and macOS once that directory exists. */
const ompXdgDataDir = Effect.fn("ompXdgDataDir")(function* (
  environment: NodeJS.ProcessEnv,
  profile: string | undefined,
) {
  const platform = yield* HostProcess.Platform;
  const xdgDataHome = environment.XDG_DATA_HOME;
  if (!xdgDataHome || (platform !== "linux" && platform !== "darwin")) return undefined;
  const path = yield* Path.Path;
  const fileSystem = yield* FileSystem.FileSystem;
  const appRoot = path.join(xdgDataHome, OMP_XDG_APP_NAME);
  const dataDir = profile === undefined ? appRoot : path.join(appRoot, "profiles", profile);
  const exists = yield* fileSystem.exists(dataDir).pipe(Effect.orElseSucceed(() => false));
  return exists ? dataDir : undefined;
});

/**
 * omp's own sessions directory, resolved as `getSessionsDir` in `@oh-my-pi/pi-utils` (`dirs.ts`)
 * does, or undefined when Pi's variables decide it.
 */
const resolveOmpSessionsDir = Effect.fn("resolveOmpSessionsDir")(function* (
  environment: NodeJS.ProcessEnv,
) {
  const profile = ompProfile(environment);
  if (isDecidedByPiVariables(environment, profile)) return undefined;
  const path = yield* Path.Path;
  const home = ompHome(environment, yield* HostProcess.HomeDirectory, yield* HostProcess.Platform);
  const xdgDataDir = yield* ompXdgDataDir(environment, profile);
  return path.join(xdgDataDir ?? ompAgentDir(environment, home, profile, path), "sessions");
});

export type OmpUsageReaderEnv = Path.Path | FileSystem.FileSystem | ProviderHost.ProviderHost;

export const ompUsageReader: ProviderUsageReader<OmpSettings, OmpUsageReaderEnv> = {
  kind: "transcripts",
  provider: "omp",
  format: ompUsageFormat,
  // omp model IDs are `<provider>/<rate model>`.
  rateModel: (model) => model.slice(model.indexOf("/") + 1),
  directories: Effect.fn("ompUsageReader.directories")(function* ({ environment }) {
    const path = yield* Path.Path;
    const host = yield* ProviderHost.ProviderHost;
    const sessionsDir = yield* resolveOmpSessionsDir(environment);
    // T3 launches pass --session-dir, so terminal sessions stay in omp's own directory.
    return [
      { dir: ompSessionsRoot(path, host.paths.stateDir) },
      ...(sessionsDir === undefined ? [] : [{ dir: sessionsDir }]),
    ];
  }),
};
