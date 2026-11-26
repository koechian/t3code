/**
 * Session-file policy: where T3's omp sessions live, matching a session to a cwd, the entry ids
 * rollback and fork use, the short-lived fork process, and file cleanup.
 */
import { isDeliveredProviderTurn, type OrchestrationV2ProviderTurn } from "@t3tools/contracts";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Predicate from "effect/Predicate";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import type { ProviderAdapterV2RollbackTarget } from "@t3tools/provider-core/server/ProviderAdapter";

import { buildOmpForkLaunch } from "./launch.ts";
import { makeOmpRpcConnection, OmpRpcError, type OmpRpcConnection } from "./rpc.ts";
import { ompRecordField, ompRecordString, type OmpBranchMessage, type OmpRecord } from "./wire.ts";

/** T3-owned, so T3 threads stay out of the TUI's resume list and its session leases. */
const OMP_SESSIONS_DIR_NAME = "omp-sessions";

/** `<stateDir>/omp-sessions/<encoded cwd>`. Pure: `cwd` must already be a realpath. */
export function ompSessionDirForCwd(path: Path.Path, stateDir: string, cwd: string): string {
  const encoded = cwd.replace(/^[/\\]/, "").replace(/[^A-Za-z0-9._-]/g, "-");
  return path.join(stateDir, OMP_SESSIONS_DIR_NAME, `--${encoded}--`);
}

const decodeJson = Schema.decodeUnknownOption(Schema.fromJsonString(Schema.Unknown));

/** Line 1 is a fixed-width title slot and line 2 the header (R§44); one spare line for safety. */
const SESSION_HEADER_SCAN_LINES = 3;

function sessionHeaderCwd(lines: ReadonlyArray<string>): string | undefined {
  for (const line of lines) {
    const record = Option.getOrUndefined(decodeJson(line));
    if (ompRecordString(record, "type") === "session") return ompRecordString(record, "cwd");
  }
  return undefined;
}

/**
 * Whether the session header (line 2, `{type: "session", cwd}`) names `cwd`, comparing realpaths.
 * A file that does not exist yet (lazy until the first assistant message) matches.
 */
export const sessionFileMatchesCwd = Effect.fn("OmpSessions.sessionFileMatchesCwd")(function* (
  sessionFile: string,
  cwd: string,
) {
  const fileSystem = yield* FileSystem.FileSystem;
  const headerCwd = yield* fileSystem.stream(sessionFile).pipe(
    Stream.decodeText(),
    Stream.splitLines,
    Stream.take(SESSION_HEADER_SCAN_LINES),
    Stream.runCollect,
    Effect.map(sessionHeaderCwd),
    Effect.orElseSucceed(() => undefined),
  );
  if (headerCwd === undefined || headerCwd === cwd) return true;
  // omp recorded `/tmp/...` for a process running in `/private/tmp/...` (R§39).
  const realPath = (path: string) =>
    fileSystem.realPath(path).pipe(Effect.orElseSucceed(() => path));
  return (yield* realPath(headerCwd)) === (yield* realPath(cwd));
});

/** The user entry the turn started with: the first `get_branch_messages` entry not in `before`. */
export function ompTurnUserEntryId(
  before: ReadonlyArray<OmpBranchMessage>,
  after: ReadonlyArray<OmpBranchMessage>,
): string | undefined {
  const known = new Set(before.map((message) => message.entryId));
  return after.find((message) => !known.has(message.entryId))?.entryId;
}

/** Strong refs carry the turn's user entry id; weak ones cannot be located in the session. */
function strongTurnEntryId(turn: OrchestrationV2ProviderTurn): string | undefined {
  const ref = turn.nativeTurnRef;
  return ref?.strength === "strong" && ref.nativeId !== null ? ref.nativeId : undefined;
}

/** Turns without a ref, or whose prompt was dropped, left nothing on the branch. */
function firstTurnWithEntryAfter(
  ordinal: number,
  turns: ReadonlyArray<OrchestrationV2ProviderTurn>,
): OrchestrationV2ProviderTurn | undefined {
  return turns
    .filter(
      (turn) =>
        turn.ordinal > ordinal && turn.nativeTurnRef !== null && isDeliveredProviderTurn(turn),
    )
    .sort((left, right) => left.ordinal - right.ordinal)[0];
}

/**
 * The `branch {entryId}` target for a rollback: the user entry of the first discarded turn that
 * left history on the branch (from its strong `nativeTurnRef`). Null when no discarded turn left
 * any. Undefined when that turn's ref is not strong, so its entry cannot be located and the
 * rollback must fail rather than keep the turn (port of Pi's `piForkBoundary`, A:4579-4601).
 */
export function ompRollbackEntryId(
  target: ProviderAdapterV2RollbackTarget,
  providerThreadTurns: ReadonlyArray<OrchestrationV2ProviderTurn>,
): string | null | undefined {
  const keptOrdinal = target.type === "thread_start" ? 0 : target.providerTurn.ordinal;
  const boundary = firstTurnWithEntryAfter(keptOrdinal, providerThreadTurns);
  return boundary === undefined ? null : strongTurnEntryId(boundary);
}

/**
 * The user entries that bound a fork through `target`: `through` is the target's own (checked to
 * be on the fork's branch), `next` the first later turn's, which the fork leaves out. No target
 * forks the whole session. A later turn with only a weak ref cannot be located.
 */
export function ompForkBoundary(
  target: OrchestrationV2ProviderTurn | undefined,
  providerThreadTurns: ReadonlyArray<OrchestrationV2ProviderTurn>,
):
  | { readonly through: string | undefined; readonly next: string | undefined }
  | { readonly error: string } {
  if (target === undefined) return { through: undefined, next: undefined };
  const through = strongTurnEntryId(target);
  const later = firstTurnWithEntryAfter(target.ordinal, providerThreadTurns);
  if (later === undefined) return { through, next: undefined };
  const next = strongTurnEntryId(later);
  return next === undefined
    ? { error: `omp turn ${later.ordinal} has no captured session entry to fork before` }
    : { through, next };
}

function ompEntriesById(entries: ReadonlyArray<OmpRecord>): ReadonlyMap<string, OmpRecord> {
  const byId = new Map<string, OmpRecord>();
  for (const entry of entries) {
    const id = ompRecordString(entry, "id");
    if (id !== undefined) byId.set(id, entry);
  }
  return byId;
}

/** Parent, grandparent, ...; bounded by the entry count so a malformed cycle ends. */
function* ompEntryAncestors(
  byId: ReadonlyMap<string, OmpRecord>,
  entry: OmpRecord,
): Generator<OmpRecord> {
  let current = byId.get(ompRecordString(entry, "parentId") ?? "");
  for (let steps = 0; current !== undefined && steps < byId.size; steps += 1) {
    yield current;
    current = byId.get(ompRecordString(current, "parentId") ?? "");
  }
}

/**
 * The `fork {entryId}` target that keeps history through a turn: the nearest `message` entry above
 * the next turn's user entry. omp forks only at `message` entries (agent-session.ts:13076); the
 * `custom` and `credential_pin` entries it writes before a prompt are refused (06-fork-entry:9-10).
 */
export function ompForkEntryId(
  entries: ReadonlyArray<OmpRecord>,
  nextTurnUserEntryId: string | undefined,
): string | undefined {
  if (nextTurnUserEntryId === undefined) return undefined;
  const byId = ompEntriesById(entries);
  const nextTurnEntry = byId.get(nextTurnUserEntryId);
  if (nextTurnEntry === undefined) return undefined;
  for (const entry of ompEntryAncestors(byId, nextTurnEntry)) {
    if (entry["type"] === "message") return ompRecordString(entry, "id");
  }
  return undefined;
}

function ompEntryDescendsFrom(
  byId: ReadonlyMap<string, OmpRecord>,
  entryId: string,
  ancestorId: string,
): boolean {
  const entry = byId.get(entryId);
  if (entry === undefined) return false;
  if (entryId === ancestorId) return true;
  for (const ancestor of ompEntryAncestors(byId, entry)) {
    if (ompRecordString(ancestor, "id") === ancestorId) return true;
  }
  return false;
}

export interface OmpForkProcessInput {
  readonly binaryPath: string;
  readonly launchArgs: ReadonlyArray<string>;
  readonly environment: NodeJS.ProcessEnv;
  /** The new thread's cwd; omp records it in the new session header. */
  readonly cwd: string;
  readonly sessionDir: string;
  readonly sourceSessionFile: string;
  /** `OmpUnattendedOverlay.path`, already written. */
  readonly unattendedOverlayPath: string;
  /** The turn to fork through; undefined skips the check that the fork target follows it. */
  readonly throughTurnUserEntryId: string | undefined;
  /** The first turn the fork leaves out; undefined forks the whole session. */
  readonly nextTurnUserEntryId: string | undefined;
}

/** Bounds `fork {entryId}`, which rewrites the kept branch, and then omp's artifact copy. */
const OMP_FORK_REQUEST_TIMEOUT_MS = 60_000;

const forkFailure = (detail: string) => new OmpRpcError({ operation: "fork", detail });

/** The tree fields of one session-file line; the rest can hold megabytes of tool output. */
function ompEntryLink(line: string): OmpRecord | undefined {
  const record = Option.getOrUndefined(decodeJson(line));
  const type = ompRecordString(record, "type");
  const id = ompRecordString(record, "id");
  if (id === undefined || type === "session") return undefined;
  return { type, id, parentId: ompRecordField(record, "parentId") };
}

// The helpers below run inside the `OmpSessions.forkInSeparateProcess` span.

/**
 * Reads the `--fork` copy from disk rather than over `get_entries`, whose reply carries every
 * entry in full with image blobs resolved. omp writes the copy before `ready` (`forkFrom`).
 */
const readOmpEntryLinks = Effect.fnUntraced(function* (sessionFile: string) {
  const fileSystem = yield* FileSystem.FileSystem;
  return yield* fileSystem.stream(sessionFile).pipe(
    Stream.decodeText(),
    Stream.splitLines,
    Stream.map(ompEntryLink),
    Stream.filter(Predicate.isNotUndefined),
    Stream.runCollect,
    Effect.mapError(
      (cause) => new OmpRpcError({ operation: "fork", detail: "could not read the copy", cause }),
    ),
  );
});

/** Every session file the fork process reports is recorded in `created`; the source never is. */
const readNewSessionFile = Effect.fnUntraced(function* (
  connection: OmpRpcConnection,
  input: OmpForkProcessInput,
  created: Array<string>,
) {
  const state = yield* connection.request({ type: "get_state" });
  const file = ompRecordString(state.data, "sessionFile");
  if (file === undefined || file === input.sourceSessionFile || created.includes(file)) {
    return yield* forkFailure("omp did not create a distinct session file");
  }
  created.push(file);
  return file;
});

const resolveForkEntryId = Effect.fnUntraced(function* (
  copyFile: string,
  input: OmpForkProcessInput,
  nextTurnUserEntryId: string,
) {
  const entries = yield* readOmpEntryLinks(copyFile);
  const entryId = ompForkEntryId(entries, nextTurnUserEntryId);
  if (entryId === undefined) {
    return yield* forkFailure("the turn after the fork point has no message entry before it");
  }
  const through = input.throughTurnUserEntryId;
  if (through !== undefined && !ompEntryDescendsFrom(ompEntriesById(entries), entryId, through)) {
    return yield* forkFailure("the fork point is not on the branch of the turn it keeps");
  }
  return entryId;
});

/** There is no RPC delete: remove the file, its `.lock.os` sibling and its artifacts directory. */
export const deleteOmpSessionFiles = Effect.fn("OmpSessions.deleteSessionFiles")(function* (
  sessionFile: string,
) {
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const directory = path.dirname(sessionFile);
  const fileName = path.basename(sessionFile);
  yield* fileSystem.remove(sessionFile, { force: true });
  yield* fileSystem.remove(path.join(directory, `.${fileName}.lock.os`), { force: true });
  if (!fileName.endsWith(".jsonl")) return;
  const artifactsDirectory = path.join(directory, fileName.slice(0, -".jsonl".length));
  // A name like `.jsonl` or `..jsonl` would join to the session dir or its parent.
  if (path.dirname(artifactsDirectory) !== directory) return;
  yield* fileSystem.remove(artifactsDirectory, { recursive: true, force: true });
});

const discardForkFile = (sessionFile: string) =>
  deleteOmpSessionFiles(sessionFile).pipe(
    Effect.catchTags({
      PlatformError: (error) =>
        Effect.logWarning("Could not remove a session file an omp fork left behind", error),
    }),
  );

/**
 * `fork {entryId}` answers before its artifact copy finishes; omp awaits that copy only in the
 * shutdown that stdin EOF starts, which exits 0 (session-manager.ts `createBranchedSession`,
 * `close`; rpc-mode.ts `disposeAndExit`). A signalled omp may have left `local://` files short.
 */
const requireCleanExit = Effect.fnUntraced(function* (connection: OmpRpcConnection) {
  yield* connection.terminateWithin(Duration.millis(OMP_FORK_REQUEST_TIMEOUT_MS));
  const exitCode = yield* connection.exited.pipe(Effect.orElseSucceed(() => undefined));
  if (exitCode !== 0) {
    return yield* forkFailure("omp stopped before it finished copying the session's artifacts");
  }
});

const forkInProcess = Effect.fnUntraced(function* (
  input: OmpForkProcessInput,
  created: Array<string>,
) {
  const launch = buildOmpForkLaunch(input);
  const connection = yield* makeOmpRpcConnection({
    command: input.binaryPath,
    args: launch.args,
    cwd: input.cwd,
    env: launch.env,
  });
  const copyFile = yield* readNewSessionFile(connection, input, created);
  if (input.nextTurnUserEntryId === undefined) return copyFile;
  const entryId = yield* resolveForkEntryId(copyFile, input, input.nextTurnUserEntryId);
  const result = yield* connection.request(
    { type: "fork", entryId },
    { timeoutMs: OMP_FORK_REQUEST_TIMEOUT_MS },
  );
  if (ompRecordField(result.data, "cancelled") === true) {
    return yield* forkFailure("omp cancelled the session fork");
  }
  const forkedFile = yield* readNewSessionFile(connection, input, created);
  yield* requireCleanExit(connection);
  return forkedFile;
});

/**
 * `omp --fork <source>` copies the source and its artifacts without taking its lease and records
 * the process cwd; a fork through a turn then runs `fork {entryId}` on that copy, which writes a
 * second file. Once the process is stopped, every file it created except the returned one is
 * removed, and all of them when the fork fails. The source is never removed.
 */
export const forkOmpSessionInSeparateProcess = Effect.fn("OmpSessions.forkInSeparateProcess")(
  function* (input: OmpForkProcessInput) {
    const created: Array<string> = [];
    const forked = yield* forkInProcess(input, created).pipe(Effect.scoped, Effect.exit);
    const kept = Exit.isSuccess(forked) ? forked.value : undefined;
    for (const file of created) {
      if (file !== kept) yield* discardForkFile(file);
    }
    return yield* forked;
  },
);
