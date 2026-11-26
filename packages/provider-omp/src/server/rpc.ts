/**
 * omp RPC transport: spawn `omp --mode rpc-ui`, LF-delimited JSON framing, protocol v2
 * negotiation, `rpc_chunk` reassembly, request correlation and bash-trace stripping.
 * Copy-and-adapt of packages/provider-pi/src/server/rpc.ts.
 */
import * as Clock from "effect/Clock";
import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Option from "effect/Option";
import * as Predicate from "effect/Predicate";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import type * as Cause from "effect/Cause";
import * as ChildProcess from "effect/process/ChildProcess";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";
import * as NodeBuffer from "node:buffer";
import * as HostProcess from "@t3tools/shared/HostProcess";
import { resolveSpawnCommand } from "@t3tools/shared/shell";
import { windowsSystem32Path } from "@t3tools/shared/windowsSystem";

import { signalProcessGroup } from "@t3tools/provider-core/server/processGroup";

import {
  OMP_MAX_FRAME_BYTES,
  OMP_MAX_REASSEMBLED_FRAME_BYTES,
  OMP_RPC_CHUNK_PAYLOAD_BYTES,
  OMP_RPC_PROTOCOL_VERSION,
  OmpReadyFrame,
  ompRecordField,
  ompRecordString,
  type OmpCommand,
  type OmpOutboundFrame,
  type OmpRecord,
} from "./wire.ts";

export class OmpRpcError extends Schema.TaggedError<OmpRpcError>()("OmpRpcError", {
  operation: Schema.String,
  detail: Schema.optional(Schema.String),
  cause: Schema.optional(Schema.Defect()),
}) {
  override get message(): string {
    return `omp RPC ${this.operation} failed${this.detail === undefined ? "" : `: ${this.detail}`}.`;
  }
}

export class OmpRpcTimeoutError extends Schema.TaggedError<OmpRpcTimeoutError>()(
  "OmpRpcTimeoutError",
  {
    operation: Schema.String,
    timeoutMs: Schema.Int.check(Schema.isBetween({ minimum: 0, maximum: Number.MAX_SAFE_INTEGER })),
  },
) {
  override get message(): string {
    return `omp RPC ${this.operation} failed: timed out after ${this.timeoutMs}ms.`;
  }
}

/** omp started but cannot speak protocol v2, so T3 cannot drive it. */
export class OmpRpcHandshakeError extends Schema.TaggedError<OmpRpcHandshakeError>()(
  "OmpRpcHandshakeError",
  {
    reason: Schema.Literals(["no_ready_frame", "protocol_unsupported", "negotiation_refused"]),
    supportedProtocolVersions: Schema.optional(Schema.Array(Schema.Int)),
    cause: Schema.optional(Schema.Defect()),
  },
) {
  override get message(): string {
    return this.reason === "protocol_unsupported"
      ? "This omp version does not support RPC protocol v2. Update omp and try again."
      : `omp RPC handshake failed (${this.reason}).`;
  }
}

export interface OmpRpcSpawnOptions {
  readonly command: string;
  readonly args: ReadonlyArray<string>;
  readonly cwd: string | undefined;
  readonly env: NodeJS.ProcessEnv;
}

export interface OmpRequestOptions {
  readonly timeoutMs?: number;
}

/** The first response for a `request` id; later responses with the same id are dropped. */
export interface OmpRpcResponse {
  readonly id: string;
  readonly data: unknown;
}

export interface OmpRpcConnection {
  /** The `ready` frame, which advertised protocol v2 with T3's frame limits. */
  readonly ready: OmpReadyFrame;
  /**
   * Completes once omp acknowledged protocol v2, and fails with the transport. It is still
   * pending after the connection opens only when omp raised a dialog while starting: omp holds
   * every command until that dialog, already on `events`, is answered.
   */
  readonly negotiated: Effect.Effect<void, OmpRpcError>;
  readonly isNegotiated: Effect.Effect<boolean>;
  /**
   * Sends a command with a fresh `t3-<n>` id and resolves on its first response; a
   * `success: false` response fails with `OmpRpcError`. Responses to these ids never reach `events`.
   */
  readonly request: (
    command: OmpCommand,
    options?: OmpRequestOptions,
  ) => Effect.Effect<OmpRpcResponse, OmpRpcError | OmpRpcTimeoutError>;
  /**
   * Writes a frame without waiting for a reply. A command (`OmpCommandFrame`) carries an id the
   * caller chose, which must not match `/^t3-\d+$/`; its responses arrive on `events` in stream
   * order, so a prompt's ack is read after the `command_output` frames written before it.
   */
  readonly send: (frame: OmpOutboundFrame) => Effect.Effect<void, OmpRpcError>;
  /**
   * Every inbound frame except `ready` and responses to `request` ids, in stream order,
   * reassembled from `rpc_chunk` and with bash `details.trace` already removed. Fails when the
   * transport goes down, after the frames already queued.
   */
  readonly events: Queue.Queue<OmpRecord, OmpRpcError>;
  readonly exited: Effect.Effect<number, OmpRpcError>;
  /**
   * Closes omp's stdin, which is omp's own clean shutdown (an idle omp exits within
   * milliseconds), and keeps reading stdout meanwhile. Signals the process group only if omp is
   * still running after a grace period. To stop without interrupting a turn, `abort` and wait
   * for its `prompt_result` first.
   */
  readonly terminate: Effect.Effect<void>;
  /** `terminate` with a caller-chosen grace between closing stdin and signalling. */
  readonly terminateWithin: (grace: Duration.Duration) => Effect.Effect<void>;
}

/** HostProcess.Platform and HostProcess.Environment are References, so they never appear here. */
export type OmpRpcEnv = Scope.Scope | ChildProcessSpawner.ChildProcessSpawner;

const DEFAULT_REQUEST_TIMEOUT_MS = 30_000;
/** omp writes `ready` before it loads extensions (UP/modes/rpc/rpc-mode.ts:1612); upstream waits 30 s. */
const HANDSHAKE_TIMEOUT_MS = 30_000;
/** omp exits 14-31 ms after stdin EOF when idle and about 1.3 s mid-turn (R§1.1 item 6). */
const INPUT_CLOSE_GRACE = Duration.seconds(2);
const TERMINATION_GRACE = Duration.seconds(1);
const TERMINATION_POLL_INTERVAL = Duration.millis(20);
const ERROR_DETAIL_MAX_CHARS = 200;
const REQUEST_ID_PATTERN = /^t3-\d+$/;

/** `extension_ui_request` methods that wait for an `extension_ui_response` (UP/modes/rpc/rpc-types.ts). */
export const OMP_DIALOG_METHODS: ReadonlySet<string> = new Set([
  "select",
  "confirm",
  "input",
  "editor",
  "ask",
  "form",
]);

function isOmpDialogRequest(record: OmpRecord): boolean {
  return (
    record["type"] === "extension_ui_request" && OMP_DIALOG_METHODS.has(String(record["method"]))
  );
}

export interface OmpJsonlFrames {
  readonly lines: ReadonlyArray<string>;
  readonly droppedLengths: ReadonlyArray<number>;
}

/** Splits stdout on LF only (never U+2028/U+2029) and drops records over `maxRecordChars`. */
export function makeOmpJsonlFramer(maxRecordChars: number): (chunk: string) => OmpJsonlFrames {
  let buffer = "";
  let dropping = false;
  return (chunk) => {
    const lines: string[] = [];
    const droppedLengths: number[] = [];
    let start = 0;
    while (start < chunk.length) {
      const newline = chunk.indexOf("\n", start);
      const end = newline < 0 ? chunk.length : newline;
      if (!dropping) {
        if (buffer.length + end - start > maxRecordChars) {
          droppedLengths.push(buffer.length + end - start);
          buffer = "";
          dropping = true;
        } else {
          buffer += chunk.slice(start, end);
        }
      }
      if (newline < 0) break;
      if (!dropping && buffer.length > 0) {
        lines.push(buffer.endsWith("\r") ? buffer.slice(0, -1) : buffer);
      }
      buffer = "";
      dropping = false;
      start = newline + 1;
    }
    return { lines, droppedLengths };
  };
}

// Report the observed length before LF arrives; an unterminated response can time out first.
const logDroppedRecords = (droppedLengths: ReadonlyArray<number>) =>
  Effect.forEach(
    droppedLengths,
    (recordLength) =>
      Effect.logWarning("Dropping oversized omp stdout record.", {
        recordLength,
        maxRecordLength: OMP_MAX_FRAME_BYTES,
      }),
    { discard: true },
  );

const UnknownFromJsonString = Schema.fromJsonString(Schema.Unknown);
const decodeJsonText = Schema.decodeSync(UnknownFromJsonString);
const encodeJsonText = Schema.encodeSync(UnknownFromJsonString);
const decodeReadyFrame = Schema.decodeUnknownOption(OmpReadyFrame);
const textEncoder = new TextEncoder();

function parseOmpRecord(line: string): OmpRecord | undefined {
  try {
    const parsed: unknown = decodeJsonText(line);
    return Predicate.isObject(parsed) ? (parsed as OmpRecord) : undefined;
  } catch {
    return undefined;
  }
}

function encodeOutboundLine(frame: OmpOutboundFrame): Uint8Array {
  return textEncoder.encode(`${encodeJsonText(frame)}\n`);
}

function summarizeOmpError(error: unknown): string {
  if (error === undefined) return "unknown error";
  const text = Predicate.isString(error) ? error : encodeJsonText(error);
  return text.length > ERROR_DETAIL_MAX_CHARS ? `${text.slice(0, ERROR_DETAIL_MAX_CHARS)}…` : text;
}

function describeStdoutClosure(exitCode: number | undefined): string {
  if (exitCode === undefined || !Number.isFinite(exitCode)) {
    return "omp process closed stdout";
  }
  return `omp process exited with code ${exitCode}`;
}

export type OmpChunkDecodeResult =
  | { readonly _tag: "Frame"; readonly frame: OmpRecord }
  | { readonly _tag: "Pending" }
  | { readonly _tag: "Invalid"; readonly reason: string };

export interface OmpChunkDecoder {
  /** Feeds one parsed JSONL record; a non-chunk record passes through unless a sequence is open. */
  readonly push: (record: OmpRecord) => OmpChunkDecodeResult;
}

interface OmpChunkMetadata {
  readonly chunkId: string;
  readonly index: number;
  readonly count: number;
  readonly byteLength: number;
}

interface OmpChunkSequence extends OmpChunkMetadata {
  readonly chunks: Array<Uint8Array>;
  nextIndex: number;
  receivedBytes: number;
}

const MAX_CHUNK_COUNT = Math.ceil(OMP_MAX_REASSEMBLED_FRAME_BYTES / OMP_RPC_CHUNK_PAYLOAD_BYTES);
const STRICT_BASE64 = /^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/;
const PENDING_CHUNK: OmpChunkDecodeResult = { _tag: "Pending" };

function isSafeInteger(value: unknown): value is number {
  return Number.isSafeInteger(value);
}

function isChunkId(value: unknown): value is string {
  return Predicate.isString(value) && value.length > 0 && value.length <= 128;
}

function isChunkPosition(index: number, count: number): boolean {
  return index >= 0 && count >= 2 && count <= MAX_CHUNK_COUNT && index < count;
}

function isChunkByteLength(byteLength: number): boolean {
  return byteLength >= OMP_MAX_FRAME_BYTES && byteLength <= OMP_MAX_REASSEMBLED_FRAME_BYTES;
}

function readChunkMetadata(record: OmpRecord): OmpChunkMetadata | undefined {
  const { chunkId, index, count, byteLength } = record;
  if (!isChunkId(chunkId) || !isSafeInteger(index) || !isSafeInteger(count)) return undefined;
  if (!isSafeInteger(byteLength) || !isChunkPosition(index, count)) return undefined;
  return isChunkByteLength(byteLength) ? { chunkId, index, count, byteLength } : undefined;
}

function decodeChunkData(data: unknown): Uint8Array | undefined {
  if (!Predicate.isString(data) || data.length === 0 || !STRICT_BASE64.test(data)) {
    return undefined;
  }
  const bytes = NodeBuffer.Buffer.from(data, "base64");
  return bytes.toString("base64") === data ? bytes : undefined;
}

function continuesSequence(sequence: OmpChunkSequence, metadata: OmpChunkMetadata): boolean {
  return (
    sequence.chunkId === metadata.chunkId &&
    sequence.count === metadata.count &&
    sequence.byteLength === metadata.byteLength &&
    sequence.nextIndex === metadata.index
  );
}

function parseReassembledFrame(chunks: ReadonlyArray<Uint8Array>): OmpChunkDecodeResult {
  let parsed: unknown;
  try {
    const text = new TextDecoder("utf-8", { fatal: true }).decode(NodeBuffer.Buffer.concat(chunks));
    parsed = decodeJsonText(text);
  } catch {
    return { _tag: "Invalid", reason: "rpc chunk payload is not a JSON frame" };
  }
  return Predicate.isObject(parsed)
    ? { _tag: "Frame", frame: parsed as OmpRecord }
    : { _tag: "Invalid", reason: "rpc frame must be an object" };
}

function appendChunk(sequence: OmpChunkSequence, bytes: Uint8Array): OmpChunkDecodeResult {
  sequence.chunks.push(bytes);
  sequence.receivedBytes += bytes.byteLength;
  sequence.nextIndex++;
  if (sequence.receivedBytes > sequence.byteLength) {
    return { _tag: "Invalid", reason: "rpc chunk sequence exceeds declared length" };
  }
  if (sequence.nextIndex < sequence.count) return PENDING_CHUNK;
  if (sequence.receivedBytes !== sequence.byteLength) {
    return { _tag: "Invalid", reason: "rpc chunk sequence length mismatch" };
  }
  return parseReassembledFrame(sequence.chunks);
}

function validateChunk(
  record: OmpRecord,
): { readonly metadata: OmpChunkMetadata; readonly bytes: Uint8Array } | string {
  const metadata = readChunkMetadata(record);
  if (metadata === undefined) return "invalid rpc chunk metadata";
  const bytes = decodeChunkData(record["data"]);
  if (bytes === undefined) return "invalid rpc chunk data";
  if (bytes.byteLength > OMP_RPC_CHUNK_PAYLOAD_BYTES) {
    return "rpc chunk payload exceeds the transport limit";
  }
  return { metadata, bytes };
}

/** Mirrors upstream RpcFrameDecoder (modes/rpc/rpc-frame.ts) without throwing. */
export function makeOmpChunkDecoder(): OmpChunkDecoder {
  let pending: OmpChunkSequence | undefined;

  const pushChunk = (record: OmpRecord): OmpChunkDecodeResult => {
    const chunk = validateChunk(record);
    if (Predicate.isString(chunk)) return { _tag: "Invalid", reason: chunk };
    const { metadata, bytes } = chunk;
    if (pending === undefined && metadata.index !== 0) {
      return { _tag: "Invalid", reason: "rpc chunk sequence must start at index 0" };
    }
    const sequence = pending ?? { ...metadata, chunks: [], nextIndex: 0, receivedBytes: 0 };
    if (!continuesSequence(sequence, metadata)) {
      return { _tag: "Invalid", reason: "rpc chunk sequence mismatch" };
    }
    pending = sequence;
    return appendChunk(sequence, bytes);
  };

  return {
    push: (record) => {
      if (record["type"] !== "rpc_chunk") {
        return pending === undefined
          ? { _tag: "Frame", frame: record }
          : { _tag: "Invalid", reason: "rpc chunk sequence interrupted" };
      }
      const result = pushChunk(record);
      if (result._tag !== "Pending") pending = undefined;
      return result;
    },
  };
}

function withoutDetailsTrace(result: unknown): unknown {
  const details = ompRecordField(result, "details");
  if (!Predicate.isObject(details) || !Object.hasOwn(details, "trace")) return result;
  const { trace: _trace, ...kept } = details as OmpRecord;
  return { ...(result as OmpRecord), details: kept };
}

function withoutBashTrace(item: unknown): unknown {
  return ompRecordString(item, "toolName") === "bash" ? withoutDetailsTrace(item) : item;
}

function withoutBashTraces(items: unknown): unknown {
  if (!Array.isArray(items)) return items;
  const stripped = items.map(withoutBashTrace);
  return stripped.some((item, index) => item !== items[index]) ? stripped : items;
}

function replaceField(
  record: OmpRecord,
  key: string,
  replace: (value: unknown) => unknown,
): OmpRecord {
  const value = record[key];
  const replaced = replace(value);
  return replaced === value ? record : { ...record, [key]: replaced };
}

function stripToolResultTrace(record: OmpRecord, key: string): OmpRecord {
  return record["toolName"] === "bash" ? replaceField(record, key, withoutDetailsTrace) : record;
}

const BASH_TRACE_STRIPPERS: ReadonlyMap<unknown, (record: OmpRecord) => OmpRecord> = new Map([
  ["tool_execution_update", (record) => stripToolResultTrace(record, "partialResult")],
  ["tool_execution_end", (record) => stripToolResultTrace(record, "result")],
  ["message_start", (record) => replaceField(record, "message", withoutBashTrace)],
  ["message_end", (record) => replaceField(record, "message", withoutBashTrace)],
  ["turn_end", (record) => replaceField(record, "toolResults", withoutBashTraces)],
  ["agent_end", (record) => replaceField(record, "messages", withoutBashTraces)],
  [
    "response",
    (record) =>
      record["command"] === "get_messages"
        ? replaceField(record, "data", (data) =>
            Predicate.isObject(data)
              ? replaceField(data as OmpRecord, "messages", withoutBashTraces)
              : data,
          )
        : record,
  ],
]);

/**
 * Removes bash `details.trace` from tool frames, toolResult messages, turn_end, agent_end and
 * `get_messages` results. One trace was 272 KB of a 333 KB frame (TSR§0.5).
 */
export function stripOmpBashTrace(record: OmpRecord): OmpRecord {
  const strip = BASH_TRACE_STRIPPERS.get(record["type"]);
  return strip === undefined ? record : strip(record);
}

/**
 * `hasExited` is consulted before each signal. Once the original child is
 * gone its pid/pgid can be recycled by the OS, so escalating blindly could
 * deliver SIGKILL to an unrelated process.
 */
const signalOmpProcessGroup = Effect.fn("OmpRpc.signalOmpProcessGroup")(function* (
  kill: (signal: NodeJS.Signals) => boolean,
  hasExited: () => boolean,
  groupGone: () => boolean,
) {
  if (hasExited()) return;
  if (!kill("SIGTERM")) return;
  yield* Effect.sleep(TERMINATION_POLL_INTERVAL).pipe(
    Effect.repeat({ until: groupGone }),
    Effect.timeoutOption(TERMINATION_GRACE),
  );
  if (hasExited()) return;
  kill("SIGKILL");
});

const spawnOmpProcess = Effect.fnUntraced(function* (options: OmpRpcSpawnOptions) {
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const platform = yield* HostProcess.Platform;
  const spawnCommand = yield* resolveSpawnCommand(options.command, [...options.args], {
    env: options.env,
  }).pipe(Effect.mapError((cause) => new OmpRpcError({ operation: "spawn", cause })));
  const child = yield* spawner
    .spawn(
      ChildProcess.make(spawnCommand.command, spawnCommand.args, {
        ...(options.cwd === undefined ? {} : { cwd: options.cwd }),
        env: options.env,
        extendEnv: false,
        shell: spawnCommand.shell,
        detached: platform !== "win32",
        // stdin EOF is omp's clean shutdown (UP/modes/rpc/rpc-mode.ts:3193-3213), so let it close.
        stdin: { stream: "pipe" },
      }),
    )
    .pipe(Effect.mapError((cause) => new OmpRpcError({ operation: "spawn", cause })));
  return { spawner, platform, child };
});

/** Signals and liveness probes for one spawned omp process (Pi's process-group handling). */
function makeOmpProcessSignals(
  child: ChildProcessSpawner.ChildProcessHandle,
  platform: NodeJS.Platform,
  spawner: ChildProcessSpawner.ChildProcessSpawner["Service"],
) {
  const pid = Number(child.pid);
  let childExited = false;

  const killProcessGroup = (signal: NodeJS.Signals): boolean => {
    try {
      if (platform === "win32") process.kill(pid, signal);
      else signalProcessGroup(pid, signal);
      return true;
    } catch {
      return false;
    }
  };

  const hasExited = (): boolean => {
    if (childExited) return true;
    try {
      if (platform === "win32") process.kill(pid, 0);
      else signalProcessGroup(pid, 0);
      return false;
    } catch {
      return true;
    }
  };

  // EPERM means a group member is still alive, so only ESRCH ends the SIGTERM grace early.
  const processGroupGone = (): boolean => {
    try {
      signalProcessGroup(pid, 0);
      return false;
    } catch (cause) {
      return (cause as NodeJS.ErrnoException | undefined)?.code === "ESRCH";
    }
  };

  // Windows process.kill leaves extension children and pipes alive; taskkill /T reaps the tree.
  const terminateWindowsTree = Effect.gen(function* () {
    if (hasExited()) return;
    const taskkill = yield* spawner.spawn(
      ChildProcess.make(windowsSystem32Path(yield* HostProcess.Environment, "taskkill.exe"), [
        "/PID",
        String(pid),
        "/T",
        "/F",
      ]),
    );
    yield* taskkill.exitCode;
  }).pipe(Effect.scoped, Effect.ignore);

  const exitsWithin = (grace: Duration.Duration) =>
    child.exitCode.pipe(Effect.exit, Effect.timeoutOption(grace), Effect.map(Option.isSome));

  // stdin is already closed; signal only when omp ignores the EOF.
  const terminateAfterInputClosed = Effect.fnUntraced(function* (grace: Duration.Duration) {
    if (hasExited()) return;
    if (yield* exitsWithin(grace)) return;
    yield* platform === "win32"
      ? terminateWindowsTree
      : signalOmpProcessGroup(killProcessGroup, hasExited, processGroupGone);
  });

  return {
    markExited: () => {
      childExited = true;
    },
    terminateAfterInputClosed,
  };
}

interface PendingOmpRequest {
  readonly deferred: Deferred.Deferred<OmpRecord, OmpRpcError>;
}

interface WrittenOmpRequest extends PendingOmpRequest {
  readonly id: string;
  readonly command: string;
}

function responseCommand(response: OmpRecord): string {
  return ompRecordString(response, "command") ?? "unknown";
}

function settleOmpResponse(
  id: string,
  response: OmpRecord,
): Effect.Effect<OmpRpcResponse, OmpRpcError> {
  if (response["success"] === true) return Effect.succeed({ id, data: response["data"] });
  return Effect.fail(
    new OmpRpcError({
      operation: responseCommand(response),
      detail: summarizeOmpError(response["error"]),
    }),
  );
}

/** Upstream's client also refuses v2 unless the frame limits match its framer (rpc-client.ts:207-215). */
function supportsProtocolV2(ready: OmpReadyFrame): boolean {
  return (
    ready.supportedProtocolVersions?.includes(OMP_RPC_PROTOCOL_VERSION) === true &&
    ready.maxFrameBytes === OMP_MAX_FRAME_BYTES &&
    ready.maxReassembledFrameBytes === OMP_MAX_REASSEMBLED_FRAME_BYTES
  );
}

function checkReadyFrame(record: OmpRecord): Effect.Effect<OmpReadyFrame, OmpRpcHandshakeError> {
  const ready = Option.getOrUndefined(decodeReadyFrame(record));
  if (ready !== undefined && supportsProtocolV2(ready)) return Effect.succeed(ready);
  const versions = ready?.supportedProtocolVersions;
  return Effect.fail(
    new OmpRpcHandshakeError({
      reason: "protocol_unsupported",
      ...(versions === undefined ? {} : { supportedProtocolVersions: versions }),
    }),
  );
}

function handshakeTimeoutError(readySeen: boolean): OmpRpcTimeoutError | OmpRpcHandshakeError {
  return readySeen
    ? new OmpRpcTimeoutError({ operation: "negotiate_protocol", timeoutMs: HANDSHAKE_TIMEOUT_MS })
    : new OmpRpcHandshakeError({ reason: "no_ready_frame" });
}

function acceptsProtocolV2(response: OmpRecord): boolean {
  const version = ompRecordField(response["data"], "protocolVersion");
  return response["success"] === true && version === OMP_RPC_PROTOCOL_VERSION;
}

/**
 * Spawns omp, writes `negotiate_protocol {protocolVersion: 2}` at once (omp holds commands until
 * `ready`), then waits for `ready` with 2 in `supportedProtocolVersions` and for the
 * acknowledgement, under one deadline. A dialog omp raises while starting opens the connection
 * before the acknowledgement; a later refusal fails the transport.
 */
export const makeOmpRpcConnection = Effect.fnUntraced(function* (
  options: OmpRpcSpawnOptions,
): Effect.fn.Return<
  OmpRpcConnection,
  OmpRpcError | OmpRpcTimeoutError | OmpRpcHandshakeError,
  OmpRpcEnv
> {
  const scope = yield* Effect.scope;
  // The readers outlive stdin EOF so omp can finish writing and exit; terminate closes them last.
  const ioScope = yield* Scope.make();
  const { spawner, platform, child } = yield* spawnOmpProcess(options).pipe(
    Effect.onError(() => Scope.close(ioScope, Exit.void)),
  );
  const signals = makeOmpProcessSignals(child, platform, spawner);

  const pendingRequests = new Map<string, PendingOmpRequest>();
  const events = yield* Queue.unbounded<OmpRecord, OmpRpcError>();
  const outgoing = yield* Queue.unbounded<Uint8Array, Cause.Done>();
  const transportDown = yield* Deferred.make<never, OmpRpcError>();
  const exitDeferred = yield* Deferred.make<number, OmpRpcError>();
  const readyRecord = yield* Deferred.make<OmpRecord>();
  const negotiation = yield* Deferred.make<void>();
  const startupDialog = yield* Deferred.make<void>();
  const chunks = makeOmpChunkDecoder();
  let nextRequestId = 0;
  let diagnosingStdoutClose = false;

  const failTransport = Effect.fn("OmpRpc.failTransport")(function* (error: OmpRpcError) {
    const claimed = yield* Deferred.fail(transportDown, error);
    if (!claimed) return;
    for (const [id, pending] of pendingRequests) {
      pendingRequests.delete(id);
      yield* Deferred.fail(pending.deferred, error);
    }
    // Ending (not failing) the queue flushes queued writes before stdin closes.
    yield* Queue.end(outgoing);
    yield* Queue.fail(events, error);
  });

  const terminateWithin = (grace: Duration.Duration) =>
    failTransport(
      new OmpRpcError({ operation: "terminate", detail: "omp process was stopped" }),
    ).pipe(
      Effect.andThen(signals.terminateAfterInputClosed(grace)),
      Effect.ensuring(Scope.close(ioScope, Exit.void)),
      Effect.ignore,
      Effect.uninterruptible,
    );
  const terminate = terminateWithin(INPUT_CLOSE_GRACE);

  // Register teardown immediately after spawn so interrupted setup cannot leak a detached process.
  yield* Scope.addFinalizer(scope, terminate);

  const routeResponse =
    /* Protocol records include streaming deltas; avoid a span per record. */
    Effect.fnUntraced(function* (response: OmpRecord) {
      const id = response["id"];
      if (!Predicate.isString(id)) {
        return yield* Effect.logWarning("Dropping omp response without an id.", {
          command: responseCommand(response),
        });
      }
      if (!REQUEST_ID_PATTERN.test(id)) {
        yield* Queue.offer(events, response);
        return;
      }
      const pending = pendingRequests.get(id);
      if (pending === undefined) {
        return yield* Effect.logDebug("Dropping late or duplicate omp response.", {
          command: responseCommand(response),
        });
      }
      pendingRequests.delete(id);
      yield* Deferred.succeed(pending.deferred, response);
    });

  const routeFrame = Effect.fnUntraced(function* (frame: OmpRecord) {
    if (frame["type"] === "ready") {
      yield* Deferred.succeed(readyRecord, frame);
      return;
    }
    const stripped = stripOmpBashTrace(frame);
    if (stripped["type"] === "response") return yield* routeResponse(stripped);
    yield* Queue.offer(events, stripped);
    if (isOmpDialogRequest(stripped)) yield* Deferred.succeed(startupDialog, undefined);
  });

  const routeLine = Effect.fnUntraced(function* (line: string) {
    const record = parseOmpRecord(line);
    if (record === undefined) {
      return yield* Effect.logDebug("Dropping non-JSON omp stdout line.", {
        lineLength: line.length,
      });
    }
    const decoded = chunks.push(record);
    if (decoded._tag === "Frame") return yield* routeFrame(decoded.frame);
    // A lost frame can be a response or a prompt_result, so the transport cannot carry on.
    if (decoded._tag === "Invalid") {
      return yield* failTransport(new OmpRpcError({ operation: "read", detail: decoded.reason }));
    }
  });

  const routeLines = Effect.fnUntraced(function* (framed: OmpJsonlFrames) {
    yield* logDroppedRecords(framed.droppedLengths);
    for (const line of framed.lines) yield* routeLine(line);
  });

  // Watch exit before reading stdout so immediate crashes populate exitDeferred for diagnosis.
  yield* child.exitCode.pipe(
    Effect.matchEffect({
      onFailure: (cause) =>
        Deferred.fail(exitDeferred, new OmpRpcError({ operation: "exit", cause })),
      onSuccess: (code) =>
        Effect.suspend(() => {
          signals.markExited();
          return Deferred.succeed(exitDeferred, Number(code));
        }),
    }),
    // Outside ioScope: `terminate` closes ioScope and must not strand `exited`.
    Effect.forkIn(scope),
  );

  const diagnoseStdoutClosure = Effect.fnUntraced(function* () {
    // Claim stdout-close diagnosis before waiting so broken stdin cannot replace the exit error.
    diagnosingStdoutClose = true;
    const polled = yield* Deferred.poll(exitDeferred);
    const maybeExitCode = Option.isSome(polled)
      ? yield* polled.value.pipe(
          Effect.asSome,
          Effect.orElseSucceed(() => Option.none<number>()),
        )
      : yield* Deferred.await(exitDeferred).pipe(
          Effect.timeoutOption(Duration.millis(250)),
          // Stdout-close waits need wall time; TestClock would strand a restart without an exit.
          Effect.provideService(Clock.Clock, Clock.Clock.defaultValue()),
          Effect.orElseSucceed(() => Option.none<number>()),
        );
    return yield* failTransport(
      new OmpRpcError({
        operation: "read",
        detail: describeStdoutClosure(Option.getOrUndefined(maybeExitCode)),
      }),
    );
  });

  yield* Effect.gen(function* () {
    const frame = makeOmpJsonlFramer(OMP_MAX_FRAME_BYTES);
    yield* child.stdout.pipe(
      Stream.decodeText(),
      Stream.runForEach((chunk) => routeLines(frame(chunk))),
    );
    yield* routeLines(frame("\n"));
  }).pipe(
    Effect.matchCauseEffect({
      onFailure: (cause) => failTransport(new OmpRpcError({ operation: "read", cause })),
      onSuccess: diagnoseStdoutClosure,
    }),
    Effect.forkIn(ioScope),
  );

  yield* child.stderr.pipe(
    Stream.decodeText(),
    Stream.runForEach((chunk) =>
      chunk.trim().length === 0
        ? Effect.void
        : // Log stderr length only; unbounded text may contain credentials or prompts.
          Effect.logDebug("omp stderr", { stderrLength: chunk.length }),
    ),
    Effect.ignore,
    Effect.forkIn(ioScope),
  );

  // Start the reader first so stdout-close diagnosis can win over broken-pipe stdin.
  yield* Stream.fromQueue(outgoing).pipe(
    Stream.run(child.stdin),
    Effect.catchCause((cause) =>
      diagnosingStdoutClose
        ? Effect.void
        : failTransport(new OmpRpcError({ operation: "write", cause })),
    ),
    Effect.forkIn(ioScope),
  );

  const writeFrame =
    /* Protocol writes include host tool updates; avoid a span per record. */
    Effect.fnUntraced(function* (frame: OmpOutboundFrame): Effect.fn.Return<void, OmpRpcError> {
      const accepted = yield* Queue.offer(outgoing, encodeOutboundLine(frame));
      if (!accepted) return yield* Deferred.await(transportDown);
    });

  const send = (frame: OmpOutboundFrame): Effect.Effect<void, OmpRpcError> =>
    REQUEST_ID_PATTERN.test(frame.id)
      ? Effect.fail(
          new OmpRpcError({ operation: frame.type, detail: `id ${frame.id} is reserved` }),
        )
      : writeFrame(frame);

  const writeRequest = Effect.fnUntraced(function* (command: OmpCommand) {
    const id = `t3-${nextRequestId++}`;
    const deferred = yield* Deferred.make<OmpRecord, OmpRpcError>();
    pendingRequests.set(id, { deferred });
    yield* writeFrame({ ...command, id }).pipe(
      Effect.tapError(() => Effect.sync(() => pendingRequests.delete(id))),
    );
    return { id, command: command.type, deferred } satisfies WrittenOmpRequest;
  });

  const awaitResponse = (written: WrittenOmpRequest, timeoutMs: number) =>
    Effect.raceFirst(Deferred.await(written.deferred), Deferred.await(transportDown)).pipe(
      Effect.timeoutOrElse({
        duration: Duration.millis(timeoutMs),
        orElse: () =>
          Effect.fail(new OmpRpcTimeoutError({ operation: written.command, timeoutMs })),
      }),
      Effect.ensuring(Effect.sync(() => pendingRequests.delete(written.id))),
    );

  const request = Effect.fn("OmpRpc.request")(function* (
    command: OmpCommand,
    requestOptions?: OmpRequestOptions,
  ): Effect.fn.Return<OmpRpcResponse, OmpRpcError | OmpRpcTimeoutError> {
    const written = yield* writeRequest(command);
    const timeoutMs = requestOptions?.timeoutMs ?? DEFAULT_REQUEST_TIMEOUT_MS;
    return yield* settleOmpResponse(written.id, yield* awaitResponse(written, timeoutMs));
  });

  const awaitReadyFrame = Effect.raceFirst(
    Deferred.await(readyRecord),
    Deferred.await(transportDown),
  ).pipe(Effect.flatMap(checkReadyFrame));

  const awaitNegotiation = (written: WrittenOmpRequest) =>
    Effect.raceFirst(Deferred.await(written.deferred), Deferred.await(transportDown)).pipe(
      Effect.flatMap((response) =>
        acceptsProtocolV2(response)
          ? Deferred.succeed(negotiation, undefined)
          : Effect.fail(
              new OmpRpcHandshakeError({ reason: "negotiation_refused", cause: response["error"] }),
            ),
      ),
    );

  // Until session_start ends, omp handles only dialog replies and set_ask_dialog and holds every
  // other command, this one included (UP/modes/rpc/rpc-mode.ts:1927-1946).
  const negotiateUnlessStartupDialog = (written: WrittenOmpRequest) =>
    Effect.raceFirst(
      awaitNegotiation(written).pipe(Effect.as(true)),
      Deferred.await(startupDialog).pipe(Effect.as(false)),
    );

  const finishNegotiation = (written: WrittenOmpRequest) =>
    awaitNegotiation(written).pipe(
      Effect.catch((error) =>
        failTransport(
          error._tag === "OmpRpcError"
            ? error
            : new OmpRpcError({
                operation: "negotiate_protocol",
                detail: error.message,
                cause: error,
              }),
        ),
      ),
    );

  const handshake = Effect.gen(function* () {
    const negotiationRequest = yield* writeRequest({
      type: "negotiate_protocol",
      protocolVersion: OMP_RPC_PROTOCOL_VERSION,
    });
    const ready = yield* awaitReadyFrame;
    const negotiated = yield* negotiateUnlessStartupDialog(negotiationRequest);
    return { ready, negotiationRequest, negotiated };
  }).pipe(
    Effect.timeoutOrElse({
      duration: Duration.millis(HANDSHAKE_TIMEOUT_MS),
      orElse: () =>
        Deferred.isDone(readyRecord).pipe(
          Effect.flatMap((readySeen) => Effect.fail(handshakeTimeoutError(readySeen))),
        ),
    }),
    // Reap a process T3 cannot drive so a retry cannot inherit it or its session lease.
    Effect.onError(() => terminate),
  );

  const { ready, negotiationRequest, negotiated } = yield* handshake;
  if (!negotiated) yield* finishNegotiation(negotiationRequest).pipe(Effect.forkIn(ioScope));

  return {
    ready,
    negotiated: Effect.raceFirst(Deferred.await(negotiation), Deferred.await(transportDown)),
    isNegotiated: Deferred.isDone(negotiation),
    request,
    send,
    events,
    exited: Deferred.await(exitDeferred),
    terminate,
    terminateWithin,
  } satisfies OmpRpcConnection;
});
