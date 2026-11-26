/**
 * Scripted omp fakes for this package's tests. `makeFakeOmpRpcConnection` needs no process;
 * `makeFakeOmpProcess` is a spawner whose processes speak omp's JSONL wire, for code that spawns
 * omp itself. Both record what T3 writes and answer from the same `FakeOmpScript`.
 */
import type * as Cause from "effect/Cause";
import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Predicate from "effect/Predicate";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Sink from "effect/Sink";
import * as Stream from "effect/Stream";
import * as ChildProcess from "effect/process/ChildProcess";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";

import * as NodeBuffer from "node:buffer";

import {
  OmpRpcError,
  OmpRpcTimeoutError,
  stripOmpBashTrace,
  type OmpRpcConnection,
  type OmpRpcResponse,
} from "./rpc.ts";
import {
  OMP_MAX_FRAME_BYTES,
  OMP_MAX_REASSEMBLED_FRAME_BYTES,
  OMP_RPC_CHUNK_PAYLOAD_BYTES,
  type OmpCommand,
  type OmpCommandFrame,
  type OmpCommandType,
  type OmpOutboundFrame,
  type OmpReadyFrame,
  type OmpRecord,
} from "./wire.ts";

export type FakeOmpReply =
  | {
      readonly _tag: "Success";
      readonly data?: unknown;
      /** Written before the response, e.g. a local command's `command_output`. */
      readonly before?: ReadonlyArray<OmpRecord>;
      /** Written after the response, e.g. `prompt_result`. */
      readonly after?: ReadonlyArray<OmpRecord>;
    }
  | { readonly _tag: "Failure"; readonly error: string; readonly code?: string }
  /** No response until the test calls `respond` or `respondFailure`. */
  | { readonly _tag: "Unanswered" };

export const fakeOmpSuccess = (
  data?: unknown,
  frames?: {
    readonly before?: ReadonlyArray<OmpRecord>;
    readonly after?: ReadonlyArray<OmpRecord>;
  },
): FakeOmpReply => ({ _tag: "Success", data, ...frames });

export const fakeOmpFailure = (error: string, code?: string): FakeOmpReply =>
  code === undefined ? { _tag: "Failure", error } : { _tag: "Failure", error, code };

export const FAKE_OMP_UNANSWERED: FakeOmpReply = { _tag: "Unanswered" };

export const FAKE_OMP_SESSION_FILE = "/fake/omp-sessions/--workspace--/2026-11-25_fake.jsonl";

/** As omp 19.1.1 writes it (TL/01:5). */
export const FAKE_OMP_READY_FRAME: OmpReadyFrame = {
  type: "ready",
  protocolVersion: 1,
  supportedProtocolVersions: [1, 2],
  maxFrameBytes: OMP_MAX_FRAME_BYTES,
  maxReassembledFrameBytes: OMP_MAX_REASSEMBLED_FRAME_BYTES,
};

/** An idle session with no model selected and nothing queued. */
export const FAKE_OMP_IDLE_STATE = {
  sessionId: "00000000-0000-4000-8000-000000000001",
  sessionFile: FAKE_OMP_SESSION_FILE,
  isStreaming: false,
  isCompacting: false,
  interruptMode: "wait",
  isSettled: true,
  hasPendingAsyncWork: false,
  queuedMessages: { steering: [], followUp: [], liveSteered: 0 },
} as const;

const DEFAULT_REPLY_DATA: Partial<Record<OmpCommandType, unknown>> = {
  get_state: FAKE_OMP_IDLE_STATE,
  get_available_thinking_levels: { levels: ["off", "low", "medium", "high"] },
  get_model_picker: { rows: [], recent: 0, total: 0 },
  get_available_commands: { commands: [] },
  get_branch_messages: { messages: [] },
  get_entries: { entries: [], leafId: null },
  get_messages: { messages: [] },
};

/** What the fake answers when the script does not: success, with idle data for reads. */
function defaultFakeOmpReply(command: OmpCommandFrame): FakeOmpReply {
  switch (command.type) {
    case "negotiate_protocol":
      return FAKE_OMP_READY_FRAME.supportedProtocolVersions?.includes(command.protocolVersion)
        ? fakeOmpSuccess({ protocolVersion: command.protocolVersion })
        : fakeOmpFailure(`Unsupported RPC protocol version: ${command.protocolVersion}`);
    case "set_host_tools":
      return fakeOmpSuccess({ toolNames: command.tools.map((tool) => tool.name) });
    case "set_ask_dialog":
      return fakeOmpSuccess({ enabled: command.enabled });
    default:
      return fakeOmpSuccess(DEFAULT_REPLY_DATA[command.type]);
  }
}

export interface FakeOmpInvocation {
  readonly command: string;
  readonly args: ReadonlyArray<string>;
  readonly cwd: string | undefined;
  readonly env: NonNullable<ChildProcess.CommandOptions["env"]>;
  /** Text written to stdin so far that was not an RPC frame (print mode's prompt). */
  readonly stdin: string;
}

export interface FakeOmpCliResult {
  readonly stdout: string;
  readonly stderr?: string;
  readonly exitCode: number;
}

export interface FakeOmpScript {
  /** `null` never writes `ready` (process fake only). Defaults to `FAKE_OMP_READY_FRAME`. */
  readonly ready?: OmpReadyFrame | null;
  /** Inbound frames written right after `ready`; omp writes `available_commands_update` first. */
  readonly startup?: ReadonlyArray<OmpRecord>;
  /** The reply to each command T3 writes; undefined uses `defaultFakeOmpReply`. */
  readonly reply?: (command: OmpCommandFrame) => FakeOmpReply | undefined;
  /** Runs that are not `--mode rpc-ui` (`--version`, `-p`); undefined exits 1 with no output. */
  readonly cli?: (invocation: FakeOmpInvocation) => FakeOmpCliResult | undefined;
}

export interface FakeOmpControl {
  /** Every command T3 wrote, in order, with its id. */
  readonly commands: () => ReadonlyArray<OmpCommandFrame>;
  /** Every other frame T3 wrote: dialog replies and host tool updates and results. */
  readonly frames: () => ReadonlyArray<OmpOutboundFrame>;
  /** The oldest command of `type` not taken yet; waits until T3 writes one. */
  readonly takeCommand: <Type extends OmpCommandType>(
    type: Type,
  ) => Effect.Effect<Extract<OmpCommandFrame, { readonly type: Type }>>;
  /** The oldest non-command frame of `type` not taken yet; waits until T3 writes one. */
  readonly takeFrame: <Type extends OmpNonCommandFrameType>(
    type: Type,
  ) => Effect.Effect<Extract<OmpOutboundFrame, { readonly type: Type }>>;
  /** Delivers one inbound frame. */
  readonly emit: (frame: OmpRecord) => Effect.Effect<void>;
  /** Delivers `response {id, command, success: true, data}` for a command T3 wrote. */
  readonly respond: (command: OmpCommandFrame, data?: unknown) => Effect.Effect<void>;
  readonly respondFailure: (
    command: OmpCommandFrame,
    error: string,
    code?: string,
  ) => Effect.Effect<void>;
  /** The process exits with `code`: stdout ends and the transport goes down. */
  readonly exit: (code: number) => Effect.Effect<void>;
}

export interface FakeOmpRpcConnection extends FakeOmpControl {
  readonly connection: OmpRpcConnection;
  readonly terminated: () => boolean;
}

export interface FakeOmpProcess extends FakeOmpControl {
  readonly spawner: ChildProcessSpawner.ChildProcessSpawner["Service"];
  /** Every spawn, in order. */
  readonly spawns: () => ReadonlyArray<FakeOmpInvocation>;
  /** Writes raw stdout text to the newest RPC process (framing and non-JSON tests). */
  readonly emitRaw: (text: string) => Effect.Effect<void>;
}

const FAKE_OMP_PID = 999_999_999;
const DEFAULT_REQUEST_TIMEOUT_MS = 30_000;
const REQUEST_ID_PATTERN = /^t3-\d+$/;
const encodeJson = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));
const decodeJson = Schema.decodeUnknownOption(Schema.fromJsonString(Schema.Unknown));
type OmpNonCommandFrame = Exclude<OmpOutboundFrame, OmpCommandFrame>;
type OmpNonCommandFrameType = OmpNonCommandFrame["type"];

const NON_COMMAND_FRAME_TYPES: ReadonlySet<string> = new Set([
  "extension_ui_response",
  "host_tool_update",
  "host_tool_result",
]);

function isOmpCommandFrame(frame: OmpOutboundFrame): frame is OmpCommandFrame {
  return !NON_COMMAND_FRAME_TYPES.has(frame.type);
}

function isFrameOfType<Type extends OmpNonCommandFrameType>(
  frame: OmpOutboundFrame,
  type: Type,
): frame is Extract<OmpOutboundFrame, { readonly type: Type }> {
  return frame.type === type;
}

function isCommandOfType<Type extends OmpCommandType>(
  command: OmpCommandFrame,
  type: Type,
): command is Extract<OmpCommandFrame, { readonly type: Type }> {
  return command.type === type;
}

function responseRecord(command: OmpCommandFrame, reply: FakeOmpReply): OmpRecord | undefined {
  if (reply._tag === "Unanswered") return undefined;
  const base = { type: "response", id: command.id, command: command.type };
  if (reply._tag === "Failure") {
    return {
      ...base,
      success: false,
      error: reply.error,
      ...(reply.code ? { code: reply.code } : {}),
    };
  }
  return reply.data === undefined
    ? { ...base, success: true }
    : { ...base, success: true, data: reply.data };
}

/** The reply as inbound records, in the order omp writes them. */
function replyRecords(command: OmpCommandFrame, reply: FakeOmpReply): ReadonlyArray<OmpRecord> {
  const response = responseRecord(command, reply);
  if (response === undefined) return [];
  if (reply._tag !== "Success") return [response];
  return [...(reply.before ?? []), response, ...(reply.after ?? [])];
}

/** Command and frame log shared by both fakes. */
function makeOutboundLog(script: FakeOmpScript) {
  const commands: Array<OmpCommandFrame> = [];
  const frames: Array<OmpOutboundFrame> = [];
  const taken = new Set<number>();
  const takenFrames = new Set<number>();
  let arrival = Deferred.makeUnsafe<void>();

  const signalArrival = Effect.suspend(() => {
    const arrived = arrival;
    arrival = Deferred.makeUnsafe<void>();
    return Deferred.succeed(arrived, undefined);
  });

  const record = (frame: OmpOutboundFrame): Effect.Effect<FakeOmpReply | undefined> =>
    Effect.suspend(() => {
      if (!isOmpCommandFrame(frame)) {
        frames.push(frame);
        return signalArrival.pipe(Effect.as(undefined));
      }
      commands.push(frame);
      return signalArrival.pipe(Effect.as(script.reply?.(frame) ?? defaultFakeOmpReply(frame)));
    });

  const takeCommand = <Type extends OmpCommandType>(
    type: Type,
  ): Effect.Effect<Extract<OmpCommandFrame, { readonly type: Type }>> =>
    Effect.suspend(() => {
      for (const [index, command] of commands.entries()) {
        if (taken.has(index) || !isCommandOfType(command, type)) continue;
        taken.add(index);
        return Effect.succeed(command);
      }
      return Deferred.await(arrival).pipe(Effect.andThen(takeCommand(type)));
    });

  const takeFrame = <Type extends OmpNonCommandFrameType>(
    type: Type,
  ): Effect.Effect<Extract<OmpOutboundFrame, { readonly type: Type }>> =>
    Effect.suspend(() => {
      for (const [index, frame] of frames.entries()) {
        if (takenFrames.has(index) || !isFrameOfType(frame, type)) continue;
        takenFrames.add(index);
        return Effect.succeed(frame);
      }
      return Deferred.await(arrival).pipe(Effect.andThen(takeFrame(type)));
    });

  return {
    record,
    takeCommand,
    takeFrame,
    commands: (): ReadonlyArray<OmpCommandFrame> => [...commands],
    frames: (): ReadonlyArray<OmpOutboundFrame> => [...frames],
  };
}

/**
 * The JSONL lines omp writes for a frame over 1 MiB under protocol v2, as upstream
 * `encodeChunkedRpcFrames` (modes/rpc/rpc-frame.ts) splits it: 256 KiB of UTF-8 per chunk.
 */
export function encodeFakeOmpChunks(frame: unknown, chunkId = "rpc-1"): string {
  const bytes = NodeBuffer.Buffer.from(encodeJson(frame), "utf8");
  const count = Math.ceil(bytes.byteLength / OMP_RPC_CHUNK_PAYLOAD_BYTES);
  return Array.from({ length: count }, (_, index) => {
    const start = index * OMP_RPC_CHUNK_PAYLOAD_BYTES;
    const data = bytes.subarray(start, start + OMP_RPC_CHUNK_PAYLOAD_BYTES).toString("base64");
    const chunk = { type: "rpc_chunk", chunkId, index, count, byteLength: bytes.byteLength, data };
    return `${encodeJson(chunk)}\n`;
  }).join("");
}

function isResponse(record: OmpRecord): boolean {
  return record["type"] === "response";
}

function settleRequest(
  request: Deferred.Deferred<OmpRpcResponse, OmpRpcError>,
  id: string,
  record: OmpRecord,
): Effect.Effect<void> {
  const settled =
    record["success"] === true
      ? Deferred.succeed(request, { id, data: record["data"] })
      : Deferred.fail(
          request,
          new OmpRpcError({
            operation: String(record["command"]),
            detail: String(record["error"]),
          }),
        );
  return Effect.asVoid(settled);
}

export const makeFakeOmpRpcConnection = Effect.fnUntraced(function* (script: FakeOmpScript = {}) {
  const log = makeOutboundLog(script);
  const events = yield* Queue.unbounded<OmpRecord, OmpRpcError>();
  const exited = yield* Deferred.make<number, OmpRpcError>();
  const pending = new Map<string, Deferred.Deferred<OmpRpcResponse, OmpRpcError>>();
  let nextRequestId = 0;
  let terminated = false;
  let down: OmpRpcError | undefined;

  const queueEvent = (record: OmpRecord) =>
    Queue.offer(events, stripOmpBashTrace(record)).pipe(Effect.asVoid);

  // As in rpc.ts: a `request` id's first response settles it, later ones and id-less ones are
  // dropped, and responses to caller-chosen ids go to `events`.
  const deliver = (record: OmpRecord): Effect.Effect<void> => {
    if (!isResponse(record)) return queueEvent(record);
    const id = record["id"];
    if (!Predicate.isString(id)) return Effect.void;
    if (!REQUEST_ID_PATTERN.test(id)) return queueEvent(record);
    const request = pending.get(id);
    if (request === undefined) return Effect.void;
    pending.delete(id);
    return settleRequest(request, id, stripOmpBashTrace(record));
  };

  const answer = (command: OmpCommandFrame, reply: FakeOmpReply | undefined) =>
    Effect.forEach(reply === undefined ? [] : replyRecords(command, reply), deliver, {
      discard: true,
    });

  const goDown = (error: OmpRpcError, code: number) =>
    Effect.gen(function* () {
      if (down !== undefined) return;
      down = error;
      for (const [id, request] of pending) {
        pending.delete(id);
        yield* Deferred.fail(request, error);
      }
      yield* Queue.fail(events, error);
      yield* Deferred.succeed(exited, code);
    });

  const request = (command: OmpCommand, options?: { readonly timeoutMs?: number }) =>
    Effect.gen(function* () {
      if (down !== undefined) return yield* down;
      const frame: OmpCommandFrame = { ...command, id: `t3-${nextRequestId++}` };
      const response = yield* Deferred.make<OmpRpcResponse, OmpRpcError>();
      pending.set(frame.id, response);
      yield* answer(frame, yield* log.record(frame));
      const timeoutMs = options?.timeoutMs ?? DEFAULT_REQUEST_TIMEOUT_MS;
      return yield* Deferred.await(response).pipe(
        Effect.timeoutOrElse({
          duration: Duration.millis(timeoutMs),
          orElse: () => Effect.fail(new OmpRpcTimeoutError({ operation: command.type, timeoutMs })),
        }),
      );
    });

  const send = (frame: OmpOutboundFrame): Effect.Effect<void, OmpRpcError> => {
    if (down !== undefined) return Effect.fail(down);
    if (REQUEST_ID_PATTERN.test(frame.id)) {
      return Effect.fail(
        new OmpRpcError({ operation: frame.type, detail: `id ${frame.id} is reserved` }),
      );
    }
    return log
      .record(frame)
      .pipe(
        Effect.flatMap((reply) => (isOmpCommandFrame(frame) ? answer(frame, reply) : Effect.void)),
      );
  };

  const stop = Effect.suspend(() => {
    terminated = true;
    return goDown(new OmpRpcError({ operation: "terminate", detail: "omp was stopped" }), 0);
  });

  const connection: OmpRpcConnection = {
    ready: script.ready ?? FAKE_OMP_READY_FRAME,
    negotiated: Effect.void,
    isNegotiated: Effect.succeed(true),
    request,
    send,
    events,
    exited: Deferred.await(exited),
    terminate: stop,
    terminateWithin: () => stop,
  };

  yield* Effect.forEach(script.startup ?? [], deliver, { discard: true });

  return {
    connection,
    terminated: () => terminated,
    commands: log.commands,
    frames: log.frames,
    takeCommand: log.takeCommand,
    takeFrame: log.takeFrame,
    emit: deliver,
    respond: (command: OmpCommandFrame, data?: unknown) => answer(command, fakeOmpSuccess(data)),
    respondFailure: (command: OmpCommandFrame, error: string, code?: string) =>
      answer(command, fakeOmpFailure(error, code)),
    exit: (code: number) =>
      goDown(new OmpRpcError({ operation: "read", detail: `omp exited with code ${code}` }), code),
  } satisfies FakeOmpRpcConnection;
});

interface FakeOmpProcessState {
  readonly invocation: Omit<FakeOmpInvocation, "stdin">;
  readonly stdout: Queue.Queue<Uint8Array, Cause.Done>;
  readonly exitCode: Deferred.Deferred<ChildProcessSpawner.ExitCode>;
  readonly decoder: TextDecoder;
  stdin: string;
  pendingLine: string;
}

function isRpcInvocation(args: ReadonlyArray<string>): boolean {
  const mode = args.indexOf("--mode");
  return mode !== -1 && args[mode + 1]?.startsWith("rpc") === true;
}

function snapshotInvocation(state: FakeOmpProcessState): FakeOmpInvocation {
  return { ...state.invocation, stdin: state.stdin };
}

export const makeFakeOmpProcess = (script: FakeOmpScript = {}): Effect.Effect<FakeOmpProcess> =>
  Effect.sync(() => {
    const log = makeOutboundLog(script);
    const processes: Array<FakeOmpProcessState> = [];
    const rpcProcesses: Array<FakeOmpProcessState> = [];
    const receivedBy = new WeakMap<OmpCommandFrame, FakeOmpProcessState>();
    const encoder = new TextEncoder();

    const writeText = (state: FakeOmpProcessState, text: string) =>
      Queue.offer(state.stdout, encoder.encode(text)).pipe(Effect.asVoid);
    const writeRecord = (state: FakeOmpProcessState, record: OmpRecord) =>
      writeText(state, `${encodeJson(record)}\n`);
    const writeRecords = (state: FakeOmpProcessState, records: ReadonlyArray<OmpRecord>) =>
      Effect.forEach(records, (record) => writeRecord(state, record), { discard: true });

    const finish = (state: FakeOmpProcessState, code: number) =>
      Queue.end(state.stdout).pipe(
        Effect.andThen(Deferred.succeed(state.exitCode, ChildProcessSpawner.ExitCode(code))),
        Effect.asVoid,
      );

    const newestRpcProcess = Effect.suspend(() => {
      const state = rpcProcesses.at(-1);
      return state === undefined
        ? Effect.die("no fake omp RPC process was spawned")
        : Effect.succeed(state);
    });

    const handleLine = (state: FakeOmpProcessState, line: string) =>
      Effect.gen(function* () {
        const parsed = decodeJson(line);
        if (Option.isNone(parsed) || !Predicate.isObject(parsed.value)) {
          yield* writeRecord(state, {
            type: "response",
            command: "parse",
            success: false,
            error: "Failed to parse command",
          });
          return;
        }
        const frame = parsed.value as OmpOutboundFrame;
        const reply = yield* log.record(frame);
        if (!isOmpCommandFrame(frame) || reply === undefined) return;
        receivedBy.set(frame, state);
        yield* writeRecords(state, replyRecords(frame, reply));
      });

    const handleStdin = (state: FakeOmpProcessState, rpc: boolean) => (chunk: Uint8Array) =>
      Effect.gen(function* () {
        const text = state.decoder.decode(chunk, { stream: true });
        if (!rpc) {
          state.stdin += text;
          return;
        }
        const lines = `${state.pendingLine}${text}`.split("\n");
        state.pendingLine = lines.pop() ?? "";
        for (const line of lines) {
          if (line.trim().length > 0) yield* handleLine(state, line);
        }
      });

    const startCli = (state: FakeOmpProcessState) =>
      Effect.gen(function* () {
        const result = script.cli?.(snapshotInvocation(state)) ?? { stdout: "", exitCode: 1 };
        yield* writeText(state, result.stdout);
        yield* finish(state, result.exitCode);
        return result.stderr ?? "";
      });

    const startRpc = (state: FakeOmpProcessState) =>
      Effect.gen(function* () {
        rpcProcesses.push(state);
        const ready = script.ready === undefined ? FAKE_OMP_READY_FRAME : script.ready;
        if (ready !== null) yield* writeRecords(state, [ready, ...(script.startup ?? [])]);
        return "";
      });

    const spawner = ChildProcessSpawner.make((command) =>
      Effect.gen(function* () {
        if (!ChildProcess.isStandardCommand(command)) {
          return yield* Effect.die("the fake omp spawner runs standard commands only");
        }
        const args = command.args.map(String);
        const state: FakeOmpProcessState = {
          invocation: {
            command: command.command,
            args,
            cwd: command.options.cwd,
            env: command.options.env ?? {},
          },
          stdout: yield* Queue.unbounded<Uint8Array, Cause.Done>(),
          exitCode: yield* Deferred.make<ChildProcessSpawner.ExitCode>(),
          decoder: new TextDecoder(),
          stdin: "",
          pendingLine: "",
        };
        processes.push(state);
        const rpc = isRpcInvocation(args);
        const stderr = rpc ? yield* startRpc(state) : yield* startCli(state);
        const stderrStream =
          stderr.length === 0 ? Stream.empty : Stream.make(encoder.encode(stderr));
        return ChildProcessSpawner.makeHandle({
          pid: ChildProcessSpawner.ProcessId(FAKE_OMP_PID),
          exitCode: Deferred.await(state.exitCode),
          isRunning: Deferred.isDone(state.exitCode).pipe(Effect.map((done) => !done)),
          kill: () => finish(state, 143),
          unref: Effect.succeed(Effect.void),
          // omp exits 0 once its stdin closes (TL/10), which is how the transport stops it.
          stdin: Sink.forEach(handleStdin(state, rpc)).pipe(Sink.ensuring(finish(state, 0))),
          stdout: Stream.fromQueue(state.stdout),
          stderr: stderrStream,
          all: rpc ? Stream.empty : stderrStream,
          getInputFd: () => Sink.drain,
          getOutputFd: () => Stream.empty,
        });
      }),
    );

    const writeReply = (command: OmpCommandFrame, reply: FakeOmpReply) =>
      Effect.gen(function* () {
        const state = receivedBy.get(command) ?? (yield* newestRpcProcess);
        yield* writeRecords(state, replyRecords(command, reply));
      });

    return {
      spawner,
      spawns: () => processes.map(snapshotInvocation),
      commands: log.commands,
      frames: log.frames,
      takeCommand: log.takeCommand,
      takeFrame: log.takeFrame,
      emit: (frame: OmpRecord) =>
        newestRpcProcess.pipe(Effect.flatMap((state) => writeRecord(state, frame))),
      emitRaw: (text: string) =>
        newestRpcProcess.pipe(Effect.flatMap((state) => writeText(state, text))),
      respond: (command: OmpCommandFrame, data?: unknown) =>
        writeReply(command, fakeOmpSuccess(data)),
      respondFailure: (command: OmpCommandFrame, error: string, code?: string) =>
        writeReply(command, fakeOmpFailure(error, code)),
      exit: (code: number) => newestRpcProcess.pipe(Effect.flatMap((state) => finish(state, code))),
    } satisfies FakeOmpProcess;
  });
