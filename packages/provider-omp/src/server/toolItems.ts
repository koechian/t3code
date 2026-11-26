/**
 * Pure mapping from omp `tool_execution_*` and `tool_stream_update` frames to orchestration-v2
 * tool item bodies. The adapter owns ids, ordinals, timestamps and emission; this module owns
 * shapes.
 */
import type { OrchestrationV2FileChangeDetail, OrchestrationV2TurnItem } from "@t3tools/contracts";
import { mcpToolPresentation } from "@t3tools/provider-core/server/mcpToolPresentation";
import { isWorkspaceImagePreviewPath } from "@t3tools/shared/filePreview";
import { structuredPatch } from "diff";
import * as Predicate from "effect/Predicate";

import {
  ompRecordBoolean,
  ompRecordField,
  ompRecordNumber,
  ompRecordString,
  type OmpRecord,
  type OmpToolExecutionEndFrame,
  type OmpToolExecutionStartFrame,
  type OmpToolExecutionUpdateFrame,
  type OmpToolStreamUpdateFrame,
} from "./wire.ts";

type ItemOf<Type extends OrchestrationV2TurnItem["type"]> = Extract<
  OrchestrationV2TurnItem,
  { readonly type: Type }
>;

export type OmpCommandExecutionBody = Pick<
  ItemOf<"command_execution">,
  "type" | "title" | "input" | "output" | "exitCode" | "outputIndicatesFailure"
>;

export type OmpFileChangeBody = Pick<
  ItemOf<"file_change">,
  "type" | "title" | "fileName" | "diffStr" | "newStr" | "additions" | "deletions" | "changes"
>;

export type OmpDynamicToolBody = Pick<
  ItemOf<"dynamic_tool">,
  "type" | "title" | "toolName" | "input" | "output" | "viewedImagePath" | "toolIcon" | "toolSource"
>;

export type OmpToolItemBody = OmpCommandExecutionBody | OmpFileChangeBody | OmpDynamicToolBody;

/**
 * `tool_command`: a bash call whose first token is a mounted tool command (ast_edit, mcp__…,
 * discoverable host tools); it renders as dynamic_tool. `ask` renders as the form dialog and
 * `todo` as the turn's todo list. A row keeps the kind it was classified with at start.
 */
export type OmpToolKind = "command" | "tool_command" | "file_change" | "ask" | "todo" | "dynamic";

/**
 * Built-in tools omp mounts as bash commands: upstream tools declaring `loadMode = "discoverable"`
 * minus TOOL_COMMAND_KEEP_TOP_LEVEL (tools/tool-commands.ts:53), plus `resolve` and `reject`.
 * Which of them are enabled depends on the user's config.
 */
const OMP_BUILTIN_TOOL_COMMANDS: ReadonlySet<string> = new Set([
  "ast_grep",
  "ast_edit",
  "debug",
  "resolve",
  "reject",
  "security_scan",
  "github",
  "checkpoint",
  "rewind",
  "retain",
  "recall",
  "reflect",
  "memory_edit",
]);

/** omp's own rule for the command a bash line runs (`toolCommandCallName`, tools/tool-commands.ts). */
const LEADING_COMMAND_WORD = /^\s*([^\s;&|<>()]+)/;
const MCP_TOOL_PREFIX = "mcp__";
const TOOL_COMMAND_NOTICE_PREFIX = "Tool commands: ";
const TOOL_COMMAND_MOUNT_NOTICE = "tool-command-mount-notice";
const TOOL_COMMAND_CALL_ID = /^(.+):cmd:\d+$/;
const HASHLINE_FILE_HEADER = /^\[(.+?)(?:#[0-9A-Fa-f]{4})?\]$/;
/** sloppy's `*** Edit File: path [all]`; checked before ENVELOPE_FILE_HEADER so the flag is dropped. */
const SLOPPY_FILE_HEADER = /^\*\*\* Edit File:\s*(.+?)(?:\s+all)?$/i;
/** apply_patch `*** Update/Add/Delete File:` and `*** Move to:`, plus sloppy's other file aliases. */
const ENVELOPE_FILE_HEADER = /^\*\*\* (?:[A-Za-z -]*File|Move to):\s*(.+)$/;
const NUMBERED_DIFF_ROW = /^([ +-])(\d+)\|(.*)$/s;
const URL_SCHEME = /^[A-Za-z][A-Za-z0-9+.-]*:\/\//;
const MAX_PREVIEW_PATH_LENGTH = 4096;
const DIFF_CONTEXT_LINES = 3;
/** Bounds Myers diff work on large edited files; past it the numbered diff is converted instead. */
const DIFF_MAX_EDIT_LENGTH = 1_000;

const KIND_BY_TOOL_NAME: ReadonlyMap<string, OmpToolKind> = new Map([
  ["edit", "file_change"],
  ["write", "file_change"],
  ["ask", "ask"],
  ["todo", "todo"],
]);

const FILE_CHANGE_OPERATIONS: ReadonlyMap<string, string> = new Map([
  ["create", "add"],
  ["delete", "delete"],
  ["update", "update"],
]);

export interface OmpToolCommandDelta {
  readonly added: ReadonlyArray<string>;
  readonly removed: ReadonlyArray<string>;
}

/**
 * Mount changes from `notice {source: "toolCommands"}` text or a `message_start`/`message_end` of a
 * hidden `tool-command-mount-notice` custom message (`details: {added, removed}`); undefined for any
 * other frame. Config can silence the notice (upstream session/session-tools.ts:1923).
 */
export function ompToolCommandDelta(frame: OmpRecord): OmpToolCommandDelta | undefined {
  switch (frame["type"]) {
    case "notice":
      return toolCommandNoticeDelta(frame);
    case "message_start":
    case "message_end":
      return mountNoticeDelta(frame["message"]);
    default:
      return undefined;
  }
}

function toolCommandNoticeDelta(frame: OmpRecord): OmpToolCommandDelta | undefined {
  const message = ompRecordString(frame, "message");
  if (frame["source"] !== "toolCommands" || !message?.startsWith(TOOL_COMMAND_NOTICE_PREFIX)) {
    return undefined;
  }
  const clauses = message.slice(TOOL_COMMAND_NOTICE_PREFIX.length).split("; ");
  return { added: clauseNames(clauses, "added "), removed: clauseNames(clauses, "removed ") };
}

function clauseNames(clauses: ReadonlyArray<string>, verb: string): ReadonlyArray<string> {
  return clauses.flatMap((clause) =>
    clause.startsWith(verb)
      ? clause
          .slice(verb.length)
          .split(", ")
          .flatMap((name) => nonBlank(name) ?? [])
      : [],
  );
}

function mountNoticeDelta(message: unknown): OmpToolCommandDelta | undefined {
  if (ompRecordString(message, "customType") !== TOOL_COMMAND_MOUNT_NOTICE) return undefined;
  const details = ompRecordField(message, "details");
  return {
    added: stringArray(ompRecordField(details, "added")),
    removed: stringArray(ompRecordField(details, "removed")),
  };
}

/**
 * The names `classifyOmpTool` treats as tool commands: OMP_BUILTIN_TOOL_COMMANDS, plus the
 * discoverable host tools omp registered (the `set_host_tools` reply `toolNames` whose
 * definitions were sent with `loadMode: "discoverable"`), with each delta applied in order.
 */
export function ompToolCommandNames(
  discoverableHostToolNames: ReadonlyArray<string>,
  deltas: ReadonlyArray<OmpToolCommandDelta>,
): ReadonlySet<string> {
  const names = new Set([...OMP_BUILTIN_TOOL_COMMANDS, ...discoverableHostToolNames]);
  for (const delta of deltas) {
    for (const name of delta.added) names.add(name);
    for (const name of delta.removed) names.delete(name);
  }
  return names;
}

/**
 * `toolCommands` comes from `ompToolCommandNames`. A bash command whose first word is in it, or
 * starts with `mcp__` (the user's MCP tools mount as commands), is a `tool_command`; the end
 * frame's `details.toolCommands[0].tool` confirms the name.
 */
export function classifyOmpTool(
  toolName: string,
  args: unknown,
  toolCommands: ReadonlySet<string>,
): OmpToolKind {
  if (toolName !== "bash") return KIND_BY_TOOL_NAME.get(toolName) ?? "dynamic";
  const word = leadingCommandWord(args);
  return word !== undefined && (toolCommands.has(word) || word.startsWith(MCP_TOOL_PREFIX))
    ? "tool_command"
    : "command";
}

/** Row title: the MCP presentation for `mcp__*` names (including a bash line running one), else the tool name. */
export function ompToolTitle(toolName: string, args: unknown): string | null {
  const name = toolName === "bash" ? (mcpCommandName(args) ?? toolName) : toolName.trim();
  return name.length > 0 ? toolPresentation(name).title : null;
}

/**
 * Body shown while the tool runs, from `tool_execution_start.args`. Pass the latest edit preview
 * to show its diff on a running edit, and again whenever a newer preview arrives.
 */
export function ompToolItemFromStart(
  frame: OmpToolExecutionStartFrame,
  kind: OmpToolKind,
  preview?: OmpEditPreview,
): OmpToolItemBody {
  return BODY_BUILDERS[kind]({
    toolName: frame.toolName,
    args: frame.args,
    result: undefined,
    isError: false,
    failed: false,
    background: false,
    preview,
  });
}

/**
 * Bash partial text is cumulative only until truncation starts; after that it is a head, an
 * elision line and a rolling tail. Either way only new text is streamed: a row snapshot per
 * update would resend the whole ~50 KB preview every 50 ms.
 */
export type OmpCommandOutputProgress =
  | { readonly kind: "append"; readonly delta: string; readonly latest: string }
  | { readonly kind: "replace"; readonly delta: string; readonly latest: string }
  | { readonly kind: "unchanged" };

/** omp's `formatMiddleElisionMarker` line between the head and the tail (streaming-output.ts). */
const OMP_ELISION_LINE = /\n\[…\d+(?:ln|B) elided…\]\n/u;

/** KMP failure function: the longest proper border of each prefix of `text`. */
function borderLengths(text: string): Int32Array {
  const borders = new Int32Array(text.length);
  for (let index = 1; index < text.length; index += 1) {
    let length = borders[index - 1] ?? 0;
    while (length > 0 && text[index] !== text[length]) length = borders[length - 1] ?? 0;
    borders[index] = text[index] === text[length] ? length + 1 : length;
  }
  return borders;
}

/** The longest suffix of `previous` that `next` starts with, in time linear in their length. */
function overlapLength(previous: string, next: string): number {
  if (next.length === 0) return 0;
  const end = previous.slice(-next.length);
  return Math.min(borderLengths(`${next}\u0000${end}`).at(-1) ?? 0, end.length);
}

function previewTail(preview: string): string {
  const elision = OMP_ELISION_LINE.exec(preview);
  return elision === null ? preview : preview.slice(elision.index + elision[0].length);
}

/**
 * New text in a truncated preview: the tail past where the previous preview ended, or the elision
 * line and the whole tail when the tail moved past it.
 */
function truncatedOutputDelta(previous: string, latest: string): string {
  const elision = OMP_ELISION_LINE.exec(latest);
  const tail = latest.slice(elision === null ? 0 : elision.index + elision[0].length);
  const overlap = overlapLength(previewTail(previous), tail);
  if (overlap > 0 || elision === null) return tail.slice(overlap);
  return latest.slice(elision.index + (previous.endsWith("\n") ? 1 : 0));
}

/** An empty partial text never replaces output: omp sends one before the first chunk and on abort. */
export function ompCommandOutputProgress(
  previousLatest: string,
  frame: OmpToolExecutionUpdateFrame,
): OmpCommandOutputProgress {
  const latest = blocksText(contentBlocks(frame.partialResult));
  if (latest.length === 0 || latest === previousLatest) return { kind: "unchanged" };
  return latest.startsWith(previousLatest)
    ? { kind: "append", delta: latest.slice(previousLatest.length), latest }
    : { kind: "replace", delta: truncatedOutputDelta(previousLatest, latest), latest };
}

export interface OmpEditPreview {
  /** True once omp stops streaming the arguments (`streaming:false`); prefer it over earlier previews. */
  readonly final: boolean;
  readonly files: ReadonlyArray<OmpEditPreviewFile>;
}

export interface OmpEditPreviewFile {
  /** As the model wrote it, usually workspace-relative; empty when omp could not tell. */
  readonly path: string;
  /** A `diff --git` section converted from omp's numbered ` N|`/`-N|`/`+N|` format. */
  readonly diffStr?: string;
  /** The edit will fail; show it on the approval card. */
  readonly error?: string;
}

type OmpStreamPreviewFile = NonNullable<
  NonNullable<OmpToolStreamUpdateFrame["update"]>["files"]
>[number];

export function ompEditPreview(frame: OmpToolStreamUpdateFrame): OmpEditPreview | undefined {
  const files = frame.update?.files;
  if (files === undefined) return undefined;
  return { final: frame.update?.streaming === false, files: files.map(editPreviewFile) };
}

function editPreviewFile(file: OmpStreamPreviewFile): OmpEditPreviewFile {
  const path = file.path.trim();
  const hunks = path.length > 0 ? numberedDiffHunks(file.diff ?? "") : undefined;
  const diffStr =
    hunks === undefined ? "" : filePatch({ path, oldPath: path, op: file.op }, hunks).text;
  return {
    path: file.path,
    ...(diffStr.length > 0 ? { diffStr } : {}),
    ...(file.error === undefined ? {} : { error: file.error }),
  };
}

export interface OmpToolItemCompletion {
  readonly body: OmpToolItemBody;
  /** `interrupted` for any error while an abort is pending ("Command aborted", "Aborted: Cancelled"). */
  readonly status: "completed" | "failed" | "interrupted";
  /** `details.async.state === "running"`: a background job outlives the row. */
  readonly backgroundJobId: string | undefined;
}

/**
 * Final body from `tool_execution_end`. Edits build `diffStr` with `structuredPatch` from
 * `details.{path,oldText,newText}` or `details.perFileResults[]`; `write` uses `args.{path,content}`.
 */
export function ompToolItemFromEnd(
  start: OmpToolExecutionStartFrame,
  end: OmpToolExecutionEndFrame,
  kind: OmpToolKind,
  abortPending: boolean,
  preview?: OmpEditPreview,
): OmpToolItemCompletion {
  const isError = end.isError === true;
  const status = isError ? (abortPending ? "interrupted" : "failed") : "completed";
  const background = backgroundState(end.result) === "running";
  const body = BODY_BUILDERS[kind]({
    toolName: start.toolName,
    args: start.args,
    result: end.result,
    isError,
    failed: status === "failed",
    background,
    preview,
  });
  const jobId = background ? backgroundJobId(end.result) : undefined;
  return { body, status, backgroundJobId: jobId };
}

const BACKGROUND_TERMINAL_STATUS: ReadonlyMap<string, OmpToolItemCompletion["status"]> = new Map([
  ["completed", "completed"],
  ["failed", "failed"],
  ["cancelled", "interrupted"],
]);

/**
 * A late update whose `details.async.state` is terminal closes a backgrounded job: `completed`,
 * `failed` (non-zero exit) or `cancelled` (tools/bash.ts).
 */
export function isOmpBackgroundJobCompletion(frame: OmpToolExecutionUpdateFrame): boolean {
  return BACKGROUND_TERMINAL_STATUS.has(backgroundState(frame.partialResult) ?? "");
}

/** Final body of a backgrounded call, from the update `isOmpBackgroundJobCompletion` accepts. */
export function ompToolItemFromBackgroundCompletion(
  start: OmpToolExecutionStartFrame,
  update: OmpToolExecutionUpdateFrame,
  kind: OmpToolKind,
): OmpToolItemCompletion {
  const status =
    BACKGROUND_TERMINAL_STATUS.get(backgroundState(update.partialResult) ?? "") ?? "completed";
  const body = BODY_BUILDERS[kind]({
    toolName: start.toolName,
    args: start.args,
    result: update.partialResult,
    isError: status !== "completed",
    failed: status === "failed",
    background: false,
    preview: undefined,
  });
  return { body, status, backgroundJobId: undefined };
}

/** The bash row a host tool call made through a tool command belongs to (`<id>:cmd:<n>`). */
export function ompParentToolCallId(hostToolCallId: string): string | undefined {
  return TOOL_COMMAND_CALL_ID.exec(hostToolCallId)?.[1];
}

interface OmpToolBodyInput {
  readonly toolName: string;
  readonly args: unknown;
  /** The end frame's `result`, or the `partialResult` that finished a background job. */
  readonly result: unknown;
  readonly isError: boolean;
  readonly failed: boolean;
  readonly background: boolean;
  readonly preview: OmpEditPreview | undefined;
}

const BODY_BUILDERS: Readonly<Record<OmpToolKind, (input: OmpToolBodyInput) => OmpToolItemBody>> = {
  command: commandBody,
  tool_command: toolCommandBody,
  file_change: fileChangeBody,
  ask: dynamicBody,
  todo: dynamicBody,
  dynamic: dynamicBody,
};

function commandBody(input: OmpToolBodyInput): OmpCommandExecutionBody {
  const output = blocksText(contentBlocks(input.result));
  const exitCode = commandExitCode(input);
  return {
    type: "command_execution",
    title: input.toolName,
    input: ompRecordString(input.args, "command") ?? "",
    ...(output.length > 0 ? { output } : {}),
    ...(exitCode === undefined ? {} : { exitCode }),
    ...(input.failed && exitCode === undefined ? { outputIndicatesFailure: true } : {}),
  };
}

/** omp reports `exitCode` only when it is non-zero; a backgrounded command has none yet. */
function commandExitCode(input: OmpToolBodyInput): number | undefined {
  if (input.result === undefined) return undefined;
  const reported = ompRecordNumber(ompRecordField(input.result, "details"), "exitCode");
  if (reported !== undefined) return Number.isInteger(reported) ? reported : undefined;
  return input.isError || input.background ? undefined : 0;
}

function toolCommandBody(input: OmpToolBodyInput): OmpDynamicToolBody {
  const dispatch = firstToolCommandDispatch(input.result);
  const toolName =
    nonBlank(ompRecordString(dispatch, "tool")) ?? leadingCommandWord(input.args) ?? input.toolName;
  const dispatchArgs = ompRecordField(dispatch, "args");
  const output = dynamicToolOutput(toolName, input.result);
  return {
    type: "dynamic_tool",
    ...toolPresentation(toolName),
    toolName,
    input: Predicate.isObject(dispatchArgs)
      ? dispatchArgs
      : { command: ompRecordString(input.args, "command") ?? "" },
    ...(output === undefined ? {} : { output }),
  };
}

function firstToolCommandDispatch(result: unknown): unknown {
  const dispatches = ompRecordField(ompRecordField(result, "details"), "toolCommands");
  return Array.isArray(dispatches) ? dispatches[0] : undefined;
}

function fileChangeBody(input: OmpToolBodyInput): OmpFileChangeBody | OmpDynamicToolBody {
  return input.toolName === "write" ? writeBody(input) : editBody(input);
}

/** omp cannot tell a create from an overwrite, so a write records no change operation. */
function writeBody(input: OmpToolBodyInput): OmpFileChangeBody | OmpDynamicToolBody {
  const fileName = nonBlank(ompRecordString(input.args, "path"));
  if (fileName === undefined) return dynamicBody(input);
  const content = ompRecordString(input.args, "content");
  const file = { type: "file_change", title: input.toolName, fileName } as const;
  return withErrorText(
    content === undefined ? file : { ...file, newStr: content },
    input.isError ? input.result : undefined,
  );
}

function editBody(input: OmpToolBodyInput): OmpFileChangeBody | OmpDynamicToolBody {
  const candidates = editPathCandidates(input.args, input.preview);
  const files = editResultFiles(input.result);
  const firstResultPath =
    files[0] === undefined ? undefined : displayPath(files[0].path, candidates);
  const fileName = candidates[0] ?? firstResultPath;
  if (fileName === undefined) return dynamicBody(input);
  const file = { type: "file_change", title: input.toolName, fileName } as const;
  if (input.isError) return withErrorText(file, input.result);
  return {
    ...file,
    ...(input.result === undefined
      ? previewChangeSummary(input.preview)
      : resultChangeSummary(files, candidates)),
  };
}

/** A failed file change shows omp's error text as its diff, as Pi's does. */
function withErrorText(body: OmpFileChangeBody, result: unknown): OmpFileChangeBody {
  const text = blocksText(contentBlocks(result));
  return text.trim().length > 0 ? { ...body, diffStr: text } : body;
}

type OmpChangeSummary = Pick<OmpFileChangeBody, "diffStr" | "additions" | "deletions" | "changes">;

function previewChangeSummary(preview: OmpEditPreview | undefined): OmpChangeSummary {
  const diffStr = (preview?.files ?? []).map((file) => file.diffStr ?? "").join("");
  return diffStr.length > 0 ? { diffStr } : {};
}

function resultChangeSummary(
  files: ReadonlyArray<OmpEditFileResult>,
  candidates: ReadonlyArray<string>,
): OmpChangeSummary {
  if (files.length === 0) return {};
  const targets = files.map((file) => ({ file, target: fileTarget(file, candidates) }));
  const patches = targets.map(({ file, target }) => filePatch(target, editFileHunks(file)));
  const diffStr = patches.map((patch) => patch.text).join("");
  return {
    ...(diffStr.length > 0 ? { diffStr } : {}),
    additions: patches.reduce((sum, patch) => sum + patch.additions, 0),
    deletions: patches.reduce((sum, patch) => sum + patch.deletions, 0),
    changes: targets.map(({ target }) => changeDetail(target)),
  };
}

interface OmpFileTarget {
  readonly path: string;
  readonly oldPath: string;
  readonly op: string | undefined;
}

function fileTarget(file: OmpEditFileResult, candidates: ReadonlyArray<string>): OmpFileTarget {
  const path = displayPath(file.path, candidates);
  const oldPath = file.sourcePath === undefined ? path : displayPath(file.sourcePath, candidates);
  return { path, oldPath, op: file.op };
}

function changeDetail(target: OmpFileTarget): OrchestrationV2FileChangeDetail {
  return {
    operation: fileChangeOperation(target.op),
    path: target.path,
    ...(target.oldPath === target.path ? {} : { oldPath: target.oldPath }),
  };
}

function fileChangeOperation(op: string | undefined): string {
  return FILE_CHANGE_OPERATIONS.get(op ?? "") ?? "update";
}

/** Paths in the order they name the edited files: args, then the stream preview. */
function editPathCandidates(
  args: unknown,
  preview: OmpEditPreview | undefined,
): ReadonlyArray<string> {
  const paths = [
    ompRecordString(args, "path"),
    ...patchRenamePaths(args),
    ...editInputHeaderPaths(ompRecordString(args, "input") ?? ""),
    ...(preview?.files ?? []).map((file) => file.path),
  ];
  return [...new Set(paths.flatMap((path) => nonBlank(path) ?? []))];
}

function patchRenamePaths(args: unknown): ReadonlyArray<string | undefined> {
  const edits = ompRecordField(args, "edits");
  return Array.isArray(edits) ? edits.map((edit) => ompRecordString(edit, "rename")) : [];
}

/**
 * apply_patch and sloppy name files in `*** … File:` rows, and sloppy block bodies are raw code
 * where `[section]` is content; only input without such rows is hashline, named by `[path#TAG]`.
 */
function editInputHeaderPaths(input: string): ReadonlyArray<string | undefined> {
  const rows = input.split("\n");
  const envelopePaths = rows.map(envelopeHeaderPath);
  return envelopePaths.some(Predicate.isNotUndefined)
    ? envelopePaths
    : rows.map((row) => HASHLINE_FILE_HEADER.exec(row.trimEnd())?.[1]);
}

function envelopeHeaderPath(line: string): string | undefined {
  const row = line.trim();
  return (SLOPPY_FILE_HEADER.exec(row) ?? ENVELOPE_FILE_HEADER.exec(row))?.[1];
}

/**
 * End details carry absolute paths, spelled as omp resolved them (`/tmp` where the workspace is
 * `/private/tmp`), so show the path the model wrote when one names the same file. The longest
 * match wins: `/ws/apps/web/package.json` is `apps/web/package.json`, not `package.json`.
 */
function displayPath(path: string, candidates: ReadonlyArray<string>): string {
  const absolute = path.replaceAll("\\", "/");
  let shown = path;
  let matchedLength = -1;
  for (const candidate of candidates) {
    const relative = candidate.replaceAll("\\", "/").replace(/^\.\//, "");
    if (relative.length > matchedLength && endsWithPath(absolute, relative)) {
      shown = candidate;
      matchedLength = relative.length;
    }
  }
  return shown;
}

function endsWithPath(absolute: string, relative: string): boolean {
  return absolute === relative || absolute.endsWith(`/${relative}`);
}

interface OmpEditFileResult {
  readonly path: string;
  readonly op: string | undefined;
  readonly diff: string | undefined;
  readonly oldText: string | undefined;
  readonly newText: string | undefined;
  readonly sourcePath: string | undefined;
  readonly snapshotsPruned: boolean;
}

/** A multi-file edit has no top-level path; its files are in `perFileResults[]`. */
function editResultFiles(result: unknown): ReadonlyArray<OmpEditFileResult> {
  const details = ompRecordField(result, "details");
  const perFileResults = ompRecordField(details, "perFileResults");
  return (Array.isArray(perFileResults) ? perFileResults : [details]).flatMap(editFileResult);
}

function editFileResult(entry: unknown): ReadonlyArray<OmpEditFileResult> {
  const path = nonBlank(ompRecordString(entry, "path"));
  if (path === undefined) return [];
  return [
    {
      path,
      op: ompRecordString(entry, "op"),
      diff: ompRecordString(entry, "diff"),
      oldText: ompRecordString(entry, "oldText"),
      newText: ompRecordString(entry, "newText"),
      sourcePath: nonBlank(ompRecordString(entry, "sourcePath")),
      snapshotsPruned: ompRecordBoolean(entry, "snapshotsPruned") === true,
    },
  ];
}

interface OmpPatchHunk {
  readonly oldStart: number;
  readonly oldLines: number;
  readonly newStart: number;
  readonly newLines: number;
  readonly lines: ReadonlyArray<string>;
}

/** Snapshots past omp's per-call size cap are pruned; then only the numbered diff is left. */
function editFileHunks(file: OmpEditFileResult): ReadonlyArray<OmpPatchHunk> {
  return snapshotHunks(file) ?? numberedDiffHunks(file.diff ?? "") ?? [];
}

function snapshotHunks(file: OmpEditFileResult): ReadonlyArray<OmpPatchHunk> | undefined {
  if (file.snapshotsPruned || (file.oldText === undefined && file.newText === undefined)) {
    return undefined;
  }
  const oldText = file.oldText ?? "";
  const newText = file.newText ?? "";
  const bounded = oldText.length > 0 && newText.length > 0;
  return structuredPatch(file.path, file.path, oldText, newText, undefined, undefined, {
    context: DIFF_CONTEXT_LINES,
    maxEditLength: bounded ? DIFF_MAX_EDIT_LENGTH : Number.POSITIVE_INFINITY,
  })?.hunks;
}

interface OmpFilePatch {
  readonly text: string;
  readonly additions: number;
  readonly deletions: number;
}

const EMPTY_FILE_PATCH: OmpFilePatch = { text: "", additions: 0, deletions: 0 };

/** A `diff --git` section per file, so sections join into one multi-file patch. */
function filePatch(target: OmpFileTarget, hunks: ReadonlyArray<OmpPatchHunk>): OmpFilePatch {
  if (hunks.length === 0) return EMPTY_FILE_PATCH;
  const oldName = JSON.stringify(`a/${target.oldPath}`);
  const newName = JSON.stringify(`b/${target.path}`);
  const rows = [
    `diff --git ${oldName} ${newName}`,
    `--- ${target.op === "create" ? "/dev/null" : oldName}`,
    `+++ ${target.op === "delete" ? "/dev/null" : newName}`,
    ...hunks.flatMap((hunk) => [hunkHeader(hunk), ...hunk.lines]),
  ];
  const lines = hunks.flatMap((hunk) => hunk.lines);
  return {
    text: `${rows.join("\n")}\n`,
    additions: lines.filter((line) => line.startsWith("+")).length,
    deletions: lines.filter((line) => line.startsWith("-")).length,
  };
}

/** Unified diff quirk: an empty side's start is one less than its first line. */
function hunkHeader(hunk: OmpPatchHunk): string {
  const oldStart = hunk.oldLines === 0 ? hunk.oldStart - 1 : hunk.oldStart;
  const newStart = hunk.newLines === 0 ? hunk.newStart - 1 : hunk.newStart;
  return `@@ -${oldStart},${hunk.oldLines} +${newStart},${hunk.newLines} @@`;
}

type OmpNumberedRowKind = " " | "+" | "-";

interface OmpNumberedRow {
  readonly kind: OmpNumberedRowKind;
  readonly line: number;
  readonly text: string;
}

interface OmpNumberedHunk {
  oldStart: number;
  oldLines: number;
  newStart: number;
  newLines: number;
  readonly lines: Array<string>;
  nextOld: number;
  nextNew: number;
}

interface OmpNumberedDiffReader {
  readonly hunks: Array<OmpNumberedHunk>;
  current: OmpNumberedHunk | undefined;
  /** New line number minus old line number after the rows read so far. */
  delta: number;
}

/**
 * omp's numbered diff (crates/edit/src/diff_string.rs) numbers context and removed rows by their
 * old line and added rows by their new line. Hunks are concatenated, or separated by blank gap
 * rows or `@@` headers; a jump in numbering also starts a hunk. Undefined when a row is not
 * numbered, as in a mid-stream preview.
 */
function numberedDiffHunks(diff: string): ReadonlyArray<OmpPatchHunk> | undefined {
  const reader: OmpNumberedDiffReader = { hunks: [], current: undefined, delta: 0 };
  for (const text of diff.split("\n")) {
    if (!readNumberedDiffRow(reader, text)) return undefined;
  }
  return reader.hunks;
}

function readNumberedDiffRow(reader: OmpNumberedDiffReader, text: string): boolean {
  if (isNumberedHunkBreak(text)) {
    reader.current = undefined;
    return true;
  }
  if (text.startsWith("\\")) {
    reader.current?.lines.push(text);
    return true;
  }
  const row = parseNumberedRow(text);
  if (row === undefined) return false;
  if (reader.current === undefined || !continuesHunk(reader.current, row)) {
    reader.current = openHunk(row, reader.delta);
    reader.hunks.push(reader.current);
  }
  reader.delta += appendNumberedRow(reader.current, row);
  return true;
}

function isNumberedHunkBreak(text: string): boolean {
  return text.length === 0 || text === "…" || text.startsWith("@@");
}

function parseNumberedRow(text: string): OmpNumberedRow | undefined {
  const match = NUMBERED_DIFF_ROW.exec(text);
  if (match === null) return undefined;
  return { kind: numberedRowKind(match[1]), line: Number(match[2]), text: match[3] ?? "" };
}

function numberedRowKind(prefix: string | undefined): OmpNumberedRowKind {
  if (prefix === "+") return "+";
  return prefix === "-" ? "-" : " ";
}

function continuesHunk(hunk: OmpNumberedHunk, row: OmpNumberedRow): boolean {
  return row.kind === "+" ? row.line === hunk.nextNew : row.line === hunk.nextOld;
}

function openHunk(row: OmpNumberedRow, delta: number): OmpNumberedHunk {
  const oldStart = row.kind === "+" ? row.line - delta : row.line;
  const newStart = row.kind === "+" ? row.line : row.line + delta;
  return {
    oldStart,
    oldLines: 0,
    newStart,
    newLines: 0,
    lines: [],
    nextOld: oldStart,
    nextNew: newStart,
  };
}

/** Returns how the row moves new line numbers against old ones. */
function appendNumberedRow(hunk: OmpNumberedHunk, row: OmpNumberedRow): number {
  hunk.lines.push(`${row.kind}${row.text}`);
  if (row.kind === "+") {
    hunk.newLines += 1;
    hunk.nextNew = row.line + 1;
    return 1;
  }
  hunk.oldLines += 1;
  hunk.nextOld = row.line + 1;
  if (row.kind === "-") return -1;
  hunk.newLines += 1;
  hunk.nextNew += 1;
  return 0;
}

function dynamicBody(input: OmpToolBodyInput): OmpDynamicToolBody {
  const toolName = nonBlank(input.toolName);
  const output = dynamicToolOutput(input.toolName, input.result);
  const viewedImagePath =
    input.toolName === "read" && !input.isError ? workspaceImagePath(input.args) : undefined;
  return {
    type: "dynamic_tool",
    ...(toolName === undefined ? { title: null } : toolPresentation(toolName)),
    toolName: toolName ?? null,
    input: input.args ?? {},
    ...(output === undefined ? {} : { output }),
    ...(viewedImagePath === undefined ? {} : { viewedImagePath }),
  };
}

/**
 * `details.displayContent` is the rendering without mode-dependent row prefixes (read, grep). A
 * read's image blocks are dropped: `viewedImagePath` shows the file instead.
 */
function dynamicToolOutput(toolName: string, result: unknown): unknown {
  const display = displayContentText(ompRecordField(result, "details"));
  if (display !== undefined) return display;
  const blocks = contentBlocks(result).filter((block) => toolName !== "read" || isTextBlock(block));
  if (!blocks.every(isTextBlock)) return { content: blocks };
  const text = blocksText(blocks);
  return text.length > 0 ? text : undefined;
}

function displayContentText(details: unknown): string | undefined {
  const display = ompRecordField(details, "displayContent");
  const text = Predicate.isString(display) ? display : ompRecordString(display, "text");
  return text === undefined || text.length === 0 ? undefined : text;
}

/** A read path may be a `local://` artifact or URL, or end in a `:range`; only plain image paths preview. */
function workspaceImagePath(args: unknown): string | undefined {
  const path = nonBlank(ompRecordString(args, "path"));
  return path !== undefined && isPlainPath(path) && isWorkspaceImagePreviewPath(path)
    ? path
    : undefined;
}

function isPlainPath(path: string): boolean {
  return path.length <= MAX_PREVIEW_PATH_LENGTH && !/[\r\n]/.test(path) && !URL_SCHEME.test(path);
}

function backgroundState(result: unknown): string | undefined {
  return ompRecordString(backgroundJob(result), "state");
}

function backgroundJobId(result: unknown): string | undefined {
  return ompRecordString(backgroundJob(result), "jobId");
}

function backgroundJob(result: unknown): unknown {
  return ompRecordField(ompRecordField(result, "details"), "async");
}

function toolPresentation(
  toolName: string,
): Pick<OmpDynamicToolBody, "toolIcon" | "toolSource"> & { readonly title: string } {
  const { title, ...presentation } = mcpToolPresentation({ toolName });
  return { ...presentation, title: title ?? toolName };
}

function leadingCommandWord(args: unknown): string | undefined {
  return LEADING_COMMAND_WORD.exec(ompRecordString(args, "command") ?? "")?.[1];
}

function mcpCommandName(args: unknown): string | undefined {
  const word = leadingCommandWord(args);
  return word?.startsWith(MCP_TOOL_PREFIX) ? word : undefined;
}

function contentBlocks(result: unknown): ReadonlyArray<unknown> {
  const content = ompRecordField(result, "content");
  if (Array.isArray(content)) return content;
  return Predicate.isString(content) ? [{ type: "text", text: content }] : [];
}

function isTextBlock(block: unknown): boolean {
  return ompRecordString(block, "type") === "text";
}

function blocksText(blocks: ReadonlyArray<unknown>): string {
  return blocks
    .map((block) => (isTextBlock(block) ? (ompRecordString(block, "text") ?? "") : ""))
    .join("");
}

function stringArray(value: unknown): ReadonlyArray<string> {
  return Array.isArray(value) ? value.filter(Predicate.isString) : [];
}

function nonBlank(value: string | undefined): string | undefined {
  const trimmed = value?.trim();
  return trimmed === undefined || trimmed.length === 0 ? undefined : trimmed;
}
