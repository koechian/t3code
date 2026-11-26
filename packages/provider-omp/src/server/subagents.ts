/**
 * Pure mapping from omp's subagent frames (subscription level `progress`) to the fields of an
 * orchestration-v2 subagent row. A child's result is never in its own frames: it arrives in an
 * `async-result` message, in the end of a `wait` call, or in the child's transcript.
 */
import type { OrchestrationV2Subagent } from "@t3tools/contracts";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";

import {
  ompContentText,
  ompRecordField,
  ompRecordString,
  type OmpRecord,
  type OmpSubagentProgress,
  type OmpSubagentProgressFrame,
  type OmpToolExecutionEndFrame,
} from "./wire.ts";

type OmpSubagentStatus = OrchestrationV2Subagent["status"];

const MAX_PROGRESS_LENGTH = 200;
const MAX_RESULT_LENGTH = 10_000;

/** omp's live `AgentProgress.status` values; a terminal one is followed by a lifecycle frame. */
const LIVE_STATUS_BY_PROGRESS_STATUS: ReadonlyMap<string, OmpSubagentStatus> = new Map([
  ["pending", "pending"],
  ["running", "running"],
]);

const TERMINAL_STATUS_BY_LIFECYCLE_STATUS: ReadonlyMap<string, OmpSubagentStatus> = new Map([
  ["completed", "completed"],
  ["failed", "failed"],
  ["aborted", "cancelled"],
]);

/**
 * prompts/tools/task-summary.md. omp does not escape the output, so a `</task-result>` inside it
 * ends the envelope early.
 */
const TASK_RESULT_ENVELOPES = /<task-result\s+id="([^"]+)"[^>]*>([\s\S]*?)<\/task-result>/g;
/** The body omp's own wait view shows (ui/styles/tools/wait.ts `unwrapTaskResult`). */
const TASK_RESULT_BODY = /<(output|preview)(?:\s[^>]*)?>\n?([\s\S]*?)\n?<\/\1>/;

const WaitJob = Schema.Struct({
  id: Schema.String,
  type: Schema.optional(Schema.String),
  resultText: Schema.optional(Schema.String),
  errorText: Schema.optional(Schema.String),
});
const decodeWaitJobs = Schema.decodeUnknownOption(Schema.Array(WaitJob));

const SubagentMessages = Schema.Struct({
  messages: Schema.Array(Schema.Record(Schema.String, Schema.Unknown)),
});
const decodeSubagentMessages = Schema.decodeUnknownOption(SubagentMessages);

export interface OmpSubagentFields {
  readonly status: OmpSubagentStatus;
  readonly title: string;
  readonly prompt: string;
  readonly model: string | null;
  readonly progress: string | undefined;
}

/** A child omp just started: running, named by its id until the model's description arrives. */
export function ompStartedSubagentFields(
  childId: string,
  description: string | undefined,
): OmpSubagentFields {
  return {
    status: "running",
    title: nonBlank(description) ?? childId,
    prompt: "",
    model: null,
    progress: undefined,
  };
}

/** The row after a progress frame. A terminal status waits for the child's lifecycle frame. */
export function ompSubagentFieldsAfterProgress(
  previous: OmpSubagentFields,
  payload: OmpSubagentProgressFrame["payload"],
): OmpSubagentFields {
  const progress = payload.progress;
  return {
    status: liveStatus(progress) ?? previous.status,
    title: nonBlank(progress.description) ?? previous.title,
    prompt: previous.prompt || (nonBlank(payload.assignment) ?? ""),
    model: progress.resolvedModelIdentity ?? previous.model,
    progress: progressLine(progress),
  };
}

export function ompSubagentFieldsEqual(a: OmpSubagentFields, b: OmpSubagentFields): boolean {
  return (
    a.status === b.status &&
    a.title === b.title &&
    a.prompt === b.prompt &&
    a.model === b.model &&
    a.progress === b.progress
  );
}

/**
 * A terminal lifecycle status, or undefined for one T3 does not know. omp reports every cancelled
 * child as `aborted`; one that T3's Stop cancelled is interrupted.
 */
export function ompSubagentTerminalStatus(
  lifecycleStatus: string,
  stoppedByT3: boolean,
): OmpSubagentStatus | undefined {
  const status = TERMINAL_STATUS_BY_LIFECYCLE_STATUS.get(lifecycleStatus);
  return status === "cancelled" && stoppedByT3 ? "interrupted" : status;
}

/** Each child's result in an `async-result` message, which can deliver several at once. */
export function ompSubagentResultsFromMessage(message: OmpRecord): ReadonlyMap<string, string> {
  if (ompRecordString(message, "customType") !== "async-result") return new Map();
  const text = ompContentText(ompRecordField(message, "content"));
  return new Map(
    Array.from(text.matchAll(TASK_RESULT_ENVELOPES), (match): [string, string] => [
      match[1] ?? "",
      taskResultBody(match[2] ?? ""),
    ]),
  );
}

/** Each task's result in the end of a `wait` call; omp then sends no `async-result` for it. */
export function ompSubagentResultsFromWait(
  end: OmpToolExecutionEndFrame,
): ReadonlyMap<string, string> {
  if (end.toolName !== "wait") return new Map();
  const jobs = decodeWaitJobs(ompRecordField(ompRecordField(end.result, "details"), "jobs"));
  return new Map(
    Option.getOrElse(jobs, () => []).flatMap((job): Array<[string, string]> => {
      const text = job.type === "task" ? (job.errorText ?? job.resultText) : undefined;
      return text === undefined ? [] : [[job.id, unwrapTaskResult(text)]];
    }),
  );
}

/** The child's last assistant text in a `get_subagent_messages` reply. */
export function ompSubagentFinalText(reply: unknown): string | null {
  const messages = Option.getOrUndefined(decodeSubagentMessages(reply))?.messages ?? [];
  const last = messages.findLast((message) => message["role"] === "assistant");
  const text = ompContentText(last?.["content"]).trim();
  return text.length === 0 ? null : bounded(text, MAX_RESULT_LENGTH);
}

function liveStatus(progress: OmpSubagentProgress): OmpSubagentStatus | undefined {
  const status = LIVE_STATUS_BY_PROGRESS_STATUS.get(progress.status);
  // A child whose current tool is `wait` waits on work of its own (task-details.ts:318-321).
  return status === "running" && progress.currentTool === "wait" ? "waiting" : status;
}

function progressLine(progress: OmpSubagentProgress): string | undefined {
  const intent = nonBlank(progress.lastIntent);
  if (intent !== undefined) return bounded(intent, MAX_PROGRESS_LENGTH);
  const tool = nonBlank(progress.currentTool);
  if (tool === undefined) return undefined;
  const args = nonBlank(progress.currentToolArgs);
  return bounded(args === undefined ? tool : `${tool}: ${args}`, MAX_PROGRESS_LENGTH);
}

function unwrapTaskResult(text: string): string {
  const [envelope] = text.matchAll(TASK_RESULT_ENVELOPES);
  return envelope === undefined
    ? bounded(text.trim(), MAX_RESULT_LENGTH)
    : taskResultBody(envelope[2] ?? "");
}

function taskResultBody(envelopeBody: string): string {
  const body = TASK_RESULT_BODY.exec(envelopeBody)?.[2]?.trim();
  return bounded(body || envelopeBody.trim(), MAX_RESULT_LENGTH);
}

function nonBlank(text: string | undefined): string | undefined {
  const trimmed = text?.trim();
  return trimmed === undefined || trimmed.length === 0 ? undefined : trimmed;
}

function bounded(text: string, limit: number): string {
  return text.length > limit ? `${text.slice(0, limit)}…` : text;
}
