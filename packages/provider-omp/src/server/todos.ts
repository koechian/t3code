/**
 * Pure mapping from omp's `todo` tool to orchestration-v2 todo-list steps. Every successful call
 * ends with the whole list in `details.phases`, never a delta, so the newest snapshot wins.
 */
import type { OrchestrationV2PlanStep } from "@t3tools/contracts";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
// @effect-diagnostics-next-line nodeBuiltinImport:off -- step ids are hashed while mapping a frame.
import * as NodeCrypto from "node:crypto";

import { ompRecordField, type OmpToolExecutionEndFrame } from "./wire.ts";

const OmpTodoTask = Schema.Struct({ content: Schema.String, status: Schema.String });
type OmpTodoTask = typeof OmpTodoTask.Type;

/** Upstream `TodoPhase` (tools/details/todo.ts). */
const OmpTodoPhase = Schema.Struct({
  name: Schema.String,
  tasks: Schema.Array(OmpTodoTask),
});
type OmpTodoPhase = typeof OmpTodoPhase.Type;

const OmpTodoDetails = Schema.Struct({
  op: Schema.optional(Schema.String),
  phases: Schema.Array(OmpTodoPhase),
});
const decodeTodoDetails = Schema.decodeUnknownOption(OmpTodoDetails);

const STEP_ID_DIGEST_LENGTH = 16;
const PHASE_SEPARATOR = " → ";

const STEP_STATUS_BY_TODO_STATUS: ReadonlyMap<string, OrchestrationV2PlanStep["status"]> = new Map([
  ["in_progress", "running"],
  ["completed", "completed"],
  // omp counts a dropped task as done ("3/3 done"), and plan steps have no cancelled status.
  ["abandoned", "completed"],
]);

export interface OmpTodoList {
  readonly steps: ReadonlyArray<OrchestrationV2PlanStep>;
  /** The phase names in order, when the list has two or more phases. */
  readonly explanation: string | undefined;
  /** True when no task is pending or in progress. */
  readonly completed: boolean;
}

/**
 * The list a `todo` call left, or undefined when it changed nothing: a failed call reports the
 * previous list, and `view` only reads it.
 */
export function ompTodoListFromEnd(end: OmpToolExecutionEndFrame): OmpTodoList | undefined {
  if (end.isError === true) return undefined;
  const details = decodeTodoDetails(ompRecordField(end.result, "details"));
  if (Option.isNone(details) || details.value.op === "view") return undefined;
  return ompTodoList(details.value.phases);
}

function ompTodoList(phases: ReadonlyArray<OmpTodoPhase>): OmpTodoList {
  const steps = phases.flatMap((phase) => phase.tasks.flatMap((task) => todoStep(phase, task)));
  return {
    steps,
    explanation:
      phases.length > 1 ? phases.map((phase) => phase.name).join(PHASE_SEPARATOR) : undefined,
    completed: steps.every((step) => step.status === "completed"),
  };
}

function todoStep(phase: OmpTodoPhase, task: OmpTodoTask): ReadonlyArray<OrchestrationV2PlanStep> {
  const text = task.content.trim();
  if (text.length === 0) return [];
  const status = STEP_STATUS_BY_TODO_STATUS.get(task.status) ?? "pending";
  return [{ id: todoStepId(phase.name, task.content), text, status }];
}

/** omp rejects a task whose content is already listed, so the pair names one task over time. */
function todoStepId(phase: string, content: string): string {
  const digest = NodeCrypto.createHash("sha256").update(`${phase}\0${content}`).digest("hex");
  return `todo-${digest.slice(0, STEP_ID_DIGEST_LENGTH)}`;
}
