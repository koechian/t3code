/**
 * Pure mapping between T3's `/goal` commands and omp's goal mode: the `goal` commands a control
 * turn sends, the provider goal that omp's frames and replies describe, and the reply a control
 * turn shows. omp runs an active goal on its own (`goal.continuationModes` includes `rpc`).
 */
import {
  OrchestrationV2ProviderGoal,
  parseGoalCommand,
  type OrchestrationV2GoalCommand,
  type RunId,
} from "@t3tools/contracts";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";

import { OmpGoalResult, type OmpCommand, type OmpGoal } from "./wire.ts";

export type OmpGoalCommand = Extract<OmpCommand, { readonly type: "goal" }>;

/** What a `goal` command left: the goal omp reports, or why omp refused the command. */
export type OmpGoalOutcome =
  | { readonly goal: OrchestrationV2ProviderGoal | null }
  | { readonly error: string };

export interface OmpGoalReply {
  readonly message: string;
  readonly level: "info" | "warning";
}

type ProviderGoal = OrchestrationV2ProviderGoal | null;

const PROVIDER_GOAL_STATUSES = {
  active: "active",
  paused: "paused",
  "budget-limited": "budget_limited",
  complete: "complete",
  dropped: null,
} as const satisfies Record<OmpGoal["status"], OrchestrationV2ProviderGoal["status"] | null>;

const GOAL_OPS = {
  show: "get",
  clear: "drop",
  pause: "pause",
  resume: "resume",
} as const satisfies Record<
  Exclude<OrchestrationV2GoalCommand["type"], "set">,
  OmpGoalCommand["op"]
>;

const NO_GOAL_REPLY = "No goal is set.";
const BUDGET_USED_REPLY =
  "This goal has used its token budget, so OMP cannot resume it. Clear it and set it again to keep working.";
const GOAL_FAILED_REPLY = "OMP could not change the goal.";
const GOAL_UNREADABLE_REPLY = "OMP changed the goal, but T3 Code could not read the result.";

/** The reply a control turn shows; state changes show in the thread's goal line instead. */
const GOAL_REPLIES: Readonly<
  Record<OrchestrationV2GoalCommand["type"], (goal: ProviderGoal) => string | undefined>
> = {
  show: (goal) => (goal === null ? NO_GOAL_REPLY : describeOmpGoal(goal)),
  pause: (goal) => (goal === null ? NO_GOAL_REPLY : undefined),
  // omp resumes only a paused goal and answers with the unchanged goal otherwise.
  resume: (goal) => (goal?.status === "budget_limited" ? BUDGET_USED_REPLY : undefined),
  set: () => undefined,
  clear: () => undefined,
};

const decodeGoalResult = Schema.decodeUnknownOption(OmpGoalResult);

export const ompProviderGoalsEqual = Schema.toEquivalence(
  Schema.NullOr(OrchestrationV2ProviderGoal),
);

const wholeCount = (value: number) => Math.max(0, Math.trunc(value));

/** A dropped goal, or one with a blank objective, is no goal. */
export function ompProviderGoal(goal: OmpGoal | null): ProviderGoal {
  if (goal === null) return null;
  const status = PROVIDER_GOAL_STATUSES[goal.status];
  const objective = goal.objective.trim();
  if (status === null || objective.length === 0) return null;
  return {
    objective,
    status,
    tokensUsed: wholeCount(goal.tokensUsed),
    tokenBudget: goal.tokenBudget == null ? null : wholeCount(goal.tokenBudget),
    timeUsedSeconds: wholeCount(goal.timeUsedSeconds),
  };
}

/** The goal in a `goal` command's result, or undefined when the result does not decode. */
function ompGoalFromResult(data: unknown): ProviderGoal | undefined {
  const result = decodeGoalResult(data);
  return Option.isNone(result) ? undefined : ompProviderGoal(result.value.goal);
}

/** `get_state.goal`: null when no goal is set, undefined when omp reported none that decodes. */
export function ompGoalFromState(goalState: unknown): ProviderGoal | undefined {
  return goalState === null ? null : ompGoalFromResult(goalState);
}

/**
 * omp forgets a completed goal when its run ends and reports nothing more, so a read that finds
 * no goal keeps a completed one until the next goal command.
 */
export function ompGoalAfterRead(current: ProviderGoal | undefined, read: ProviderGoal) {
  return read === null && current?.status === "complete" ? current : read;
}

/** The `goal` commands for a T3 goal command. A new objective first drops the goal T3 knows of. */
export function ompGoalCommands(
  command: OrchestrationV2GoalCommand,
  hasGoal: boolean,
): ReadonlyArray<OmpGoalCommand> {
  if (command.type !== "set") return [{ type: "goal", op: GOAL_OPS[command.type] }];
  const create: OmpGoalCommand = { type: "goal", op: "create", objective: command.objective };
  return hasGoal ? [{ type: "goal", op: "drop" }, create] : [create];
}

export function ompGoalOutcome(response: {
  readonly success: boolean;
  readonly data?: unknown;
  readonly error?: string | undefined;
}): OmpGoalOutcome {
  if (!response.success) return { error: response.error?.trim() || GOAL_FAILED_REPLY };
  const goal = ompGoalFromResult(response.data);
  return goal === undefined ? { error: GOAL_UNREADABLE_REPLY } : { goal };
}

/** The outcome's goal is the one the thread shows once the command applied. */
export function ompGoalReply(
  command: OrchestrationV2GoalCommand,
  outcome: OmpGoalOutcome,
): OmpGoalReply | undefined {
  if ("error" in outcome) return { message: outcome.error, level: "warning" };
  const message = GOAL_REPLIES[command.type](outcome.goal);
  return message === undefined ? undefined : { message, level: "info" };
}

function describeOmpGoal(goal: OrchestrationV2ProviderGoal): string {
  return `Goal ${goal.status.replace("_", " ")}: ${goal.objective}`;
}

/** A `/goal` control message. A restart continuation resends earlier text, never a new command. */
export function ompGoalCommandOf(turn: {
  readonly restartContinuationOfRunId?: RunId | undefined;
  readonly message: { readonly text: string; readonly attachments: ReadonlyArray<unknown> };
}): OrchestrationV2GoalCommand | null {
  return turn.restartContinuationOfRunId === undefined ? parseGoalCommand(turn.message) : null;
}
