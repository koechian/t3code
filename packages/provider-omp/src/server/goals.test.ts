import { RunId, type OrchestrationV2ProviderGoal } from "@t3tools/contracts";
import * as Schema from "effect/Schema";
import { describe, expect, it } from "vite-plus/test";

import goalFrames from "./__fixtures__/goal/frames.json" with { type: "json" };
import {
  ompGoalAfterRead,
  ompGoalCommandOf,
  ompGoalCommands,
  ompGoalFromState,
  ompGoalOutcome,
  ompGoalReply,
  ompProviderGoal,
} from "./goals.ts";
import { OmpGoalUpdatedFrame } from "./wire.ts";

const decodeUpdate = Schema.decodeUnknownSync(OmpGoalUpdatedFrame);
const goalAt = (frame: unknown) => ompProviderGoal(decodeUpdate(frame).goal);
const { budgetLimited, lifecycle, complete } = goalFrames;
const budgetUpdate = budgetLimited.run.find((frame) => frame.type === "goal_updated");

const completedGoal: OrchestrationV2ProviderGoal = {
  objective: "Ship it",
  status: "complete",
  tokensUsed: 10,
  tokenBudget: null,
  timeUsedSeconds: 2,
};

describe("ompProviderGoal", () => {
  it("maps each goal status omp reports and drops a dropped goal", () => {
    expect(
      [
        budgetLimited.createUpdated,
        budgetUpdate,
        lifecycle.pauseUpdates[1],
        complete.completeUpdated,
        budgetLimited.dropUpdates[1],
      ].map((frame) => goalAt(frame)?.status ?? null),
    ).toEqual(["active", "budget_limited", "paused", "complete", null]);
    expect(goalAt(budgetUpdate)).toEqual({
      objective: budgetLimited.createResult.goal.objective,
      status: "budget_limited",
      tokensUsed: 13_959,
      tokenBudget: 1_000,
      timeUsedSeconds: 1,
    });
  });

  it("reads get_state's goal, and nothing from a reply it cannot decode", () => {
    expect(ompGoalFromState(lifecycle.pausedStateGoal)?.status).toBe("paused");
    expect(ompGoalFromState(null)).toBeNull();
    expect(ompGoalFromState(undefined)).toBeUndefined();
  });
});

describe("ompGoalAfterRead", () => {
  it("keeps a completed goal omp forgot, and takes every other read as it is", () => {
    const paused = ompGoalFromState(lifecycle.pausedStateGoal) ?? null;
    expect(ompGoalAfterRead(completedGoal, null)).toBe(completedGoal);
    expect(ompGoalAfterRead(paused, null)).toBeNull();
    expect(ompGoalAfterRead(completedGoal, paused)).toBe(paused);
    expect(ompGoalAfterRead(undefined, null)).toBeNull();
  });
});

describe("ompGoalCommands", () => {
  it.each([
    [{ type: "show" } as const, false, [{ type: "goal", op: "get" }]],
    [{ type: "clear" } as const, true, [{ type: "goal", op: "drop" }]],
    [{ type: "pause" } as const, true, [{ type: "goal", op: "pause" }]],
    [{ type: "resume" } as const, true, [{ type: "goal", op: "resume" }]],
    [
      { type: "set", objective: "Ship it" } as const,
      false,
      [{ type: "goal", op: "create", objective: "Ship it" }],
    ],
    [
      { type: "set", objective: "Ship it" } as const,
      true,
      [
        { type: "goal", op: "drop" },
        { type: "goal", op: "create", objective: "Ship it" },
      ],
    ],
  ])("sends %j (a goal is set: %s)", (command, hasGoal, expected) => {
    expect(ompGoalCommands(command, hasGoal)).toEqual(expected);
  });
});

describe("ompGoalOutcome and ompGoalReply", () => {
  it("reads omp's reply, its refusal, and a reply it cannot decode", () => {
    expect(ompGoalOutcome({ success: true, data: lifecycle.resumeResult })).toEqual({
      goal: goalAt(lifecycle.resumeUpdated),
    });
    expect(ompGoalOutcome({ success: false, error: lifecycle.createRefusedError })).toEqual({
      error: "A goal is already active. Drop it before creating another.",
    });
    expect(ompGoalOutcome({ success: true })).toEqual({
      error: "OMP changed the goal, but T3 Code could not read the result.",
    });
  });

  it("replies only where the goal line cannot answer", () => {
    const limited = goalAt(budgetUpdate);
    expect(ompGoalReply({ type: "show" }, { goal: limited })).toEqual({
      message: `Goal budget limited: ${budgetLimited.createResult.goal.objective}`,
      level: "info",
    });
    expect(ompGoalReply({ type: "show" }, { goal: null })?.message).toBe("No goal is set.");
    expect(ompGoalReply({ type: "pause" }, { goal: null })?.message).toBe("No goal is set.");
    expect(ompGoalReply({ type: "resume" }, { goal: limited })?.message).toContain(
      "used its token budget",
    );
    expect(ompGoalReply({ type: "resume" }, { goal: goalAt(lifecycle.resumeUpdated) })).toBe(
      undefined,
    );
    expect(ompGoalReply({ type: "set", objective: "Ship it" }, { goal: limited })).toBe(undefined);
    expect(ompGoalReply({ type: "clear" }, { error: "busy" })).toEqual({
      message: "busy",
      level: "warning",
    });
  });
});

describe("ompGoalCommandOf", () => {
  it("reads /goal, but not with attachments or in a restart's continuation", () => {
    const message = { text: "/goal pause", attachments: [] };
    expect(ompGoalCommandOf({ message })).toEqual({ type: "pause" });
    expect(ompGoalCommandOf({ message: { ...message, attachments: [{}] } })).toBeNull();
    expect(
      ompGoalCommandOf({ message, restartContinuationOfRunId: RunId.make("run:restarted") }),
    ).toBeNull();
  });
});
