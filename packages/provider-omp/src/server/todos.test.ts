import * as Schema from "effect/Schema";
import { describe, expect, it } from "vite-plus/test";

import todoFrames from "./__fixtures__/todos/frames.json" with { type: "json" };
import { ompTodoListFromEnd } from "./todos.ts";
import { OmpToolExecutionEndFrame } from "./wire.ts";

const decodeEnd = Schema.decodeUnknownSync(OmpToolExecutionEndFrame);
const listAt = (frame: unknown) => ompTodoListFromEnd(decodeEnd(frame));
const { createCompleteDrop, twoPhases } = todoFrames;

describe("ompTodoListFromEnd", () => {
  it("maps each snapshot, counting a dropped task as completed", () => {
    const snapshots = [
      createCompleteDrop.initEnd,
      createCompleteDrop.doneAlphaEnd,
      createCompleteDrop.doneBetaEnd,
      createCompleteDrop.dropGammaEnd,
    ].map(listAt);

    expect(snapshots.map((list) => list?.steps.map((step) => step.status))).toEqual([
      ["running", "pending", "pending"],
      ["completed", "running", "pending"],
      ["completed", "completed", "running"],
      ["completed", "completed", "completed"],
    ]);
    expect(snapshots.map((list) => list?.completed)).toEqual([false, false, false, true]);
    expect(snapshots[0]?.steps.map((step) => step.text)).toEqual(["Alpha", "Beta", "Gamma"]);
    expect(snapshots[0]?.explanation).toBeUndefined();
  });

  it("keeps each task's step id across snapshots", () => {
    const first = listAt(createCompleteDrop.initEnd)?.steps.map((step) => step.id) ?? [];
    const last = listAt(createCompleteDrop.dropGammaEnd)?.steps.map((step) => step.id);

    expect(new Set(first).size).toBe(3);
    expect(last).toEqual(first);
  });

  it("flattens phases in order and names them when there are two or more", () => {
    const list = listAt(twoPhases.initEnd);

    expect(list?.steps.map((step) => [step.text, step.status])).toEqual([
      ["One", "running"],
      ["Two", "pending"],
      ["Three", "pending"],
    ]);
    expect(list?.explanation).toBe("Build → Ship");
  });

  it("leaves out a task with no text, which a plan step cannot hold", () => {
    const { initEnd } = createCompleteDrop;
    const [phase] = initEnd.result.details.phases;
    const withBlankTask = {
      ...initEnd,
      result: {
        ...initEnd.result,
        details: {
          ...initEnd.result.details,
          phases: [
            {
              ...phase,
              tasks: phase!.tasks.map((task, index) =>
                index === 1 ? { ...task, content: "  " } : task,
              ),
            },
          ],
        },
      },
    };

    expect(listAt(withBlankTask)?.steps.map((step) => step.text)).toEqual(["Alpha", "Gamma"]);
  });

  it("skips a view and a failed call, which leave the list as it was", () => {
    expect(listAt(twoPhases.viewEnd)).toBeUndefined();
    expect(listAt(twoPhases.missingTaskEnd)).toBeUndefined();
  });
});
