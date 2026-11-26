import * as Schema from "effect/Schema";
import { describe, expect, it } from "vite-plus/test";

import frames from "./__fixtures__/subagents/frames.json" with { type: "json" };
import {
  ompStartedSubagentFields,
  ompSubagentFieldsAfterProgress,
  ompSubagentFieldsEqual,
  ompSubagentFinalText,
  ompSubagentResultsFromMessage,
  ompSubagentResultsFromWait,
  ompSubagentTerminalStatus,
  type OmpSubagentFields,
} from "./subagents.ts";
import { OmpSubagentProgressFrame, OmpToolExecutionEndFrame } from "./wire.ts";

const decodeProgress = Schema.decodeUnknownSync(OmpSubagentProgressFrame);
const decodeEnd = Schema.decodeUnknownSync(OmpToolExecutionEndFrame);
const { parentAbort, detachedNoWait, waited, cancelled } = frames;

const afterProgress = (fields: OmpSubagentFields, ...progress: ReadonlyArray<unknown>) =>
  progress.reduce<OmpSubagentFields>(
    (current, frame) => ompSubagentFieldsAfterProgress(current, decodeProgress(frame).payload),
    fields,
  );

describe("ompSubagentFieldsAfterProgress", () => {
  it("names a child by its id until the model's description arrives", () => {
    const started = ompStartedSubagentFields("Napper", undefined);
    const running = afterProgress(started, parentAbort.napperRunning);
    const described = afterProgress(running, parentAbort.napperDescribed);

    expect(started).toEqual({
      status: "running",
      title: "Napper",
      prompt: "",
      model: null,
      progress: undefined,
    });
    expect(running.title).toBe("Napper");
    expect(running.prompt).toBe(parentAbort.napperRunning.payload.assignment);
    expect(running.model).toBe("anthropic/claude-haiku-5-5");
    expect(described.title).toBe("Run sleep command and report its output");
  });

  it("shows the running tool and its arguments, then clears them between tools", () => {
    const busy = afterProgress(
      ompStartedSubagentFields("Quick", undefined),
      parentAbort.quickRunning,
      parentAbort.quickBash,
    );
    const idle = afterProgress(busy, parentAbort.quickIdle);

    expect(busy.progress).toBe("bash: echo quick-done");
    expect(idle.progress).toBeUndefined();
  });

  it("keeps the prompt from the first progress and reports a repeated frame as unchanged", () => {
    const first = afterProgress(
      ompStartedSubagentFields("Napper", undefined),
      parentAbort.napperRunning,
      parentAbort.napperBash,
    );
    const again = afterProgress(first, parentAbort.napperBashAgain);

    expect(first.progress).toBe("bash: sleep 15; echo nap-done");
    expect(ompSubagentFieldsEqual(first, again)).toBe(true);
  });

  it("leaves a terminal progress status to the child's lifecycle frame", () => {
    const running = afterProgress(
      ompStartedSubagentFields("Quick", undefined),
      parentAbort.quickBash,
    );
    const done = afterProgress(running, parentAbort.quickCompletedProgress);

    expect(done.status).toBe("running");
  });

  it("shortens a long intent to 200 characters", () => {
    const longIntent = {
      ...parentAbort.napperBash,
      payload: {
        ...parentAbort.napperBash.payload,
        progress: { ...parentAbort.napperBash.payload.progress, lastIntent: "x".repeat(300) },
      },
    };

    expect(afterProgress(ompStartedSubagentFields("Napper", undefined), longIntent).progress).toBe(
      `${"x".repeat(200)}…`,
    );
  });

  it("shows a child waiting on its own work as waiting", () => {
    const waiting = {
      ...parentAbort.napperBash,
      payload: {
        ...parentAbort.napperBash.payload,
        progress: { ...parentAbort.napperBash.payload.progress, currentTool: "wait" },
      },
    };

    expect(afterProgress(ompStartedSubagentFields("Napper", undefined), waiting).status).toBe(
      "waiting",
    );
  });
});

describe("ompSubagentTerminalStatus", () => {
  it("maps omp's lifecycle ends, and an abort T3's Stop caused to interrupted", () => {
    expect(ompSubagentTerminalStatus(parentAbort.quickCompleted.payload.status, false)).toBe(
      "completed",
    );
    expect(ompSubagentTerminalStatus("failed", false)).toBe("failed");
    expect(ompSubagentTerminalStatus(cancelled.sleeperAborted.payload.status, false)).toBe(
      "cancelled",
    );
    expect(ompSubagentTerminalStatus(cancelled.sleeperAborted.payload.status, true)).toBe(
      "interrupted",
    );
    expect(ompSubagentTerminalStatus("started", false)).toBeUndefined();
  });
});

describe("subagent results", () => {
  it("reads each child's output from omp's async-result delivery", () => {
    expect(ompSubagentResultsFromMessage(parentAbort.quickResultEnd.message)).toEqual(
      new Map([["Quick", "quick-done"]]),
    );
    expect(ompSubagentResultsFromMessage(detachedNoWait.resultEnd.message)).toEqual(
      new Map([
        [
          "Lister",
          "Output of `sleep 4; ls -A` in the current directory:\n\n- `.git`\n- `NOTES.md`",
        ],
      ]),
    );
    expect(ompSubagentResultsFromMessage(cancelled.sleeperResultEnd.message)).toEqual(
      new Map([["Sleeper", "(no output) after 2 req"]]),
    );
  });

  it("reads a waited child's output from the wait call's end", () => {
    expect(ompSubagentResultsFromWait(decodeEnd(waited.waitEnd))).toEqual(
      new Map([["Counter", "2"]]),
    );
    expect(ompSubagentResultsFromWait(decodeEnd(parentAbort.waitAborted))).toEqual(new Map());
    expect(ompSubagentResultsFromWait(decodeEnd(parentAbort.taskEnd))).toEqual(new Map());
  });

  it("reads only task jobs from a wait call's end", () => {
    const [counterJob] = waited.waitEnd.result.details.jobs;
    const withCommand = {
      ...waited.waitEnd,
      result: {
        ...waited.waitEnd.result,
        details: {
          ...waited.waitEnd.result.details,
          jobs: [counterJob, { ...counterJob, id: "bash-1", type: "bash", resultText: "ok" }],
        },
      },
    };

    expect(ompSubagentResultsFromWait(decodeEnd(withCommand))).toEqual(new Map([["Counter", "2"]]));
  });

  it("takes the child's last assistant text from its transcript", () => {
    expect(ompSubagentFinalText(waited.messagesReply)).toBe("2");
    expect(ompSubagentFinalText({ messages: [] })).toBeNull();
    expect(ompSubagentFinalText(undefined)).toBeNull();
  });
});
