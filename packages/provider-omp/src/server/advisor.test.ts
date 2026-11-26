import { describe, expect, it } from "vite-plus/test";

import advisorFrames from "./__fixtures__/advisor/frames.json" with { type: "json" };
import { isOmpAdvisorCard, isOmpTerminalTextAnswer, ompAdvisorNotice } from "./advisor.ts";
import { ompRecordField, ompRecordString } from "./wire.ts";

const endedMessages = (frames: ReadonlyArray<Record<string, unknown>>, role: string) =>
  frames.flatMap((frame) =>
    frame["type"] === "message_end" && ompRecordString(frame["message"], "role") === role
      ? [ompRecordField(frame, "message")]
      : [],
  );

const sameRunCards = endedMessages(advisorFrames.sameRun, "custom");
const midTurnCards = endedMessages(advisorFrames.midTurn, "custom");
const sameRunAnswers = endedMessages(advisorFrames.sameRun, "assistant");
const nit = sameRunCards[0] as Record<string, unknown>;

describe("isOmpAdvisorCard", () => {
  it("accepts the cards omp shows and nothing else omp sends as a custom message", () => {
    expect([...sameRunCards, ...midTurnCards].map(isOmpAdvisorCard)).toStrictEqual(
      Array(5).fill(true),
    );
    expect(isOmpAdvisorCard({ ...nit, display: false })).toBe(false);
    expect(isOmpAdvisorCard({ ...nit, customType: "async-result" })).toBe(false);
  });
});

describe("isOmpTerminalTextAnswer", () => {
  it("takes the text reply as the answer and a reply that calls tools as work", () => {
    expect(sameRunAnswers.map(isOmpTerminalTextAnswer)).toStrictEqual([false, true, true]);
  });

  it("allows thinking beside the text and refuses a stopped or empty reply", () => {
    const answer = sameRunAnswers[1] as Record<string, unknown>;
    const thinking = { type: "thinking", thinking: "Checking the file." };
    const content = [thinking, ...(answer["content"] as ReadonlyArray<unknown>)];
    expect(isOmpTerminalTextAnswer({ ...answer, content })).toBe(true);
    expect(isOmpTerminalTextAnswer({ ...answer, stopReason: "aborted" })).toBe(false);
    expect(isOmpTerminalTextAnswer({ ...answer, content: [thinking] })).toBe(false);
  });
});

describe("ompAdvisorNotice", () => {
  it("shows each note under its severity, without omp's preamble for the agent", () => {
    expect(ompAdvisorNotice(nit)).toStrictEqual({
      message:
        "Nit: In divide.py, add a guard so divide(a, 0) raises ValueError when b is 0, instead of leaking ZeroDivisionError. Keep it within the two-line constraint, e.g. a one-line `if b == 0: raise ValueError(...)` check before the return, or fold it into a single line.",
      level: "info",
    });
    expect(ompAdvisorNotice(midTurnCards[0])?.message).toMatch(/^Blocker: divide\.py was just/u);
    expect(ompAdvisorNotice(midTurnCards[0])?.level).toBe("warning");
  });

  it("names the advisor and how many steps ago it wrote an older note", () => {
    const notes = [
      { note: "Re-check the chamfer at both ends.", severity: "concern", turnsAgo: 3 },
      { note: "Prefer one helper.", advisor: "style", turnsAgo: 1 },
    ];
    expect(ompAdvisorNotice({ ...nit, details: { notes } })).toStrictEqual({
      message:
        "Concern (3 steps ago): Re-check the chamfer at both ends.\n\nNit from style (1 step ago): Prefer one helper.",
      level: "warning",
    });
  });

  it("falls back to the card's text without its preamble or tags", () => {
    expect(ompAdvisorNotice({ ...nit, details: undefined })?.message).toBe(
      "In divide.py, add a guard so divide(a, 0) raises ValueError when b is 0, instead of leaking ZeroDivisionError. Keep it within the two-line constraint, e.g. a one-line `if b == 0: raise ValueError(...)` check before the return, or fold it into a single line.",
    );
    expect(ompAdvisorNotice({ ...nit, details: undefined, content: "  " })).toBeUndefined();
  });

  it("keeps a long review to a bounded notice", () => {
    const notes = [{ note: "x".repeat(10_000), severity: "nit" }];
    const message = ompAdvisorNotice({ ...nit, details: { notes } })?.message ?? "";
    expect(message.length).toBe(4_001);
    expect(message.endsWith("…")).toBe(true);
  });
});
