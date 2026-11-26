/**
 * omp's advisor: a second model that reviews the agent's work and posts its notes as `advisor`
 * cards (role `custom`). A card that follows the agent's final answer can make omp continue the
 * run (session/session-advisors.ts, #routeAdvice and #deliverAdvisorBatch).
 */
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";

import { ompContentText, ompRecordBoolean, ompRecordField, ompRecordString } from "./wire.ts";

export const OMP_ADVISOR_NOTICE_TITLE = "Advisor review";

const MAX_ADVISOR_NOTICE_LENGTH = 4_000;

const SEVERITY_LABELS: Readonly<Record<string, string>> = {
  nit: "Nit",
  concern: "Concern",
  blocker: "Blocker",
};
const WARNING_SEVERITIES: ReadonlySet<string> = new Set(["concern", "blocker"]);
/** Blocks a terminal answer may hold besides its text (session/queued-messages.ts). */
const ANSWER_EXTRA_BLOCK_TYPES: ReadonlySet<string> = new Set([
  "thinking",
  "redactedThinking",
  "fallback",
  "anthropicServerTool",
]);

const AdvisorNote = Schema.Struct({
  note: Schema.String,
  severity: Schema.optional(Schema.String),
  advisor: Schema.optional(Schema.String),
  turnsAgo: Schema.optional(Schema.Number),
});
type AdvisorNote = typeof AdvisorNote.Type;

const decodeAdvisorNotes = Schema.decodeUnknownOption(Schema.Array(AdvisorNote));

export interface OmpAdvisorNotice {
  readonly message: string;
  readonly level: "info" | "warning";
}

export function isOmpAdvisorCard(message: unknown): boolean {
  return (
    ompRecordString(message, "role") === "custom" &&
    ompRecordString(message, "customType") === "advisor" &&
    ompRecordBoolean(message, "display") === true
  );
}

function isAnswerBlock(block: unknown): boolean {
  const type = ompRecordString(block, "type") ?? "";
  return type === "text" || ANSWER_EXTRA_BLOCK_TYPES.has(type);
}

/** omp's own test for a final answer: text and no tool calls (isTerminalTextAssistantAnswer). */
export function isOmpTerminalTextAnswer(message: unknown): boolean {
  if (ompRecordString(message, "stopReason") !== "stop") return false;
  const content = ompRecordField(message, "content");
  if (!Array.isArray(content) || !content.every(isAnswerBlock)) return false;
  return ompContentText(content).trim().length > 0;
}

function noteHeading(note: AdvisorNote): string {
  const severity = SEVERITY_LABELS[note.severity ?? "nit"] ?? "Note";
  const source = note.advisor === undefined ? "" : ` from ${note.advisor}`;
  const age =
    note.turnsAgo === undefined || note.turnsAgo < 1
      ? ""
      : ` (${note.turnsAgo} ${note.turnsAgo === 1 ? "step" : "steps"} ago)`;
  return `${severity}${source}${age}`;
}

/** Without structured notes, the card's text loses omp's preamble and its `<advisory>` tags. */
function plainAdvisorText(content: unknown): string {
  const text = ompContentText(content);
  const firstNote = text.indexOf("<advisory");
  return (firstNote >= 0 ? text.slice(firstNote) : text)
    .replace(/<\/?advisory[^>]*>/gu, "")
    .replace(/\n{3,}/gu, "\n\n")
    .trim();
}

function boundedNotice(text: string): string {
  return text.length > MAX_ADVISOR_NOTICE_LENGTH
    ? `${text.slice(0, MAX_ADVISOR_NOTICE_LENGTH)}…`
    : text;
}

export function ompAdvisorNotice(message: unknown): OmpAdvisorNotice | undefined {
  const notes = Option.getOrElse(
    decodeAdvisorNotes(ompRecordField(ompRecordField(message, "details"), "notes")),
    (): ReadonlyArray<AdvisorNote> => [],
  ).filter((note) => note.note.trim().length > 0);
  const text =
    notes.length === 0
      ? plainAdvisorText(ompRecordField(message, "content"))
      : notes.map((note) => `${noteHeading(note)}: ${note.note.trim()}`).join("\n\n");
  if (text.length === 0) return undefined;
  const warns = notes.some((note) => WARNING_SEVERITIES.has(note.severity ?? ""));
  return { message: boundedNotice(text), level: warns ? "warning" : "info" };
}
