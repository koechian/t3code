import { assert } from "@effect/vitest";
import type { ProviderReplayTranscript } from "@t3tools/contracts";

import type { OrchestratorV2ScenarioResult } from "../../OrchestratorScenario.ts";
import {
  assertBaseProjection,
  assertSemanticProjectionIntegrity,
  assertUserMessagesInclude,
  assertVisibleTurnItemsMirrorLocalTurnItems,
  projectionFor,
  TOOL_CALL_WRITE_PROMPT,
} from "../shared.ts";

const PROBE_FILE = ".codex-probe-write-action.txt";
const PROBE_CONTENT = "codex app-server approval fixture";

function field(value: unknown, key: string): unknown {
  return typeof value === "object" && value !== null ? Reflect.get(value, key) : undefined;
}

function framesOf(
  transcript: ProviderReplayTranscript,
  direction: "emit_inbound" | "expect_outbound",
  type: string,
) {
  return transcript.entries.flatMap((entry) =>
    entry.type === direction && field(entry.frame, "type") === type ? [entry.frame] : [],
  );
}

/** omp asks with a `select` dialog; T3 answers Approve with the exact label omp offered. */
export function assertOmpSupervisedWriteOutput(
  result: OrchestratorV2ScenarioResult,
  transcript: ProviderReplayTranscript,
) {
  assertBaseProjection({ result, transcript, runCount: 1, runStatuses: ["completed"] });
  const projection = projectionFor(result, transcript.scenario);
  assertSemanticProjectionIntegrity(projection);
  assertVisibleTurnItemsMirrorLocalTurnItems(projection);
  assertUserMessagesInclude(projection, [TOOL_CALL_WRITE_PROMPT]);

  const dialogs = framesOf(transcript, "emit_inbound", "extension_ui_request").filter(
    (frame) => field(frame, "method") === "select",
  );
  assert.lengthOf(dialogs, 1, "always-ask must ask before the write exactly once");
  const dialogId = field(dialogs[0], "id");
  assert.deepEqual(framesOf(transcript, "expect_outbound", "extension_ui_response"), [
    { type: "extension_ui_response", id: dialogId, value: "Approve" },
  ]);
  assert.deepEqual(
    projection.runtimeRequests.map((request) => [
      request.kind,
      request.status,
      request.nativeRequestRef?.nativeId,
    ]),
    [["file-change", "resolved", dialogId]],
  );

  const approval = projection.turnItems.find((item) => item.type === "approval_request");
  assert.equal(
    approval?.type === "approval_request" ? approval.requestKind : undefined,
    "file-change",
  );
  assert.deepEqual(
    approval?.type === "approval_request"
      ? approval.options?.map((option) => [option.decision, option.label])
      : [],
    [
      ["decline", "Deny"],
      ["acceptForSession", "Approve write for this session"],
      ["accept", "Approve"],
    ],
  );

  const write = projection.turnItems.find(
    (item) => item.type === "file_change" && item.fileName.endsWith(PROBE_FILE),
  );
  assert.equal(write?.status, "completed", "the approved write must complete");
  assert.include(write?.type === "file_change" ? write.newStr : undefined, PROBE_CONTENT);
}
