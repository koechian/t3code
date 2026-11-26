import { assert } from "@effect/vitest";
import type {
  OrchestrationV2Subagent,
  OrchestrationV2ThreadProjection,
  ProviderReplayTranscript,
} from "@t3tools/contracts";

import type { OrchestratorV2ScenarioResult } from "../../OrchestratorScenario.ts";
import {
  assertSemanticProjectionIntegrity,
  assertUserMessagesInclude,
  assertVisibleTurnItemsMirrorLocalTurnItems,
  projectionFor,
  SUBAGENT_V2_PROMPT,
} from "../shared.ts";

/** The row sits under the `task` call that spawned the child and comes after it. */
function assertRowUnderTaskCall(
  projection: OrchestrationV2ThreadProjection,
  subagent: OrchestrationV2Subagent,
) {
  const taskCall = projection.turnItems.find(
    (item) => item.type === "dynamic_tool" && item.toolName === "task",
  );
  const row = projection.turnItems.find(
    (item) => item.type === "subagent" && item.subagentId === subagent.id,
  );
  if (taskCall === undefined || row?.type !== "subagent") {
    throw new Error("omp's task call or its subagent row is missing");
  }
  assert.equal(subagent.parentNodeId, taskCall.nodeId);
  assert.isAbove(row.ordinal, taskCall.ordinal);
  assert.equal(row.status, "completed");
  assert.equal(row.result, subagent.result);
}

/** omp's child is a row under the `task` call that spawned it, with no thread of its own. */
export function assertOmpSubagentV2Output(
  result: OrchestratorV2ScenarioResult,
  transcript: ProviderReplayTranscript,
) {
  const projection = projectionFor(result, transcript.scenario);
  assertSemanticProjectionIntegrity(projection);
  assertVisibleTurnItemsMirrorLocalTurnItems(projection);
  assertUserMessagesInclude(projection, [SUBAGENT_V2_PROMPT]);
  assert.isNotEmpty(projection.runs);
  assert.isTrue(projection.runs.every((run) => run.status === "completed"));
  assert.lengthOf(projection.subagents, 1);
  assert.lengthOf(result.shellSnapshot.threads, 1);

  const subagent = projection.subagents[0]!;
  assert.equal(subagent.origin, "provider_native");
  assert.equal(subagent.driver, "omp");
  assert.isNull(subagent.childThreadId);
  assert.equal(subagent.runId, projection.runs[0]!.id);
  assert.equal(subagent.status, "completed");
  assert.isNotEmpty(subagent.prompt);
  assert.match(subagent.result ?? "", /^Hello\.?$/u);
  assert.isNotNull(subagent.completedAt);
  assertRowUnderTaskCall(projection, subagent);
  assert.deepEqual(projection.providerThreads[0]?.pendingBackgroundTasks ?? [], []);
}
