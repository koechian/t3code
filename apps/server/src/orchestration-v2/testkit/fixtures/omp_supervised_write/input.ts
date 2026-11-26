import { TOOL_CALL_WRITE_PROMPT, type OrchestratorFixtureInput } from "../shared.ts";

/** Supervised runs omp with `tools.approvalMode: always-ask`, so omp asks before the write. */
export function ompSupervisedWriteInput(): OrchestratorFixtureInput {
  return {
    runtimeMode: "approval-required",
    steps: [
      { type: "message", text: TOOL_CALL_WRITE_PROMPT },
      { type: "approve_next_runtime_request" },
    ],
  };
}
