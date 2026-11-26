import {
  THREAD_ROLLBACK_FIRST_PROMPT,
  THREAD_ROLLBACK_SECOND_PROMPT,
  type OrchestratorFixtureInput,
} from "../shared.ts";

// Anthropic refuses the shared "Repeat the conversation verbatim." under omp's system prompt
// as reasoning extraction, so this prompt asks only for the reply that survived the rollback.
export const OMP_THREAD_ROLLBACK_AFTER_PROMPT =
  "What did you reply to my previous message? Answer with that reply only.";

export function ompThreadRollbackInput(): OrchestratorFixtureInput {
  return {
    steps: [
      { type: "message", text: THREAD_ROLLBACK_FIRST_PROMPT },
      { type: "message", text: THREAD_ROLLBACK_SECOND_PROMPT },
      { type: "rollback", checkpointScopeSuffix: "root", checkpointSuffix: "1" },
      { type: "message", text: OMP_THREAD_ROLLBACK_AFTER_PROMPT },
    ],
  };
}
