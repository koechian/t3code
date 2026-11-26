/**
 * Titles, commit messages and PR text through `omp -p --mode json` (print mode, no tools,
 * no extensions, advisor and memory off). Not RPC: omp never emits Pi's `agent_settled`.
 */
import { ProviderDriverKind, TextGenerationError } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as ChildProcess from "effect/process/ChildProcess";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";
import { resolveSpawnCommand } from "@t3tools/shared/shell";
import {
  brokenVersionDetail,
  type ProviderCompatibilityLookup,
} from "@t3tools/provider-core/server/compatibility";
import {
  collectStreamAsString,
  parseCliVersionResult,
} from "@t3tools/provider-core/server/snapshotProbe";
import type { ProviderTextGeneration } from "@t3tools/provider-core/server/textGeneration";
import * as TextGenerationOperations from "@t3tools/provider-core/server/textGenerationOperations";

import type { OmpSettings } from "../settings.ts";
import {
  buildOmpPrintLaunch,
  renderOmpTextGenerationOverlay,
  resolveOmpLaunchArgs,
  writeOmpConfigOverlay,
  type OmpLaunch,
  type OmpUnattendedOverlay,
} from "./launch.ts";
import {
  findOmpPickerModel,
  OMP_DEFAULT_MODEL_SLUG,
  OMP_MODEL_PICKER_LIMIT,
} from "./modelCapabilities.ts";
import { probeOmpModelPicker, runOmpVersionCommand } from "./status.ts";

/** Wall-clock and spend caps passed as `--max-time` / `--max-cost` to every print run. */
const OMP_TEXT_GENERATION_MAX_TIME_SECONDS = 60;
const OMP_TEXT_GENERATION_MAX_COST_USD = 0.25;

const OMP_DRIVER_KIND = ProviderDriverKind.make("omp");
// omp enforces --max-time itself; this covers the version check, model lookup and shutdown too.
const OMP_TEXT_GENERATION_TIMEOUT_MS = (OMP_TEXT_GENERATION_MAX_TIME_SECONDS + 60) * 1_000;
const OMP_VERSION_CHECK_TIMEOUT_MS = 4_000;
const OMP_PRINT_MAX_STDOUT_BYTES = 8 * 1024 * 1024;
/** `PRINT_MODE_COST_CAP_EXIT_CODE` in omp's modes/print-mode.ts. */
const OMP_PRINT_COST_CAP_EXIT_CODE = 3;
const OMP_TEXT_GENERATION_OVERLAY_FILE = "text-generation.yml";
// Replaces omp's default system prompt (tool docs, rules), so each request costs one short call.
const OMP_TEXT_GENERATION_SYSTEM_PROMPT =
  "You write short text for a developer tool: thread titles, commit messages, branch names and pull request descriptions. Follow the user's instructions exactly and reply with only the requested output.";

const isTextGenerationError = Schema.is(TextGenerationError);

const PrintMessageEnd = Schema.Struct({
  type: Schema.Literal("message_end"),
  message: Schema.Struct({
    role: Schema.String,
    content: Schema.optional(Schema.Array(Schema.Unknown)),
    stopReason: Schema.optional(Schema.String),
    errorMessage: Schema.optional(Schema.String),
  }),
});
const decodePrintMessageEnd = Schema.decodeUnknownOption(Schema.fromJsonString(PrintMessageEnd));
const decodeTextContent = Schema.decodeUnknownOption(
  Schema.Struct({ type: Schema.Literal("text"), text: Schema.String }),
);

interface OmpPrintReply {
  readonly text: string;
  readonly stopReason: string | undefined;
  readonly errorMessage: string | undefined;
}

/** The last assistant `message_end`: omp can retry inside one run and still answer. */
function lastAssistantReply(stdout: string): OmpPrintReply | undefined {
  let reply: OmpPrintReply | undefined;
  for (const line of stdout.split("\n")) {
    if (!line.includes('"message_end"')) continue;
    const frame = decodePrintMessageEnd(line);
    if (Option.isNone(frame) || frame.value.message.role !== "assistant") continue;
    const { content, stopReason, errorMessage } = frame.value.message;
    reply = {
      text: (content ?? [])
        .flatMap((part) => Option.toArray(decodeTextContent(part)))
        .map((part) => part.text)
        .join("")
        .trim(),
      stopReason,
      errorMessage,
    };
  }
  return reply;
}

// omp's `errorMessage` is the provider's raw response text and can echo credentials, so only a
// leading HTTP status reaches the error detail.
function providerErrorDetail(errorMessage: string | undefined): string {
  const status = errorMessage?.match(/^([45]\d\d)\b/)?.[1];
  return status === undefined
    ? "OMP could not generate text: the model provider reported an error."
    : `OMP could not generate text: the model provider returned HTTP ${status}.`;
}

function missingReplyDetail(exitCode: number): string {
  if (exitCode === OMP_PRINT_COST_CAP_EXIT_CODE) {
    return `OMP stopped text generation at its $${OMP_TEXT_GENERATION_MAX_COST_USD} cost cap.`;
  }
  return exitCode === 0
    ? "OMP returned no reply."
    : `OMP could not generate text (exit code ${exitCode}).`;
}

/** The reply text, or why there is none. */
function printReplyText(
  stdout: string,
  exitCode: number,
): { readonly text: string } | { readonly error: string } {
  const reply = lastAssistantReply(stdout);
  if (reply === undefined) return { error: missingReplyDetail(exitCode) };
  if (reply.stopReason === "error") return { error: providerErrorDetail(reply.errorMessage) };
  if (reply.stopReason === "aborted") {
    return { error: "OMP stopped text generation before it finished." };
  }
  return reply.text.length === 0 ? { error: "OMP returned empty output." } : { text: reply.text };
}

export interface OmpTextGenerationInput {
  readonly settings: OmpSettings;
  readonly environment: NodeJS.ProcessEnv;
  readonly compatibility: ProviderCompatibilityLookup;
  /** Directory for the text-generation `--config` overlay. */
  readonly overlayDir: string;
  readonly unattendedOverlay: OmpUnattendedOverlay;
}

/**
 * Captures its services at construction, so the returned runner needs none. A custom model slug
 * resolves through `status.probeOmpModelPicker`.
 */
export const makeOmpTextGeneration = Effect.fn("makeOmpTextGeneration")(function* (
  input: OmpTextGenerationInput,
) {
  const { settings, environment, compatibility, overlayDir, unattendedOverlay } = input;
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const overlayPath = path.join(overlayDir, OMP_TEXT_GENERATION_OVERLAY_FILE);
  const binaryPath = settings.binaryPath || "omp";
  let overlayWritten = false;

  const ensureOverlay = Effect.suspend(() =>
    overlayWritten
      ? Effect.void
      : unattendedOverlay.ensure.pipe(
          Effect.andThen(writeOmpConfigOverlay(overlayPath, renderOmpTextGenerationOverlay())),
          Effect.tap(() =>
            Effect.sync(() => {
              overlayWritten = true;
            }),
          ),
          Effect.provideService(FileSystem.FileSystem, fileSystem),
          Effect.provideService(Path.Path, path),
        ),
  );

  /** The broken-version message when the policy rates this omp broken; unknown versions pass. */
  const brokenVersion = Effect.fn("OmpTextGeneration.brokenVersion")(function* (cwd: string) {
    const output = yield* runOmpVersionCommand(settings, environment, cwd).pipe(
      Effect.timeoutOption(OMP_VERSION_CHECK_TIMEOUT_MS),
      Effect.option,
      Effect.map(Option.flatten),
      Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
    );
    const version = Option.isSome(output) ? parseCliVersionResult(output.value) : null;
    if (version === null) return undefined;
    return brokenVersionDetail({
      name: "OMP",
      version,
      advisory: yield* compatibility(OMP_DRIVER_KIND, version),
    });
  });

  // `--model` falls back to omp's default for an unknown id instead of failing, so a custom slug
  // must match a model the passive probe lists.
  const resolvePrintModel = Effect.fn("OmpTextGeneration.resolvePrintModel")(function* (
    operation: TextGenerationOperations.Operation,
    slug: string,
    cwd: string,
  ) {
    if (slug === OMP_DEFAULT_MODEL_SLUG) return undefined;
    const picker = yield* probeOmpModelPicker(
      settings,
      environment,
      cwd,
      { query: slug, limit: OMP_MODEL_PICKER_LIMIT },
      unattendedOverlay,
    ).pipe(
      Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
      Effect.mapError(
        (cause) =>
          new TextGenerationError({ operation, detail: "OMP could not list its models.", cause }),
      ),
    );
    const row = findOmpPickerModel(slug, picker.rows);
    if (row === undefined) {
      return yield* new TextGenerationError({
        operation,
        detail: `OMP model '${slug}' is not available for text generation, which runs omp without extensions. Pick a model omp lists, or use the OMP default.`,
      });
    }
    return { provider: row.provider, modelId: row.id };
  });

  const runPrint = Effect.fn("OmpTextGeneration.runPrint")(function* (
    launch: OmpLaunch,
    cwd: string,
    prompt: string,
  ) {
    const spawnCommand = yield* resolveSpawnCommand(binaryPath, launch.args, {
      env: launch.env,
    });
    const child = yield* spawner.spawn(
      ChildProcess.make(spawnCommand.command, spawnCommand.args, {
        env: launch.env,
        cwd,
        shell: spawnCommand.shell,
        forceKillAfter: "1 second",
      }),
    );
    const [stdout, exitCode] = yield* Effect.all(
      [
        collectStreamAsString(child.stdout, { maxBytes: OMP_PRINT_MAX_STDOUT_BYTES }),
        child.exitCode,
        Stream.run(Stream.encodeText(Stream.make(prompt)), child.stdin),
        Stream.runDrain(child.stderr),
      ],
      { concurrency: "unbounded" },
    );
    return printReplyText(stdout, Number(exitCode));
  }, Effect.scoped);

  const runOmpJson: TextGenerationOperations.Runner = Effect.fn("OmpTextGeneration.runOmpJson")(
    function* (request) {
      const { operation, cwd, prompt, modelSelection } = request;
      const broken = yield* brokenVersion(cwd);
      if (broken !== undefined) {
        return yield* new TextGenerationError({ operation, detail: broken });
      }
      const launchArgs = resolveOmpLaunchArgs(settings.launchArgs);
      if (!launchArgs.ok) {
        return yield* new TextGenerationError({ operation, detail: launchArgs.message });
      }
      const model = yield* resolvePrintModel(operation, modelSelection.model, cwd);
      yield* ensureOverlay;
      const launch = buildOmpPrintLaunch({
        launchArgs: launchArgs.args,
        environment,
        model,
        systemPrompt: OMP_TEXT_GENERATION_SYSTEM_PROMPT,
        unattendedOverlayPath: unattendedOverlay.path,
        overlayPath,
        maxTimeSeconds: OMP_TEXT_GENERATION_MAX_TIME_SECONDS,
        maxCostUsd: OMP_TEXT_GENERATION_MAX_COST_USD,
      });
      const reply = yield* runPrint(launch, cwd, prompt);
      if ("error" in reply) {
        return yield* new TextGenerationError({ operation, detail: reply.error });
      }
      return yield* TextGenerationOperations.decodeJsonReply(request, "OMP", reply.text);
    },
    (effect, request) =>
      effect.pipe(
        Effect.timeoutOption(OMP_TEXT_GENERATION_TIMEOUT_MS),
        Effect.flatMap(
          Option.match({
            onNone: () =>
              Effect.fail(
                new TextGenerationError({
                  operation: request.operation,
                  detail: "OMP request timed out.",
                }),
              ),
            onSome: Effect.succeed,
          }),
        ),
        Effect.mapError((cause) =>
          isTextGenerationError(cause)
            ? cause
            : new TextGenerationError({
                operation: request.operation,
                detail: "OMP text generation failed.",
                cause,
              }),
        ),
      ),
  );

  return TextGenerationOperations.fromRunner(
    "OmpTextGeneration",
    TextGenerationOperations.withoutInvestigation("OMP", runOmpJson),
  ) satisfies ProviderTextGeneration;
});
