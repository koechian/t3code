import { assert, describe, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as ChildProcess from "effect/process/ChildProcess";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";

import { makeOmpRpcConnection } from "./rpc.ts";
import {
  FAKE_OMP_IDLE_STATE,
  FAKE_OMP_UNANSWERED,
  fakeOmpSuccess,
  makeFakeOmpProcess,
  makeFakeOmpRpcConnection,
} from "./rpc.testkit.ts";

const decodeJsonLine = Schema.decodeSync(Schema.fromJsonString(Schema.Unknown));

describe("makeFakeOmpRpcConnection", () => {
  it.effect(
    "queues a caller-id command's reply in order and keeps request replies off events",
    () =>
      Effect.gen(function* () {
        const fake = yield* makeFakeOmpRpcConnection({
          reply: (command) =>
            command.type === "prompt"
              ? fakeOmpSuccess(
                  { agentInvoked: false },
                  {
                    before: [
                      { type: "command_output", text: "done" },
                      { type: "response", command: "parse", success: false, error: "bad" },
                    ],
                  },
                )
              : undefined,
        });

        const state = yield* fake.connection.request({ type: "get_state" });
        yield* fake.connection.send({ type: "prompt", id: "t3-prompt-0", message: "/fresh" });

        assert.deepStrictEqual(state, { id: "t3-0", data: FAKE_OMP_IDLE_STATE });
        assert.deepStrictEqual(yield* Queue.takeAll(fake.connection.events), [
          { type: "command_output", text: "done" },
          {
            type: "response",
            id: "t3-prompt-0",
            command: "prompt",
            success: true,
            data: { agentInvoked: false },
          },
        ]);
        assert.deepStrictEqual(
          fake.commands().map((command) => command.id),
          ["t3-0", "t3-prompt-0"],
        );
      }),
  );

  it.effect("settles an unanswered request when the test responds", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeOmpRpcConnection({
        reply: (command) => (command.type === "compact" ? FAKE_OMP_UNANSWERED : undefined),
      });

      const compact = yield* fake.connection.request({ type: "compact" }).pipe(Effect.forkChild);
      yield* fake.respond(yield* fake.takeCommand("compact"), { summary: "s" });

      assert.deepStrictEqual(yield* Fiber.join(compact), { id: "t3-0", data: { summary: "s" } });
    }),
  );
});

describe("makeFakeOmpProcess", () => {
  it.effect("connects through the real transport and exits once it closes stdin", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const fake = yield* makeFakeOmpProcess();
        const connection = yield* makeOmpRpcConnection({
          command: "omp",
          args: ["--mode", "rpc-ui"],
          cwd: undefined,
          env: {},
        }).pipe(Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, fake.spawner));

        yield* connection.terminate;

        assert.strictEqual(yield* connection.exited, 0);
        assert.deepStrictEqual(
          fake.commands().map((command) => command.type),
          ["negotiate_protocol"],
        );
      }),
    ),
  );

  it.effect("speaks the JSONL wire to a spawned RPC process and scripts CLI runs", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const fake = yield* makeFakeOmpProcess({
          cli: ({ args }) =>
            args[0] === "--version" ? { stdout: "omp/19.1.1\n", exitCode: 0 } : undefined,
        });

        const rpc = yield* fake.spawner.spawn(ChildProcess.make("omp", ["--mode", "rpc-ui"]));
        yield* Stream.run(
          Stream.make(
            new TextEncoder().encode(
              '{"type":"negotiate_protocol","protocolVersion":2,"id":"n"}\n',
            ),
          ),
          rpc.stdin,
        );
        const lines = yield* rpc.stdout.pipe(
          Stream.decodeText(),
          Stream.splitLines,
          Stream.take(2),
          Stream.runCollect,
        );
        const version = yield* fake.spawner.string(ChildProcess.make("omp", ["--version"]));

        assert.deepStrictEqual(Array.from(lines, (line) => decodeJsonLine(line))[1], {
          type: "response",
          id: "n",
          command: "negotiate_protocol",
          success: true,
          data: { protocolVersion: 2 },
        });
        assert.strictEqual(version, "omp/19.1.1\n");
        assert.deepStrictEqual(
          fake.spawns().map((spawn) => spawn.args),
          [["--mode", "rpc-ui"], ["--version"]],
        );
      }),
    ),
  );
});
