import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import { describe, expect, it } from "@effect/vitest";
import { ProjectId, type TerminalEvent } from "@t3tools/contracts";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Queue from "effect/Queue";
import { TestClock } from "effect/testing";

import * as ServerSettings from "../serverSettings.ts";
import * as TerminalManager from "../terminal/Manager.ts";
import * as ProjectService from "./ProjectService.ts";
import * as ProjectSetupScriptRunner from "./ProjectSetupScriptRunner.ts";

const threadId = "thread-1";
const terminalId = "setup-setup";

type TerminalCall =
  | { readonly type: "subscribe"; readonly listener: (event: TerminalEvent) => Effect.Effect<void> }
  | { readonly type: "write"; readonly data: string };

const setup = Effect.gen(function* () {
  const calls = yield* Queue.unbounded<TerminalCall>();
  const terminals = Layer.mock(TerminalManager.TerminalManager)({
    open: (input) =>
      Effect.succeed({
        threadId: input.threadId,
        terminalId: input.terminalId ?? terminalId,
        cwd: input.cwd,
        worktreePath: null,
        status: "running",
        pid: 123,
        history: "",
        exitCode: null,
        exitSignal: null,
        label: "zsh",
        updatedAt: "2026-01-01T00:00:00.000Z",
      }),
    subscribe: (listener) =>
      Queue.offer(calls, { type: "subscribe", listener }).pipe(Effect.as(() => {})),
    write: (input) => Queue.offer(calls, { type: "write", data: input.data }).pipe(Effect.asVoid),
  });
  const runner = yield* ProjectSetupScriptRunner.make.pipe(
    Effect.provide(
      Layer.mergeAll(
        terminals,
        Layer.mock(ProjectService.ProjectService)({}),
        ServerSettings.layerTest(),
        NodeCrypto.layer,
      ),
    ),
    Effect.provideService(HostProcess.Platform, "darwin"),
    Effect.provideService(HostProcess.Environment, { SHELL: "/bin/zsh" }),
  );
  const run = yield* runner
    .runForThread({
      threadId,
      worktreePath: "/repo/.t3/worktrees/feature",
      project: {
        id: ProjectId.make("project-1"),
        workspaceRoot: "/repo",
        scripts: [
          {
            id: "setup",
            name: "Setup",
            command: "vp i",
            icon: "configure",
            runOnWorktreeCreate: true,
          },
        ],
      },
    })
    .pipe(Effect.forkChild);
  const first = yield* Queue.take(calls);
  expect(first.type).toBe("subscribe");
  const emit = first.type === "subscribe" ? first.listener : () => Effect.void;
  const event = { threadId, terminalId, sequence: 1, createdAt: "2026-01-01T00:00:00.000Z" };
  return { run, calls, emit, event };
});

describe("ProjectSetupScriptRunner", () => {
  it.effect("waits for the line editor so a startup prompt cannot read the command", () =>
    Effect.gen(function* () {
      const { run, calls, emit, event } = yield* setup;
      yield* emit({ ...event, type: "output", data: "Do you want to install it? [y/N]: " });
      yield* emit({ ...event, type: "activity", hasRunningSubprocess: true, label: "fnm" });
      yield* TestClock.adjust("1 minute");
      expect(yield* Queue.size(calls)).toBe(0);

      yield* emit({ ...event, type: "activity", hasRunningSubprocess: false, label: "zsh" });
      yield* emit({ ...event, type: "output", data: "\r\n% \x1b[?2004h" });
      yield* Fiber.join(run);
      expect(yield* Queue.take(calls)).toEqual({ type: "write", data: " vp i\r" });
    }),
  );

  it.effect("writes to a quiet shell that has no line editor", () =>
    Effect.gen(function* () {
      const { run, calls } = yield* setup;
      yield* TestClock.adjust("2 seconds");
      yield* Fiber.join(run);
      expect(yield* Queue.take(calls)).toEqual({ type: "write", data: " vp i\r" });
    }),
  );
});
