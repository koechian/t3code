import { describe, expect, it } from "vite-plus/test";

import catalog from "./__fixtures__/commands/available-commands.json" with { type: "json" };
import contextOutput from "./__fixtures__/commands/context-command-output.json" with { type: "json" };
import {
  classifyOmpSlashInput,
  isOmpCommandInvocation,
  OMP_INTERCEPTED_COMMANDS,
  parseOmpAvailableCommands,
  stripAnsi,
} from "./commands.ts";

describe("parseOmpAvailableCommands", () => {
  const discovered = parseOmpAvailableCommands(catalog.commands);

  it("turns skill commands into skills with a synthesized path", () => {
    expect(discovered.skills).toEqual([
      {
        name: "handoff",
        path: "omp:skill:handoff",
        enabled: true,
        description:
          "Compact the current conversation into a handoff document for another agent to pick up.",
      },
      expect.objectContaining({ name: "research", path: "omp:skill:research", enabled: true }),
    ]);
  });

  it("keeps real input hints, drops the placeholder hint and leaves out intercepted commands", () => {
    const byName = new Map(discovered.slashCommands.map((command) => [command.name, command]));
    expect(byName.get("compact")).toEqual({
      name: "compact",
      description: "Compact the conversation",
      input: { hint: "[soft|remote|snapcompact|handoff] [focus]" },
    });
    expect(byName.get("autoresearch")?.input).toBeUndefined();
    expect(byName.get("review")).toEqual({
      name: "review",
      description: "Launch interactive code review",
    });
    expect(byName.get("init")).toEqual({
      name: "init",
      description: "Generate AGENTS.md for current codebase",
    });
    expect([...byName.keys()].filter((name) => OMP_INTERCEPTED_COMMANDS.has(name))).toEqual([]);
    expect(discovered.slashCommands.some((command) => command.name.startsWith("skill:"))).toBe(
      false,
    );
  });

  it("leaves out omp's /model, which T3's composer always offers, and keeps names it offers only sometimes", () => {
    const names = parseOmpAvailableCommands([
      ...catalog.commands,
      { name: "side", description: "A user's side command", source: "file" },
      { name: "btw", description: "An extension's btw command", source: "extension" },
      { name: "clear", description: "A custom clear command", source: "custom" },
    ]).slashCommands.map((command) => command.name);
    expect(catalog.commands.map((command) => command.name)).toContain("model");
    expect(names).not.toContain("model");
    expect(names).toEqual(expect.arrayContaining(["side", "btw", "clear", "switch"]));
  });

  it("leaves out /move and /wt but keeps /session and catalog commands with terminal-only names", () => {
    const names = parseOmpAvailableCommands([
      ...catalog.commands,
      { name: "plan", description: "Draft a plan", source: "custom" },
    ]).slashCommands.map((command) => command.name);
    expect(names).not.toContain("move");
    expect(names).not.toContain("wt");
    expect(names).toContain("session");
    expect(names).toContain("plan");
  });

  it("keeps the first of two commands with the same name and skips blank names", () => {
    expect(
      parseOmpAvailableCommands([
        { name: "init", description: "Generate AGENTS.md", source: "file" },
        { name: "init", description: "Shadowed", source: "custom" },
        { name: "  ", description: "Blank", source: "custom" },
      ]).slashCommands,
    ).toEqual([{ name: "init", description: "Generate AGENTS.md" }]);
  });
});

describe("classifyOmpSlashInput", () => {
  const intercepted = (name: string) => ({
    kind: "intercept",
    notice: OMP_INTERCEPTED_COMMANDS.get(name),
  });
  const terminalOnly = (name: string) => ({
    kind: "intercept",
    notice: `/${name} only works in omp's terminal app, so T3 Code does not send it.`,
  });
  const sessionNotice = (verb: string) => ({
    kind: "intercept",
    notice: expect.stringContaining(`/session ${verb}`),
  });

  it.each([
    ["plain text", "Reply with pong", { kind: "send" }],
    ["a slash command omp runs", "/model anthropic/claude-haiku-5-5", { kind: "send" }],
    ["a skill", "/skill:handoff keep the API notes", { kind: "send" }],
    ["a longer name", "/compactor", { kind: "send" }],
    ["a bare slash", "/", { kind: "send" }],
    ["/compact", "/compact", { kind: "compact" }],
    [
      "/compact with focus",
      "/compact  keep the API notes ",
      { kind: "compact", customInstructions: "keep the API notes" },
    ],
    ["/compact with a mode", "/compact handoff keep notes", { kind: "send" }],
    ["/compact with an upper-case mode", "/compact SOFT", { kind: "send" }],
    ["/stats with args", "/stats --port 0", intercepted("stats")],
    ["/usage after whitespace", "  /usage", intercepted("usage")],
    ["a colon-separated builtin", "/export:out.html", intercepted("export")],
    ["/move", "/move ../elsewhere", intercepted("move")],
    ["/wt", "/wt feature-branch", intercepted("wt")],
    ["the /wt alias", "/worktree feature-branch", intercepted("worktree")],
    ["/mcp add", '/mcp add probe --scope user -- sh -c "touch x"', intercepted("mcp")],
    ["/mcp test", "/mcp test probe", intercepted("mcp")],
    ["/marketplace install", "/marketplace install tool@market", intercepted("marketplace")],
    ["/plugins install", "/plugins install tool", intercepted("plugins")],
    ["the /plugins alias", "/plugin enable tool", intercepted("plugin")],
    ["/reload-plugins", "/reload-plugins", intercepted("reload-plugins")],
    ["/ssh add", "/ssh add build --host build.example", intercepted("ssh")],
    ["/session", "/session", { kind: "send" }],
    ["/session info", "/session info", { kind: "send" }],
    ["/session delete", "/session delete", sessionNotice("delete")],
    ["/session delete in upper case", "/session DELETE", sessionNotice("delete")],
    ["/session delete after a colon", "/session:delete", sessionNotice("delete")],
    ["/session pin", "/session pin", sessionNotice("pin")],
    ["/session pin with an account", "/session pin 2", sessionNotice("pin")],
    ["a terminal-only builtin", "/clear", terminalOnly("clear")],
    ["a terminal-only alias", "/rewind", terminalOnly("rewind")],
    ["the /quit alias", "/q", terminalOnly("q")],
    [
      "/plan",
      "/plan",
      { kind: "intercept", notice: "Plan mode is not available for omp in T3 Code." },
    ],
  ])("classifies %s", (_label, text, disposition) => {
    expect(classifyOmpSlashInput(text)).toEqual(disposition);
  });

  it("sends a terminal-only name that the session's catalog provides", () => {
    const catalogNames = new Set(["plan", "q", "stats", "session"]);
    expect(classifyOmpSlashInput("/plan draft it", catalogNames)).toEqual({ kind: "send" });
    expect(classifyOmpSlashInput("/q", catalogNames)).toEqual({ kind: "send" });
    expect(classifyOmpSlashInput("/clear", catalogNames)).toEqual(terminalOnly("clear"));
    expect(classifyOmpSlashInput("/stats", catalogNames)).toEqual(intercepted("stats"));
    expect(classifyOmpSlashInput("/session delete", catalogNames)).toEqual(sessionNotice("delete"));
  });

  it("sends every builtin omp advertises over RPC except the intercepted ones", () => {
    const sent = catalog.commands
      .filter((command) => command.source === "builtin")
      .filter((command) => !OMP_INTERCEPTED_COMMANDS.has(command.name))
      .filter((command) => command.name !== "compact")
      .map((command) => [command.name, classifyOmpSlashInput(`/${command.name}`).kind]);
    expect(sent.filter(([, kind]) => kind !== "send")).toEqual([]);
  });
});

describe("isOmpCommandInvocation", () => {
  const catalogNames = new Set(["model", "skill:review", "deploy"]);

  it.each([
    ["a catalog builtin", "/model openai/gpt-5"],
    ["a skill", "/skill:review the diff"],
    ["a user command", "/deploy"],
    ["an intercepted builtin", "/stats"],
    ["a terminal-only builtin", "/clear"],
    ["/compact", "/compact keep the plan"],
  ])("treats %s as a command", (_label, text) => {
    expect(isOmpCommandInvocation(text, catalogNames)).toBe(true);
  });

  it.each([
    ["plain text", "also update the docs"],
    ["a path", "/src/index.ts is the entry point"],
    ["an unknown name", "/nope do it"],
  ])("treats %s as text for the model", (_label, text) => {
    expect(isOmpCommandInvocation(text, catalogNames)).toBe(false);
  });
});

describe("stripAnsi", () => {
  it("removes the colour codes from /context output and keeps its text", () => {
    const stripped = stripAnsi(contextOutput.text);
    expect(stripped).not.toContain("\u001b");
    expect(stripped.split("\n").slice(0, 2)).toEqual([
      "Context window: 100000 tokens (13% used)",
      "  System prompt    [█░░░░░░░░░░░░░░░░░░░░░░░] 3%  2746 tokens",
    ]);
  });
});
