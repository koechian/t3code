/**
 * Slash-command catalog from `get_available_commands` / `available_commands_update`, the
 * commands T3 intercepts instead of sending, and ANSI stripping for `command_output`.
 */
import * as NodeUtil from "node:util";

import type { ServerProviderSkill, ServerProviderSlashCommand } from "@t3tools/contracts";

import type { OmpAvailableSlashCommand } from "./wire.ts";

export interface OmpDiscoveredCommands {
  readonly slashCommands: ReadonlyArray<ServerProviderSlashCommand>;
  readonly skills: ReadonlyArray<ServerProviderSkill>;
}

const SKILL_COMMAND_PREFIX = "skill:";
// omp fills `input.hint` with this placeholder for extension, custom and skill commands.
const PLACEHOLDER_INPUT_HINT = "arguments";

/**
 * Commands T3's composer offers in every state (`apps/web/src/components/chat/useComposerMenuItems.ts`,
 * `apps/mobile/src/features/threads/use-composer-command-menu.ts`). It offers `/side`, `/btw` and
 * `/clear` only sometimes and hides a provider's `/clear` itself, so omp's commands of those names stay.
 */
const T3_PERMANENT_COMPOSER_COMMANDS: ReadonlySet<string> = new Set(["model"]);

/** T3 runs `/goal` through omp's `goal` command; omp offers `/goal` only in its terminal app. */
export const OMP_GOAL_SLASH_COMMAND: ServerProviderSlashCommand = {
  name: "goal",
  description: "Set a goal OMP keeps working toward until it is done",
  input: { hint: "Objective, or pause, resume, clear" },
};

function offeredInComposer(name: string): boolean {
  return (
    !OMP_INTERCEPTED_COMMANDS.has(name) &&
    !T3_PERMANENT_COMPOSER_COMMANDS.has(name) &&
    name !== OMP_GOAL_SLASH_COMMAND.name
  );
}

/**
 * `skill:<name>` → skill with path `omp:skill:<name>`; drops the placeholder hint `arguments`.
 * Intercepted commands and names T3's composer always offers are left out, so the composer never
 * offers a command that only produces a notice or that duplicates one of T3's. `/goal` is T3's.
 */
export function parseOmpAvailableCommands(
  commands: ReadonlyArray<OmpAvailableSlashCommand>,
): OmpDiscoveredCommands {
  const slashCommands: Array<ServerProviderSlashCommand> = [];
  const skills: Array<ServerProviderSkill> = [];
  const seen = new Set<string>();
  for (const command of commands) {
    const name = command.name.trim();
    if (name.length === 0 || seen.has(name)) continue;
    seen.add(name);
    if (command.source === "skill") {
      const skill = ompSkill(name, command);
      if (skill !== undefined) skills.push(skill);
    } else if (offeredInComposer(name)) {
      slashCommands.push(ompSlashCommand(name, command));
    }
  }
  return { slashCommands: [...slashCommands, OMP_GOAL_SLASH_COMMAND], skills };
}

function ompSkill(
  commandName: string,
  command: OmpAvailableSlashCommand,
): ServerProviderSkill | undefined {
  const name = commandName.startsWith(SKILL_COMMAND_PREFIX)
    ? commandName.slice(SKILL_COMMAND_PREFIX.length).trim()
    : commandName;
  if (name.length === 0) return undefined;
  const description = nonEmptyTrimmed(command.description);
  return {
    name,
    path: `omp:skill:${name}`,
    enabled: true,
    ...(description === undefined ? {} : { description }),
  };
}

function ompSlashCommand(
  name: string,
  command: OmpAvailableSlashCommand,
): ServerProviderSlashCommand {
  const description = nonEmptyTrimmed(command.description);
  const hint = nonEmptyTrimmed(command.input?.hint);
  return {
    name,
    ...(description === undefined ? {} : { description }),
    ...(hint === undefined || hint === PLACEHOLDER_INPUT_HINT ? {} : { input: { hint } }),
  };
}

function nonEmptyTrimmed(value: string | undefined): string | undefined {
  const trimmed = value?.trim();
  return trimmed === undefined || trimmed.length === 0 ? undefined : trimmed;
}

/**
 * Built-ins omp runs over RPC that act on the omp host, move the session file T3 tracks, or
 * print account details. Built-ins win over every other command, so these names always mean
 * the built-in. Key: command name or alias without `/`; value: the notice T3 shows instead.
 */
export const OMP_INTERCEPTED_COMMANDS: ReadonlyMap<string, string> = new Map([
  // These install or start code on the host with no tool approval, whatever the thread's mode.
  [
    "mcp",
    "/mcp changes omp's MCP servers and can start their commands on the omp host, so T3 Code does not run it. Run it in omp's terminal app.",
  ],
  [
    "marketplace",
    "/marketplace installs plugin code on the omp host, so T3 Code does not run it. Run it in omp's terminal app.",
  ],
  [
    "plugins",
    "/plugins installs or enables plugin code on the omp host, so T3 Code does not run it. Run it in omp's terminal app.",
  ],
  [
    "plugin",
    "/plugin installs or enables plugin code on the omp host, so T3 Code does not run it. Run it in omp's terminal app.",
  ],
  [
    "reload-plugins",
    "/reload-plugins loads plugin code on the omp host, so T3 Code does not run it. Run it in omp's terminal app.",
  ],
  [
    "ssh",
    "/ssh changes the SSH hosts omp can connect to, so T3 Code does not run it. Run it in omp's terminal app.",
  ],
  ["stats", "/stats opens a local web dashboard on the omp host, so T3 Code does not run it."],
  ["usage", "/usage prints account details, so T3 Code does not relay it. See Usage in T3 Code."],
  ["trace", "/trace is not available in T3 Code."],
  ["share", "/share is not available in T3 Code."],
  ["export", "/export is not available in T3 Code."],
  ["browser", "/browser is not available in T3 Code."],
  [
    "move",
    "/move moves this thread's omp session to another directory, so T3 Code does not run it.",
  ],
  ["wt", "/wt moves this thread's omp session into another worktree, so T3 Code does not run it."],
  [
    "worktree",
    "/worktree moves this thread's omp session into another worktree, so T3 Code does not run it.",
  ],
]);

// `/session` subcommands that delete the thread's session file or list stored OAuth accounts.
const OMP_INTERCEPTED_SESSION_SUBCOMMANDS: ReadonlyMap<string, string> = new Map([
  [
    "delete",
    "/session delete deletes this thread's omp session, so T3 Code does not run it. Delete the thread in T3 Code instead.",
  ],
  ["pin", "/session pin prints account details, so T3 Code does not run it."],
]);

/**
 * omp built-ins with a terminal handler and no RPC handler (`handleTui` without `handle` in
 * omp's slash-commands/builtin-*.ts), with their aliases. Over RPC they reach the model as
 * literal text. They reserve no name, so a catalog command of the same name runs instead.
 */
const OMP_TERMINAL_ONLY_COMMANDS: ReadonlySet<string> = new Set([
  "settings",
  "setup",
  "providers",
  "plan",
  "plan-review",
  "vibe",
  "goal",
  "guided-goal",
  "loop",
  "queue",
  "collab",
  "join",
  "leave",
  "copy",
  "open",
  "hotkeys",
  "extensions",
  "status",
  "agents",
  "git",
  "local",
  "hub",
  "subagents",
  "branch",
  "rewind",
  "fork",
  "tree",
  "login",
  "logout",
  "new",
  "clear",
  "delete",
  "resume",
  "btw",
  "tan",
  "omfg",
  "cleanse",
  "debug",
  "exit",
  "restart",
  "skills",
  "live",
  "record",
  "pause",
  "quit",
  "q",
]);

const OMP_TERMINAL_ONLY_NOTICES: ReadonlyMap<string, string> = new Map([
  ["login", "Sign in to omp from a terminal with `omp`, then retry."],
  ["settings", "/settings is a terminal screen; edit omp's config file instead."],
  ["plan", "Plan mode is not available for omp in T3 Code."],
  ["goal", "/goal takes text only. Send it again without attachments."],
  ["new", "Start a new thread in T3 Code instead of /new."],
  ["resume", "Open the thread in T3 Code instead of /resume."],
]);

export type OmpSlashDisposition =
  | { readonly kind: "send" }
  | { readonly kind: "intercept"; readonly notice: string }
  | { readonly kind: "compact"; readonly customInstructions?: string };

const SEND: OmpSlashDisposition = { kind: "send" };
const NO_CATALOG_NAMES: ReadonlySet<string> = new Set();

// The typed `compact` command carries no mode, so `/compact <mode> …` stays a slash command.
const OMP_COMPACT_MODES: ReadonlySet<string> = new Set([
  "soft",
  "remote",
  "snapcompact",
  "handoff",
]);

interface OmpSlashCommand {
  readonly name: string;
  readonly args: string;
}

/** omp's own split (slash-commands/helpers/parse.ts): the name ends at whitespace or `:`. */
function parseOmpSlashCommand(text: string): OmpSlashCommand | undefined {
  const trimmed = text.trim();
  if (!trimmed.startsWith("/")) return undefined;
  const body = trimmed.slice(1);
  const separator = body.search(/[\s:]/);
  if (separator === -1) return body.length === 0 ? undefined : { name: body, args: "" };
  return { name: body.slice(0, separator), args: body.slice(separator + 1).trim() };
}

/** omp's `parseSubcommand` verb: the first word of the arguments, lower-cased. */
function subcommandVerb(args: string): string {
  return (args.split(/\s/, 1)[0] ?? "").toLowerCase();
}

function terminalOnlyNotice(name: string): string {
  return (
    OMP_TERMINAL_ONLY_NOTICES.get(name) ??
    `/${name} only works in omp's terminal app, so T3 Code does not send it.`
  );
}

function interceptNotice(
  command: OmpSlashCommand,
  catalogNames: ReadonlySet<string>,
): string | undefined {
  if (command.name === "session") {
    return OMP_INTERCEPTED_SESSION_SUBCOMMANDS.get(subcommandVerb(command.args));
  }
  const notice = OMP_INTERCEPTED_COMMANDS.get(command.name);
  if (notice !== undefined) return notice;
  if (!OMP_TERMINAL_ONLY_COMMANDS.has(command.name) || catalogNames.has(command.name)) {
    return undefined;
  }
  return terminalOnlyNotice(command.name);
}

function compactDisposition(command: OmpSlashCommand): OmpSlashDisposition {
  if (command.name !== "compact" || OMP_COMPACT_MODES.has(subcommandVerb(command.args))) {
    return SEND;
  }
  return command.args.length === 0
    ? { kind: "compact" }
    : { kind: "compact", customInstructions: command.args };
}

/**
 * `/compact [instructions]` maps to the typed `compact` command; everything else is a `prompt`.
 * `catalogNames` are the command names of the session's latest catalog: a terminal-only name
 * is sent when the catalog has a command of that name.
 */
export function classifyOmpSlashInput(
  text: string,
  catalogNames: ReadonlySet<string> = NO_CATALOG_NAMES,
): OmpSlashDisposition {
  const command = parseOmpSlashCommand(text);
  if (command === undefined) return SEND;
  const notice = interceptNotice(command, catalogNames);
  return notice === undefined ? compactDisposition(command) : { kind: "intercept", notice };
}

/**
 * Whether `text` runs a command rather than reaching the model: T3 intercepts or compacts it, or
 * the catalog names it (`/name` or `/skill:name`). omp delivers a steer as plain text and never
 * dispatches a command from one (R§59), so such a steer must start a turn instead.
 */
export function isOmpCommandInvocation(text: string, catalogNames: ReadonlySet<string>): boolean {
  const command = parseOmpSlashCommand(text);
  if (command === undefined) return false;
  if (classifyOmpSlashInput(text, catalogNames).kind !== "send") return true;
  const token = text.trim().slice(1).split(/\s/, 1)[0] ?? "";
  return catalogNames.has(command.name) || catalogNames.has(token);
}

export function stripAnsi(text: string): string {
  return NodeUtil.stripVTControlCharacters(text);
}
