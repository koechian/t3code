# OMP

T3 Code runs [OMP](https://omp.sh) (Oh My Pi), a coding agent built on Pi, with your own OMP
models, sign-ins, and configuration.

## Set Up OMP

1. Install OMP on the machine running the T3 Code server. T3 Code supports OMP 19.1.1 or newer.

   ```sh
   curl -fsSL https://omp.sh/install | sh
   ```

   With Homebrew, run `brew install can1357/tap/omp`. With Bun 1.4.4 or newer, run
   `bun install -g @oh-my-pi/pi-coding-agent`. OMP's
   [install guide](https://github.com/can1357/oh-my-pi#install) lists Windows, Nix, and mise
   options.

2. Run `omp` in a terminal on that machine, then `/login` to sign in to a model provider or save
   its API key.
3. Open **Settings → Providers**, enable OMP, and refresh it.

If `omp` is not on the server's `PATH`, set **Binary path** to the executable. **Launch arguments**
are added to every OMP session, for example `--profile work` or `--model <provider>/<model>`.
T3 Code rejects arguments that set OMP's run mode, session, working directory, approval mode, plan
or goal mode, or `--append-system-prompt`, because T3 Code controls those, and it rejects prompts
in the arguments. Put extra instructions in `APPEND_SYSTEM.md` instead. A `--config` file you pass
still applies, but cannot change the permission mode.

On a remote or headless server, connect over SSH and run `omp`, then `/login`. Open the sign-in
link on your own computer. If the redirect cannot reach the server, run `/login` followed by the
final redirect URL to finish. You can also set your model provider's API-key environment variable
in the OMP instance's settings; mark secret values as sensitive.

When a newer OMP release is out, its card in **Settings → Providers** offers **Update now**. To
update by hand, run `omp update` on the server's machine. It updates through Homebrew, mise, Bun, or
npm when one of them installed OMP. Update a Nix installation through Nix.

## What Carries Over

The model picker lists the models OMP can use with your sign-ins. **OMP default** runs whatever
model OMP would start with: your OMP default, or `--model` from the launch arguments. A model's
**Thinking** setting lists the levels OMP offers for it, and **Auto** lets OMP choose for each
turn. The level marked as default is the one OMP uses for that model.

OMP loads your configuration as it does in a terminal: settings, MCP servers, skills, rules,
context files, plugins and extensions, and the advisor. T3 Code adds its own instructions to OMP's
system prompt and keeps your `APPEND_SYSTEM.md`.

OMP threads can also use T3 Code's own tools, such as `delegate_task`. If those tools cannot be
reached, the thread shows a notice and OMP carries on without them.

Switching a thread to another provider passes the conversation as
[portable context](./portable-handoffs.md). OMP can also write thread titles, commit messages, and
other generated text. Those runs use your OMP models and sign-ins, but none of your tools,
extensions, skills, rules, or context files.

## Permission Modes

T3 Code sets OMP's own approval mode for each thread:

| T3 Code mode          | OMP approval mode | Behavior                                                                                                                      |
| --------------------- | ----------------- | ----------------------------------------------------------------------------------------------------------------------------- |
| **Supervised**        | `always-ask`      | OMP asks before file changes, commands, and MCP tools. Read-only tools, including T3 Code's, run without asking.              |
| **Auto-accept edits** | `write`           | OMP changes files and runs tools from your MCP servers without asking. It asks before commands and other tools that run code. |
| **Full access**       | `yolo`            | OMP runs tools without asking.                                                                                                |

T3 Code offers no **Auto** for OMP. A thread already set to **Auto** runs in **Supervised**. The
mode applies only to T3 Code's sessions, so OMP in your terminal keeps its own default. Your
`tools.approval` rules and `tools.restrictedFolders` still apply, so OMP can ask even in **Full
access**.

Changing the mode stops the thread's OMP process. Your next message starts OMP in the new mode and
reopens the same conversation. While OMP still runs background commands, T3 Code refuses the change
until they finish or you press **Stop**.

Approvals show OMP's own choices: approve once, approve that tool for the rest of the session when
OMP offers it, or **Deny**. To deny with an explanation, type a reply while the approval is pending;
OMP gets the denial and your message.

## Questions and Forms

Questions from OMP's ask tool and from extensions appear in the thread: choices, yes or no
confirmations, text input, and longer text to edit.

Forms that OMP shows appear as a set of questions with OMP's defaults filled in. If you dismiss a
form without answering, OMP receives its default values and continues. Press **Stop** to cancel the
turn instead. A form T3 Code cannot show, or one with a password field, is submitted with its
defaults, and the thread shows a notice.

T3 Code declines prompts for secret input, such as an extension asking for an API key, and shows a
notice. Enter those values in OMP in a terminal.

Extension notifications appear in the thread. Extension status text, widgets, and terminal titles
have no T3 Code equivalent.

## Commands and Steering

The `/` menu lists OMP's commands. Commands from extensions and MCP servers appear once the thread's
OMP session has started. OMP skills appear in the composer's `$` menu. `/compact`, with optional
instructions, runs OMP's own compaction.

Some commands show a notice instead of running:

- `/stats`, `/usage`, `/trace`, `/share`, `/export`, and `/browser`.
- `/move`, `/wt`, `/worktree`, `/session delete`, and `/session pin`, which would move or delete the
  thread's session or print account details.
- `/mcp`, `/marketplace`, `/plugins`, `/reload-plugins`, and `/ssh`, which change OMP's setup on the
  server or install and start code there without asking for approval. Run them in OMP in a terminal.
- Commands that only work in OMP's terminal app, such as `/settings`, `/login`, `/plan`, `/new`,
  `/resume`, and `/exit`. Use T3 Code's own controls instead, such as a new thread for `/new`.

A message you send while OMP is working joins the running turn when OMP finishes its current step,
without stopping running tools. It can include images. A slash command sent while OMP is working
waits and runs as its own turn.

## Rewind and Fork

**Edit from here** also rewinds OMP's own conversation to before that message. Forking a thread from
a message gives the fork OMP's own copy of the conversation up to that message, including a fork
into another worktree. Wait for OMP to finish its turn before you rewind or fork.

## Sessions

T3 Code keeps its OMP sessions in `omp-sessions` in the server's `userdata` directory, such as
`~/.t3/userdata/omp-sessions`, apart from OMP's own sessions, so they do not appear in OMP's
session list for `/resume` or `omp --resume`. Deleting a thread leaves its session file in that
folder.

OMP sessions from your terminal do not appear in T3 Code, and the welcome page does not import them
yet. To continue one in T3 Code, start a new thread and give it the context it needs.

## Usage

[Usage](./usage.md) includes OMP threads that ran in T3 Code and the OMP sessions you run in a
terminal on the server. Models appear with OMP's provider prefix, such as `anthropic/…`. Text OMP
writes for T3 Code, such as thread titles, is not counted.

## Known Limits

- The model picker lists up to 200 of OMP's models. Add any other model under **Custom models** in
  the OMP instance settings as `<provider>/<model>`.
- Secret input prompts are declined, and forms with password fields are submitted with their
  defaults.
- T3 Code needs OMP 19.1.1 or newer.
- OMP's plan mode and goals are not available. Work OMP hands to its own subagents shows as one tool
  call, not as child threads.
- With `--profile`, T3 Code keeps the `APPEND_SYSTEM.md` from the project or your default OMP
  folder, not one from the profile.

## Move From a Local ACP Command

If you ran OMP through **Local ACP command** (`omp acp`), move to the OMP provider:

1. Enable OMP in **Settings → Providers**. Copy the ACP instance's environment variables to it, and
   put any OMP flags the ACP command used, apart from `acp`, in **Launch arguments**.
2. Existing threads stay on the ACP instance. To continue one on the OMP provider, pick an OMP model
   in that thread's model picker. T3 Code passes the conversation as
   [portable context](./portable-handoffs.md), and OMP starts a new session.
3. When no thread still needs the ACP instance, turn it off or remove it.

If the ACP instance's ID is `omp`, the built-in OMP entry does not appear. Add OMP with **Add
provider** instead, which picks a free ID.

## Troubleshooting

- If OMP is unavailable, confirm that the configured binary runs on the server machine, then
  refresh the provider in Settings.
- If the OMP card reports no default model or no usable models, run `omp` on the server machine and
  use `/login`, set a provider API-key environment variable in the instance settings, or pass
  `--model` in the launch arguments. If OMP's `enabledModels` setting limits your models, include
  one you are signed in to.
- If the provider check times out, allow more time with `T3CODE_OMP_PROBE_TIMEOUT_MS`. See
  [provider check timeouts](./install.md#provider-check-timeouts).
