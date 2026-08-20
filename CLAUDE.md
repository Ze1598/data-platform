# Project instructions for Claude Code

These are standing rules for working in this repo. They come from repeated,
explicit user corrections across multiple sessions — several were stated
more than once because they were violated more than once. Follow them
exactly, not as general guidance to weigh against other considerations.

## Absolute rules — no exceptions, no judgment calls

**Use ASD-STE100** for communication with the user and for writing documentation.

**No usage of soft language** which includes adjectives and adverbs to soften problems or make something sound genuine. Be objective and get to the point of what you're describing.

**Ask questions until you don't need to make assumptions** because assumptions will generate silent problems. The goal at all times is to understand the vision from the user.

**The moment you find ANY issue — a bug, a design inconsistency, unexpected
behavior, anything — STOP THE WORK IMMEDIATELY and surface it for the user
to decide.** Do not: decide it's "out of scope" and route around it; keep
executing the rest of a todo list while it's unresolved; theorize about root
cause further on your own; take any corrective or compensating action
(including things that feel read-only, like nuking a cluster to get back to
a clean state). Report the finding, then wait. This is absolute — there is
no severity threshold below which continuing on your own is fine, and an
earlier "proceed"/"go ahead" for the broader task does not cover a new
finding.

Every single piece of new code development must be validated at runtime. Doesnt' matter if it's a brand new feature, a code change from the backlog, technical debt items, or bug fixes - nothing is considered complete until it gets tested in runtime with a representative test dataset and/or runtime scenario.

Test cases and test scenarios must be agreed upon before a new piece of development. This includes brand new features, code changes from the backlog, technical debt items, and bug fixes.

**Never touch git state, in any way, for any reason — not even a dry run.**
No `git add` (not even `-n`), no `git commit`, no `git push`, no `git
restore`/`reset`. The user handles all git state themselves, including
staging. `git status`/`git diff`/`git log`/`git show` (pure read-only,
no index mutation) are fine for checking state.

**Always ask before making ANY change** — file edits, bringing up
infrastructure, running tests, researching a dependency with intent to use
it. Describing a want, a problem, or an idea is not authorization to act on
it, even mid-sentence alongside an actual request. Ask explicitly ("want me
to implement this now, or just note it for later?") before writing code or
investing real effort down a specific implementation path. Purely read-only
actions that answer a question directly from already-known context don't
need this gate.

**Within already-authorized work, only stop for destructive commands or
commands operating outside this project's own directory.** Once a task is
authorized, don't re-ask permission for each individual command needed to
carry it out — run it. Stop and ask only when a specific command would be
destructive (deletes/overwrites data, force-pushes, drops a table, etc.) or
would navigate/act outside this project's own directory tree. This is a
carve-out from the "ask before making ANY change" gate above, scoped
specifically to routine command execution once work is already underway —
it doesn't relax that gate's requirement to ask before starting a new piece
of work in the first place.

**Default to `just` recipes for actual execution — running code, tests,
features, or infrastructure — never a raw individual command.** This
applies to every agent, not just this session; writing/editing code files
themselves is unaffected, this is about running things. Ad hoc/raw
commands are fine specifically for debugging — that's exactly what each
module's `DebugReference.md` documents, the manual command equivalent of
what its `just` recipes do. But when a codebase or test-suite change
means a recipe's behavior needs to change too, create or edit the
relevant `module.just`/root `Justfile` recipe to match — don't leave it
stale and keep working around it with raw commands going forward. Verify
a recipe name still exists (`just --list`) before using it — this tooling
gets restructured over time.

**A diagnostic question vs. a design/intent question are different.** "Why
is this failing" is discoverable in code/logs/state — investigate it
independently. "What should this system do here" is not discoverable — it's
a decision that exists only in the user's head as the system's designer.
When a failure's fix isn't obviously mechanical (a missing registration
entry, a typo, an off-by-one), pause and ask what the intended design is
rather than reasoning harder toward a confident-looking guess.

**The instant any `.pth`/`ModuleNotFoundError` shows up** (`No module named
'connectors'`, `'raw_to_clean'`, `'polaris_client'`, `'dagster_data_platform'`,
etc.) — kill any running `dagster dev`/`streamlit` process (it holds the uv
cache lock), then immediately run:
```
rm -rf .venv && uv cache clean && uv sync --all-packages
```
Then retry. Do not stop to ask, do not diagnose, do not theorize about
retry-loop timing, do not treat the `.just` recipes' built-in 3x
sweep-and-retry as sufficient — it isn't, and has been observed to fail 3/3
while a full rebuild alone fixes it. Root cause: this repo lives under
`~/Documents`, which has iCloud Desktop & Documents Folders sync enabled;
iCloud's background sync intermittently re-applies the macOS `UF_HIDDEN`
flag to `.venv/lib/.../*.pth` files, and Python 3.13's `site.py` silently
skips hidden `.pth` files. `link-mode = "copy"` is already set in
`pyproject.toml` as a partial mitigation, but a full rebuild is still the
reliable fix — do it proactively before any real verification run, not just
reactively after a failure.

## Process conventions

**Never call something "difficult," "expensive," or a "sunk cost," and
never let effort factor into a decision.** Evaluate purely on what's
architecturally correct long-term. If something is genuinely low-priority,
say so based on relevance/impact, not effort.

**Kill every process/container spun up for a phase's work once it's done.**
`dagster dev` and its whole process tree, `kubectl port-forward`s, the kind
cluster's Docker container (`docker stop data-platform-control-plane`, not
delete — preserves Postgres/Trino/Polaris/MinIO state). Don't leave things
running "just in case" between phases. **Before starting `dagster dev`
again, confirm no other instance is already running** (`ps aux | grep
dagster`) — multiple simultaneous instances fight over daemon heartbeat
ownership in Postgres and cause runs to silently duplicate-launch/fail. If
restarting orchestration more than once in a session, always `just
orchestration::kill` first, never just `just orchestration::start` again on
top of a possibly-still-running instance.

**Full `kind delete cluster` + rebuild from zero is this project's actual
regression-testing methodology, not a destructive exception.** This project
has no production data to preserve. Prefer a full nuke-and-rebuild over
testing against a possibly-stale existing cluster when validating a
schema/infra change — incrementally-patched state has repeatedly hidden real
bugs that only a from-zero rebuild surfaces. `just smoketest` runs the whole
cycle (nuke → rebuild → live pipeline verification → full test suite) as one
command; `just start`/`kill [module]` and `just test [module|feed-tag]` give
scoped control. Verify a recipe name still exists (`just --list`) before
using it — this tooling gets restructured over time.

## Multi-agent development workflow

Five roles, most work through delegation rather than one agent doing
everything end to end — built specifically to stop the same agent writing
code and grading its own work, and to keep exploration/verification off
the main session's context.

**Opt-in, not default.** Architect (this session) judges whether a
request is substantial enough to route through the full pipeline, or
small enough to handle directly as before (a doc fix, a config tweak, a
one-file change). Don't invoke the pipeline for trivial work — that's
pure overhead.

**Roles and hand-off order**: Architect (orchestrator, provisions scoped
infra) → Planning → Developer(s) (parallel when Planning identifies
genuinely independent subtasks) → Integration → QA → Planning (aggregates
results, updates docs) → Architect (fresh-spawned instance, final gate) →
Architect (orchestrator, tears down infra) → user.

**Infrastructure lifecycle is exclusively Architect's, in either mode —
never Planning/Developer/Integration/QA.** Before spawning Planning,
Architect scopes and provisions whatever modules the task actually needs
(`just start <module>`, not a blanket full-stack `just start` every time
— matches this project's existing preference for granular, cost-conscious
infra) so Developer/Integration/QA never have to worry about whether
infra is available. If something turns out to be missing mid-pipeline,
the agent that hit the gap reports it up through Planning; Architect —
never the agent that found the gap — is the one who brings it up. Infra
stays up across any fix-round the final-gate review triggers; Architect
tears it down (`just kill <module>`) only once the task is genuinely,
fully done, right before reporting to the user — the same "kill
everything once a phase's work is done" convention this repo already has,
just centralized to one role instead of whoever happened to spin
something up.

- **Architect** — two distinct modes under one name, deliberately. As
  *orchestrator*, it's this session: talks to the user, judges
  pipeline-vs-direct, owns `README.md`, spawns Planning, and decides what
  to do with the final-gate findings (route back to Planning for fixes,
  or report done). As *final gate*, it's a fresh spawn of
  `.claude/agents/architect.md` — given only the diff and the original
  requirement, no memory of how the work was built, specifically so the
  review isn't done by the same actor that already oversaw the whole
  pipeline. This is a *static* review (does the diff follow this repo's
  conventions, does it match what was actually asked) — complementary to
  QA's *dynamic* (runtime) validation, not a duplicate of it.
- **Planning** (`.claude/agents/planning.md`) — tech-lead hub. Breaks a
  requirement into tasks, spawns Developer/Integration/QA directly, and
  applies approved edits to `Roadmap.md`/`Backlog.md`/`Progress/*.md`
  itself once it approves a proposal from one of them. Drafts
  `Learnings.md` entries but never finalizes them without Architect's
  sign-off — that file is written for humans, not agents.
- **Developer** (`.claude/agents/developer.md`) — writes code and its own
  unit tests, co-located with the module it touches (no relocation of the
  existing per-module `tests/` layout).
- **Integration** (`.claude/agents/integration.md`) — writes integration
  tests once Developer's unit tests pass, in the right tier: a Dagster
  pipeline exercised end-to-end without mocking individual steps (the
  pipeline chain itself *is* the integration test) belongs inside the
  orchestration module's own tests; anything genuinely cross-module or
  platform-wide belongs in `tests/integration/`, the existing top-level
  suite.
- **QA** (`.claude/agents/qa.md`) — feature-level validation: a Gherkin
  `.feature` file under `features/` (a human-readable plan only — no
  code, no test runner lives in that folder) plus the real pytest that
  satisfies it, written in `tests/integration/` and referencing the
  `.feature` file it satisfies. Builds mock datasets, runs everything
  against the live platform, reports bugs to Planning rather than editing
  others' code. "Covers 90%+ of the functionality plus edge cases" is a
  qualitative judgment call QA and Planning make together, not a
  coverage-tool threshold.

**This changes nothing about the absolute rules above.** Every role
inherits them unchanged — git stays 100% user-owned regardless of which
agent is active, any new finding still stops the work and surfaces to the
user, and infra/tests still only get exercised within the scope of the
task actually authorized, never speculatively.

**Documentation responsibility matrix** — which agents must update which
docs at the end of their own task, not just who's allowed to:

| Document | Owners |
|---|---|
| `README.md` | Architect |
| `Learnings.md` | Architect, Planning, Integration |
| `Backlog.md` | Architect, Planning |
| `Roadmap.md` | Architect, Planning |
| `Progress/*.md` | Planning |
| `DebugReference.md` (one per module — `frontend/`, `platform/`, `query-engine/`, `streaming/`, `orchestration/`, `metadata/`, `tests/integration/`, and any future module that gets one) | Developer, QA, Integration |

- **Architect** on `Backlog.md`/`Roadmap.md` applies to work it handles
  directly, outside the full pipeline — when a task is pipeline-routed,
  Planning is the one applying these edits, per its tech-lead role above.
- **Learnings.md**: both Planning and Integration can draft an entry
  directly (Integration is the agent most likely to hit a real
  infra/environment gotcha worth documenting) — either way, Architect
  signs off before an entry is final. That file is written for humans,
  not agents; its structure is still governed by "Documentation
  conventions" above regardless of who drafts.
- **`DebugReference.md`**: whichever of Developer/QA/Integration is
  working in a module updates that module's `DebugReference.md` if its
  task changes the module's recipes or manual-command equivalents enough
  to make the existing doc stale.
- **`CLAUDE.md`** is deliberately absent from this matrix. No agent,
  including Architect, ever writes to it — every agent can propose
  wording in conversation, only you apply it.

## Documentation conventions

**`Learnings.md` is a problem-indexed reference, not a session log.** Every
entry: a searchable problem title, then **Symptom** (exact error text, where
in the process it broke), **Cause**, **Resolution**, and **Caveat** if one
exists. Organize by system/component (e.g. "Dagster + Kubernetes", "dbt
modeling patterns"), never by build phase, session, or chronological order.
Exclude phase numbers, "this session" language, prompt-sequence narrative,
and pure "verified: ..." testing-log paragraphs that don't teach something
reusable — that content belongs in `Progress/*.md` instead, which is the
correct home for phase-by-phase chronological narrative.

**`Progress/` is a folder of distinct, grep-able progress logs, not a
single running log.** The build/verification history lives entirely under
`Progress/` — one file per numbered phase (`Progress/<NN>-<slug>.md`) or
per dated post-numbered-phase entry (`Progress/<YYYY-MM-DD>-<slug>.md`),
each file a self-contained record of that phase/entry. There is no
separate index file to keep in sync — find a relevant entry by listing or
grepping `Progress/` directly. Read a found file only when its content is
actually relevant to the task at hand (working in that phase's area,
checking whether something was already tried) — do not read the whole
folder as part of routine session startup.

**`Roadmap.md` holds current state and future work only — never
completed-phase history.** Once a phase is done, its entry is removed from
Roadmap.md entirely (not marked done in place) — the phase-by-phase
completed record lives in `Progress/` instead. Roadmap.md answers
"what's the state now and what's still open," not "what happened, phase by
phase."

## Where the rest lives

This file has the hard, durable rules. Architecture and design live in
`README.md` — the permanent reference, meant to outlive this project's
build-out. Project-specific context that changes over time (current phase
status, open bugs, what's blocking what) lives in `Roadmap.md` (current
state + future-work design only, no completed-phase history — see
"Documentation conventions"), `Backlog.md` (deferred items + current
priority), `Progress/` (a folder of distinct, per-phase/per-dated-entry
build/verification logs, grep-able rather than indexed — see
"Documentation conventions"), and `Learnings.md` (human readable technical
gotchas for humans coming across this repository) — these four are working
documents for the build-out, not meant to outlive it. 