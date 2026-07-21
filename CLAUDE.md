# Project instructions for Claude Code

These are standing rules for working in this repo. They come from repeated,
explicit user corrections across multiple sessions — several were stated
more than once because they were violated more than once. Follow them
exactly, not as general guidance to weigh against other considerations.

## Absolute rules — no exceptions, no judgment calls

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

**After 2 failed attempts at fixing the same bug, stop and ask — do not try
a 3rd hypothesis on your own.** Count explicitly: attempt 1 fails, try
attempt 2; attempt 2 fails, STOP and report both failed hypotheses, ask what
to do next.

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

## Documentation conventions

**`Learnings.md` is a problem-indexed reference, not a session log.** Every
entry: a searchable problem title, then **Symptom** (exact error text, where
in the process it broke), **Cause**, **Resolution**, and **Caveat** if one
exists. Organize by system/component (e.g. "Dagster + Kubernetes", "dbt
modeling patterns"), never by build phase, session, or chronological order.
Exclude phase numbers, "this session" language, prompt-sequence narrative,
and pure "verified: ..." testing-log paragraphs that don't teach something
reusable — that content belongs in `Progress.md` instead, which is the
correct home for phase-by-phase chronological narrative.

## Where the rest lives

This file has the hard, durable rules. Architecture and design live in
`README.md` — the permanent reference, meant to outlive this project's
build-out. Project-specific context that changes over time (current phase
status, open bugs, what's blocking what) lives in `Roadmap.md` (phase
status only for completed work, draft design for pending work), `Backlog.md` (deferred items + current
priority), `Progress.md` (verified build/test history), and `Learnings.md`
(human readable technical gotchas for humans coming across this repository) — these four are working documents for the build-out,
not meant to outlive it. 