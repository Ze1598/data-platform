# features/

Human-readable Gherkin (`Given`/`When`/`Then`) specs, written by the QA agent, describing feature-level and end-to-end scenarios a piece of delivered work needs to satisfy — meant to be read and reviewed by a person, not executed by any test runner. No BDD framework (`pytest-bdd`, `behave`, etc.) is wired in here; if that ever changes, it'll be a deliberate decision, not an assumption baked into this convention.

This folder holds only the `.feature` files themselves — no code, no `pyproject.toml`, no test-execution mechanism of its own.

## Where the actual tests live

Each `.feature` file's scenarios are satisfied by real pytest, but that pytest lives wherever it makes the most sense given what it's actually testing, not here:

- **Unit tests** — co-located with the module they test (existing convention, e.g. `orchestration/dagster_data_platform/tests/`, `frontend/tests/`).
- **Dagster pipeline-level integration tests** (a multi-step asset chain exercised without mocking individual steps — the pipeline itself *is* the integration test in that case) — live inside the orchestration module's own test directory alongside its unit tests, not in `tests/integration/`.
- **Broader feature-level and end-to-end tests** (cross-module, against the live platform) — live in `tests/integration/`, this repo's existing top-level integration suite. A test satisfying a `.feature` file here should reference it by name (e.g. a comment: `# Satisfies features/<name>.feature`) so the link between the human-readable plan and the executable proof is traceable.

See CLAUDE.md's "Multi-agent development workflow" for how Developer/Integration/QA's test responsibilities map onto this split.
