"""The pipeline-step vocabulary (extraction/transformation/serving) -- a
different axis from the raw/clean/staging/model/serve *schemas*
data_processing_runs tracks (see metadata/DataModel.md's `pipeline_steps`
section). Extraction covers both the raw and clean schema stages as one
step/job -- raw exists specifically to feed clean, so they aren't split
into separate steps (see Roadmap.md "Master pipeline orchestration").
Pure parsing, no I/O -- matches connectors.compute_schema_sync()'s pattern
of keeping logic separate from the resource that fetches the raw value.
"""

STEP_LABELS = {0: "extraction", 1: "transformation", 2: "serving"}


def parse_selected_steps(pipeline_steps_csv: str) -> set[str]:
    return {STEP_LABELS[int(s)] for s in pipeline_steps_csv.split(",") if s.strip()}
