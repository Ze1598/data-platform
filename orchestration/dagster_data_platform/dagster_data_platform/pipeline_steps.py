"""The pipeline-step vocabulary (extraction/validation/transformation/
serving) -- a different axis from the landing/raw/clean/staging/model/serve
*schemas* data_processing_runs tracks (see metadata/DataModel.md's
`pipeline_steps` section). Pure parsing, no I/O -- matches
connectors.compute_schema_sync()'s pattern of keeping logic separate from
the resource that fetches the raw value.
"""

STEP_LABELS = {0: "extraction", 1: "validation", 2: "transformation", 3: "serving"}


def parse_selected_steps(pipeline_steps_csv: str) -> set[str]:
    return {STEP_LABELS[int(s)] for s in pipeline_steps_csv.split(",") if s.strip()}
