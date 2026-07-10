# raw_to_clean (PyIceberg + Polars raw->clean library). No `start`/`kill` --
# this is a library imported by orchestration's op pods, not a standalone
# deployment.

# Unit tests -- pure logic (validate_schema/schema mapping), no live
# cluster or I/O needed.
test:
    cd raw_to_clean && uv run pytest
