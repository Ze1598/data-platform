# Composite: sequences minio -> polaris -> trino, the real dependency order
# (MinIO's bucket must exist before Polaris catalog registration; Polaris's
# catalog should exist before Trino queries are meaningful). Each service
# also remains independently addressable: `just start minio`, etc.

start:
    just minio::start
    just polaris::start
    just trino::start

kill:
    just trino::kill
    just polaris::kill
    just minio::kill
