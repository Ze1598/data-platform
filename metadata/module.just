# Postgres (source_system/data_feed/schema_registry/lakehouse_models/load_type/
# schedule/data_processing_runs + polaris_db).
# See DebugReference.md for the manual-command equivalents this wraps.
# POSTGRES_* env vars come from the root Justfile's `set dotenv-load`.

# Deploy Postgres (regenerating the init-scripts ConfigMap from db/init/ as
# the single source of truth) and seed the platform metadata tables.
start:
    #!/usr/bin/env bash
    set -euo pipefail
    kubectl create configmap postgres-init-scripts \
        --from-file=db/init/ -n metadata \
        --dry-run=client -o yaml | kubectl apply -f -
    kubectl apply -f k8s/secret.yaml -f k8s/statefulset.yaml -f k8s/service.yaml
    echo "Waiting for postgres StatefulSet to become ready..."
    kubectl rollout status statefulset/postgres -n metadata --timeout=180s
    cd {{justfile_directory()}} && uv run python scripts/seed_metadata_db.py

# Tear down Postgres's k8s resources (StatefulSet/Service/Secret + its PVC
# via the StatefulSet's volumeClaimTemplate) — data is NOT preserved across
# this, a subsequent `start` recreates an empty database.
kill:
    kubectl delete -f k8s/secret.yaml -f k8s/statefulset.yaml -f k8s/service.yaml --ignore-not-found
    kubectl delete pvc -n metadata postgres-data-postgres-0 --ignore-not-found
    kubectl delete configmap postgres-init-scripts -n metadata --ignore-not-found

# Interactive psql session against the live database.
psql:
    kubectl exec -it -n metadata postgres-0 -- psql -U "$POSTGRES_USER" -d "$POSTGRES_DB"

# Re-run the idempotent seed script standalone (e.g. after adding a new feed).
seed:
    cd {{justfile_directory()}} && uv run python scripts/seed_metadata_db.py
