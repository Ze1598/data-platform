# Streamlit CRUD app -- runs fully in-cluster now (frontend/k8s/), not a
# local `nohup streamlit run app.py` process (Roadmap.md "Kubernetes
# Hosting Model" -- the last of this project's documented-but-never-built
# in-cluster Deployments). POSTGRES_* env vars come from the root
# Justfile's `set dotenv-load` (used by `test` below, and by host tools
# generally) -- the in-cluster Deployment itself gets its own POSTGRES_*
# via frontend/k8s/deployment.yaml + postgres-credentials-secret.yaml.
# See DebugReference.md for the manual-command equivalents this wraps.

start:
    #!/usr/bin/env bash
    set -euo pipefail
    docker build -f Dockerfile -t data-platform-frontend:latest {{justfile_directory()}}
    kind load docker-image data-platform-frontend:latest --name data-platform

    kubectl apply -f k8s/postgres-credentials-secret.yaml
    kubectl apply -f k8s/deployment.yaml -f k8s/service.yaml

    echo "Waiting for frontend to roll out..."
    kubectl rollout status deployment/frontend -n frontend --timeout=120s

    echo "Waiting for the app to come up on localhost:8501..."
    until curl -s -o /dev/null http://localhost:8501; do sleep 2; done
    echo "frontend up (logs: kubectl logs -n frontend deployment/frontend)"

kill:
    kubectl delete -f k8s/deployment.yaml -f k8s/service.yaml -f k8s/postgres-credentials-secret.yaml --ignore-not-found

# Tests against the live Postgres (not mocks) -- needs the cluster up.
test:
    uv run pytest
