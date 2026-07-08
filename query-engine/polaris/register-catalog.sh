#!/usr/bin/env bash
# Registers the "data_platform" Iceberg catalog in Polaris against MinIO's
# "lakehouse" bucket. Not idempotent in the strict sense — re-running against
# an already-registered catalog will fail with 409 (safe to ignore) rather
# than update it, since PUT-updating storageConfigInfo fields (pathStyleAccess,
# stsUnavailable) has been observed to silently not apply them. Delete the
# catalog first (via the Polaris REST API) if you need to change its config.
set -euo pipefail

kubectl port-forward -n query-engine svc/polaris 8181:8181 >/tmp/polaris-pf-register.log 2>&1 &
PF_PID=$!
trap 'kill $PF_PID 2>/dev/null' EXIT
sleep 3

TOKEN=$(curl -s http://localhost:8181/api/catalog/v1/oauth/tokens \
  --user root:s3cr3t \
  -d 'grant_type=client_credentials' \
  -d 'scope=PRINCIPAL_ROLE:ALL' | python3 -c "import json,sys;print(json.load(sys.stdin)['access_token'])")

curl -sf -X POST http://localhost:8181/api/management/v1/catalogs \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "catalog": {
      "name": "data_platform",
      "type": "INTERNAL",
      "properties": {
        "default-base-location": "s3://lakehouse",
        "s3.endpoint": "http://minio.query-engine.svc.cluster.local:9000",
        "s3.path-style-access": "true",
        "s3.access-key-id": "admin",
        "s3.secret-access-key": "password",
        "s3.region": "us-east-1"
      },
      "storageConfigInfo": {
        "roleArn": "arn:aws:iam::000000000000:role/minio-polaris-role",
        "storageType": "S3",
        "pathStyleAccess": true,
        "stsUnavailable": true,
        "allowedLocations": ["s3://lakehouse/*"]
      }
    }
  }'

echo "Catalog 'data_platform' registered."
