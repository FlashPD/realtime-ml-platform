#!/usr/bin/env bash
set -euo pipefail

repository_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
helm_binary="${TRIPML_HELM:-${repository_root}/.tools/bin/helm}"
chart_directory="${repository_root}/deploy/helm/tripml"
rendered_manifest="$(mktemp)"
trap 'rm -f "${rendered_manifest}"' EXIT

"${helm_binary}" lint "${chart_directory}" --values "${chart_directory}/values-local.yaml"
"${helm_binary}" template tripml "${chart_directory}" \
  --namespace tripml \
  --values "${chart_directory}/values-local.yaml" > "${rendered_manifest}"

if grep -Eq '^kind: Secret$' "${rendered_manifest}"; then
  echo "Rendered chart must not contain Secret resources" >&2
  exit 1
fi

for workload in postgresql redis minio redpanda airflow; do
  if ! grep -q "app.kubernetes.io/component: ${workload}" "${rendered_manifest}"; then
    echo "Rendered chart is missing the ${workload} workload" >&2
    exit 1
  fi
done

echo "Chart lint and render checks passed"
