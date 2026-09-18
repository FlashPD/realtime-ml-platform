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

for workload in postgresql redis minio redpanda airflow mlflow; do
  if ! grep -q "app.kubernetes.io/component: ${workload}" "${rendered_manifest}"; then
    echo "Rendered chart is missing the ${workload} workload" >&2
    exit 1
  fi
done

"${helm_binary}" lint "${chart_directory}" --set serving.enabled=true \
  --set serving.autoscaling.enabled=true
"${helm_binary}" template tripml "${chart_directory}" --namespace tripml \
  --set serving.enabled=true --set serving.autoscaling.enabled=true > "${rendered_manifest}"
if ! grep -q '^kind: HorizontalPodAutoscaler$' "${rendered_manifest}"; then
  echo "Serving autoscaling profile must render an HPA" >&2
  exit 1
fi

"${helm_binary}" lint "${chart_directory}" --set monitoring.enabled=true
"${helm_binary}" template tripml "${chart_directory}" --namespace tripml \
  --set monitoring.enabled=true > "${rendered_manifest}"
for workload in prometheus grafana; do
  if ! grep -q "app.kubernetes.io/component: ${workload}" "${rendered_manifest}"; then
    echo "Monitoring profile is missing ${workload}" >&2
    exit 1
  fi
done
if grep -Eq '^kind: (Secret|ClusterRole|ClusterRoleBinding)$' "${rendered_manifest}"; then
  echo "Monitoring must use existing credentials and namespace-scoped discovery" >&2
  exit 1
fi

echo "Chart lint and render checks passed"
