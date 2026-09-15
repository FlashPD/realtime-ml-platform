#!/usr/bin/env bash
set -euo pipefail

repository_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
tool_directory="${TRIPML_TOOL_DIR:-${repository_root}/.tools/bin}"
cluster_name="${TRIPML_CLUSTER_NAME:-tripml}"
namespace="${TRIPML_NAMESPACE:-tripml}"
release_name="${TRIPML_RELEASE_NAME:-tripml}"
credentials_secret="tripml-infra-credentials"

require_command() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Required command not found: $1" >&2
    exit 1
  fi
}

create_credentials() {
  if kubectl --context "kind-${cluster_name}" --namespace "${namespace}" \
    get secret "${credentials_secret}" >/dev/null 2>&1; then
    return
  fi

  local postgres_password redis_password minio_root_user minio_root_password
  postgres_password="$(openssl rand -hex 24)"
  redis_password="$(openssl rand -hex 24)"
  minio_root_user="tripml-$(openssl rand -hex 6)"
  minio_root_password="$(openssl rand -hex 24)"

  kubectl --context "kind-${cluster_name}" --namespace "${namespace}" \
    create secret generic "${credentials_secret}" \
    --from-literal=postgres-password="${postgres_password}" \
    --from-literal=redis-password="${redis_password}" \
    --from-literal=minio-root-user="${minio_root_user}" \
    --from-literal=minio-root-password="${minio_root_password}"
}

create_cluster() {
  require_command docker
  require_command kubectl
  require_command openssl
  docker info >/dev/null
  "${repository_root}/scripts/bootstrap-tools.sh" all

  if ! "${tool_directory}/kind" get clusters | grep -qx "${cluster_name}"; then
    "${tool_directory}/kind" create cluster \
      --name "${cluster_name}" \
      --config "${repository_root}/infra/kind/cluster.yaml" \
      --wait 120s
  fi

  kubectl --context "kind-${cluster_name}" create namespace "${namespace}" \
    --dry-run=client --output yaml | kubectl --context "kind-${cluster_name}" apply -f -
  create_credentials

  "${tool_directory}/helm" upgrade --install "${release_name}" \
    "${repository_root}/deploy/helm/tripml" \
    --kube-context "kind-${cluster_name}" \
    --namespace "${namespace}" \
    --values "${repository_root}/deploy/helm/tripml/values-local.yaml" \
    --wait \
    --timeout 10m

  run_tests
  kubectl --context "kind-${cluster_name}" --namespace "${namespace}" get pods
}

run_tests() {
  "${repository_root}/scripts/bootstrap-tools.sh" helm
  "${tool_directory}/helm" test "${release_name}" \
    --kube-context "kind-${cluster_name}" \
    --namespace "${namespace}" \
    --logs \
    --timeout 3m
  kubectl --context "kind-${cluster_name}" --namespace "${namespace}" \
    delete pods --selector tripml.io/test=true --ignore-not-found --wait=false
}

show_status() {
  require_command kubectl
  kubectl --context "kind-${cluster_name}" --namespace "${namespace}" get pods,services,persistentvolumeclaims
}

delete_cluster() {
  "${repository_root}/scripts/bootstrap-tools.sh" kind
  "${tool_directory}/kind" delete cluster --name "${cluster_name}"
}

case "${1:-}" in
  create) create_cluster ;;
  test) run_tests ;;
  status) show_status ;;
  delete) delete_cluster ;;
  *) echo "Usage: $0 {create|test|status|delete}" >&2; exit 2 ;;
esac
