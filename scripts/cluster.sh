#!/usr/bin/env bash
set -euo pipefail

repository_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
tool_directory="${TRIPML_TOOL_DIR:-${repository_root}/.tools/bin}"
cluster_name="${TRIPML_CLUSTER_NAME:-tripml}"
namespace="${TRIPML_NAMESPACE:-tripml}"
release_name="${TRIPML_RELEASE_NAME:-tripml}"
credentials_secret="tripml-infra-credentials"
airflow_image="tripml-airflow:0.1.0"
postgres_username="tripml"
airflow_database="${TRIPML_AIRFLOW_DATABASE:-airflow}"

require_command() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Required command not found: $1" >&2
    exit 1
  fi
}

read_secret_value() {
  local key="$1"
  local encoded
  encoded="$(kubectl --context "kind-${cluster_name}" --namespace "${namespace}" \
    get secret "${credentials_secret}" \
    --output "go-template={{with index .data \"${key}\"}}{{.}}{{end}}" 2>/dev/null || true)"
  if [[ -n "${encoded}" ]]; then
    printf '%s' "${encoded}" | openssl base64 -d -A
  fi
}

create_credentials() {
  local postgres_password redis_password minio_root_user minio_root_password
  local airflow_admin_password airflow_fernet_key airflow_jwt_secret
  local airflow_database_url lineage_database_url database_host

  postgres_password="$(read_secret_value postgres-password)"
  redis_password="$(read_secret_value redis-password)"
  minio_root_user="$(read_secret_value minio-root-user)"
  minio_root_password="$(read_secret_value minio-root-password)"
  airflow_admin_password="$(read_secret_value airflow-admin-password)"
  airflow_fernet_key="$(read_secret_value airflow-fernet-key)"
  airflow_jwt_secret="$(read_secret_value airflow-jwt-secret)"

  postgres_password="${postgres_password:-$(openssl rand -hex 24)}"
  redis_password="${redis_password:-$(openssl rand -hex 24)}"
  minio_root_user="${minio_root_user:-tripml-$(openssl rand -hex 6)}"
  minio_root_password="${minio_root_password:-$(openssl rand -hex 24)}"
  airflow_admin_password="${airflow_admin_password:-$(openssl rand -hex 24)}"
  airflow_fernet_key="${airflow_fernet_key:-$(openssl rand -base64 32 | tr '+/' '-_')}"
  airflow_jwt_secret="${airflow_jwt_secret:-$(openssl rand -hex 32)}"

  database_host="${release_name}-tripml-postgresql"
  airflow_database_url="postgresql+psycopg://${postgres_username}:${postgres_password}@${database_host}:5432/${airflow_database}"
  lineage_database_url="postgresql://${postgres_username}:${postgres_password}@${database_host}:5432/tripml"

  kubectl --context "kind-${cluster_name}" --namespace "${namespace}" create secret generic \
    "${credentials_secret}" \
    --from-literal=postgres-password="${postgres_password}" \
    --from-literal=redis-password="${redis_password}" \
    --from-literal=minio-root-user="${minio_root_user}" \
    --from-literal=minio-root-password="${minio_root_password}" \
    --from-literal=airflow-admin-password="${airflow_admin_password}" \
    --from-literal=airflow-admin-passwords="{\"admin\":\"${airflow_admin_password}\"}" \
    --from-literal=airflow-fernet-key="${airflow_fernet_key}" \
    --from-literal=airflow-jwt-secret="${airflow_jwt_secret}" \
    --from-literal=airflow-database-url="${airflow_database_url}" \
    --from-literal=lineage-database-url="${lineage_database_url}" \
    --dry-run=client --output yaml | \
    kubectl --context "kind-${cluster_name}" --namespace "${namespace}" apply -f -
}

build_airflow_image() {
  docker build \
    --file "${repository_root}/docker/airflow/Dockerfile" \
    --tag "${airflow_image}" \
    "${repository_root}"
  "${tool_directory}/kind" load docker-image "${airflow_image}" --name "${cluster_name}"
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
  build_airflow_image

  kubectl --context "kind-${cluster_name}" create namespace "${namespace}" \
    --dry-run=client --output yaml | kubectl --context "kind-${cluster_name}" apply -f -
  create_credentials

  "${tool_directory}/helm" upgrade --install "${release_name}" \
    "${repository_root}/deploy/helm/tripml" \
    --kube-context "kind-${cluster_name}" \
    --namespace "${namespace}" \
    --values "${repository_root}/deploy/helm/tripml/values-local.yaml" \
    --set-string airflow.metadataDatabase="${airflow_database}" \
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

show_airflow_password() {
  require_command kubectl
  require_command openssl
  read_secret_value airflow-admin-password
  printf '\n'
}

forward_airflow() {
  require_command kubectl
  kubectl --context "kind-${cluster_name}" --namespace "${namespace}" \
    port-forward "service/${release_name}-tripml-airflow" 8080:8080
}

delete_cluster() {
  "${repository_root}/scripts/bootstrap-tools.sh" kind
  "${tool_directory}/kind" delete cluster --name "${cluster_name}"
}

case "${1:-}" in
  create) create_cluster ;;
  test) run_tests ;;
  status) show_status ;;
  airflow-password) show_airflow_password ;;
  airflow-ui) forward_airflow ;;
  delete) delete_cluster ;;
  *) echo "Usage: $0 {create|test|status|airflow-password|airflow-ui|delete}" >&2; exit 2 ;;
esac
