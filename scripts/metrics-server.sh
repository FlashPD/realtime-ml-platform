#!/usr/bin/env bash
set -euo pipefail

repository_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cluster_name="${TRIPML_CLUSTER_NAME:-tripml}"
"${repository_root}/scripts/bootstrap-tools.sh" helm
# The kubelet certificate exception is limited to this explicit local kind context.
"${repository_root}/.tools/bin/helm" upgrade --install metrics-server metrics-server \
  --repo https://kubernetes-sigs.github.io/metrics-server --version 3.13.0 \
  --kube-context "kind-${cluster_name}" --namespace kube-system \
  --set 'args[0]=--kubelet-insecure-tls' --wait --timeout 3m
kubectl --context "kind-${cluster_name}" wait apiservice/v1beta1.metrics.k8s.io \
  --for=condition=Available --timeout=30s
