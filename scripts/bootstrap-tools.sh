#!/usr/bin/env bash
set -euo pipefail

repository_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=tool-versions.env
source "${repository_root}/scripts/tool-versions.env"

tool_directory="${TRIPML_TOOL_DIR:-${repository_root}/.tools/bin}"
requested_tool="${1:-all}"

case "$(uname -s)" in
  Darwin) platform="darwin" ;;
  Linux) platform="linux" ;;
  *) echo "Unsupported operating system: $(uname -s)" >&2; exit 1 ;;
esac

case "$(uname -m)" in
  arm64 | aarch64) architecture="arm64" ;;
  x86_64 | amd64) architecture="amd64" ;;
  *) echo "Unsupported architecture: $(uname -m)" >&2; exit 1 ;;
esac

mkdir -p "${tool_directory}"

download_kind() {
  local destination="${tool_directory}/kind"
  if [[ -x "${destination}" ]] && [[ "$("${destination}" version 2>/dev/null)" == *"${KIND_VERSION#v}"* ]]; then
    return
  fi

  local temporary_directory
  temporary_directory="$(mktemp -d)"
  trap 'rm -rf "${temporary_directory}"' RETURN
  local asset="kind-${platform}-${architecture}"
  local base_url="https://kind.sigs.k8s.io/dl/${KIND_VERSION}"
  curl --proto '=https' --tlsv1.2 --fail --silent --show-error \
    --location "${base_url}/${asset}" --output "${temporary_directory}/${asset}"
  curl --proto '=https' --tlsv1.2 --fail --silent --show-error \
    --location "${base_url}/${asset}.sha256sum" --output "${temporary_directory}/${asset}.sha256sum"
  (cd "${temporary_directory}" && shasum -a 256 --check "${asset}.sha256sum")
  install -m 0755 "${temporary_directory}/${asset}" "${destination}"
}

download_helm() {
  local destination="${tool_directory}/helm"
  if [[ -x "${destination}" ]] && [[ "$("${destination}" version --short 2>/dev/null)" == *"${HELM_VERSION}"* ]]; then
    return
  fi

  local temporary_directory
  temporary_directory="$(mktemp -d)"
  trap 'rm -rf "${temporary_directory}"' RETURN
  local archive="helm-${HELM_VERSION}-${platform}-${architecture}.tar.gz"
  local base_url="https://get.helm.sh"
  curl --proto '=https' --tlsv1.2 --fail --silent --show-error \
    --location "${base_url}/${archive}" --output "${temporary_directory}/${archive}"
  curl --proto '=https' --tlsv1.2 --fail --silent --show-error \
    --location "${base_url}/${archive}.sha256sum" --output "${temporary_directory}/${archive}.sha256sum"
  (cd "${temporary_directory}" && shasum -a 256 --check "${archive}.sha256sum")
  tar -xzf "${temporary_directory}/${archive}" -C "${temporary_directory}"
  install -m 0755 "${temporary_directory}/${platform}-${architecture}/helm" "${destination}"
}

case "${requested_tool}" in
  all) download_kind; download_helm ;;
  kind) download_kind ;;
  helm) download_helm ;;
  *) echo "Usage: $0 [all|kind|helm]" >&2; exit 2 ;;
esac

