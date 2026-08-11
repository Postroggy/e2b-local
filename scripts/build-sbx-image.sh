#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
IMAGE="${E2B_SBX_IMAGE:-e2b-local/sbx-envd:dev}"
ENVD_REPOSITORY="${E2B_SBX_ENVD_REPOSITORY:-https://github.com/e2b-dev/infra.git}"
SBX_DOCKER_HOST="${E2B_SBX_DOCKER_HOST:-unix://${HOME}/.sbx/run/d/docker.sock}"

case "$(uname -m)" in
  arm64|aarch64) default_arch="arm64" ;;
  x86_64|amd64) default_arch="amd64" ;;
  *)
    echo "unsupported host architecture: $(uname -m)" >&2
    exit 1
    ;;
esac
PLATFORM="${E2B_SBX_PLATFORM:-linux/${default_arch}}"

ENVD_REF="$(git ls-remote "${ENVD_REPOSITORY}" HEAD | awk 'NR == 1 { print $1 }')"
if [[ -z "${ENVD_REF}" ]]; then
  echo "could not resolve the current public envd source revision from ${ENVD_REPOSITORY}" >&2
  exit 1
fi

echo "Building ${IMAGE} from ${ENVD_REPOSITORY}@${ENVD_REF} for ${PLATFORM}"
docker build --pull --no-cache --platform "${PLATFORM}" \
  --build-arg "ENVD_REPOSITORY=${ENVD_REPOSITORY}" \
  --build-arg "ENVD_REF=${ENVD_REF}" \
  --tag "${IMAGE}" \
  --file "${ROOT_DIR}/internal/backends/sbx/image/Dockerfile" \
  "${ROOT_DIR}"

echo "Importing ${IMAGE} into sbx Docker at ${SBX_DOCKER_HOST}"
docker save "${IMAGE}" | docker --host "${SBX_DOCKER_HOST}" image load
