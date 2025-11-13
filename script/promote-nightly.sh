set -euo pipefail

echo "=== SPQR Router Image Promotion ==="

set -x
docker login -u "pgsharding"
set +x

IMAGE_REPO="pgsharding/spqr-router"

NIGHTLY_TAG="${1:-}"
if [[ -z "${NIGHTLY_TAG}" ]]; then
  read -rp "Nightly image tag to promote (e.g. nightly-2.8.0-1777-e2479243): " NIGHTLY_TAG
fi
if [[ -z "${NIGHTLY_TAG}" ]]; then
  echo "Nightly tag is required." >&2
  exit 1
fi

RELEASE_VERSION="${NIGHTLY_TAG#nightly-}"
RELEASE_VERSION="${RELEASE_VERSION%%-*}"

set -x
docker tag "${IMAGE_REPO}:${NIGHTLY_TAG}" "${IMAGE_REPO}:latest"
docker tag "${IMAGE_REPO}:${NIGHTLY_TAG}" "${IMAGE_REPO}:stable"
docker tag "${IMAGE_REPO}:${NIGHTLY_TAG}" "${IMAGE_REPO}:${RELEASE_VERSION}"
docker push "${IMAGE_REPO}:latest"
docker push "${IMAGE_REPO}:stable"
docker push "${IMAGE_REPO}:${RELEASE_VERSION}"
set +x

echo "Promotion complete (version ${RELEASE_VERSION})."