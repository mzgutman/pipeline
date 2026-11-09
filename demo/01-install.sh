#!/usr/bin/env bash
# Copyright 2026 The Tekton Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd -- "${SCRIPT_DIR}/.." && pwd)"
export KIND_CLUSTER_NAME="${KIND_CLUSTER_NAME:-poc-partial-retry}"
export KO_DOCKER_REPO="${KO_DOCKER_REPO:-kind.local}"

for command in kubectl ko; do
  if ! command -v "$command" >/dev/null 2>&1; then
    echo "$command is required; install it before running this script." >&2
    exit 1
  fi
done

echo "Installing into Kind cluster '$KIND_CLUSTER_NAME'..."
echo "Creating Tekton namespaces..."
kubectl apply -f "${ROOT}/config/100-namespace/"
kubectl apply -f "${ROOT}/config/resolvers/100-namespace.yaml"

echo "Applying Tekton Pipelines CRDs, including the partial-retry status schema..."
kubectl apply -f "${ROOT}/config/300-crds/"
kubectl wait --for=condition=Established crd/pipelineruns.tekton.dev --timeout=60s

echo "Building and installing the local Tekton Pipelines controller..."
ko apply -f "${ROOT}/config/"

for deployment in tekton-pipelines-controller tekton-pipelines-webhook tekton-events-controller; do
  kubectl rollout status "deployment/${deployment}" -n tekton-pipelines --timeout=180s
done

echo "Partial-retry POC installation is ready."
