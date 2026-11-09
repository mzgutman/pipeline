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

KIND_CLUSTER_NAME="${KIND_CLUSTER_NAME:-poc-partial-retry}"

if ! command -v kind >/dev/null 2>&1; then
  echo "kind is required; install kind before running this script." >&2
  exit 1
fi
if ! command -v kubectl >/dev/null 2>&1; then
  echo "kubectl is required; install kubectl before running this script." >&2
  exit 1
fi

if kind get clusters | grep -Fxq "$KIND_CLUSTER_NAME"; then
  echo "Using existing kind cluster '$KIND_CLUSTER_NAME'."
else
  echo "Creating kind cluster '$KIND_CLUSTER_NAME'..."
  kind create cluster --name "$KIND_CLUSTER_NAME" --wait 60s
fi

kind export kubeconfig --name "$KIND_CLUSTER_NAME"
kubectl cluster-info --context "kind-${KIND_CLUSTER_NAME}"
