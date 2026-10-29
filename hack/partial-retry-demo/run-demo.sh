#!/usr/bin/env bash
#
# End-to-end demo of partial PipelineRun retry (SRVKP-15170 POC).
#
# Prerequisites:
#   - A running kind cluster (e.g. via hack/setup-kind.sh)
#   - kubectl pointed at that cluster
#   - ko installed, with the POC fork installed on the cluster
#
# This script is self-contained: it installs the POC fork via `ko apply`,
# applies the demo Pipeline/Tasks, runs a failing source PipelineRun, retries
# it, and asserts that `clone` was bypassed while `build`/`deploy` reran.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
NAMESPACE="partial-retry-demo"
TIMEOUT_SECS=180

pass() { echo "✅ $*"; }
fail() { echo "❌ $*"; exit 1; }

echo "--- Installing POC fork via ko apply"
export KO_DOCKER_REPO="${KO_DOCKER_REPO:-kind.local}"
ko apply -f "${REPO_ROOT}/config/" | tail -5
kubectl -n tekton-pipelines rollout status deployment/tekton-pipelines-controller --timeout="${TIMEOUT_SECS}s"

echo "--- Setting up namespace ${NAMESPACE}"
kubectl create namespace "${NAMESPACE}" --dry-run=client -o yaml | kubectl apply -f -

echo "--- Applying demo Pipeline and Tasks"
kubectl apply -n "${NAMESPACE}" -f "${SCRIPT_DIR}/demo-pipeline.yaml"

# wait_for_succeeded <pipelinerun-name> <expected: True|False>
wait_for_succeeded() {
  local name="$1" expected="$2" waited=0
  while true; do
    status=$(kubectl -n "${NAMESPACE}" get pipelinerun "${name}" \
      -o jsonpath='{.status.conditions[?(@.type=="Succeeded")].status}' 2>/dev/null || true)
    if [[ "${status}" == "${expected}" ]]; then
      return 0
    fi
    if (( waited >= TIMEOUT_SECS )); then
      echo "--- timed out waiting for pipelinerun/${name} Succeeded=${expected} (last status: ${status:-<none>})"
      kubectl -n "${NAMESPACE}" get pipelinerun "${name}" -o yaml || true
      return 1
    fi
    sleep 2
    waited=$((waited + 2))
  done
}

SOURCE_NAME="demo-source-run"

echo "--- [1/5] Creating source PipelineRun (should-fail=true)"
kubectl -n "${NAMESPACE}" delete pipelinerun "${SOURCE_NAME}" --ignore-not-found
cat <<EOF | kubectl apply -f -
apiVersion: tekton.dev/v1
kind: PipelineRun
metadata:
  name: ${SOURCE_NAME}
  namespace: ${NAMESPACE}
spec:
  pipelineRef:
    name: demo-partial-retry-pipeline
  params:
    - name: should-fail
      value: "true"
EOF

wait_for_succeeded "${SOURCE_NAME}" "False" \
  || fail "source PipelineRun did not fail as expected"
pass "source PipelineRun failed at build, as expected"

DEPLOY_TASKRUNS=$(kubectl -n "${NAMESPACE}" get taskrun \
  -l "tekton.dev/pipelineRun=${SOURCE_NAME},tekton.dev/pipelineTask=deploy" \
  --no-headers 2>/dev/null | wc -l)
[[ "${DEPLOY_TASKRUNS}" -eq 0 ]] \
  || fail "expected deploy to never run on the source PipelineRun"
pass "deploy never ran on the source PipelineRun (correctly blocked by build failure)"

SOURCE_UID=$(kubectl -n "${NAMESPACE}" get pipelinerun "${SOURCE_NAME}" -o jsonpath='{.metadata.uid}')

echo "--- [2/5] Creating retry PipelineRun (should-fail=false, tekton.dev/retryOf=${SOURCE_NAME})"
RETRY_NAME=$(cat <<EOF | kubectl create -f - -o jsonpath='{.metadata.name}'
apiVersion: tekton.dev/v1
kind: PipelineRun
metadata:
  generateName: ${SOURCE_NAME}-retry-
  namespace: ${NAMESPACE}
  annotations:
    tekton.dev/retryOf: "${SOURCE_NAME}"
    tekton.dev/retryOfUID: "${SOURCE_UID}"
    tekton.dev/retryStrategy: "partial"
spec:
  pipelineRef:
    name: demo-partial-retry-pipeline
  params:
    - name: should-fail
      value: "false"
EOF
)
echo "    created retry PipelineRun: ${RETRY_NAME}"

wait_for_succeeded "${RETRY_NAME}" "True" \
  || fail "retry PipelineRun did not succeed"
pass "retry PipelineRun succeeded"

echo "--- [3/5] Confirming clone was bypassed (no new TaskRun)"
CLONE_TASKRUNS=$(kubectl -n "${NAMESPACE}" get taskrun \
  -l "tekton.dev/pipelineRun=${RETRY_NAME},tekton.dev/pipelineTask=clone" \
  --no-headers 2>/dev/null | wc -l)
[[ "${CLONE_TASKRUNS}" -eq 0 ]] \
  || fail "expected no TaskRun for clone on the retry (it should be bypassed)"
pass "clone was bypassed on the retry — no new TaskRun created"

echo "--- [4/5] Confirming build and deploy reran"
for t in build deploy; do
  count=$(kubectl -n "${NAMESPACE}" get taskrun \
    -l "tekton.dev/pipelineRun=${RETRY_NAME},tekton.dev/pipelineTask=${t}" \
    --no-headers 2>/dev/null | wc -l)
  [[ "${count}" -eq 1 ]] || fail "expected exactly 1 TaskRun for ${t} on the retry, got ${count}"
done
pass "build and deploy both reran exactly once on the retry"

echo "--- [5/6] Confirming deploy received clone's memoized result (PipelineRun-level)"
RESULT_SHA=$(kubectl -n "${NAMESPACE}" get pipelinerun "${RETRY_NAME}" \
  -o jsonpath='{.status.results[?(@.name=="commit-sha")].value}')
[[ "${RESULT_SHA}" == "abc1234" ]] \
  || fail "expected retry PipelineRun result commit-sha=abc1234, got '${RESULT_SHA}'"
pass "deploy consumed clone's memoized commit-sha (${RESULT_SHA}) without clone rerunning"

# This check is the one that actually proves result propagation works: the
# PipelineRun-level status.results (checked above) are computed via a
# separate code path (GetTaskRunsResults/ApplyTaskResultsToPipelineResults)
# from the *TaskRun* params actually used to run build/deploy. A pipeline can
# have correct status.results while build/deploy's real TaskRun specs still
# contain unresolved `$(tasks.clone.results...)` literals — that regression
# previously passed every check above silently. Assert directly on the
# created TaskRuns' resolved spec.params (and, belt-and-suspenders, that the
# literal, unresolved variable reference never appears in deploy's pod logs).
echo "--- [6/6] Confirming build/deploy TaskRuns have fully resolved params (not literal \$(tasks...) refs)"
BUILD_COMMIT_SHA=$(kubectl -n "${NAMESPACE}" get taskrun \
  -l "tekton.dev/pipelineRun=${RETRY_NAME},tekton.dev/pipelineTask=build" \
  -o jsonpath='{.items[0].spec.params[?(@.name=="commit-sha")].value}')
[[ "${BUILD_COMMIT_SHA}" == "abc1234" ]] \
  || fail "expected build TaskRun param commit-sha=abc1234, got '${BUILD_COMMIT_SHA}' (looks unresolved — bypassed-result propagation into TaskRun params is broken)"

DEPLOY_COMMIT_SHA=$(kubectl -n "${NAMESPACE}" get taskrun \
  -l "tekton.dev/pipelineRun=${RETRY_NAME},tekton.dev/pipelineTask=deploy" \
  -o jsonpath='{.items[0].spec.params[?(@.name=="commit-sha")].value}')
[[ "${DEPLOY_COMMIT_SHA}" == "abc1234" ]] \
  || fail "expected deploy TaskRun param commit-sha=abc1234, got '${DEPLOY_COMMIT_SHA}' (looks unresolved — bypassed-result propagation into TaskRun params is broken)"

DEPLOY_IMAGE_DIGEST=$(kubectl -n "${NAMESPACE}" get taskrun \
  -l "tekton.dev/pipelineRun=${RETRY_NAME},tekton.dev/pipelineTask=deploy" \
  -o jsonpath='{.items[0].spec.params[?(@.name=="image-digest")].value}')
[[ "${DEPLOY_IMAGE_DIGEST}" == "sha256:deadbeef" ]] \
  || fail "expected deploy TaskRun param image-digest=sha256:deadbeef, got '${DEPLOY_IMAGE_DIGEST}'"

DEPLOY_LOGS=$(kubectl -n "${NAMESPACE}" logs \
  -l "tekton.dev/pipelineRun=${RETRY_NAME},tekton.dev/pipelineTask=deploy" --tail=50 2>/dev/null || true)
echo "${DEPLOY_LOGS}" | grep -q '\$(tasks\.' \
  && fail "deploy pod logs still contain an unresolved \$(tasks....) reference:\n${DEPLOY_LOGS}"
echo "${DEPLOY_LOGS}" | grep -q "commit=abc1234 image=sha256:deadbeef" \
  || fail "expected deploy pod log line 'commit=abc1234 image=sha256:deadbeef', got:\n${DEPLOY_LOGS}"
pass "build and deploy TaskRuns have fully resolved params; deploy actually ran with the real values"

echo ""
echo "================================================================"
echo " PARTIAL RETRY DEMO: PASS"
echo "   source=${SOURCE_NAME} (failed at build)"
echo "   retry=${RETRY_NAME} (succeeded; clone bypassed, build+deploy reran)"
echo "================================================================"
