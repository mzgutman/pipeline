#!/usr/bin/env bash
# Scripted use cases for the partial-retry POC (dag-fanout pipeline).
#
#   1. Partial retry  — only the failed subgraph (c, e) re-executes; a, b, d
#                       are bypassed and their memoized results feed the rerun.
#   2. Full retry     — same annotation contract, strategy=full: every task
#                       re-executes, nothing is preserved.
#   3. Fail-close     — retrying a *succeeded* run fails terminally with
#                       reason RetrySetupFailed and creates zero TaskRuns.
#
# Usage: demo/use-cases.sh   (NAMESPACE=default WAIT_TIMEOUT=180s override)
# Prerequisites: demo/00-setup.sh + demo/01-install.sh, kubectl, real tkn.

set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
export PATH="$SCRIPT_DIR/bin:$PATH"   # POC tkn shim

NAMESPACE="${NAMESPACE:-default}"
WAIT_TIMEOUT="${WAIT_TIMEOUT:-180s}"

PASS=0
FAIL=0

check() { # check <description> <expected> <actual>
  local desc="$1" expected="$2" actual="$3"
  if [[ "$expected" == "$actual" ]]; then
    echo "  PASS  $desc"
    PASS=$((PASS + 1))
  else
    echo "  FAIL  $desc"
    echo "        expected: '$expected'"
    echo "        actual:   '$actual'"
    FAIL=$((FAIL + 1))
  fi
}

# normalize "[a b d]" / "a,b,d" → "a b d" (sorted, space-separated)
normalize() {
  local s="$1"
  s="${s#[}"; s="${s%]}"
  s="${s//\"/}"; s="${s//,/ }"
  # shellcheck disable=SC2086
  echo $s | tr ' ' '\n' | sed '/^$/d' | sort | tr '\n' ' ' | sed 's/ $//'
}

bypassed_tasks() {
  normalize "$(kubectl get pipelinerun "$1" -n "$NAMESPACE" \
    -o jsonpath='{.status.bypassedTaskNames}')"
}

taskrun_tasks() { # taskrun_tasks <pipelinerun> → sorted set of pipelineTask names
  normalize "$(kubectl get taskrun -n "$NAMESPACE" \
    -l "tekton.dev/pipelineRun=$1" \
    -o jsonpath='{range .items[*]}{.metadata.labels.tekton\.dev/pipelineTask}{" "}{end}')"
}

create_source() { # create_source <fail-c> → prints the new run's name
  kubectl create -n "$NAMESPACE" -o jsonpath='{.metadata.name}' -f - <<EOF
apiVersion: tekton.dev/v1
kind: PipelineRun
metadata:
  generateName: poc-usecase-source-
spec:
  pipelineRef:
    name: poc-dag-fanout
  params:
  - name: fail-c
    value: "$1"
EOF
}

echo "=== Partial PipelineRun Retry POC — use cases ==="
echo "namespace=$NAMESPACE  timeout=$WAIT_TIMEOUT"
echo

kubectl apply -n "$NAMESPACE" -f "$SCRIPT_DIR/dag-fanout/tasks.yaml"
kubectl apply -n "$NAMESPACE" -f "$SCRIPT_DIR/dag-fanout/pipeline.yaml"

# ---------------------------------------------------------------------------
echo "--- Use case 1: partial retry preserves succeeded work ---"
SOURCE_1="$(create_source true)"
echo "Source run: $SOURCE_1 (fail-c=true → c fails, e skipped)"
kubectl wait -n "$NAMESPACE" pipelinerun "$SOURCE_1" \
  --for=condition=Succeeded=False --timeout="$WAIT_TIMEOUT"
kubectl wait -n "$NAMESPACE" taskrun \
  -l "tekton.dev/pipelineRun=$SOURCE_1,tekton.dev/pipelineTask=d" \
  --for=condition=Succeeded=True --timeout=60s   # let the independent branch finish

OUT_1="$(tkn pipelinerun retry --partial "$SOURCE_1" --param fail-c=false -n "$NAMESPACE" 2>&1)"
echo "$OUT_1"
RETRY_1="$(awk '/^Created:/{print $2}' <<<"$OUT_1")"
kubectl wait -n "$NAMESPACE" pipelinerun "$RETRY_1" \
  --for=condition=Succeeded=True --timeout="$WAIT_TIMEOUT"

check "bypassed tasks are a, b, d"      "a b d"   "$(bypassed_tasks "$RETRY_1")"
check "new TaskRuns only for c, e"      "c e"     "$(taskrun_tasks "$RETRY_1")"
MEMOIZED_PARAM="$(kubectl get taskrun -n "$NAMESPACE" \
  -l "tekton.dev/pipelineRun=$RETRY_1,tekton.dev/pipelineTask=c" \
  -o jsonpath='{.items[0].spec.params[?(@.name=="build-id")].value}')"
check "c consumed b's memoized result"  "build-source-123" "$MEMOIZED_PARAM"
echo

# ---------------------------------------------------------------------------
echo "--- Use case 2: full retry re-executes everything ---"
SOURCE_2="$(create_source true)"
echo "Source run: $SOURCE_2 (same failure shape)"
kubectl wait -n "$NAMESPACE" pipelinerun "$SOURCE_2" \
  --for=condition=Succeeded=False --timeout="$WAIT_TIMEOUT"
kubectl wait -n "$NAMESPACE" taskrun \
  -l "tekton.dev/pipelineRun=$SOURCE_2,tekton.dev/pipelineTask=d" \
  --for=condition=Succeeded=True --timeout=60s

OUT_2="$(tkn pipelinerun retry --full "$SOURCE_2" --param fail-c=false -n "$NAMESPACE" 2>&1)"
echo "$OUT_2"
RETRY_2="$(awk '/^Created:/{print $2}' <<<"$OUT_2")"
kubectl wait -n "$NAMESPACE" pipelinerun "$RETRY_2" \
  --for=condition=Succeeded=True --timeout="$WAIT_TIMEOUT"

check "nothing bypassed on full retry"  ""        "$(bypassed_tasks "$RETRY_2")"
check "TaskRuns for all five tasks"     "a b c d e" "$(taskrun_tasks "$RETRY_2")"
echo

# ---------------------------------------------------------------------------
echo "--- Use case 3: fail-close — no fallback, ever ---"
SOURCE_3="$(create_source false)"
echo "Source run: $SOURCE_3 (fail-c=false → the run succeeds)"
kubectl wait -n "$NAMESPACE" pipelinerun "$SOURCE_3" \
  --for=condition=Succeeded=True --timeout="$WAIT_TIMEOUT"

set +e
OUT_3="$(tkn pipelinerun retry --partial "$SOURCE_3" -n "$NAMESPACE" 2>&1)"
SHIM_EXIT=$?
set -e
echo "$OUT_3"
RETRY_3="$(awk '/^Created:/{print $2}' <<<"$OUT_3")"

FAIL_REASON="$(kubectl get pipelinerun "$RETRY_3" -n "$NAMESPACE" \
  -o jsonpath='{.status.conditions[?(@.type=="Succeeded")].reason}')"
check "shim reports setup failure"      "1"       "$SHIM_EXIT"
check "retry fails with RetrySetupFailed" "RetrySetupFailed" "$FAIL_REASON"
check "zero TaskRuns created"           "0"       "$(kubectl get taskrun -n "$NAMESPACE" \
  -l "tekton.dev/pipelineRun=$RETRY_3" -o name | wc -l | tr -d ' ')"
echo

# ---------------------------------------------------------------------------
echo "=== Summary: $PASS passed, $FAIL failed ==="
[[ "$FAIL" -eq 0 ]] || exit 1
