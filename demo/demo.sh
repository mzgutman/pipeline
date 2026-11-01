# Use the POC tkn shim
export PATH="$( cd "$(dirname "$0")/bin" && pwd ):$PATH"

#!/usr/bin/env bash
set -euo pipefail

NAMESPACE="${NAMESPACE:-default}"
WAIT_TIMEOUT="${WAIT_TIMEOUT:-120s}"

echo "=== POC: Partial PipelineRun Retry Demo ==="

# 1. Apply pipeline and tasks
echo "[1/5] Applying pipeline and tasks..."
kubectl apply -f "$(dirname "$0")/tasks.yaml" -n "$NAMESPACE"
kubectl apply -f "$(dirname "$0")/pipeline.yaml" -n "$NAMESPACE"

# 2. Create source PipelineRun (will fail at build)
echo "[2/5] Creating source PipelineRun (should-fail=true)..."
SOURCE_NAME=$(kubectl create -f "$(dirname "$0")/source-run.yaml" -n "$NAMESPACE" -o jsonpath='{.metadata.name}')
echo "      Source run: $SOURCE_NAME"

# 3. Wait for source run to fail
echo "[3/5] Waiting for source run to fail..."
kubectl wait pipelinerun "$SOURCE_NAME" \
  --for=condition=Succeeded=False \
  --timeout="$WAIT_TIMEOUT" \
  -n "$NAMESPACE"
echo "      Source run failed as expected ✓"

# 4. Create retry PipelineRun (should-fail=false, with retryOf annotation)
echo "[4/5] Creating retry PipelineRun (should-fail=false)..."
SOURCE_UID=$(kubectl get pipelinerun "$SOURCE_NAME" -n "$NAMESPACE" -o jsonpath='{.metadata.uid}')
RETRY_NAME=$(kubectl create -n "$NAMESPACE" -o jsonpath='{.metadata.name}' -f - <<EOF
apiVersion: tekton.dev/v1
kind: PipelineRun
metadata:
  generateName: poc-demo-retry-
  annotations:
    tekton.dev/retryOf: "$SOURCE_NAME"
    tekton.dev/retryOfUID: "$SOURCE_UID"
    tekton.dev/retryStrategy: "partial"
spec:
  pipelineRef:
    name: poc-demo-pipeline
  params:
  - name: should-fail
    value: "false"
EOF
)
echo "      Retry run: $RETRY_NAME"

# 5. Wait for retry to succeed and verify
echo "[5/5] Waiting for retry run to succeed..."
kubectl wait pipelinerun "$RETRY_NAME" \
  --for=condition=Succeeded=True \
  --timeout="$WAIT_TIMEOUT" \
  -n "$NAMESPACE"

# Verify clone was NOT re-run (no new TaskRun for clone)
CLONE_TASKRUNS=$(kubectl get taskrun -n "$NAMESPACE" \
  -l "tekton.dev/pipelineRun=$RETRY_NAME,tekton.dev/pipelineTask=clone" \
  --no-headers 2>/dev/null | wc -l)

echo ""
echo "=== Results ==="
if [ "$CLONE_TASKRUNS" -eq 0 ]; then
  echo "✅ PASS: 'clone' was bypassed — no new TaskRun created for it"
else
  echo "❌ FAIL: 'clone' was unexpectedly re-run ($CLONE_TASKRUNS TaskRun(s) found)"
  exit 1
fi
echo "✅ PASS: retry PipelineRun succeeded"
echo ""
echo "Demo complete! Partial retry is working."
