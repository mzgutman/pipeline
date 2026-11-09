# Fan-out DAG partial-retry demo

This example models the DAG:

```text
        ┌── B ── C (fails) ── E
A ──────┤
        └── D (succeeds)
```

A produces a result consumed by B and D. B feeds C, and C feeds E. D is
independent of the B-C-E branch after A. The source run should complete A, B,
and D, fail C, and skip E. The partial retry should preserve A, B, and D,
then rerun C and E.

## Prerequisites

Run `demo/00-setup.sh` and `demo/01-install.sh` from the repository root.
The installation script targets the `poc-partial-retry` Kind cluster by
default. You also need `kubectl`, `ko`, and a real `tkn` CLI on your PATH.

## Run the demo

All three use cases (partial retry, full retry, fail-close) are scripted with
a pass/fail summary:

```bash
demo/use-cases.sh    # from the repository root; NAMESPACE/WAIT_TIMEOUT override
```

Or step through them manually:

```bash
export PATH="$PWD/demo/bin:$PATH"
export NAMESPACE=default

kubectl apply -n "$NAMESPACE" -f demo/dag-fanout/tasks.yaml
kubectl apply -n "$NAMESPACE" -f demo/dag-fanout/pipeline.yaml

SOURCE_RUN=$(kubectl create -n "$NAMESPACE" -f demo/dag-fanout/source-run.yaml \
  -o jsonpath='{.metadata.name}')
echo "Source run: $SOURCE_RUN"

kubectl wait -n "$NAMESPACE" pipelinerun "$SOURCE_RUN" \
  --for=condition=Succeeded=False --timeout=180s

tkn pipelinerun logs -f "$SOURCE_RUN" -n "$NAMESPACE"
```

C waits eight seconds before failing to give the independent D branch time to
finish. Wait for D to succeed before creating the retry, so the demo reliably
shows D being preserved:

```bash
kubectl wait -n "$NAMESPACE" taskrun \
  -l "tekton.dev/pipelineRun=$SOURCE_RUN,tekton.dev/pipelineTask=d" \
  --for=condition=Succeeded=True --timeout=60s
kubectl get taskrun -n "$NAMESPACE" \
  -l "tekton.dev/pipelineRun=$SOURCE_RUN" \
  -o custom-columns=NAME:.metadata.name,TASK:.metadata.labels.tekton\\.dev/pipelineTask,SUCCEEDED:.status.conditions[0].status
```

Create a partial retry and change C's parameter so it succeeds:

```bash
tkn pipelinerun retry --partial "$SOURCE_RUN" \
  --param fail-c=false -n "$NAMESPACE"
```

The shim prints the retry name and the preserved tasks. Copy the printed name:

```bash
RETRY_RUN=<retry-run-name>

kubectl wait -n "$NAMESPACE" pipelinerun "$RETRY_RUN" \
  --for=condition=Succeeded=True --timeout=180s
tkn pipelinerun logs -f "$RETRY_RUN" -n "$NAMESPACE"
```

### Full retry

The same annotation contract supports `strategy: full` — every task
re-executes, nothing is preserved, no results are memoized. This goes through
the same `setupRetry` validation and plan persistence as `--partial`, so both
strategies share one code path (the contrast the TEP argues for):

```bash
tkn pipelinerun retry --full "$SOURCE_RUN" \
  --param fail-c=false -n "$NAMESPACE"
# → "Retry plan ready. Full retry: every task will re-execute (nothing preserved)."

kubectl get taskrun -n "$NAMESPACE" \
  -l "tekton.dev/pipelineRun=<retry-run-name>" \
  -o custom-columns=TASK:.metadata.labels.tekton\\.dev/pipelineTask
# → TaskRuns exist for all five tasks (a, b, c, d, e)
```

Calling `tkn pipelinerun retry` with neither flag still delegates to the real
`tkn` (native rerun, outside the POC contract); `--partial --full` together is
rejected.

Verify that A, B, and D were bypassed and only C and E got new TaskRuns:

```bash
kubectl get pipelinerun "$RETRY_RUN" -n "$NAMESPACE" \
  -o jsonpath='{.status.bypassedTaskNames}{"\n"}{.status.retriedTaskResults}{"\n"}'
kubectl get taskrun -n "$NAMESPACE" \
  -l "tekton.dev/pipelineRun=$RETRY_RUN" \
  -o custom-columns=NAME:.metadata.name,TASK:.metadata.labels.tekton\\.dev/pipelineTask,SUCCEEDED:.status.conditions[0].status
```

Expected preserved task names are `a`, `b`, and `d`; the retry should
create TaskRuns for `c` and `e`. Task scheduling order can vary.

## Fail-close behavior (TEP-0194)

A malformed or unsatisfiable retry never falls back to a full rerun and never
hangs: the controller fails the retry PipelineRun terminally with reason
`RetrySetupFailed` and an actionable message, before creating any TaskRun.
This is enforced in `setupRetry` (`pkg/reconciler/pipelinerun/retry.go`) for:

- an invalid or missing `tekton.dev/retryStrategy` (must be `full` or `partial`);
- a `tekton.dev/retryOf` source that does not exist (or whose TaskRuns were
  pruned);
- a `tekton.dev/retryOfUID` that does not match the source's current UID
  (the source was deleted and recreated under the same name);
- a source that is not terminal yet;
- an empty rerun subgraph (e.g. retrying a succeeded run) — retrying a
  succeeded run fails with "nothing to re-run; request a full retry instead".

`strategy: "full"` reruns every task (empty bypass list, no memoized
results). Transient errors (API unreachable) still requeue; only permanent
conditions fail closed.

You can see it directly by retrying the successful retry run (the shim also
surfaces it: `tkn pipelinerun retry --partial <succeeded-run>` prints the
controller's failure message and exits nonzero instead of hanging):

```bash
# assuming $RETRY_RUN succeeded above
kubectl create -n "$NAMESPACE" -f - <<EOF
apiVersion: tekton.dev/v1
kind: PipelineRun
metadata:
  name: $RETRY_RUN-retry-of-success
  annotations:
    tekton.dev/retryOf: "$RETRY_RUN"
    tekton.dev/retryOfUID: "$(kubectl get pipelinerun "$RETRY_RUN" -n "$NAMESPACE" -o jsonpath='{.metadata.uid}')"
    tekton.dev/retryStrategy: "partial"
spec:
  pipelineRef:
    name: poc-dag-fanout
EOF

kubectl wait -n "$NAMESPACE" pipelineruns.tekton.dev "$RETRY_RUN-retry-of-success" \
  --for=condition=Succeeded=False --timeout=60s
kubectl get pipelineruns.tekton.dev "$RETRY_RUN-retry-of-success" -n "$NAMESPACE" \
  -o jsonpath='{.status.conditions[?(@.type=="Succeeded")].reason}{"\n"}'
# RetrySetupFailed — no TaskRuns were created
```
