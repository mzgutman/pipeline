# Partial PipelineRun Retry — PoC demo

This directory runs a live demo of partial PipelineRun retry (the PoC for
[TEP-0194](https://github.com/tektoncd/community/pull/1307)): a pipeline
fails, and a retry re-executes **only the failed portion**, reusing the
results of the tasks that already succeeded. The controller (not the client)
computes what to re-run, and the retry is a new PipelineRun linked to its
source by annotations.

## Prerequisites

- docker (or podman), kind, kubectl, ko
- ~1.3 GB disk for the kind node image (first run only)

## Setup

```bash
./00-setup.sh     # kind cluster "poc-partial-retry"
./01-install.sh   # builds and deploys the partial-retry controller (ko)
```

`01-install.sh` deploys the controller from this branch, including the
partial-retry status schema in the PipelineRun CRD.

## The demo pipeline

`dag-fanout/` defines five tasks:

```
a → b → c → e        (c fails on the source run; e is skipped after it)
        d            (independent of the failure, succeeds)
```

- `a` produces `source-id`; `b` builds `build-id` from it; `c` tests and
  produces `candidate-id` (and is the failure point); `e` consumes the
  candidate; `d` produces an independent `audit-id`.

## Run the use cases

The demo runs in its own namespace, so nothing else in the cluster can
interfere (and it can be re-run on the same cluster):

```bash
export NS=retry-demo
kubectl create namespace "$NS"
NAMESPACE="$NS" ./use-cases.sh
```

Three scenarios run, each with automated checks:

1. **Partial retry** — the source run fails at `c` (so `e` is skipped) while
   `a`, `b`, `d` succeed. The retry re-executes only `c` and `e`; `a`, `b`,
   `d` are bypassed with no new TaskRuns, and their memoized results feed
   the re-run tasks.
2. **Full retry** — same failure, but the retry re-executes every task;
   nothing is preserved.
3. **Fail-close** — retrying a *succeeded* run fails terminally with reason
   `RetrySetupFailed` and creates zero TaskRuns. There is no fallback to a
   full retry; the user may explicitly request one.

The script prints a PASS/FAIL line per check. All checks passing means the
controller computed the retry plan, persisted it before scheduling, and
re-injected the preserved results correctly.

## Inspecting a partial retry

Pick the runs from use case 1 (the partial retry is the first retry in
creation order within the namespace):

```bash
PARTIAL_RETRY=$(kubectl get pr -n "$NS" --sort-by=.metadata.creationTimestamp -o name | grep -- '-retry-' | head -1 | cut -d/ -f2)
SOURCE=${PARTIAL_RETRY%-retry-*}
echo "source: $SOURCE   retry: $PARTIAL_RETRY"
```

**Which TaskRuns exist** — the core of partial retry:

```bash
kubectl get taskrun -n "$NS" -o name | grep "$SOURCE" | sort
```

The source run has TaskRuns for `a`, `b`, `c`, `d`; the retry has **only
`c` and `e`.** The bypassed tasks have no TaskRun on the retry — their
results are reused.

**The controller-owned retry status:**

```bash
kubectl get pr -n "$NS" "$PARTIAL_RETRY" -o jsonpath='{.status}' | jq '{bypassedTaskNames, retriedTaskResults}'
```

The plan is persisted before any TaskRun is created: which tasks were
bypassed, and the memoized results they contribute (values and owning
pipeline task).

**The lineage annotations** (set by the client, immutable after creation):

```bash
kubectl get pr -n "$NS" "$PARTIAL_RETRY" -o jsonpath='{.metadata.annotations}' | jq 'with_entries(select(.key | startswith("tekton.dev/retry")))'
```

## Retry status — the structure a provenance consumer reads

All retry state is written by the controller once, before any TaskRun is
created:

- **Preserved/rerun split** — `status.bypassedTaskNames` lists the tasks
  whose results are reused from the source run; every other DAG task was
  re-executed by this retry (and is visible as one of the retry's child
  TaskRuns).
- **Reused results** — `status.retriedTaskResults` lists each memoized
  result together with the pipeline task that produced it.
- **Lineage** — the `tekton.dev/retryOf` / `retryOfUID` /
  `retryStrategy` annotations identify the source run and the requested
  strategy.

The PoC predates the final TEP naming; the semantics match:

| PoC (this demo) | TEP-0194 |
|---|---|
| `status.bypassedTaskNames` | `status.retryMetadata.preservedTasks` |
| `status.retriedTaskResults` | `status.preservedTaskResults` (typed results + owning-run refs) |
| `tekton.dev/retryOf` / `retryOfUID` / `retryStrategy` annotations | also mirrored in `status.retryMetadata`, validated at admission |
| `status.retrySetupComplete` | internal idempotency marker, not part of the TEP API |

## Teardown

```bash
kind delete cluster --name poc-partial-retry
```
