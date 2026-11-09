package pipelinerun

import (
	"context"
	"fmt"

	v1 "github.com/tektoncd/pipeline/pkg/apis/pipeline/v1"
	"github.com/tektoncd/pipeline/pkg/reconciler/pipelinerun/resources"
	"knative.dev/pkg/apis"
	"knative.dev/pkg/controller"
	"knative.dev/pkg/logging"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// setupRetry is called on every reconciliation of a retry PipelineRun.
// It validates the retry request, fetches the source PipelineRun, computes
// the preserved/rerun subgraph, harvests memoized results, and stores both
// on pr.Status so that the rest of ReconcileKind can treat preserved tasks
// as already-succeeded.
//
// Fail-close contract (TEP-0194): a malformed or unsatisfiable retry must
// fail the PipelineRun with reason RetrySetupFailed and an actionable
// message BEFORE any TaskRun is created. There is no fallback to a full
// retry and no indefinite requeue loop for permanent conditions; only
// transient errors (API unreachable, cache lag) are requeued.
//
// POC-2: ComputeRetrySubgraph
// POC-3: HarvestRetryResults → pr.Status.RetriedTaskResults
// POC-4: preserved task names → pr.Status.BypassedTaskNames
func (c *Reconciler) setupRetry(ctx context.Context, pr *v1.PipelineRun) (bool, error) {
	logger := logging.FromContext(ctx)

	sourceRunName := pr.Annotations[v1.RetryOfAnnotation]
	strategy := pr.Annotations[v1.RetryStrategyAnnotation]
	logger.Infof("PipelineRun %s/%s is a retry of %s (strategy=%s)",
		pr.Namespace, pr.Name, sourceRunName, strategy)

	// Already computed on a previous reconciliation — nothing to do.
	if pr.Status.RetrySetupComplete {
		return false, nil
	}

	// A retry can be requeued before the informer cache reflects the status
	// update from the previous reconciliation. Read the live object in that
	// case so we don't recompute the plan forever from a stale cached copy.
	persistedPR, err := c.PipelineClientSet.TektonV1().PipelineRuns(pr.Namespace).Get(ctx, pr.Name, metav1.GetOptions{})
	if err != nil {
		return false, fmt.Errorf("setupRetry: could not refresh retry PipelineRun %q: %w", pr.Name, err)
	}
	if persistedPR.Status.RetrySetupComplete {
		pr.Status.RetriedTaskResults = append([]v1.RetriedTaskResult(nil), persistedPR.Status.RetriedTaskResults...)
		pr.Status.BypassedTaskNames = append([]string(nil), persistedPR.Status.BypassedTaskNames...)
		pr.Status.RetrySetupComplete = true
		logger.Infof("setupRetry: using persisted retry plan from the API")
		return false, nil
	}

	// ---- Fail-close validation (permanent → RetrySetupFailed) -------------

	// The strategy must name a supported retry mode. Anything else (including
	// a missing annotation) is a malformed request: fail, never guess.
	if strategy != v1.RetryStrategyFull && strategy != v1.RetryStrategyPartial {
		return c.failRetrySetup(pr, sourceRunName,
			"invalid retry strategy %q: must be %q or %q", strategy, v1.RetryStrategyFull, v1.RetryStrategyPartial)
	}

	// Fetch the source live (not from the lister) so an informer-cache miss is
	// never mistaken for a deleted source, and a deleted source is never
	// mistaken for a transient error.
	sourcePR, err := c.PipelineClientSet.TektonV1().PipelineRuns(pr.Namespace).Get(ctx, sourceRunName, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return c.failRetrySetup(pr, sourceRunName, "source PipelineRun %q not found in namespace %q", sourceRunName, pr.Namespace)
	}
	if err != nil {
		return false, fmt.Errorf("setupRetry: could not fetch source PipelineRun %q: %w", sourceRunName, err)
	}

	// UID guard: the retry was planned against a specific source object. If
	// the name now points at a different object (deleted and recreated), the
	// precomputed plan would be wrong — fail.
	if uid := pr.Annotations[v1.RetryOfUIDAnnotation]; uid == "" || uid != string(sourcePR.UID) {
		return c.failRetrySetup(pr, sourceRunName,
			"source PipelineRun UID mismatch: annotation %q vs actual %q (was the source deleted and recreated?)",
			uid, sourcePR.UID)
	}

	// The source must be terminal: retrying a run that is still in flight is
	// a race with an unpredictable subgraph.
	if cond := sourcePR.Status.GetCondition(apis.ConditionSucceeded); cond == nil || cond.Status == corev1.ConditionUnknown {
		return c.failRetrySetup(pr, sourceRunName, "source PipelineRun %q is not terminal yet; retry it after it completes", sourceRunName)
	}

	// Full retry: rerun everything — no bypassing, no memoization.
	if strategy == v1.RetryStrategyFull {
		pr.Status.BypassedTaskNames = []string{}
		pr.Status.RetriedTaskResults = nil
		pr.Status.RetrySetupComplete = true
		logger.Infof("setupRetry: strategy=full — rerunning every task")
		return true, nil
	}

	// ---- Partial retry: compute the plan -----------------------------------

	// POC-2/3/4: build the source state from the source's TaskRuns. Fetched
	// live for the same reason as the source itself; a missing TaskRun means
	// pruned/unreadable source data, which the plan cannot be computed from.
	var sourceState resources.PipelineRunState
	for _, ref := range sourcePR.Status.ChildReferences {
		if ref.Kind != "TaskRun" {
			continue
		}
		tr, err := c.PipelineClientSet.TektonV1().TaskRuns(pr.Namespace).Get(ctx, ref.Name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			return c.failRetrySetup(pr, sourceRunName,
				"source TaskRun %q not found (pruned?); the retry plan cannot be computed — request a full retry", ref.Name)
		}
		if err != nil {
			return false, fmt.Errorf("setupRetry: could not fetch source TaskRun %q: %w", ref.Name, err)
		}
		rpt := &resources.ResolvedPipelineTask{
			PipelineTask: &v1.PipelineTask{Name: ref.PipelineTaskName},
			TaskRuns:     []*v1.TaskRun{tr},
		}
		sourceState = append(sourceState, rpt)
	}
	sourceFacts, err := resources.BuildMinimalFacts(sourceState)
	if err != nil {
		return c.failRetrySetup(pr, sourceRunName, "could not build source state: %v", err)
	}
	subgraph := resources.ComputeRetrySubgraph(sourceState, sourceFacts)

	// An empty rerun subgraph means there is nothing to partially retry (the
	// source succeeded, or only finally tasks failed). Fail with an
	// actionable message instead of running a no-op retry (TEP test case 12).
	if len(subgraph.RerunTasks) == 0 {
		return c.failRetrySetup(pr, sourceRunName,
			"nothing to re-run: the source PipelineRun has an empty failed subgraph (did it succeed?); request a full retry instead")
	}

	// POC-3: harvest memoized results from source pipeline results.
	pr.Status.RetriedTaskResults = resources.HarvestRetryResults(
		sourcePR.Status.PipelineSpec, sourcePR.Status.Results, subgraph.PreservedTasks,
	)

	// POC-4: record preserved task names so ReconcileKind can populate
	// pipelineRunFacts.BypassedTasks after facts are constructed.
	pr.Status.BypassedTaskNames = make([]string, 0, len(subgraph.PreservedTasks))
	for name := range subgraph.PreservedTasks {
		pr.Status.BypassedTaskNames = append(pr.Status.BypassedTaskNames, name)
	}
	pr.Status.RetrySetupComplete = true

	logger.Infof("setupRetry: bypassed=%v rerun=%v memoized=%d results",
		pr.Status.BypassedTaskNames, keys(subgraph.RerunTasks), len(pr.Status.RetriedTaskResults))
	return true, nil
}

// failRetrySetup marks the retry PipelineRun terminally failed with reason
// RetrySetupFailed and an actionable message, before any TaskRun is created.
// The returned permanent error stops reconciliation (no requeue loop); the
// marked status is persisted by the reconciler's status update.
func (c *Reconciler) failRetrySetup(pr *v1.PipelineRun, sourceRunName, format string, args ...any) (bool, error) {
	msg := fmt.Sprintf(format, args...)
	err := fmt.Errorf("retry setup failed for PipelineRun %s/%s (retry of %q): %s",
		pr.Namespace, pr.Name, sourceRunName, msg)
	pr.Status.MarkFailed(v1.PipelineRunReasonRetrySetupFailed.String(), err.Error())
	return false, controller.NewPermanentError(err)
}

// keys returns the keys of a bool map as a slice (for logging).
func keys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
