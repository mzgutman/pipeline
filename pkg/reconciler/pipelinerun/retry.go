package pipelinerun

import (
	"context"
	"fmt"

	v1 "github.com/tektoncd/pipeline/pkg/apis/pipeline/v1"
	"github.com/tektoncd/pipeline/pkg/reconciler/pipelinerun/resources"
	"knative.dev/pkg/logging"
)

// setupRetry is called on every reconciliation of a retry PipelineRun.
// It fetches the source PipelineRun, computes the preserved/rerun subgraph,
// harvests memoized results, and stores both on pr.Status so that the
// rest of ReconcileKind can treat preserved tasks as already-succeeded.
//
// POC-2: ComputeRetrySubgraph
// POC-3: HarvestRetryResults → pr.Status.RetriedTaskResults
// POC-4: preserved task names → pr.Status.BypassedTaskNames
func (c *Reconciler) setupRetry(ctx context.Context, pr *v1.PipelineRun) error {
	logger := logging.FromContext(ctx)

	sourceRunName := pr.Annotations[v1.RetryOfAnnotation]
	strategy := pr.Annotations[v1.RetryStrategyAnnotation]
	logger.Infof("PipelineRun %s/%s is a retry of %s (strategy=%s)",
		pr.Namespace, pr.Name, sourceRunName, strategy)

	// Already computed on a previous reconciliation — nothing to do.
	if pr.Status.RetrySetupComplete {
		return nil
	}

	// POC-2/3/4: fetch the source PipelineRun.
	sourcePR, err := c.pipelineRunLister.PipelineRuns(pr.Namespace).Get(sourceRunName)
	if err != nil {
		return fmt.Errorf("setupRetry: could not fetch source PipelineRun %q: %w", sourceRunName, err)
	}

	// In setupRetry, replace the BuildRetrySourceState call with:
	var sourceState resources.PipelineRunState
	for _, ref := range sourcePR.Status.ChildReferences {
		if ref.Kind != "TaskRun" {
			continue
		}
		tr, err := c.taskRunLister.TaskRuns(pr.Namespace).Get(ref.Name)
		if err != nil {
			continue // best-effort for POC
		}
		rpt := &resources.ResolvedPipelineTask{
			PipelineTask: &v1.PipelineTask{Name: ref.PipelineTaskName},
			TaskRuns:     []*v1.TaskRun{tr},
		}
		sourceState = append(sourceState, rpt)
	}
	sourceFacts, err := resources.BuildMinimalFacts(sourceState)
	if err != nil {
		return fmt.Errorf("setupRetry: %w", err)
	}
	subgraph := resources.ComputeRetrySubgraph(sourceState, sourceFacts)

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
	return nil
}

// keys returns the keys of a bool map as a slice (for logging).
func keys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
