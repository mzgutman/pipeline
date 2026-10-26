package resources

import (
    v1 "github.com/tektoncd/pipeline/pkg/apis/pipeline/v1"
)

// HarvestRetryResults extracts memoized results from the source PipelineRun's
// bubbled pipeline results (status.results). It uses the source's pipelineSpec
// to map each pipeline result name back to its originating pipelineTaskName
// and result name, producing a list ready to store in status.retriedTaskResults.
//
// Only tasks in the preservedTasks set are included — results from rerun tasks
// are not memoized here.
func HarvestRetryResults(
    pipelineSpec *v1.PipelineSpec,
    statusResults []v1.PipelineRunResult,
    preservedTasks map[string]bool,
) []v1.RetriedTaskResult {
    // Build a lookup: pipeline result name → resolved value
    resolvedByName := make(map[string]string, len(statusResults))
    for _, r := range statusResults {
        resolvedByName[r.Name] = r.Value.StringVal
    }

    var memoized []v1.RetriedTaskResult
    seen := make(map[string]bool) // deduplicate pipelineTaskName+name pairs

    for _, pr := range pipelineSpec.Results {
        expressions, ok := pr.GetVarSubstitutionExpressions()
        if !ok {
            continue
        }
        refs := v1.NewResultRefs(expressions)
        for _, ref := range refs {
            // Only memoize results from preserved tasks
            if !preservedTasks[ref.PipelineTask] {
                continue
            }
            key := ref.PipelineTask + "/" + ref.Result
            if seen[key] {
                continue
            }
            seen[key] = true
            val, found := resolvedByName[pr.Name]
            if !found {
                continue
            }
            memoized = append(memoized, v1.RetriedTaskResult{
                PipelineTaskName: ref.PipelineTask,
                Name:             ref.Result,
                Value:            val,
            })
        }
    }
    return memoized
}