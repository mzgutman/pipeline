package resources

import (
    "time"

    "github.com/tektoncd/pipeline/pkg/reconciler/pipeline/dag"
    v1 "github.com/tektoncd/pipeline/pkg/apis/pipeline/v1"
    "k8s.io/utils/clock"
)
// RetrySubgraph holds the result of ComputeRetrySubgraph.
type RetrySubgraph struct {
    PreservedTasks map[string]bool // succeeded on source → bypass on retry
    RerunTasks     map[string]bool // failed or cascade-skipped → re-execute
}

// cascadeSkipReasons are skip reasons caused by upstream failure.
// WhenExpressionsSkip is intentionally excluded: those re-evaluate on retry.
var cascadeSkipReasons = map[v1.SkippingReason]bool{
    v1.ParentTasksSkip:    true,
    v1.StoppingSkip:       true,
    v1.MissingResultsSkip: true,
}

// ComputeRetrySubgraph splits the source PipelineRun's resolved state into
// preserved (succeeded) and rerun (failed + cascade-skipped) task sets.
// Finally tasks are excluded entirely (Rule 6).
func ComputeRetrySubgraph(state PipelineRunState, facts *PipelineRunFacts) RetrySubgraph {
    preserved := make(map[string]bool)
    rerun     := make(map[string]bool)

    for _, rpt := range state {
        name := rpt.PipelineTask.Name

        // Rule 6: exclude finally tasks
        if rpt.IsFinalTask(facts) {
            continue
        }

        skipReason := rpt.Skip(facts).SkippingReason

        switch {
        case rpt.isFailure():
            // Rule 1: explicit failure → rerun
            rerun[name] = true
        case cascadeSkipReasons[skipReason]:
            // Rule 2 (partial): cascade skip from upstream failure → rerun
            rerun[name] = true
        case rpt.isSuccessful():
            // Succeeded → preserve
            preserved[name] = true
        // WhenExpressionsSkip falls through here: neither preserved nor rerun.
        // It will be re-evaluated naturally on retry — no special handling needed.
        }
    }

    return RetrySubgraph{
        PreservedTasks: preserved,
        RerunTasks:     rerun,
    }
}

// BuildMinimalFacts constructs a PipelineRunFacts sufficient for
// ComputeRetrySubgraph from an already-populated PipelineRunState.
func BuildMinimalFacts(state PipelineRunState) (*PipelineRunFacts, error) {
    var dagTasks []v1.PipelineTask
    for _, rpt := range state {
        dagTasks = append(dagTasks, *rpt.PipelineTask)
    }
    d, err := dag.Build(v1.PipelineTaskList(dagTasks), v1.PipelineTaskList(dagTasks).Deps())
    if err != nil {
        return nil, err
    }
    df, _ := dag.Build(v1.PipelineTaskList(nil), map[string][]string{})
    now := time.Now()
    return &PipelineRunFacts{
        State:           state,
        TasksGraph:      d,
        FinalTasksGraph: df,
        TimeoutsState:   PipelineRunTimeoutsState{StartTime: &now, Clock: clock.RealClock{}},
    }, nil
}