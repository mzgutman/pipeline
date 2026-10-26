/*
Copyright 2024 The Tekton Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package resources

import (
	"testing"
	"time"

	v1 "github.com/tektoncd/pipeline/pkg/apis/pipeline/v1"
	"github.com/tektoncd/pipeline/pkg/reconciler/pipeline/dag"
)

// buildFacts constructs a minimal PipelineRunFacts for the given state.
// The DAG is built from the PipelineTasks in the state so that IsFinalTask
// and Skip work correctly.
func buildFacts(t *testing.T, state PipelineRunState, finalTasks []v1.PipelineTask) *PipelineRunFacts {
	t.Helper()
	var dagTasks []v1.PipelineTask
	for _, rpt := range state {
		dagTasks = append(dagTasks, *rpt.PipelineTask)
	}
	d, err := dag.Build(v1.PipelineTaskList(dagTasks), v1.PipelineTaskList(dagTasks).Deps())
	if err != nil {
		t.Fatalf("buildFacts: dag.Build failed: %v", err)
	}
	df, err := dag.Build(v1.PipelineTaskList(finalTasks), map[string][]string{})
	if err != nil {
		t.Fatalf("buildFacts: dag.Build (finally) failed: %v", err)
	}
	start := time.Now()
	return &PipelineRunFacts{
		State:           state,
		TasksGraph:      d,
		FinalTasksGraph: df,
		TimeoutsState: PipelineRunTimeoutsState{
			StartTime: &start,
			Clock:     testClock,
		},
	}
}

// rpt builds a minimal ResolvedPipelineTask with the given name.
func rptNamed(name string) *ResolvedPipelineTask {
	return &ResolvedPipelineTask{
		PipelineTask: &v1.PipelineTask{Name: name},
	}
}

// TestComputeRetrySubgraph_LinearFailure covers the basic A→B→C chain where B
// fails and C is cascade-skipped via ParentTasksSkip.
// Expected: preserved=[A], rerun=[B, C].
func TestComputeRetrySubgraph_LinearFailure(t *testing.T) {
	taskA := rptNamed("A")
	taskA.TaskRuns = []*v1.TaskRun{makeSucceeded(trs[0])}

	taskB := rptNamed("B")
	taskB.TaskRuns = []*v1.TaskRun{makeFailed(trs[1])}

	// C runs after B; because B failed the pipeline is in stopping state,
	// so C gets StoppingSkip (which is a cascade reason).
	taskC := &ResolvedPipelineTask{
		PipelineTask: &v1.PipelineTask{
			Name:     "C",
			RunAfter: []string{"B"},
		},
	}

	state := PipelineRunState{taskA, taskB, taskC}
	facts := buildFacts(t, state, nil)

	got := ComputeRetrySubgraph(state, facts)

	if !got.PreservedTasks["A"] {
		t.Errorf("expected A to be preserved, got preserved=%v rerun=%v", got.PreservedTasks, got.RerunTasks)
	}
	if !got.RerunTasks["B"] {
		t.Errorf("expected B to be in rerun set, got preserved=%v rerun=%v", got.PreservedTasks, got.RerunTasks)
	}
	if !got.RerunTasks["C"] {
		t.Errorf("expected C to be in rerun set (cascade skip), got preserved=%v rerun=%v", got.PreservedTasks, got.RerunTasks)
	}
	if got.RerunTasks["A"] {
		t.Errorf("A must NOT be in rerun set, got rerun=%v", got.RerunTasks)
	}
}

// TestComputeRetrySubgraph_AllSucceeded verifies that when every task succeeded,
// all are preserved and none are in the rerun set.
func TestComputeRetrySubgraph_AllSucceeded(t *testing.T) {
	taskA := rptNamed("A")
	taskA.TaskRuns = []*v1.TaskRun{makeSucceeded(trs[0])}

	taskB := rptNamed("B")
	taskB.TaskRuns = []*v1.TaskRun{makeSucceeded(trs[1])}

	state := PipelineRunState{taskA, taskB}
	facts := buildFacts(t, state, nil)

	got := ComputeRetrySubgraph(state, facts)

	if !got.PreservedTasks["A"] || !got.PreservedTasks["B"] {
		t.Errorf("expected both tasks preserved, got preserved=%v rerun=%v", got.PreservedTasks, got.RerunTasks)
	}
	if len(got.RerunTasks) != 0 {
		t.Errorf("expected empty rerun set, got rerun=%v", got.RerunTasks)
	}
}

// TestComputeRetrySubgraph_WhenExpressionSkipNotRerun verifies that a task
// skipped due to WhenExpressionsSkip is neither preserved nor in the rerun set
// (it will re-evaluate its when-expressions on retry).
func TestComputeRetrySubgraph_WhenExpressionSkipNotRerun(t *testing.T) {
	taskA := rptNamed("A")
	taskA.TaskRuns = []*v1.TaskRun{makeSucceeded(trs[0])}

	// B has a when expression that evaluates to false → WhenExpressionsSkip.
	taskB := &ResolvedPipelineTask{
		PipelineTask: &v1.PipelineTask{
			Name: "B",
			When: v1.WhenExpressions{{
				Input:    "foo",
				Operator: "in",
				Values:   []string{"bar"}, // "foo" not in ["bar"] → false
			}},
		},
	}

	state := PipelineRunState{taskA, taskB}
	facts := buildFacts(t, state, nil)

	got := ComputeRetrySubgraph(state, facts)

	if !got.PreservedTasks["A"] {
		t.Errorf("expected A to be preserved, got preserved=%v", got.PreservedTasks)
	}
	if got.RerunTasks["B"] {
		t.Errorf("B was WhenExpressionsSkip and must NOT be in rerun set, got rerun=%v", got.RerunTasks)
	}
	if got.PreservedTasks["B"] {
		t.Errorf("B was skipped and must NOT be in preserved set, got preserved=%v", got.PreservedTasks)
	}
}

// TestComputeRetrySubgraph_FinallyTaskExcluded verifies that finally tasks are
// completely excluded from both sets regardless of their outcome (Rule 6).
func TestComputeRetrySubgraph_FinallyTaskExcluded(t *testing.T) {
	taskA := rptNamed("A")
	taskA.TaskRuns = []*v1.TaskRun{makeSucceeded(trs[0])}

	finallyTask := &ResolvedPipelineTask{
		PipelineTask: &v1.PipelineTask{Name: "finally-notify"},
		TaskRuns:     []*v1.TaskRun{makeSucceeded(trs[1])},
	}

	state := PipelineRunState{taskA, finallyTask}
	// Pass finallyTask's PipelineTask in the finally list so IsFinalTask returns true.
	facts := buildFacts(t, PipelineRunState{taskA}, []v1.PipelineTask{*finallyTask.PipelineTask})
	// Re-add finallyTask to the state after building facts so it is iterated.
	facts.State = state

	got := ComputeRetrySubgraph(state, facts)

	if got.PreservedTasks["finally-notify"] || got.RerunTasks["finally-notify"] {
		t.Errorf("finally task must be excluded from both sets, got preserved=%v rerun=%v",
			got.PreservedTasks, got.RerunTasks)
	}
	if !got.PreservedTasks["A"] {
		t.Errorf("expected A to be preserved, got preserved=%v", got.PreservedTasks)
	}
}
