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

	v1 "github.com/tektoncd/pipeline/pkg/apis/pipeline/v1"
)

func TestIsBypassed_SkipReason(t *testing.T) {
	taskA := rptNamed("A")
	taskA.IsBypassed = true // was: facts.BypassedTasks = map[string]bool{"A": true}

	state := PipelineRunState{taskA}
	facts := buildFacts(t, state, nil)

	skipStatus := taskA.Skip(facts)
	if !skipStatus.IsSkipped {
		t.Errorf("expected A to be skipped (bypassed), but IsSkipped=false")
	}
	if skipStatus.SkippingReason != v1.BypassedRetrySkip {
		t.Errorf("expected BypassedRetrySkip, got %q", skipStatus.SkippingReason)
	}
}

func TestIsBypassed_DownstreamRuns(t *testing.T) {
	taskA := rptNamed("A")
	taskA.IsBypassed = true

	taskB := &ResolvedPipelineTask{
		PipelineTask: &v1.PipelineTask{
			Name:     "B",
			RunAfter: []string{"A"},
		},
	}

	state := PipelineRunState{taskA, taskB}
	facts := buildFacts(t, state, nil)

	if !taskA.Skip(facts).IsSkipped {
		t.Fatalf("expected A to be bypassed")
	}

	bSkip := taskB.Skip(facts)
	if bSkip.IsSkipped {
		t.Errorf("expected B to run (not skipped), but got SkippingReason=%q", bSkip.SkippingReason)
	}
}

func TestIsBypassed_NonBypassedTaskUnaffected(t *testing.T) {
	taskA := rptNamed("A")
	taskA.IsBypassed = true

	taskB := rptNamed("B")
	taskB.TaskRuns = []*v1.TaskRun{makeFailed(trs[1])}

	state := PipelineRunState{taskA, taskB}
	facts := buildFacts(t, state, nil)

	skipA := taskA.Skip(facts)
	if skipA.SkippingReason != v1.BypassedRetrySkip {
		t.Errorf("A: expected BypassedRetrySkip, got %q", skipA.SkippingReason)
	}
	skipB := taskB.Skip(facts)
	if skipB.SkippingReason == v1.BypassedRetrySkip {
		t.Errorf("B must NOT be bypassed, got BypassedRetrySkip")
	}
	if !taskB.isFailure() {
		t.Errorf("B should still be recognised as a failure")
	}
}

func TestIsBypassed_EmptyBypassedTasks(t *testing.T) {
	taskA := rptNamed("A")
	taskA.TaskRuns = []*v1.TaskRun{makeSucceeded(trs[0])}

	state := PipelineRunState{taskA}
	facts := buildFacts(t, state, nil)
	// BypassedTasks intentionally left nil.

	skipA := taskA.Skip(facts)
	if skipA.SkippingReason == v1.BypassedRetrySkip {
		t.Errorf("expected no bypass on normal run, got BypassedRetrySkip")
	}
}

// TestResolveResultRef_BypassedTask verifies that a result reference to a
// bypassed task resolves from its pre-populated ResultsCache, with no
// TaskRuns required on the bypassed task.
func TestResolveResultRef_BypassedTask(t *testing.T) {
	taskA := rptNamed("A")
	taskA.IsBypassed = true
	taskA.ResultsCache = map[string][]string{"foo": {"bar"}}

	taskB := &ResolvedPipelineTask{
		PipelineTask: &v1.PipelineTask{
			Name: "B",
			Params: []v1.Param{{
				Name:  "input",
				Value: *v1.NewStructuredValues("$(tasks.A.results.foo)"),
			}},
		},
	}

	state := PipelineRunState{taskA, taskB}

	got, pt, err := ResolveResultRef(state, taskB)
	if err != nil {
		t.Fatalf("ResolveResultRef() unexpected error: %v (pt=%q)", err, pt)
	}
	if len(got) != 1 {
		t.Fatalf("expected 1 resolved ref, got %d: %v", len(got), got)
	}
	if got[0].Value.StringVal != "bar" {
		t.Errorf("expected resolved value %q, got %q", "bar", got[0].Value.StringVal)
	}
	if got[0].FromTaskRun != "" {
		t.Errorf("expected FromTaskRun to be empty for a bypassed task, got %q", got[0].FromTaskRun)
	}
}

// TestResolveBypassedResultRef_MissingResult verifies that resolving a
// result not present in a bypassed task's ResultsCache returns an error.
func TestResolveBypassedResultRef_MissingResult(t *testing.T) {
	taskA := rptNamed("A")
	taskA.IsBypassed = true
	taskA.ResultsCache = map[string][]string{"foo": {"bar"}}

	ref := &v1.ResultRef{PipelineTask: "A", Result: "missing"}

	_, err := resolveBypassedResultRef(taskA, ref)
	if err == nil {
		t.Fatalf("expected an error for missing result, got nil")
	}
}

// TestGetTaskRunsResults_BypassedTask verifies that GetTaskRunsResults
// includes a bypassed task's memoized results.
func TestGetTaskRunsResults_BypassedTask(t *testing.T) {
	taskA := rptNamed("A")
	taskA.IsBypassed = true
	taskA.ResultsCache = map[string][]string{"foo": {"bar"}}

	state := PipelineRunState{taskA}

	results := state.GetTaskRunsResults()
	got, ok := results["A"]
	if !ok {
		t.Fatalf("expected results for bypassed task A, got none: %v", results)
	}
	if len(got) != 1 || got[0].Name != "foo" || got[0].Value.StringVal != "bar" {
		t.Errorf("expected [{foo bar}], got %v", got)
	}
}

// TestResolveResultRef_NonBypassedTaskUnaffected is a regression check: a
// normal successful task's result still resolves via TaskRuns, unaffected
// by the bypassed-task code path.
func TestResolveResultRef_NonBypassedTaskUnaffected(t *testing.T) {
	taskA := rptNamed("A")
	taskA.TaskRuns = []*v1.TaskRun{makeSucceeded(trs[0])}
	// trs[0] does not have Results populated by default — set it explicitly.
	taskA.TaskRuns[0].Status.TaskRunStatusFields.Results = []v1.TaskRunResult{{
		Name:  "foo",
		Value: *v1.NewStructuredValues("live-value"),
	}}

	taskB := &ResolvedPipelineTask{
		PipelineTask: &v1.PipelineTask{
			Name: "B",
			Params: []v1.Param{{
				Name:  "input",
				Value: *v1.NewStructuredValues("$(tasks.A.results.foo)"),
			}},
		},
	}

	state := PipelineRunState{taskA, taskB}

	got, pt, err := ResolveResultRef(state, taskB)
	if err != nil {
		t.Fatalf("ResolveResultRef() unexpected error: %v (pt=%q)", err, pt)
	}
	if len(got) != 1 || got[0].Value.StringVal != "live-value" {
		t.Errorf("expected live-value from TaskRun, got %v", got)
	}
	if got[0].FromTaskRun == "" {
		t.Errorf("expected FromTaskRun to be set for a normal task")
	}
}
