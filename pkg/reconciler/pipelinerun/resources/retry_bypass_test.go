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

// TestIsBypassed_SkipReason verifies that a task whose name is in BypassedTasks
// is skipped with BypassedRetrySkip and is not executed.
func TestIsBypassed_SkipReason(t *testing.T) {
	// On the retry run, A has no TaskRuns yet — it's being bypassed before execution.
	taskA := rptNamed("A")
	// No taskA.TaskRuns — bypassed tasks start fresh on the retry run.

	state := PipelineRunState{taskA}
	facts := buildFacts(t, state, nil)
	facts.BypassedTasks = map[string]bool{"A": true}

	skipStatus := taskA.Skip(facts)
	if !skipStatus.IsSkipped {
		t.Errorf("expected A to be skipped (bypassed), but IsSkipped=false")
	}
	if skipStatus.SkippingReason != v1.BypassedRetrySkip {
		t.Errorf("expected BypassedRetrySkip, got %q", skipStatus.SkippingReason)
	}
}

func TestIsBypassed_DownstreamRuns(t *testing.T) {
	taskA := rptNamed("A") // no TaskRuns — bypassed on retry run

	taskB := &ResolvedPipelineTask{
		PipelineTask: &v1.PipelineTask{
			Name:     "B",
			RunAfter: []string{"A"},
		},
	}

	state := PipelineRunState{taskA, taskB}
	facts := buildFacts(t, state, nil)
	facts.BypassedTasks = map[string]bool{"A": true}

	if !taskA.Skip(facts).IsSkipped {
		t.Fatalf("expected A to be bypassed")
	}

	bSkip := taskB.Skip(facts)
	if bSkip.IsSkipped {
		t.Errorf("expected B to run (not skipped), but got SkippingReason=%q", bSkip.SkippingReason)
	}
}

func TestIsBypassed_NonBypassedTaskUnaffected(t *testing.T) {
	taskA := rptNamed("A") // no TaskRuns — bypassed

	taskB := rptNamed("B")
	taskB.TaskRuns = []*v1.TaskRun{makeFailed(trs[1])}

	state := PipelineRunState{taskA, taskB}
	facts := buildFacts(t, state, nil)
	facts.BypassedTasks = map[string]bool{"A": true}

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