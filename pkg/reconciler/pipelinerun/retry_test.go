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

package pipelinerun

import (
	"fmt"
	"testing"

	"github.com/tektoncd/pipeline/pkg/apis/pipeline"
	v1 "github.com/tektoncd/pipeline/pkg/apis/pipeline/v1"
	th "github.com/tektoncd/pipeline/pkg/reconciler/testing"
	"github.com/tektoncd/pipeline/test"
	"github.com/tektoncd/pipeline/test/parse"
	corev1 "k8s.io/api/core/v1"
	"knative.dev/pkg/apis"
)

var (
	retryTestTask = &v1.Task{ObjectMeta: baseObjectMeta("hello-world", "foo")}

	retryTestPipeline = &v1.Pipeline{
		ObjectMeta: baseObjectMeta("retry-test-pipeline", "foo"),
		Spec: v1.PipelineSpec{
			Tasks: []v1.PipelineTask{
				{
					Name:    "task-a",
					TaskRef: &v1.TaskRef{Name: "hello-world"},
				},
				{
					Name:     "task-b",
					TaskRef:  &v1.TaskRef{Name: "hello-world"},
					RunAfter: []string{"task-a"},
				},
			},
		},
	}
)

const (
	retrySourceUID = "abc-123"
	retryRunName   = "my-retry-run"
	retrySource    = "my-source-run"
)

// buildRetryRun builds the retry PipelineRun "my-retry-run" of "my-source-run"
// with the given strategy and retryOfUID annotations, referencing the named
// pipeline.
func buildRetryRun(t *testing.T, pipelineRef, strategy, uid string) *v1.PipelineRun {
	t.Helper()
	yaml := "metadata:\n" +
		"  name: " + retryRunName + "\n" +
		"  namespace: foo\n" +
		"  annotations:\n" +
		"    tekton.dev/retryOf: \"" + retrySource + "\"\n" +
		"    tekton.dev/retryOfUID: \"" + uid + "\"\n" +
		"    tekton.dev/retryStrategy: \"" + strategy + "\"\n" +
		"spec:\n" +
		"  pipelineRef:\n" +
		"    name: " + pipelineRef + "\n" +
		"  taskRunTemplate:\n" +
		"    serviceAccountName: test-sa\n"
	return parse.MustParseV1PipelineRun(t, yaml)
}

// buildRetrySource builds the source PipelineRun "my-source-run" of
// retry-test-pipeline (task-a → task-b), terminal with the given Succeeded
// condition, plus one child TaskRun per entry of taskConds (in order task-a,
// task-b, each terminal with the given condition). The source's UID is set to
// retrySourceUID so it matches the retry annotation used in these tests.
func buildRetrySource(t *testing.T, succeeded string, taskConds ...string) (*v1.PipelineRun, []*v1.TaskRun) {
	t.Helper()
	taskNames := []string{"task-a", "task-b"}
	if len(taskConds) > len(taskNames) {
		t.Fatalf("at most %d task conditions supported", len(taskNames))
	}
	children := ""
	taskRuns := make([]*v1.TaskRun, 0, len(taskConds))
	for i, cond := range taskConds {
		taskName := taskNames[i]
		children += "  - name: " + retrySource + "-" + taskName + "\n" +
			"    pipelineTaskName: " + taskName + "\n" +
			"    kind: TaskRun\n"
		taskRuns = append(taskRuns, parse.MustParseTaskRunWithObjectMeta(t,
			taskRunObjectMeta(retrySource+"-"+taskName, "foo", retrySource, "retry-test-pipeline", taskName, true),
			fmt.Sprintf(`
spec:
  taskRef:
    name: hello-world
  serviceAccountName: test-sa
status:
  conditions:
  - type: Succeeded
    status: %q
`, cond)))
	}
	yaml := "metadata:\n" +
		"  name: " + retrySource + "\n" +
		"  namespace: foo\n" +
		"spec:\n" +
		"  pipelineRef:\n" +
		"    name: retry-test-pipeline\n" +
		"  taskRunTemplate:\n" +
		"    serviceAccountName: test-sa\n" +
		"status:\n" +
		"  conditions:\n" +
		"  - type: Succeeded\n" +
		"    status: \"" + succeeded + "\"\n" +
		"  pipelineSpec:\n" +
		"    tasks:\n" +
		"    - name: task-a\n" +
		"      taskRef:\n" +
		"        name: hello-world\n" +
		"    - name: task-b\n" +
		"      taskRef:\n" +
		"        name: hello-world\n" +
		"      runAfter: [\"task-a\"]\n" +
		"  childReferences:\n" + children
	sourceRun := parse.MustParseV1PipelineRun(t, yaml)
	sourceRun.UID = retrySourceUID
	return sourceRun, taskRuns
}

// TestReconcileRetryPipelineRun_DetectsAnnotation verifies that a PipelineRun
// carrying tekton.dev/retryOf enters the setupRetry path, persists the retry
// plan, and then schedules normally. The strategy here is "full" with a
// source that has no recorded child references, so nothing is bypassed and
// the single pipeline task is rerun like a fresh PipelineRun.
func TestReconcileRetryPipelineRun_DetectsAnnotation(t *testing.T) {
	prName := "my-retry-run"
	namespace := "foo"

	retryRun := buildRetryRun(t, "test-pipeline", v1.RetryStrategyFull, retrySourceUID)

	sourceRunYAML := "metadata:\n" +
		"  name: my-source-run\n" +
		"  namespace: foo\n" +
		"spec:\n" +
		"  pipelineRef:\n" +
		"    name: test-pipeline\n" +
		"  taskRunTemplate:\n" +
		"    serviceAccountName: test-sa\n" +
		"status:\n" +
		"  conditions:\n" +
		"  - type: Succeeded\n" +
		"    status: \"False\"\n" +
		"  pipelineSpec:\n" +
		"    tasks:\n" +
		"    - name: hello-world-1\n" +
		"      taskRef:\n" +
		"        name: hello-world\n"
	sourceRun := parse.MustParseV1PipelineRun(t, sourceRunYAML)
	sourceRun.UID = retrySourceUID

	prs := []*v1.PipelineRun{retryRun, sourceRun}

	d := test.Data{
		PipelineRuns: prs,
		Pipelines:    []*v1.Pipeline{simpleHelloWorldPipeline},
		Tasks:        []*v1.Task{simpleHelloWorldTask},
	}

	prt := newPipelineRunTest(t, d)
	defer prt.Cancel()

	reconciledRun, clients := prt.reconcileRun(namespace, prName, []string{}, false)
	if !reconciledRun.Status.RetrySetupComplete {
		t.Fatal("expected retry plan to be persisted before execution")
	}
	if taskRuns := getTaskRunsForPipelineRun(prt.TestAssets.Ctx, t, clients, namespace, prName); len(taskRuns) != 0 {
		t.Fatalf("expected no TaskRuns before the retry plan is persisted, got %d", len(taskRuns))
	}

	// The next reconciliation schedules work from the persisted plan.
	reconciledRun, clients = prt.reconcileRun(namespace, prName, []string{}, false)

	// The retry PipelineRun must carry the annotation
	if !reconciledRun.IsRetry() {
		t.Errorf("expected IsRetry() to be true, got false")
	}

	// Full strategy with no preserved tasks: the single pipeline task
	// schedules normally.
	taskRuns := getTaskRunsForPipelineRun(prt.TestAssets.Ctx, t, clients, namespace, prName)
	validateTaskRunsCount(t, taskRuns, 1)
}

// TestReconcileRetryPipelineRun_NonRetryUnaffected verifies that a normal
// PipelineRun (no retryOf annotation) is completely unaffected by the new
// retry detection branch: it still creates a TaskRun as before.
func TestReconcileRetryPipelineRun_NonRetryUnaffected(t *testing.T) {
	prName := "normal-run"
	namespace := "foo"

	normalRunYAML := "metadata:\n" +
		"  name: normal-run\n" +
		"  namespace: foo\n" +
		"spec:\n" +
		"  pipelineRef:\n" +
		"    name: test-pipeline\n" +
		"  taskRunTemplate:\n" +
		"    serviceAccountName: test-sa\n"

	prs := []*v1.PipelineRun{parse.MustParseV1PipelineRun(t, normalRunYAML)}

	d := test.Data{
		PipelineRuns: prs,
		Pipelines:    []*v1.Pipeline{simpleHelloWorldPipeline},
		Tasks:        []*v1.Task{simpleHelloWorldTask},
	}

	prt := newPipelineRunTest(t, d)
	defer prt.Cancel()

	reconciledRun, clients := prt.reconcileRun(namespace, prName, []string{
		"Normal Started.*",
		"Normal Running.*",
	}, false)

	// Must NOT be detected as a retry
	if reconciledRun.IsRetry() {
		t.Errorf("expected IsRetry() to be false for a normal PipelineRun, got true")
	}

	// Normal reconcile path must still create a TaskRun
	taskRuns := getTaskRunsForPipelineRun(prt.TestAssets.Ctx, t, clients, namespace, prName)
	validateTaskRunsCount(t, taskRuns, 1)

	// Confirm the TaskRun has the expected owner label
	for _, tr := range taskRuns {
		if tr.Labels[pipeline.PipelineRunLabelKey] != prName {
			t.Errorf("TaskRun %s missing expected PipelineRun label", tr.Name)
		}
	}
}

// TestReconcileRetryPipelineRun_FullStrategyRerunsSucceededTasks verifies
// that a full retry does not preserve the source's succeeded tasks: task-a
// (succeeded on the source run) is scheduled with a fresh TaskRun instead of
// being bypassed. task-b is not scheduled yet because it runs after task-a,
// which is still in flight — normal DAG semantics, identical to a fresh run.
func TestReconcileRetryPipelineRun_FullStrategyRerunsSucceededTasks(t *testing.T) {
	namespace := "foo"
	prName := retryRunName

	retryRun := buildRetryRun(t, "retry-test-pipeline", v1.RetryStrategyFull, retrySourceUID)
	sourceRun, sourceTaskRuns := buildRetrySource(t, "False", "True", "False")

	d := test.Data{
		PipelineRuns: []*v1.PipelineRun{retryRun, sourceRun},
		Pipelines:    []*v1.Pipeline{retryTestPipeline},
		Tasks:        []*v1.Task{retryTestTask},
		TaskRuns:     sourceTaskRuns,
	}

	prt := newPipelineRunTest(t, d)
	defer prt.Cancel()

	reconciledRun, clients := prt.reconcileRun(namespace, prName, []string{}, false)
	if !reconciledRun.Status.RetrySetupComplete {
		t.Fatal("expected retry plan to be persisted before execution")
	}
	if taskRuns := getTaskRunsForPipelineRun(prt.TestAssets.Ctx, t, clients, namespace, prName); len(taskRuns) != 0 {
		t.Fatalf("expected no TaskRuns before the retry plan is persisted, got %d", len(taskRuns))
	}
	reconciledRun, clients = prt.reconcileRun(namespace, prName, []string{}, false)

	if len(reconciledRun.Status.BypassedTaskNames) != 0 {
		t.Errorf("expected no bypassed tasks for a full retry, got %v", reconciledRun.Status.BypassedTaskNames)
	}

	// task-a succeeded on the source run but a full retry must NOT preserve
	// it: it gets a fresh TaskRun like any other task.
	taskATaskRuns := getTaskRunsForPipelineTask(prt.TestAssets.Ctx, t, clients, namespace, prName, "task-a")
	validateTaskRunsCount(t, taskATaskRuns, 1)

	// task-b runs after task-a and task-a has not completed yet, so it is
	// not scheduled in this reconciliation.
	taskBTaskRuns := getTaskRunsForPipelineTask(prt.TestAssets.Ctx, t, clients, namespace, prName, "task-b")
	validateTaskRunsCount(t, taskBTaskRuns, 0)
}

// TestReconcileRetryPipelineRun_OnlyRerunTaskScheduled verifies that when the
// source PipelineRun has task-a succeeded and task-b failed, the retry
// PipelineRun creates a TaskRun only for task-b (task-a is bypassed).
func TestReconcileRetryPipelineRun_OnlyRerunTaskScheduled(t *testing.T) {
	namespace := "foo"
	prName := retryRunName

	retryRun := buildRetryRun(t, "retry-test-pipeline", v1.RetryStrategyPartial, retrySourceUID)
	sourceRun, sourceTaskRuns := buildRetrySource(t, "False", "True", "False")

	d := test.Data{
		PipelineRuns: []*v1.PipelineRun{retryRun, sourceRun},
		Pipelines:    []*v1.Pipeline{retryTestPipeline},
		Tasks:        []*v1.Task{retryTestTask},
		TaskRuns:     sourceTaskRuns,
	}

	prt := newPipelineRunTest(t, d)
	defer prt.Cancel()

	reconciledRun, clients := prt.reconcileRun(namespace, prName, []string{}, false)
	if !reconciledRun.Status.RetrySetupComplete {
		t.Fatal("expected retry plan to be persisted before execution")
	}
	if taskRuns := getTaskRunsForPipelineRun(prt.TestAssets.Ctx, t, clients, namespace, prName); len(taskRuns) != 0 {
		t.Fatalf("expected no TaskRuns before the retry plan is persisted, got %d", len(taskRuns))
	}
	reconciledRun, clients = prt.reconcileRun(namespace, prName, []string{}, false)

	if !reconciledRun.IsRetry() {
		t.Errorf("expected IsRetry() to be true, got false")
	}

	allTaskRuns := getTaskRunsForPipelineRun(prt.TestAssets.Ctx, t, clients, namespace, prName)
	validateTaskRunsCount(t, allTaskRuns, 1)

	taskATaskRuns := getTaskRunsForPipelineTask(prt.TestAssets.Ctx, t, clients, namespace, prName, "task-a")
	validateTaskRunsCount(t, taskATaskRuns, 0)

	taskBTaskRuns := getTaskRunsForPipelineTask(prt.TestAssets.Ctx, t, clients, namespace, prName, "task-b")
	validateTaskRunsCount(t, taskBTaskRuns, 1)
}

// TestReconcileRetryPipelineRun_SucceedsWhenRerunTaskSucceeds verifies that
// once task-b (the rerun task) has already succeeded, the retry PipelineRun
// reaches Succeeded=True even though task-a was bypassed and never ran on
// this retry.
func TestReconcileRetryPipelineRun_SucceedsWhenRerunTaskSucceeds(t *testing.T) {
	namespace := "foo"
	prName := retryRunName

	retryRun := buildRetryRun(t, "retry-test-pipeline", v1.RetryStrategyPartial, retrySourceUID)
	sourceRun, sourceTaskRuns := buildRetrySource(t, "False", "True", "False")

	// Pre-seed an already-succeeded TaskRun for task-b owned by the RETRY
	// PipelineRun itself, simulating that it completed on a prior reconcile.
	retryTaskRunB := parse.MustParseTaskRunWithObjectMeta(t,
		taskRunObjectMeta("my-retry-run-task-b", namespace, retryRunName, "retry-test-pipeline", "task-b", true),
		`
spec:
  taskRef:
    name: hello-world
  serviceAccountName: test-sa
status:
  conditions:
  - type: Succeeded
    status: "True"
`)

	d := test.Data{
		PipelineRuns: []*v1.PipelineRun{retryRun, sourceRun},
		Pipelines:    []*v1.Pipeline{retryTestPipeline},
		Tasks:        []*v1.Task{retryTestTask},
		TaskRuns:     append(sourceTaskRuns, retryTaskRunB),
	}

	prt := newPipelineRunTest(t, d)
	defer prt.Cancel()

	reconciledRun, _ := prt.reconcileRun(namespace, prName, []string{}, false)
	if !reconciledRun.Status.RetrySetupComplete {
		t.Fatal("expected retry plan to be persisted before execution")
	}
	reconciledRun, _ = prt.reconcileRun(namespace, prName, []string{}, false)

	condition := reconciledRun.Status.GetCondition(apis.ConditionSucceeded)
	if condition == nil || condition.Status != corev1.ConditionTrue {
		t.Fatalf("expected retry PipelineRun to be Succeeded=True, got %+v", condition)
	}
}

// TestReconcileRetryPipelineRun_MalformedRequestFailsClosed verifies the
// TEP-0194 fail-close contract: a malformed or unsatisfiable retry request
// must fail the retry PipelineRun terminally with reason RetrySetupFailed
// BEFORE any TaskRun is created. There is no fallback to a full retry and no
// indefinite requeue loop.
func TestReconcileRetryPipelineRun_MalformedRequestFailsClosed(t *testing.T) {
	namespace := "foo"
	prName := retryRunName

	testCases := []struct {
		name string
		// retry annotations
		strategy   string
		retryOfUID string
		// source seeding; seedSource=false leaves "my-source-run" absent
		seedSource      bool
		sourceSucceeded string
		sourceTaskConds []string
	}{{
		name:            "invalid strategy",
		strategy:        "bogus",
		retryOfUID:      retrySourceUID,
		seedSource:      true,
		sourceSucceeded: "False",
		sourceTaskConds: []string{"True", "False"},
	}, {
		name:            "missing strategy",
		strategy:        "",
		retryOfUID:      retrySourceUID,
		seedSource:      true,
		sourceSucceeded: "False",
		sourceTaskConds: []string{"True", "False"},
	}, {
		name:       "missing source",
		strategy:   v1.RetryStrategyPartial,
		retryOfUID: retrySourceUID,
	}, {
		name:            "source UID mismatch",
		strategy:        v1.RetryStrategyPartial,
		retryOfUID:      "stale-uid",
		seedSource:      true,
		sourceSucceeded: "False",
		sourceTaskConds: []string{"True", "False"},
	}, {
		name:            "non-terminal source",
		strategy:        v1.RetryStrategyPartial,
		retryOfUID:      retrySourceUID,
		seedSource:      true,
		sourceSucceeded: "Unknown",
		sourceTaskConds: []string{"True", "False"},
	}, {
		name:            "empty rerun subgraph (retry of a succeeded run)",
		strategy:        v1.RetryStrategyPartial,
		retryOfUID:      retrySourceUID,
		seedSource:      true,
		sourceSucceeded: "True",
		sourceTaskConds: []string{"True", "True"},
	}}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			retryRun := buildRetryRun(t, "retry-test-pipeline", tc.strategy, tc.retryOfUID)

			prs := []*v1.PipelineRun{retryRun}
			var taskRuns []*v1.TaskRun
			if tc.seedSource {
				sourceRun, sourceTaskRuns := buildRetrySource(t, tc.sourceSucceeded, tc.sourceTaskConds...)
				prs = append(prs, sourceRun)
				taskRuns = sourceTaskRuns
			}

			d := test.Data{
				PipelineRuns: prs,
				Pipelines:    []*v1.Pipeline{retryTestPipeline},
				Tasks:        []*v1.Task{retryTestTask},
				TaskRuns:     taskRuns,
			}

			prt := newPipelineRunTest(t, d)
			defer prt.Cancel()

			// The malformed request must reconcile to a permanent error...
			reconciledRun, clients := prt.reconcileRun(namespace, prName, []string{}, true)

			// ...and the PipelineRun must be terminally failed with the
			// RetrySetupFailed reason, before any TaskRun was created.
			th.CheckPipelineRunConditionStatusAndReason(t, reconciledRun.Status,
				corev1.ConditionFalse, v1.PipelineRunReasonRetrySetupFailed.String())
			if reconciledRun.Status.RetrySetupComplete {
				t.Error("expected RetrySetupComplete to stay false for a failed setup")
			}
			if msg := reconciledRun.Status.GetCondition(apis.ConditionSucceeded).Message; msg == "" {
				t.Error("expected an actionable failure message on the condition")
			}
			createdTaskRuns := getTaskRunsForPipelineRun(prt.TestAssets.Ctx, t, clients, namespace, prName)
			validateTaskRunsCount(t, createdTaskRuns, 0)

			// A later reconciliation must not resurrect the run (no requeue
			// loop, no fallback to a full retry): it stays terminally failed
			// and still has no TaskRuns.
			reconciledRun, clients = prt.reconcileRun(namespace, prName, []string{}, false)
			th.CheckPipelineRunConditionStatusAndReason(t, reconciledRun.Status,
				corev1.ConditionFalse, v1.PipelineRunReasonRetrySetupFailed.String())
			createdTaskRuns = getTaskRunsForPipelineRun(prt.TestAssets.Ctx, t, clients, namespace, prName)
			validateTaskRunsCount(t, createdTaskRuns, 0)
		})
	}
}
