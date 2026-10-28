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
	"testing"

	"github.com/tektoncd/pipeline/pkg/apis/pipeline"
	v1 "github.com/tektoncd/pipeline/pkg/apis/pipeline/v1"
	"github.com/tektoncd/pipeline/test"
	"github.com/tektoncd/pipeline/test/parse"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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

// TestReconcileRetryPipelineRun_DetectsAnnotation verifies that a PipelineRun
// carrying tekton.dev/retryOf enters the setupRetry path, fetches the source
// PipelineRun, and schedules normally when nothing is preserved (the source
// run here has no recorded child references, so nothing is bypassed and the
// single pipeline task is rerun like a fresh PipelineRun).
func TestReconcileRetryPipelineRun_DetectsAnnotation(t *testing.T) {
	prName := "my-retry-run"
	namespace := "foo"

	retryRunYAML := "metadata:\n" +
		"  name: my-retry-run\n" +
		"  namespace: foo\n" +
		"  annotations:\n" +
		"    tekton.dev/retryOf: \"my-source-run\"\n" +
		"    tekton.dev/retryOfUID: \"abc-123\"\n" +
		"    tekton.dev/retryStrategy: \"partial\"\n" +
		"spec:\n" +
		"  pipelineRef:\n" +
		"    name: test-pipeline\n" +
		"  taskRunTemplate:\n" +
		"    serviceAccountName: test-sa\n"

	sourceRunYAML := "metadata:\n" +
		"  name: my-source-run\n" +
		"  namespace: foo\n" +
		"spec:\n" +
		"  pipelineRef:\n" +
		"    name: test-pipeline\n" +
		"  taskRunTemplate:\n" +
		"    serviceAccountName: test-sa\n" +
		"status:\n" +
		"  pipelineSpec:\n" +
		"    tasks:\n" +
		"    - name: hello-world-1\n" +
		"      taskRef:\n" +
		"        name: hello-world\n"

	prs := []*v1.PipelineRun{
		parse.MustParseV1PipelineRun(t, retryRunYAML),
		parse.MustParseV1PipelineRun(t, sourceRunYAML),
	}

	d := test.Data{
		PipelineRuns: prs,
		Pipelines:    []*v1.Pipeline{simpleHelloWorldPipeline},
		Tasks:        []*v1.Task{simpleHelloWorldTask},
	}

	prt := newPipelineRunTest(t, d)
	defer prt.Cancel()

	reconciledRun, clients := prt.reconcileRun(namespace, prName, []string{}, false)

	// The retry PipelineRun must carry the annotation
	if !reconciledRun.IsRetry() {
		t.Errorf("expected IsRetry() to be true, got false")
	}

	// No preserved/rerun tasks recorded on the source run, so nothing is
	// bypassed — the single pipeline task schedules normally.
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
		_ = metav1.Now() // keep metav1 import used
	}
}

// TestReconcileRetryPipelineRun_OnlyRerunTaskScheduled verifies that when the
// source PipelineRun has task-a succeeded and task-b failed, the retry
// PipelineRun creates a TaskRun only for task-b (task-a is bypassed).
func TestReconcileRetryPipelineRun_OnlyRerunTaskScheduled(t *testing.T) {
	namespace := "foo"
	prName := "my-retry-run"

	retryRunYAML := "metadata:\n" +
		"  name: my-retry-run\n" +
		"  namespace: foo\n" +
		"  annotations:\n" +
		"    tekton.dev/retryOf: \"my-source-run\"\n" +
		"    tekton.dev/retryOfUID: \"abc-123\"\n" +
		"    tekton.dev/retryStrategy: \"partial\"\n" +
		"spec:\n" +
		"  pipelineRef:\n" +
		"    name: retry-test-pipeline\n" +
		"  taskRunTemplate:\n" +
		"    serviceAccountName: test-sa\n"

	sourceRunYAML := "metadata:\n" +
		"  name: my-source-run\n" +
		"  namespace: foo\n" +
		"spec:\n" +
		"  pipelineRef:\n" +
		"    name: retry-test-pipeline\n" +
		"  taskRunTemplate:\n" +
		"    serviceAccountName: test-sa\n" +
		"status:\n" +
		"  pipelineSpec:\n" +
		"    tasks:\n" +
		"    - name: task-a\n" +
		"      taskRef:\n" +
		"        name: hello-world\n" +
		"    - name: task-b\n" +
		"      taskRef:\n" +
		"        name: hello-world\n" +
		"      runAfter: [\"task-a\"]\n" +
		"  childReferences:\n" +
		"  - name: my-source-run-task-a\n" +
		"    pipelineTaskName: task-a\n" +
		"    kind: TaskRun\n" +
		"  - name: my-source-run-task-b\n" +
		"    pipelineTaskName: task-b\n" +
		"    kind: TaskRun\n"

	prs := []*v1.PipelineRun{
		parse.MustParseV1PipelineRun(t, retryRunYAML),
		parse.MustParseV1PipelineRun(t, sourceRunYAML),
	}

	sourceTaskRunA := parse.MustParseTaskRunWithObjectMeta(t,
		taskRunObjectMeta("my-source-run-task-a", namespace, "my-source-run", "retry-test-pipeline", "task-a", true),
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
	sourceTaskRunB := parse.MustParseTaskRunWithObjectMeta(t,
		taskRunObjectMeta("my-source-run-task-b", namespace, "my-source-run", "retry-test-pipeline", "task-b", true),
		`
spec:
  taskRef:
    name: hello-world
  serviceAccountName: test-sa
status:
  conditions:
  - type: Succeeded
    status: "False"
`)

	d := test.Data{
		PipelineRuns: prs,
		Pipelines:    []*v1.Pipeline{retryTestPipeline},
		Tasks:        []*v1.Task{retryTestTask},
		TaskRuns:     []*v1.TaskRun{sourceTaskRunA, sourceTaskRunB},
	}

	prt := newPipelineRunTest(t, d)
	defer prt.Cancel()

	reconciledRun, clients := prt.reconcileRun(namespace, prName, []string{}, false)

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
	prName := "my-retry-run"

	retryRunYAML := "metadata:\n" +
		"  name: my-retry-run\n" +
		"  namespace: foo\n" +
		"  annotations:\n" +
		"    tekton.dev/retryOf: \"my-source-run\"\n" +
		"    tekton.dev/retryOfUID: \"abc-123\"\n" +
		"    tekton.dev/retryStrategy: \"partial\"\n" +
		"spec:\n" +
		"  pipelineRef:\n" +
		"    name: retry-test-pipeline\n" +
		"  taskRunTemplate:\n" +
		"    serviceAccountName: test-sa\n"

	sourceRunYAML := "metadata:\n" +
		"  name: my-source-run\n" +
		"  namespace: foo\n" +
		"spec:\n" +
		"  pipelineRef:\n" +
		"    name: retry-test-pipeline\n" +
		"  taskRunTemplate:\n" +
		"    serviceAccountName: test-sa\n" +
		"status:\n" +
		"  pipelineSpec:\n" +
		"    tasks:\n" +
		"    - name: task-a\n" +
		"      taskRef:\n" +
		"        name: hello-world\n" +
		"    - name: task-b\n" +
		"      taskRef:\n" +
		"        name: hello-world\n" +
		"      runAfter: [\"task-a\"]\n" +
		"  childReferences:\n" +
		"  - name: my-source-run-task-a\n" +
		"    pipelineTaskName: task-a\n" +
		"    kind: TaskRun\n" +
		"  - name: my-source-run-task-b\n" +
		"    pipelineTaskName: task-b\n" +
		"    kind: TaskRun\n"

	prs := []*v1.PipelineRun{
		parse.MustParseV1PipelineRun(t, retryRunYAML),
		parse.MustParseV1PipelineRun(t, sourceRunYAML),
	}

	sourceTaskRunA := parse.MustParseTaskRunWithObjectMeta(t,
		taskRunObjectMeta("my-source-run-task-a", namespace, "my-source-run", "retry-test-pipeline", "task-a", true),
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
	sourceTaskRunB := parse.MustParseTaskRunWithObjectMeta(t,
		taskRunObjectMeta("my-source-run-task-b", namespace, "my-source-run", "retry-test-pipeline", "task-b", true),
		`
spec:
  taskRef:
    name: hello-world
  serviceAccountName: test-sa
status:
  conditions:
  - type: Succeeded
    status: "False"
`)

	// Pre-seed an already-succeeded TaskRun for task-b owned by the RETRY
	// PipelineRun itself, simulating that it completed on a prior reconcile.
	retryTaskRunB := parse.MustParseTaskRunWithObjectMeta(t,
		taskRunObjectMeta("my-retry-run-task-b", namespace, "my-retry-run", "retry-test-pipeline", "task-b", true),
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
		PipelineRuns: prs,
		Pipelines:    []*v1.Pipeline{retryTestPipeline},
		Tasks:        []*v1.Task{retryTestTask},
		TaskRuns:     []*v1.TaskRun{sourceTaskRunA, sourceTaskRunB, retryTaskRunB},
	}

	prt := newPipelineRunTest(t, d)
	defer prt.Cancel()

	reconciledRun, _ := prt.reconcileRun(namespace, prName, []string{}, false)

	condition := reconciledRun.Status.GetCondition(apis.ConditionSucceeded)
	if condition == nil || condition.Status != corev1.ConditionTrue {
		t.Fatalf("expected retry PipelineRun to be Succeeded=True, got %+v", condition)
	}
}
