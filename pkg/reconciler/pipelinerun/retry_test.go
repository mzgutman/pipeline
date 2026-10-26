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

	v1 "github.com/tektoncd/pipeline/pkg/apis/pipeline/v1"
	"github.com/tektoncd/pipeline/test"
	"github.com/tektoncd/pipeline/test/parse"
	"github.com/tektoncd/pipeline/pkg/apis/pipeline"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// TestReconcileRetryPipelineRun_DetectsAnnotation verifies that a PipelineRun
// carrying tekton.dev/retryOf enters the setupRetry path and does NOT create
// any TaskRuns (stub exits early in POC-1).
func TestReconcileRetryPipelineRun_DetectsAnnotation(t *testing.T) {
	prName := "my-retry-run"
	namespace := "foo"

	prs := []*v1.PipelineRun{parse.MustParseV1PipelineRun(t, `
metadata:
  name: my-retry-run
  namespace: foo
  annotations:
    tekton.dev/retryOf: "my-source-run"
    tekton.dev/retryOfUID: "abc-123"
    tekton.dev/retryStrategy: "partial"
spec:
  pipelineRef:
    name: test-pipeline
  taskRunTemplate:
    serviceAccountName: test-sa
`)}

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

	// The stub should exit before scheduling any TaskRuns
	taskRuns := getTaskRunsForPipelineRun(prt.TestAssets.Ctx, t, clients, namespace, prName)
	validateTaskRunsCount(t, taskRuns, 0)
}

// TestReconcileRetryPipelineRun_NonRetryUnaffected verifies that a normal
// PipelineRun (no retryOf annotation) is completely unaffected by the new
// retry detection branch: it still creates a TaskRun as before.
func TestReconcileRetryPipelineRun_NonRetryUnaffected(t *testing.T) {
	prName := "normal-run"
	namespace := "foo"

	prs := []*v1.PipelineRun{parse.MustParseV1PipelineRun(t, `
metadata:
  name: normal-run
  namespace: foo
spec:
  pipelineRef:
    name: test-pipeline
  taskRunTemplate:
    serviceAccountName: test-sa
`)}

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