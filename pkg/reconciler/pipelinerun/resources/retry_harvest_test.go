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

// TestHarvestRetryResults_HappyPath verifies that a preserved task's result
// is correctly harvested from the source pipeline's bubbled results.
func TestHarvestRetryResults_HappyPath(t *testing.T) {
	spec := &v1.PipelineSpec{
		Results: []v1.PipelineResult{{
			Name:  "clone-digest",
			Value: *v1.NewStructuredValues("$(tasks.clone.results.digest)"),
		}},
	}
	statusResults := []v1.PipelineRunResult{{
		Name:  "clone-digest",
		Value: *v1.NewStructuredValues("sha256:abc123"),
	}}
	preserved := map[string]bool{"clone": true}

	got := HarvestRetryResults(spec, statusResults, preserved)

	if len(got) != 1 {
		t.Fatalf("expected 1 result, got %d: %v", len(got), got)
	}
	if got[0].PipelineTaskName != "clone" {
		t.Errorf("expected PipelineTaskName=clone, got %q", got[0].PipelineTaskName)
	}
	if got[0].Name != "digest" {
		t.Errorf("expected Name=digest, got %q", got[0].Name)
	}
	if got[0].Value != "sha256:abc123" {
		t.Errorf("expected Value=sha256:abc123, got %q", got[0].Value)
	}
}

// TestHarvestRetryResults_RerunTaskExcluded verifies that results from a task
// in the rerun set (not preserved) are NOT memoized.
func TestHarvestRetryResults_RerunTaskExcluded(t *testing.T) {
	spec := &v1.PipelineSpec{
		Results: []v1.PipelineResult{{
			Name:  "build-image",
			Value: *v1.NewStructuredValues("$(tasks.build.results.image)"),
		}},
	}
	statusResults := []v1.PipelineRunResult{{
		Name:  "build-image",
		Value: *v1.NewStructuredValues("registry.io/app:latest"),
	}}
	// build is being rerun, not preserved
	preserved := map[string]bool{}

	got := HarvestRetryResults(spec, statusResults, preserved)

	if len(got) != 0 {
		t.Errorf("expected 0 results for rerun task, got %d: %v", len(got), got)
	}
}
