package pipelinerun

import (
    "context"

    v1 "github.com/tektoncd/pipeline/pkg/apis/pipeline/v1"
    "knative.dev/pkg/logging"
)

// setupRetry is called on every reconciliation of a retry PipelineRun.
// POC-1: detects and logs the retry annotations.
// TODO(poc-2): compute failed subgraph
// TODO(poc-3): harvest and memoize results
// TODO(poc-4): wire isBypassed() predicate
func (c *Reconciler) setupRetry(ctx context.Context, pr *v1.PipelineRun) error {
    logger := logging.FromContext(ctx)
    sourceRunName := pr.Annotations[v1.RetryOfAnnotation]
    sourceRunUID  := pr.Annotations[v1.RetryOfUIDAnnotation]
    strategy      := pr.Annotations[v1.RetryStrategyAnnotation]
    logger.Infof("PipelineRun %s/%s is a retry of %s (uid=%s, strategy=%s)",
        pr.Namespace, pr.Name, sourceRunName, sourceRunUID, strategy)
    return nil
}
