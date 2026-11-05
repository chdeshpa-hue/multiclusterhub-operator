// Copyright Contributors to the Open Cluster Management project

package controllers

import (
	"context"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// blockingListClient stands in for an API server that cannot answer within the
// scan budget: List never returns until the context expires.
type blockingListClient struct {
	client.Client
}

func (blockingListClient) List(ctx context.Context, _ client.ObjectList, _ ...client.ListOption) error {
	<-ctx.Done()
	return ctx.Err()
}

func TestBuildDependencyGraphIsBounded(t *testing.T) {
	previous := teardownScanTimeout
	teardownScanTimeout = 100 * time.Millisecond
	defer func() { teardownScanTimeout = previous }()

	reconciler := &HubTeardownReconciler{Client: blockingListClient{Client: classifierReconciler().Client}}
	started := time.Now()
	if _, err := reconciler.buildDependencyGraph(context.Background(), testTeardown(), logr.Discard()); err == nil {
		t.Fatalf("a scan that cannot list CRDs must fail instead of hanging")
	}
	if elapsed := time.Since(started); elapsed > 5*time.Second {
		t.Fatalf("scan was not bounded: %s", elapsed)
	}
}
