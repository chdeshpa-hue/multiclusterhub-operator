// Copyright Contributors to the Open Cluster Management project

package controllers

import (
	"context"
	"testing"
	"time"

	operatorv1 "github.com/stolostron/multiclusterhub-operator/api/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apixv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	ctrllog "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
)

const (
	obsAddonGroup   = "observability.open-cluster-management.io"
	obsAddonVersion = "v1beta1"
	obsAddonKind    = "ObservabilityAddon"
	obsAddonCRDName = "observabilityaddons.observability.open-cluster-management.io"
	obsAddonFin     = "observability.open-cluster-management.io/addon-cleanup"
)

func obsAddonCRD() *apixv1.CustomResourceDefinition {
	return &apixv1.CustomResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: obsAddonCRDName},
		Spec: apixv1.CustomResourceDefinitionSpec{
			Group: obsAddonGroup,
			Names: apixv1.CustomResourceDefinitionNames{
				Kind:   obsAddonKind,
				Plural: "observabilityaddons",
			},
			Versions: []apixv1.CustomResourceDefinitionVersion{
				{Name: obsAddonVersion, Served: true, Storage: true},
			},
		},
	}
}

func stuckObsAddon(deletedAgo time.Duration, finalizers ...string) *unstructured.Unstructured {
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(schema.GroupVersionKind{Group: obsAddonGroup, Version: obsAddonVersion, Kind: obsAddonKind})
	obj.SetName("rfe9332-unreachable-1")
	obj.SetNamespace("open-cluster-management-observability")
	obj.SetFinalizers(finalizers)
	ts := metav1.NewTime(time.Now().Add(-deletedAgo))
	obj.SetDeletionTimestamp(&ts)
	return obj
}

func amcrdTestReconciler(t *testing.T, td *operatorv1.HubTeardown, objs ...runtime.Object) *HubTeardownReconciler {
	t.Helper()
	s := newTestScheme()
	builder := fake.NewClientBuilder().WithScheme(s)
	typed := make([]client.Object, 0, len(objs))
	for _, o := range objs {
		typed = append(typed, o.(client.Object))
	}
	builder = builder.WithObjects(typed...)
	return &HubTeardownReconciler{
		Client:   builder.Build(),
		Scheme:   s,
		Log:      ctrllog.Log,
		Recorder: record.NewFakeRecorder(20),
	}
}

func phaseDeleteACMCRDsTestTD() *operatorv1.HubTeardown {
	return &operatorv1.HubTeardown{
		ObjectMeta: metav1.ObjectMeta{Name: "teardown", Namespace: "open-cluster-management"},
		Spec: operatorv1.HubTeardownSpec{
			DryRun:                     false,
			ApprovedDestructiveActions: true,
		},
	}
}

// A CR left behind by a dead controller must not wedge CRD deletion forever:
// the phase resolves the allowlisted finalizer itself.
func TestPhaseDeleteACMCRDsResolvesStuckFinalizer(t *testing.T) {
	ctrllog.SetLogger(zap.New(zap.UseDevMode(true)))
	td := phaseDeleteACMCRDsTestTD()
	obj := stuckObsAddon(6*time.Minute, obsAddonFin)
	r := amcrdTestReconciler(t, td, obsAddonCRD(), obj)

	done, err := r.phaseDeleteACMCRDs(context.Background(), ctrllog.Log.WithName("test"), td)
	if err != nil {
		t.Fatalf("phaseDeleteACMCRDs error: %v", err)
	}
	if done {
		t.Fatal("phase reported done while the CR was still terminating")
	}

	got := &unstructured.Unstructured{}
	got.SetGroupVersionKind(obj.GroupVersionKind())
	err = r.Client.Get(context.Background(), types.NamespacedName{
		Namespace: obj.GetNamespace(), Name: obj.GetName(),
	}, got)
	switch {
	case apierrors.IsNotFound(err):
		// finalizer gone and the terminating object was collected: the CRD is
		// no longer blocked.
	case err != nil:
		t.Fatalf("reading CR after finalizer resolution: %v", err)
	case len(got.GetFinalizers()) != 0:
		t.Fatalf("finalizers = %v, want none", got.GetFinalizers())
	}
}

// Before forceFinalizerTimeout the controller may still be working: do not patch.
func TestPhaseDeleteACMCRDsLeavesRecentStuckCR(t *testing.T) {
	ctrllog.SetLogger(zap.New(zap.UseDevMode(true)))
	td := phaseDeleteACMCRDsTestTD()
	obj := stuckObsAddon(30*time.Second, obsAddonFin)
	r := amcrdTestReconciler(t, td, obsAddonCRD(), obj)

	done, err := r.phaseDeleteACMCRDs(context.Background(), ctrllog.Log.WithName("test"), td)
	if err != nil {
		t.Fatalf("phaseDeleteACMCRDs error: %v", err)
	}
	if done {
		t.Fatal("phase reported done while the CR was still terminating")
	}

	got := &unstructured.Unstructured{}
	got.SetGroupVersionKind(obj.GroupVersionKind())
	_ = r.Client.Get(context.Background(), types.NamespacedName{Namespace: obj.GetNamespace(), Name: obj.GetName()}, got)
	if len(got.GetFinalizers()) != 1 {
		t.Fatalf("finalizers = %v, want the finalizer left in place", got.GetFinalizers())
	}
}

// A finalizer outside the allowlist is never touched, and must not spin the phase.
func TestPhaseDeleteACMCRDsLeavesUnlistedFinalizer(t *testing.T) {
	ctrllog.SetLogger(zap.New(zap.UseDevMode(true)))
	td := phaseDeleteACMCRDsTestTD()
	obj := stuckObsAddon(10*time.Minute, "example.com/third-party")
	r := amcrdTestReconciler(t, td, obsAddonCRD(), obj)

	done, err := r.phaseDeleteACMCRDs(context.Background(), ctrllog.Log.WithName("test"), td)
	if err != nil {
		t.Fatalf("phaseDeleteACMCRDs error: %v", err)
	}
	if done {
		t.Fatal("phase reported done while the CR was still terminating")
	}

	got := &unstructured.Unstructured{}
	got.SetGroupVersionKind(obj.GroupVersionKind())
	_ = r.Client.Get(context.Background(), types.NamespacedName{Namespace: obj.GetNamespace(), Name: obj.GetName()}, got)
	if len(got.GetFinalizers()) != 1 || got.GetFinalizers()[0] != "example.com/third-party" {
		t.Fatalf("finalizers = %v, want the unlisted finalizer preserved", got.GetFinalizers())
	}
}

// Tier 2 still requires approvedDestructiveActions.
func TestPhaseDeleteACMCRDsRequiresApproval(t *testing.T) {
	ctrllog.SetLogger(zap.New(zap.UseDevMode(true)))
	td := phaseDeleteACMCRDsTestTD()
	td.Spec.ApprovedDestructiveActions = false
	obj := stuckObsAddon(10*time.Minute, obsAddonFin)
	r := amcrdTestReconciler(t, td, obsAddonCRD(), obj)

	done, err := r.phaseDeleteACMCRDs(context.Background(), ctrllog.Log.WithName("test"), td)
	if err != nil {
		t.Fatalf("phaseDeleteACMCRDs error: %v", err)
	}
	if done {
		t.Fatal("phase reported done without approval")
	}

	got := &unstructured.Unstructured{}
	got.SetGroupVersionKind(obj.GroupVersionKind())
	_ = r.Client.Get(context.Background(), types.NamespacedName{Namespace: obj.GetNamespace(), Name: obj.GetName()}, got)
	if len(got.GetFinalizers()) != 1 {
		t.Fatalf("finalizers = %v, want the finalizer preserved without approval", got.GetFinalizers())
	}
}

// A Job that exhausted its backoff is reported as failed with the Job's own
// reason, not mistaken for a Job that is still running.
func TestTeardownJobFailureReportsFailedJob(t *testing.T) {
	ctrllog.SetLogger(zap.New(zap.UseDevMode(true)))
	td := phaseDeleteACMCRDsTestTD()
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: prepareJobName, Namespace: td.Namespace},
		Status: batchv1.JobStatus{
			Conditions: []batchv1.JobCondition{
				{Type: batchv1.JobFailed, Status: corev1.ConditionTrue, Reason: "BackoffLimitExceeded"},
			},
		},
	}
	r := amcrdTestReconciler(t, td, job)

	failed, reason, err := r.teardownJobFailure(context.Background(), td, prepareJobName)
	if err != nil {
		t.Fatalf("teardownJobFailure error: %v", err)
	}
	if !failed {
		t.Fatal("failed = false, want true for a terminal Job")
	}
	if reason != "BackoffLimitExceeded" {
		t.Fatalf("reason = %q, want BackoffLimitExceeded", reason)
	}
}

// A running Job is not a failure.
func TestTeardownJobFailureIgnoresRunningJob(t *testing.T) {
	ctrllog.SetLogger(zap.New(zap.UseDevMode(true)))
	td := phaseDeleteACMCRDsTestTD()
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: prepareJobName, Namespace: td.Namespace},
		Status:     batchv1.JobStatus{Active: 1},
	}
	r := amcrdTestReconciler(t, td, job)

	failed, _, err := r.teardownJobFailure(context.Background(), td, prepareJobName)
	if err != nil {
		t.Fatalf("teardownJobFailure error: %v", err)
	}
	if failed {
		t.Fatal("failed = true, want false for a running Job")
	}
}
