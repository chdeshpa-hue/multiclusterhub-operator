// Copyright Contributors to the Open Cluster Management project

package controllers

import (
	"context"
	"testing"

	operatorv1 "github.com/stolostron/multiclusterhub-operator/api/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	ctrllog "sigs.k8s.io/controller-runtime/pkg/log"
)

// newVerifyTestReconciler registers the status subresource: Verify is the one
// phase that writes terminal status, and a fake client without the subresource
// cannot model that write.
func newVerifyTestReconciler(t *testing.T, objs ...client.Object) *HubTeardownReconciler {
	t.Helper()
	s := newTestScheme()
	return &HubTeardownReconciler{
		Client: fake.NewClientBuilder().
			WithScheme(s).
			WithStatusSubresource(&operatorv1.HubTeardown{}).
			WithObjects(objs...).
			Build(),
		Scheme: s,
		Log:    ctrllog.Log,
	}
}

func TestTeardownPhasePlan(t *testing.T) {
	r := &HubTeardownReconciler{}

	// Prepare phases moved into the Job (increment 8); the dry-run report still
	// lists them because it describes the whole teardown, not one executor's share.
	want := []operatorv1.TeardownPhase{
		operatorv1.TeardownPhaseDeleteMCH,
		operatorv1.TeardownPhaseMonitorMCEChain,
		operatorv1.TeardownPhaseDeleteACMCRDs,
		operatorv1.TeardownPhaseVerifying,
	}

	plan := r.teardownPhasePlan()
	if len(plan) != len(want) {
		t.Fatalf("phase plan has %d steps, want %d", len(plan), len(want))
	}
	for i := range want {
		if plan[i].phase != want[i] {
			t.Fatalf("plan[%d] = %s, want %s", i, plan[i].phase, want[i])
		}
		if plan[i].execute == nil {
			t.Fatalf("plan[%d] (%s) has no executor", i, plan[i].phase)
		}
	}
}

func TestPhaseVerifyCleanHub(t *testing.T) {
	td := testTeardown()
	reconciler := newVerifyTestReconciler(t, td, &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: td.Namespace},
	})

	done, err := reconciler.phaseVerify(context.Background(), ctrllog.Log, td)
	if err != nil || !done {
		t.Fatalf("phaseVerify = %t, %v", done, err)
	}

	if td.Status.Phase != operatorv1.TeardownPhaseComplete {
		t.Fatalf("phase = %s, want Complete", td.Status.Phase)
	}
	if td.Status.Residue == nil || !td.Status.Residue.Complete {
		t.Fatalf("residue = %+v", td.Status.Residue)
	}
	if len(td.Status.Residue.Residue) != 0 {
		t.Fatalf("clean hub has residue: %+v", td.Status.Residue.Residue)
	}
	if len(td.Status.Residue.ExpectedSurvivors) == 0 {
		t.Fatal("the preserved operator namespace must be listed as an expected survivor")
	}
	if reason := conditionReason(td, conditionTypeTeardownComplete); reason != "Complete" {
		t.Fatalf("Complete condition reason = %q", reason)
	}
}

func TestPhaseVerifyResidueNamespace(t *testing.T) {
	td := testTeardown()
	reconciler := newVerifyTestReconciler(t, td,
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: td.Namespace}},
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "clusters"}},
	)

	done, err := reconciler.phaseVerify(context.Background(), ctrllog.Log, td)
	if err != nil || !done {
		t.Fatalf("phaseVerify = %t, %v", done, err)
	}

	if td.Status.Phase != operatorv1.TeardownPhaseCompleteWithResidue {
		t.Fatalf("phase = %s, want CompleteWithResidue", td.Status.Phase)
	}
	if td.Status.Residue == nil || td.Status.Residue.Complete {
		t.Fatalf("residue = %+v", td.Status.Residue)
	}
	found := false
	for _, item := range td.Status.Residue.Residue {
		if item.Kind == "Namespace" && item.Name == "clusters" {
			found = true
		}
	}
	if !found {
		t.Fatalf("residue does not name the clusters namespace: %+v", td.Status.Residue.Residue)
	}
	if reason := conditionReason(td, conditionTypeTeardownComplete); reason != "ResidueDetected" {
		t.Fatalf("Complete condition reason = %q, want ResidueDetected", reason)
	}
}
