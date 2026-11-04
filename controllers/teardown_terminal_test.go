// Copyright Contributors to the Open Cluster Management project

package controllers

import (
	"context"
	"testing"
	"time"

	operatorv1 "github.com/stolostron/multiclusterhub-operator/api/v1"
	batchv1 "k8s.io/api/batch/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	ctrllog "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
)

func newTerminalReconciler(t *testing.T, objs ...client.Object) *HubTeardownReconciler {
	t.Helper()
	ctrllog.SetLogger(zap.New(zap.UseDevMode(true)))
	s := newTestScheme()
	return &HubTeardownReconciler{
		Client: fake.NewClientBuilder().WithScheme(s).
			WithStatusSubresource(&operatorv1.HubTeardown{}).
			WithObjects(objs...).
			Build(),
		Scheme:   s,
		Log:      ctrllog.Log,
		Recorder: record.NewFakeRecorder(20),
	}
}

func timedOutCR() *operatorv1.HubTeardown {
	started := metav1.NewTime(time.Now().Add(-3 * time.Hour))
	return &operatorv1.HubTeardown{
		ObjectMeta: metav1.ObjectMeta{Name: "teardown", Namespace: "open-cluster-management"},
		Spec:       operatorv1.HubTeardownSpec{DryRun: false},
		Status: operatorv1.HubTeardownStatus{
			Phase:       operatorv1.TeardownPhaseDetachManagedClusters,
			ActiveSince: &started,
			Phases: []operatorv1.TeardownPhaseStatus{{
				Phase: operatorv1.TeardownPhaseDetachManagedClusters,
				State: operatorv1.PhaseStateInProgress,
			}},
		},
	}
}

func TestFailOnTimeout_WritesFailedAndResidue(t *testing.T) {
	td := timedOutCR()
	job := runningJob()
	r := newTerminalReconciler(t, td, job)

	if _, err := r.failOnTimeout(context.Background(), ctrllog.Log, td, 3*time.Hour, 2*time.Hour); err != nil {
		t.Fatalf("failOnTimeout: %v", err)
	}

	if td.Status.Phase != operatorv1.TeardownPhaseFailed {
		t.Errorf("phase = %s, want Failed", td.Status.Phase)
	}
	if td.Status.Residue == nil || td.Status.Residue.ObservedAt.IsZero() {
		t.Errorf("residue report not written: %+v", td.Status.Residue)
	}
	if td.Status.Residue != nil && td.Status.Residue.Complete {
		t.Error("timeout must not claim a complete postcondition")
	}

	stuck := operatorv1.TeardownPhaseStatus{}
	for _, ps := range td.Status.Phases {
		if ps.Phase == operatorv1.TeardownPhaseDetachManagedClusters {
			stuck = ps
		}
	}
	if stuck.State != operatorv1.PhaseStateFailed || stuck.Message != "Deadline exceeded" {
		t.Errorf("in-progress phase = %+v, want Failed/Deadline exceeded", stuck)
	}

	stalled, progressing := false, false
	for _, c := range td.Status.Conditions {
		if c.Type == conditionTypeTeardownStalled && c.Status == metav1.ConditionTrue && c.Reason == "DeadlineExceeded" {
			stalled = true
		}
		if c.Type == conditionTypeTeardownProgressing && c.Status == metav1.ConditionFalse && c.Reason == "TeardownFailed" {
			progressing = true
		}
	}
	if !stalled {
		t.Errorf("Stalled condition missing: %+v", td.Status.Conditions)
	}
	if !progressing {
		t.Errorf("Progressing=False missing: %+v", td.Status.Conditions)
	}

	if exists(t, r.Client, types.NamespacedName{Name: prepareJobName, Namespace: td.Namespace}, &batchv1.Job{}) {
		t.Error("a failed run left the handoff Job running")
	}
}

func TestReconcileActive_FailedIsTerminal(t *testing.T) {
	td := timedOutCR()
	td.Status.Phase = operatorv1.TeardownPhaseFailed
	r := newTerminalReconciler(t, td)

	phasesBefore := len(td.Status.Phases)
	result, err := r.reconcileActive(context.Background(), ctrllog.Log, td)
	if err != nil {
		t.Fatalf("reconcileActive: %v", err)
	}
	if result.Requeue || result.RequeueAfter != 0 {
		t.Errorf("failed run requested a requeue: %+v", result)
	}
	if len(td.Status.Phases) != phasesBefore {
		t.Errorf("failed run executed a phase: %+v", td.Status.Phases)
	}
	if td.Status.Phase != operatorv1.TeardownPhaseFailed {
		t.Errorf("phase changed away from Failed: %s", td.Status.Phase)
	}
}

func TestReconcileActive_PauseDoesNotConsumeDeadline(t *testing.T) {
	td := timedOutCR()
	td.Status.ActiveSince = &metav1.Time{Time: time.Now().Add(-90 * time.Minute)}
	paused := metav1.NewTime(time.Now().Add(-60 * time.Minute))
	td.Status.PausedAt = &paused
	td.Spec.MaxDuration = &metav1.Duration{Duration: time.Hour}
	r := newTerminalReconciler(t, td)

	if _, err := r.reconcileActive(context.Background(), ctrllog.Log, td); err != nil {
		t.Fatalf("reconcileActive: %v", err)
	}

	if td.Status.Phase == operatorv1.TeardownPhaseFailed {
		t.Error("a 60-minute pause counted against the deadline")
	}
	if td.Status.PausedAt != nil {
		t.Error("PausedAt not cleared on resume")
	}
	elapsed := time.Since(td.Status.ActiveSince.Time)
	if elapsed > 45*time.Minute {
		t.Errorf("clock not shifted forward: elapsed %s, want about 30m", elapsed.Round(time.Second))
	}
}

func TestReconcileDryRun_MarksPaused(t *testing.T) {
	td := timedOutCR()
	td.Spec.DryRun = true
	td.Status.Phase = operatorv1.TeardownPhaseDisableAddons
	r := newTerminalReconciler(t, td)

	if _, err := r.reconcileDryRun(context.Background(), ctrllog.Log, td); err != nil {
		t.Fatalf("reconcileDryRun: %v", err)
	}
	if td.Status.PausedAt == nil {
		t.Error("pausing an active run did not record PausedAt")
	}
}
