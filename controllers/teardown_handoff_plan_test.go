// Copyright Contributors to the Open Cluster Management project

package controllers

import (
	"context"
	stderrors "errors"
	"reflect"
	"strings"
	"testing"
	"time"

	operatorv1 "github.com/stolostron/multiclusterhub-operator/api/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrllog "sigs.k8s.io/controller-runtime/pkg/log"
)

func TestHandoffPhasePlan(t *testing.T) {
	r := &HubTeardownReconciler{}
	want := []operatorv1.TeardownPhase{
		operatorv1.TeardownPhaseRemoveBlockingCRs,
		operatorv1.TeardownPhaseMonitorMCEChain,
		operatorv1.TeardownPhaseDeleteACMCRDs,
	}

	plan := r.handoffPhasePlan()
	if len(plan) != len(want) {
		t.Fatalf("handoff plan has %d steps, want %d", len(plan), len(want))
	}
	for i := range want {
		if plan[i].phase != want[i] {
			t.Errorf("plan[%d] = %s, want %s", i, plan[i].phase, want[i])
		}
		if plan[i].execute == nil {
			t.Errorf("plan[%d] (%s) has no executor", i, plan[i].phase)
		}
	}
}

// Progress records are keyed by phase, so a phase in both plans would let one
// executor's completion satisfy the other's gate.
func TestJobPlansAreDisjoint(t *testing.T) {
	r := &HubTeardownReconciler{}
	seen := map[operatorv1.TeardownPhase]string{}
	for _, step := range r.preparePhasePlan() {
		seen[step.phase] = "prepare"
	}
	for _, step := range r.handoffPhasePlan() {
		if other, ok := seen[step.phase]; ok {
			t.Errorf("phase %s is in both the %s and handoff plans", step.phase, other)
		}
	}
}

// The operator plan keeps the phase names for status, but the executors must be
// the waiters. A pointer comparison catches a re-wiring that a status assertion
// would miss, because both paths return the same phase name.
func TestOperatorPlanUsesWaitersForJobPhases(t *testing.T) {
	r := &HubTeardownReconciler{}
	for _, step := range r.teardownPhasePlan() {
		switch step.phase {
		case operatorv1.TeardownPhaseMonitorMCEChain:
			if reflect.ValueOf(step.execute).Pointer() == reflect.ValueOf(r.phaseMonitorMCEChain).Pointer() {
				t.Error("operator plan executes phaseMonitorMCEChain directly")
			}
		case operatorv1.TeardownPhaseDeleteACMCRDs:
			if reflect.ValueOf(step.execute).Pointer() == reflect.ValueOf(r.phaseDeleteACMCRDs).Pointer() {
				t.Error("operator plan executes phaseDeleteACMCRDs directly")
			}
		}
	}
}

func seedJobProgress(t *testing.T, r *HubTeardownReconciler, td *operatorv1.HubTeardown, records ...jobPhaseRecord) {
	t.Helper()
	ctx := context.Background()
	progress, err := r.loadJobProgress(ctx, td)
	if err != nil {
		t.Fatalf("loadJobProgress: %v", err)
	}
	for _, rec := range records {
		recordJobPhase(progress, rec.Phase, rec.State, rec.Message)
	}
	if err := r.saveJobProgress(ctx, td, progress); err != nil {
		t.Fatalf("saveJobProgress: %v", err)
	}
}

func TestAwaitJobPhase(t *testing.T) {
	cases := []struct {
		name        string
		seed        []jobPhaseRecord
		job         *batchv1.Job
		wantDone    bool
		wantErr     bool
		errContains string
	}{
		{
			name:     "recorded complete",
			seed:     []jobPhaseRecord{{Phase: operatorv1.TeardownPhaseMonitorMCEChain, State: operatorv1.PhaseStateComplete}},
			wantDone: true,
		},
		{
			name:     "no record and a running Job waits",
			job:      jobWithCondition(handoffJobName, ""),
			wantDone: false,
		},
		{
			name:        "recorded failed is terminal",
			seed:        []jobPhaseRecord{{Phase: operatorv1.TeardownPhaseMonitorMCEChain, State: operatorv1.PhaseStateFailed}},
			wantErr:     true,
			errContains: "recorded Failed",
		},
		{
			name:        "Job failed without a record is terminal",
			job:         jobWithCondition(handoffJobName, batchv1.JobFailed),
			wantErr:     true,
			errContains: string(batchv1.JobFailed),
		},
		{
			name:        "Job complete without a record is a contradiction",
			job:         jobWithCondition(handoffJobName, batchv1.JobComplete),
			wantErr:     true,
			errContains: "not recorded complete",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			td := handoffTestCR()
			r := newHandoffReconciler(t)
			if tc.job != nil {
				if err := r.Client.Create(context.Background(), tc.job); err != nil {
					t.Fatalf("creating Job: %v", err)
				}
				if err := r.Client.Status().Update(context.Background(), tc.job); err != nil {
					t.Fatalf("updating Job status: %v", err)
				}
			}
			if len(tc.seed) > 0 {
				seedJobProgress(t, r, td, tc.seed...)
			}

			await := r.awaitJobPhase(operatorv1.TeardownPhaseMonitorMCEChain)
			done, err := await(context.Background(), ctrllog.Log, td)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected an error, got done=%t", done)
				}
				if !stderrors.Is(err, errHandoffJobFailed) {
					t.Errorf("error is not errHandoffJobFailed: %v", err)
				}
				if tc.errContains != "" && !strings.Contains(err.Error(), tc.errContains) {
					t.Errorf("error %q does not mention %q", err.Error(), tc.errContains)
				}
				return
			}
			if err != nil {
				t.Fatalf("awaitJobPhase: %v", err)
			}
			if done != tc.wantDone {
				t.Errorf("done = %t, want %t", done, tc.wantDone)
			}
		})
	}
}

// Prepare completion must never satisfy the handoff gate.
func TestHandoffProgressComplete_IgnoresPrepareRecords(t *testing.T) {
	td := handoffTestCR()
	r := newHandoffReconciler(t)
	seedJobProgress(t, r, td,
		jobPhaseRecord{Phase: operatorv1.TeardownPhaseRemoveBlockingCRs, State: operatorv1.PhaseStateComplete},
		jobPhaseRecord{Phase: operatorv1.TeardownPhaseDisableAddons, State: operatorv1.PhaseStateComplete},
		jobPhaseRecord{Phase: operatorv1.TeardownPhaseDeleteInfrastructureCRs, State: operatorv1.PhaseStateComplete},
		jobPhaseRecord{Phase: operatorv1.TeardownPhaseDetachManagedClusters, State: operatorv1.PhaseStateComplete},
	)

	prepareDone, _, err := r.prepareProgressComplete(context.Background(), td)
	if err != nil || !prepareDone {
		t.Fatalf("prepareProgressComplete = %t, %v; want true", prepareDone, err)
	}
	handoffDone, _, err := r.handoffProgressComplete(context.Background(), td)
	if err != nil {
		t.Fatalf("handoffProgressComplete: %v", err)
	}
	if handoffDone {
		t.Error("prepare records satisfied the handoff gate")
	}
}

func seedPrepareComplete(t *testing.T, r *HubTeardownReconciler, td *operatorv1.HubTeardown) {
	t.Helper()
	records := []jobPhaseRecord{}
	for _, step := range r.preparePhasePlan() {
		records = append(records, jobPhaseRecord{Phase: step.phase, State: operatorv1.PhaseStateComplete})
	}
	seedJobProgress(t, r, td, records...)
}

func TestEnsureHandoffJob(t *testing.T) {
	t.Setenv("OPERATOR_IMAGE", "quay.io/test/operator:latest")

	t.Run("not before prepare completes", func(t *testing.T) {
		td := handoffTestCR()
		r := newHandoffReconciler(t, handoffRole())

		if err := r.ensureHandoffJob(context.Background(), ctrllog.Log, td); err != nil {
			t.Fatalf("ensureHandoffJob: %v", err)
		}
		if exists(t, r.Client, types.NamespacedName{Name: handoffJobName, Namespace: td.Namespace}, &batchv1.Job{}) {
			t.Error("handoff Job created before the prepare plan was complete")
		}
	})

	t.Run("not while the prepare Job is still running", func(t *testing.T) {
		td := handoffTestCR()
		r := newHandoffReconciler(t, handoffRole(), runningJob())
		seedPrepareComplete(t, r, td)

		if err := r.ensureHandoffJob(context.Background(), ctrllog.Log, td); err != nil {
			t.Fatalf("ensureHandoffJob: %v", err)
		}
		if exists(t, r.Client, types.NamespacedName{Name: handoffJobName, Namespace: td.Namespace}, &batchv1.Job{}) {
			t.Error("handoff Job created while the prepare pod could still be writing progress")
		}
	})

	t.Run("created once the prepare Job is terminal", func(t *testing.T) {
		td := handoffTestCR()
		r := newHandoffReconciler(t, handoffRole(), finishedJob(batchv1.JobComplete))
		seedPrepareComplete(t, r, td)

		if err := r.ensureHandoffJob(context.Background(), ctrllog.Log, td); err != nil {
			t.Fatalf("ensureHandoffJob: %v", err)
		}

		job := &batchv1.Job{}
		if err := r.Client.Get(context.Background(), types.NamespacedName{Name: handoffJobName, Namespace: td.Namespace}, job); err != nil {
			t.Fatalf("handoff Job not created: %v", err)
		}
		container := job.Spec.Template.Spec.Containers[0]
		if len(container.Args) != 1 || container.Args[0] != "--teardown-plan=handoff" {
			t.Errorf("args = %v, want [--teardown-plan=handoff]", container.Args)
		}
		if container.Name != "teardown" || job.Spec.Template.Spec.ServiceAccountName != handoffSAName {
			t.Errorf("handoff Job must run as %s, got %s", handoffSAName, job.Spec.Template.Spec.ServiceAccountName)
		}
		if job.Spec.Template.Labels["operator.open-cluster-management.io/teardown-plan"] != TeardownPlanHandoff {
			t.Errorf("plan label missing: %v", job.Spec.Template.Labels)
		}
		if job.Spec.ActiveDeadlineSeconds == nil || *job.Spec.ActiveDeadlineSeconds != int64(defaultMaxDuration.Seconds()) {
			t.Errorf("deadline = %v, want %v", job.Spec.ActiveDeadlineSeconds, defaultMaxDuration.Seconds())
		}
	})

	t.Run("refuses after a failed prepare Job", func(t *testing.T) {
		td := handoffTestCR()
		r := newHandoffReconciler(t, handoffRole(), finishedJob(batchv1.JobFailed))
		seedPrepareComplete(t, r, td)

		err := r.ensureHandoffJob(context.Background(), ctrllog.Log, td)
		if err == nil {
			t.Fatal("expected the handoff Job to be refused after a failed prepare Job")
		}
		if exists(t, r.Client, types.NamespacedName{Name: handoffJobName, Namespace: td.Namespace}, &batchv1.Job{}) {
			t.Error("handoff Job created after a failed prepare Job")
		}
	})
}

func TestMirrorHandoffProgress(t *testing.T) {
	td := handoffTestCR()
	r := newHandoffReconciler(t)
	seedJobProgress(t, r, td,
		jobPhaseRecord{Phase: operatorv1.TeardownPhaseMonitorMCEChain, State: operatorv1.PhaseStateComplete},
		jobPhaseRecord{Phase: operatorv1.TeardownPhaseDeleteACMCRDs, State: operatorv1.PhaseStateInProgress},
	)

	progress, err := r.loadJobProgress(context.Background(), td)
	if err != nil {
		t.Fatalf("loadJobProgress: %v", err)
	}
	r.mirrorHandoffProgress(td, progress)

	if !r.isPhaseComplete(td, operatorv1.TeardownPhaseMonitorMCEChain) {
		t.Error("completed handoff phase not mirrored into status")
	}
	if r.isPhaseComplete(td, operatorv1.TeardownPhaseDeleteACMCRDs) {
		t.Error("in-progress handoff phase mirrored as complete")
	}
}

func TestFailOnHandoffJob(t *testing.T) {
	td := handoffTestCR()
	td.Status.Phase = operatorv1.TeardownPhaseMonitorMCEChain
	td.Status.Phases = []operatorv1.TeardownPhaseStatus{{
		Phase: operatorv1.TeardownPhaseMonitorMCEChain,
		State: operatorv1.PhaseStateInProgress,
	}}
	r := newTerminalReconciler(t, td,
		handoffServiceAccount(td.Namespace),
		handoffClusterRoleBinding(td.Namespace),
		jobWithCondition(prepareJobName, ""),
		jobWithCondition(handoffJobName, ""),
	)

	cause := stderrors.Join(errHandoffJobFailed, stderrors.New("Job condition Failed"))
	if _, err := r.failOnHandoffJob(context.Background(), ctrllog.Log, td, cause); err != nil {
		t.Fatalf("failOnHandoffJob: %v", err)
	}

	if td.Status.Phase != operatorv1.TeardownPhaseFailed {
		t.Errorf("phase = %s, want Failed", td.Status.Phase)
	}
	if td.Status.Residue == nil || td.Status.Residue.Complete {
		t.Errorf("residue must be recorded and must not claim completeness: %+v", td.Status.Residue)
	}
	stuck := operatorv1.TeardownPhaseStatus{}
	for _, ps := range td.Status.Phases {
		if ps.Phase == operatorv1.TeardownPhaseMonitorMCEChain {
			stuck = ps
		}
	}
	if stuck.State != operatorv1.PhaseStateFailed || stuck.Message != "Handoff Job failed" {
		t.Errorf("stuck phase = %+v, want Failed/Handoff Job failed", stuck)
	}
	found := false
	for _, c := range td.Status.Conditions {
		if c.Type == conditionTypeTeardownStalled && c.Reason == "HandoffJobFailed" {
			found = true
		}
	}
	if !found {
		t.Errorf("Stalled/HandoffJobFailed condition missing: %+v", td.Status.Conditions)
	}
	for _, name := range []string{prepareJobName, handoffJobName} {
		if exists(t, r.Client, types.NamespacedName{Name: name, Namespace: td.Namespace}, &batchv1.Job{}) {
			t.Errorf("Job %s kept running after a terminal failure", name)
		}
	}
	if exists(t, r.Client, types.NamespacedName{Name: handoffSAName, Namespace: td.Namespace}, &corev1.ServiceAccount{}) {
		t.Error("handoff identity kept after a terminal failure")
	}
}

func TestRunHandoffJob_MissingCR(t *testing.T) {
	r := newHandoffReconciler(t)
	if err := RunHandoffJob(context.Background(), r.Client, nil, nil, "absent", "open-cluster-management"); err != nil {
		t.Fatalf("RunHandoffJob on a missing CR must be a no-op, got %v", err)
	}
}

func TestHandoffJobDeadlineBound(t *testing.T) {
	td := handoffTestCR()
	td.Spec.MaxDuration = &metav1.Duration{Duration: 30 * time.Minute}
	if got := teardownJobDeadline(td); got == nil || *got != 1800 {
		t.Errorf("handoff deadline = %v, want 1800", got)
	}
}
