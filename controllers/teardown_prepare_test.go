// Copyright Contributors to the Open Cluster Management project

package controllers

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/go-logr/logr"
	operatorv1 "github.com/stolostron/multiclusterhub-operator/api/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrllog "sigs.k8s.io/controller-runtime/pkg/log"
)

func TestPreparePhasePlan(t *testing.T) {
	r := &HubTeardownReconciler{}
	want := []operatorv1.TeardownPhase{
		operatorv1.TeardownPhaseDisableAddons,
		operatorv1.TeardownPhaseDeleteInfrastructureCRs,
		operatorv1.TeardownPhaseDetachManagedClusters,
	}

	plan := r.preparePhasePlan()
	if len(plan) != len(want) {
		t.Fatalf("prepare plan has %d steps, want %d", len(plan), len(want))
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

func TestOperatorPlanExcludesPreparePhases(t *testing.T) {
	r := &HubTeardownReconciler{}
	prepare := map[operatorv1.TeardownPhase]bool{}
	for _, step := range r.preparePhasePlan() {
		prepare[step.phase] = true
	}
	for _, step := range r.teardownPhasePlan() {
		if prepare[step.phase] {
			t.Errorf("operator plan still contains prepare phase %s", step.phase)
		}
	}
}

func fixedStep(phase operatorv1.TeardownPhase, done bool, err error) teardownPhaseStep {
	return teardownPhaseStep{
		phase: phase,
		execute: func(context.Context, logr.Logger, *operatorv1.HubTeardown) (bool, error) {
			return done, err
		},
	}
}

func TestRunPrepareLoop_CompletesAndRecordsProgress(t *testing.T) {
	td := handoffTestCR()
	r := newHandoffReconciler(t)
	plan := []teardownPhaseStep{
		fixedStep(operatorv1.TeardownPhaseRemoveBlockingCRs, true, nil),
		fixedStep(operatorv1.TeardownPhaseDisableAddons, true, nil),
	}

	if err := r.runJobLoop(context.Background(), ctrllog.Log, td, plan, time.Now().Add(time.Minute), time.Millisecond); err != nil {
		t.Fatalf("runJobLoop: %v", err)
	}

	progress, err := r.loadJobProgress(context.Background(), td)
	if err != nil {
		t.Fatalf("loadJobProgress: %v", err)
	}
	if len(progress.Phases) != len(plan) {
		t.Fatalf("recorded %d phases, want %d", len(progress.Phases), len(plan))
	}
	for i, record := range progress.Phases {
		if record.Phase != plan[i].phase || record.State != operatorv1.PhaseStateComplete {
			t.Errorf("record %d = %s/%s, want %s/Complete", i, record.Phase, record.State, plan[i].phase)
		}
	}
}

func TestRunPrepareLoop_ResumesFromProgress(t *testing.T) {
	td := handoffTestCR()
	r := newHandoffReconciler(t)

	first := operatorv1.TeardownPhaseRemoveBlockingCRs
	second := operatorv1.TeardownPhaseDisableAddons
	if err := r.saveJobProgress(context.Background(), td, &jobProgress{Phases: []jobPhaseRecord{
		{Phase: first, State: operatorv1.PhaseStateComplete, Time: metav1.Now()},
	}}); err != nil {
		t.Fatalf("seed progress: %v", err)
	}

	ran := map[operatorv1.TeardownPhase]int{}
	makeStep := func(phase operatorv1.TeardownPhase) teardownPhaseStep {
		return teardownPhaseStep{
			phase: phase,
			execute: func(context.Context, logr.Logger, *operatorv1.HubTeardown) (bool, error) {
				ran[phase]++
				return true, nil
			},
		}
	}

	plan := []teardownPhaseStep{makeStep(first), makeStep(second)}
	if err := r.runJobLoop(context.Background(), ctrllog.Log, td, plan, time.Now().Add(time.Minute), time.Millisecond); err != nil {
		t.Fatalf("runJobLoop: %v", err)
	}
	if ran[first] != 0 {
		t.Errorf("completed phase %s re-executed %d times", first, ran[first])
	}
	if ran[second] != 1 {
		t.Errorf("pending phase %s executed %d times, want 1", second, ran[second])
	}
}

func TestRunPrepareLoop_WaitsThenCompletes(t *testing.T) {
	td := handoffTestCR()
	r := newHandoffReconciler(t)

	calls := 0
	step := teardownPhaseStep{
		phase: operatorv1.TeardownPhaseDisableAddons,
		execute: func(context.Context, logr.Logger, *operatorv1.HubTeardown) (bool, error) {
			calls++
			return calls > 1, nil
		},
	}

	if err := r.runJobLoop(context.Background(), ctrllog.Log, td, []teardownPhaseStep{step}, time.Now().Add(time.Minute), time.Millisecond); err != nil {
		t.Fatalf("runJobLoop: %v", err)
	}
	if calls != 2 {
		t.Errorf("phase executed %d times, want 2 (wait then complete)", calls)
	}
}

func TestRunPrepareLoop_Error(t *testing.T) {
	td := handoffTestCR()
	r := newHandoffReconciler(t)
	step := fixedStep(operatorv1.TeardownPhaseDisableAddons, false, fmt.Errorf("boom"))

	err := r.runJobLoop(context.Background(), ctrllog.Log, td, []teardownPhaseStep{step}, time.Now().Add(time.Minute), time.Millisecond)
	if err == nil || !strings.Contains(err.Error(), "boom") {
		t.Fatalf("expected the phase error, got %v", err)
	}

	progress, _ := r.loadJobProgress(context.Background(), td)
	if progress.Phases[0].State != operatorv1.PhaseStateFailed {
		t.Errorf("state = %s, want Failed", progress.Phases[0].State)
	}
}

func TestRunPrepareLoop_Deadline(t *testing.T) {
	td := handoffTestCR()
	r := newHandoffReconciler(t)
	step := fixedStep(operatorv1.TeardownPhaseDisableAddons, false, nil)

	err := r.runJobLoop(context.Background(), ctrllog.Log, td, []teardownPhaseStep{step}, time.Now().Add(-time.Second), time.Millisecond)
	if err == nil || !strings.Contains(strings.ToLower(err.Error()), "deadline") {
		t.Fatalf("expected a deadline error, got %v", err)
	}

	progress, _ := r.loadJobProgress(context.Background(), td)
	if progress.Phases[0].State != operatorv1.PhaseStateFailed || !strings.Contains(progress.Phases[0].Message, "Deadline") {
		t.Errorf("record = %+v, want Failed/Deadline exceeded", progress.Phases[0])
	}
}

func TestProgressIsNotAReport(t *testing.T) {
	td := handoffTestCR()
	report := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "hubteardown-report-teardown-20261104t000000z",
			Namespace: td.Namespace,
			Labels:    map[string]string{reportLabelUninstall: "true", reportLabelTeardown: td.Name},
		},
		Data: map[string]string{reportKeyMeta: `{"digest":"abc"}`},
	}
	r := newHandoffReconciler(t, report)

	step := fixedStep(operatorv1.TeardownPhaseDisableAddons, true, nil)
	if err := r.runJobLoop(context.Background(), ctrllog.Log, td, []teardownPhaseStep{step}, time.Now().Add(time.Minute), time.Millisecond); err != nil {
		t.Fatalf("runJobLoop: %v", err)
	}

	after := &corev1.ConfigMap{}
	if err := r.Client.Get(context.Background(), types.NamespacedName{Name: report.Name, Namespace: td.Namespace}, after); err != nil {
		t.Fatalf("reading report: %v", err)
	}
	if after.Data[reportKeyMeta] != `{"digest":"abc"}` {
		t.Errorf("report meta changed: %s", after.Data[reportKeyMeta])
	}

	progress := &corev1.ConfigMap{}
	if err := r.Client.Get(context.Background(), types.NamespacedName{Name: progressConfigMapName(td), Namespace: td.Namespace}, progress); err != nil {
		t.Fatalf("reading progress: %v", err)
	}
	if progress.Name == report.Name {
		t.Error("progress reused the report object")
	}
	if _, ok := progress.Labels[reportLabelTeardown]; ok {
		t.Error("progress carries the report label and would be pruned as a report")
	}
}

func TestPrepareLoopDoesNotWriteStatus(t *testing.T) {
	td := handoffTestCR()
	base := newHandoffReconciler(t)
	calls := []string{}
	r := &HubTeardownReconciler{
		Client: &recordingClient{Client: base.Client, calls: &calls},
		Scheme: base.Scheme,
		Log:    base.Log,
	}

	step := fixedStep(operatorv1.TeardownPhaseDisableAddons, true, nil)
	if err := r.runJobLoop(context.Background(), ctrllog.Log, td, []teardownPhaseStep{step}, time.Now().Add(time.Minute), time.Millisecond); err != nil {
		t.Fatalf("runJobLoop: %v", err)
	}
	for _, call := range calls {
		if call == "status" {
			t.Errorf("prepare loop called Status() — the Job must never write status: %v", calls)
		}
	}
}

func TestPrepareProgressComplete(t *testing.T) {
	td := handoffTestCR()
	r := newHandoffReconciler(t)

	done, _, err := r.prepareProgressComplete(context.Background(), td)
	if err != nil {
		t.Fatalf("prepareProgressComplete: %v", err)
	}
	if done {
		t.Error("empty progress read as complete")
	}

	progress := &jobProgress{}
	for _, step := range r.preparePhasePlan() {
		recordJobPhase(progress, step.phase, operatorv1.PhaseStateComplete, "done")
	}
	if err := r.saveJobProgress(context.Background(), td, progress); err != nil {
		t.Fatalf("save progress: %v", err)
	}
	done, _, err = r.prepareProgressComplete(context.Background(), td)
	if err != nil {
		t.Fatalf("prepareProgressComplete: %v", err)
	}
	if !done {
		t.Error("fully recorded progress not read as complete")
	}

	cm := &corev1.ConfigMap{}
	if err := r.Client.Get(context.Background(), types.NamespacedName{Name: progressConfigMapName(td), Namespace: td.Namespace}, cm); err != nil {
		t.Fatalf("reading progress: %v", err)
	}
	cm.Data[progressKeyPhases] = "{not json"
	if err := r.Client.Update(context.Background(), cm); err != nil {
		t.Fatalf("corrupting progress: %v", err)
	}
	done, _, err = r.prepareProgressComplete(context.Background(), td)
	if err != nil {
		t.Fatalf("prepareProgressComplete: %v", err)
	}
	if done {
		t.Error("corrupt progress read as complete")
	}
}

func TestMirrorPrepareProgress(t *testing.T) {
	td := handoffTestCR()
	r := newHandoffReconciler(t)
	progress := &jobProgress{}
	recordJobPhase(progress, operatorv1.TeardownPhaseDisableAddons, operatorv1.PhaseStateComplete, "done")

	r.mirrorPrepareProgress(td, progress)

	if !r.isPhaseComplete(td, operatorv1.TeardownPhaseDisableAddons) {
		t.Errorf("phase not mirrored into status: %+v", td.Status.Phases)
	}
	if r.isPhaseComplete(td, operatorv1.TeardownPhaseDetachManagedClusters) {
		t.Error("unrecorded phase marked complete")
	}
}

func TestRunPrepareJob_MissingCR(t *testing.T) {
	td := handoffTestCR()
	r := newHandoffReconciler(t)

	if err := RunPrepareJob(context.Background(), r.Client, nil, nil, td.Name, td.Namespace); err != nil {
		t.Fatalf("missing CR should be a clean exit, got %v", err)
	}
}

func TestProgressConfigMapNameIsStable(t *testing.T) {
	td := handoffTestCR()
	first := progressConfigMapName(td)
	td.Status.LastInspection = &metav1.Time{Time: time.Now()}
	if second := progressConfigMapName(td); second != first {
		t.Errorf("progress name depends on status: %s != %s", second, first)
	}
	long := handoffTestCR()
	long.Name = strings.Repeat("a", 300)
	if got := progressConfigMapName(long); len(got) > reportMaxNameLength {
		t.Errorf("progress name too long: %d chars", len(got))
	}
}
