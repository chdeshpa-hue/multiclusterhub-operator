// Copyright Contributors to the Open Cluster Management project

package controllers

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	"github.com/go-logr/logr"
	operatorv1 "github.com/stolostron/multiclusterhub-operator/api/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/discovery"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrllog "sigs.k8s.io/controller-runtime/pkg/log"
)

const (
	progressConfigMapPrefix = "hubteardown-progress-"
	progressKeyPhases       = "phases.json"
	progressLabelProgress   = "operator.open-cluster-management.io/teardown-progress"

	// prepareResyncInterval is how long the prepare Job waits before re-running
	// a phase that reported "not done yet".
	prepareResyncInterval = 15 * time.Second
)

// preparePhaseRecord is one line of the Job's progress record.
type preparePhaseRecord struct {
	Phase   operatorv1.TeardownPhase      `json:"phase"`
	State   operatorv1.TeardownPhaseState `json:"state"`
	Message string                        `json:"message,omitempty"`
	Time    metav1.Time                   `json:"time"`
}

type prepareProgress struct {
	Phases []preparePhaseRecord `json:"phases"`
}

// preparePhasePlan is the work that runs in the Job. It is separate from the
// operator plan so the operator can never execute a prepare phase by accident.
func (r *HubTeardownReconciler) preparePhasePlan() []teardownPhaseStep {
	return []teardownPhaseStep{
		{operatorv1.TeardownPhaseRemoveBlockingCRs, r.phaseRemoveBlockingCRs},
		{operatorv1.TeardownPhaseDisableAddons, r.phaseDisableAddons},
		{operatorv1.TeardownPhaseDeleteInfrastructureCRs, r.phaseDeleteInfrastructureCRs},
		{operatorv1.TeardownPhaseDetachManagedClusters, r.phaseDetachManagedClusters},
	}
}

// progressConfigMapName is deterministic per teardown. The progress object is
// never labelled as a report: pruneInspectReports selects on the report label
// and would otherwise delete the Job's own progress.
func progressConfigMapName(td *operatorv1.HubTeardown) string {
	base := progressConfigMapPrefix + td.Name
	if len(base) <= reportMaxNameLength {
		return base
	}
	sum := sha256.Sum256([]byte(base))
	suffix := "-" + hex.EncodeToString(sum[:4])
	keep := reportMaxNameLength - len(progressConfigMapPrefix) - len(suffix)
	if keep < 1 {
		keep = 1
	}
	return progressConfigMapPrefix + td.Name[:minInt(keep, len(td.Name))] + suffix
}

func (r *HubTeardownReconciler) loadPrepareProgress(ctx context.Context, td *operatorv1.HubTeardown) (*prepareProgress, error) {
	cm := &corev1.ConfigMap{}
	key := types.NamespacedName{Name: progressConfigMapName(td), Namespace: teardownNamespace(td)}
	if err := r.Client.Get(ctx, key, cm); err != nil {
		if errors.IsNotFound(err) {
			return &prepareProgress{}, nil
		}
		return nil, fmt.Errorf("reading prepare progress: %w", err)
	}

	progress := &prepareProgress{}
	raw := cm.Data[progressKeyPhases]
	if raw == "" {
		return progress, nil
	}
	if err := json.Unmarshal([]byte(raw), progress); err != nil {
		// A corrupt record is treated as "nothing recorded". The caller must
		// never read corruption as completion.
		return &prepareProgress{}, nil
	}
	return progress, nil
}

func (r *HubTeardownReconciler) savePrepareProgress(ctx context.Context, td *operatorv1.HubTeardown, progress *prepareProgress) error {
	body, err := json.Marshal(progress)
	if err != nil {
		return fmt.Errorf("encoding prepare progress: %w", err)
	}

	key := types.NamespacedName{Name: progressConfigMapName(td), Namespace: teardownNamespace(td)}
	cm := &corev1.ConfigMap{}
	getErr := r.Client.Get(ctx, key, cm)
	if getErr != nil && !errors.IsNotFound(getErr) {
		return fmt.Errorf("reading prepare progress for write: %w", getErr)
	}

	if errors.IsNotFound(getErr) {
		cm = &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      key.Name,
				Namespace: key.Namespace,
				Labels: map[string]string{
					"app":                 "hubteardown",
					progressLabelProgress: "true",
				},
			},
			Data: map[string]string{progressKeyPhases: string(body)},
		}
		if err := r.Client.Create(ctx, cm); err != nil {
			return fmt.Errorf("creating prepare progress: %w", err)
		}
		return nil
	}

	if cm.Data == nil {
		cm.Data = map[string]string{}
	}
	cm.Data[progressKeyPhases] = string(body)
	if err := r.Client.Update(ctx, cm); err != nil {
		return fmt.Errorf("updating prepare progress: %w", err)
	}
	return nil
}

func recordPreparePhase(progress *prepareProgress, phase operatorv1.TeardownPhase, state operatorv1.TeardownPhaseState, message string) {
	now := metav1.Now()
	for i := range progress.Phases {
		if progress.Phases[i].Phase == phase {
			progress.Phases[i].State = state
			progress.Phases[i].Message = message
			progress.Phases[i].Time = now
			return
		}
	}
	progress.Phases = append(progress.Phases, preparePhaseRecord{
		Phase: phase, State: state, Message: message, Time: now,
	})
}

func preparePhaseState(progress *prepareProgress, phase operatorv1.TeardownPhase) operatorv1.TeardownPhaseState {
	for _, record := range progress.Phases {
		if record.Phase == phase {
			return record.State
		}
	}
	return ""
}

// nopRecorder satisfies record.EventRecorder without writing events. The Job
// runs as the escalation identity, which deliberately has no create verb, and
// its real record is the progress ConfigMap, not the event stream.
type nopRecorder struct{}

func (nopRecorder) Event(runtime.Object, string, string, string)                  {}
func (nopRecorder) Eventf(runtime.Object, string, string, string, ...interface{}) {}
func (nopRecorder) AnnotatedEventf(runtime.Object, map[string]string, string, string, string, ...interface{}) {
}

// RunPrepareJob is the Job entrypoint. It is a one-shot loop, not a controller:
// no watch, no leader election and no status writes, so it can never be a second
// writer over the HubTeardown status.
func RunPrepareJob(ctx context.Context, c client.Client, dc discovery.DiscoveryInterface, name, namespace string) error {
	td := &operatorv1.HubTeardown{}
	if err := c.Get(ctx, types.NamespacedName{Name: name, Namespace: namespace}, td); err != nil {
		if errors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("reading HubTeardown %s/%s: %w", namespace, name, err)
	}

	maxDur := defaultMaxDuration
	if td.Spec.MaxDuration != nil {
		maxDur = td.Spec.MaxDuration.Duration
	}

	r := &HubTeardownReconciler{
		Client:          c,
		UncachedClient:  c,
		Scheme:          c.Scheme(),
		DiscoveryClient: dc,
		Recorder:        nopRecorder{},
	}
	log := ctrllog.FromContext(ctx).WithName("prepare")
	return r.runPrepareLoop(ctx, log, td, r.preparePhasePlan(), time.Now().Add(maxDur), prepareResyncInterval)
}

// runPrepareLoop executes the plan until every phase reports done or the
// deadline passes. It is separate from RunPrepareJob so tests can drive it with
// synthetic phases and a short resync.
func (r *HubTeardownReconciler) runPrepareLoop(ctx context.Context, log logr.Logger, td *operatorv1.HubTeardown, plan []teardownPhaseStep, deadline time.Time, resync time.Duration) error {
	progress, err := r.loadPrepareProgress(ctx, td)
	if err != nil {
		return err
	}

	for {
		pending := false
		for _, step := range plan {
			if preparePhaseState(progress, step.phase) == operatorv1.PhaseStateComplete {
				continue
			}

			recordPreparePhase(progress, step.phase, operatorv1.PhaseStateInProgress, "Executing phase")
			if err := r.savePrepareProgress(ctx, td, progress); err != nil {
				return err
			}

			done, execErr := step.execute(ctx, log, td)
			if execErr != nil {
				recordPreparePhase(progress, step.phase, operatorv1.PhaseStateFailed, execErr.Error())
				if saveErr := r.savePrepareProgress(ctx, td, progress); saveErr != nil {
					log.Error(saveErr, "Failed to record phase failure", "phase", step.phase)
				}
				return fmt.Errorf("phase %s failed: %w", step.phase, execErr)
			}

			if !done {
				recordPreparePhase(progress, step.phase, operatorv1.PhaseStateInProgress, "Waiting for phase to complete")
				if err := r.savePrepareProgress(ctx, td, progress); err != nil {
					return err
				}
				pending = true
				break
			}

			recordPreparePhase(progress, step.phase, operatorv1.PhaseStateComplete, "Phase completed")
			if err := r.savePrepareProgress(ctx, td, progress); err != nil {
				return err
			}
			log.Info("Prepare phase complete", "phase", step.phase)
		}

		if !pending {
			return nil
		}

		if time.Now().After(deadline) {
			for _, record := range progress.Phases {
				if record.State == operatorv1.PhaseStateInProgress {
					recordPreparePhase(progress, record.Phase, operatorv1.PhaseStateFailed, "Deadline exceeded")
				}
			}
			if err := r.savePrepareProgress(ctx, td, progress); err != nil {
				log.Error(err, "Failed to record deadline")
			}
			return fmt.Errorf("prepare phases did not complete before the deadline")
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(resync):
		}
	}
}

// prepareProgressComplete is what the operator uses to decide whether the Job
// has finished. Absence of a record is never read as completion.
func (r *HubTeardownReconciler) prepareProgressComplete(ctx context.Context, td *operatorv1.HubTeardown) (bool, *prepareProgress, error) {
	progress, err := r.loadPrepareProgress(ctx, td)
	if err != nil {
		return false, nil, err
	}
	for _, step := range r.preparePhasePlan() {
		if preparePhaseState(progress, step.phase) != operatorv1.PhaseStateComplete {
			return false, progress, nil
		}
	}
	return true, progress, nil
}

// mirrorPrepareProgress copies completed prepare phases into the status record.
// The operator stays the only writer of status; the Job only writes progress.
func (r *HubTeardownReconciler) mirrorPrepareProgress(td *operatorv1.HubTeardown, progress *prepareProgress) {
	if progress == nil {
		return
	}
	for _, step := range r.preparePhasePlan() {
		for _, record := range progress.Phases {
			if record.Phase != step.phase || record.State != operatorv1.PhaseStateComplete {
				continue
			}
			if !r.isPhaseComplete(td, step.phase) {
				now := record.Time
				r.setPhaseStatusWithTime(td, step.phase, operatorv1.PhaseStateComplete, "Phase completed by prepare Job", &now)
			}
		}
	}
}
