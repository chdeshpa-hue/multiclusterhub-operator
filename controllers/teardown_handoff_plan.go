// Copyright Contributors to the Open Cluster Management project

package controllers

import (
	"context"
	stderrors "errors"
	"fmt"

	"github.com/go-logr/logr"
	operatorv1 "github.com/stolostron/multiclusterhub-operator/api/v1"
	"k8s.io/client-go/discovery"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// errHandoffJobFailed marks a terminal handoff Job failure so Reconcile can end
// the CR instead of waiting out spec.maxDuration for a Job that already died.
var errHandoffJobFailed = stderrors.New("handoff Job failed")

// handoffPhasePlan is the work the handoff Job owns: the two phases whose waits
// were unbounded in the controller (F-35). The Job's activeDeadlineSeconds is
// the bound the controller cannot give itself while it holds the same phases.
func (r *HubTeardownReconciler) handoffPhasePlan() []teardownPhaseStep {
	return []teardownPhaseStep{
		{operatorv1.TeardownPhaseMonitorMCEChain, r.phaseMonitorMCEChain},
		{operatorv1.TeardownPhaseDeleteACMCRDs, r.phaseDeleteACMCRDs},
	}
}

// RunHandoffJob is the handoff Job entrypoint. Same one-shot contract as
// RunPrepareJob: no watch, no leader election, no status writes.
func RunHandoffJob(ctx context.Context, c client.Client, dc discovery.DiscoveryInterface, name, namespace string) error {
	r, td, log, deadline, err := newJobReconciler(ctx, c, dc, name, namespace, "handoff")
	if err != nil || r == nil {
		return err
	}
	return r.runJobLoop(ctx, log, td, r.handoffPhasePlan(), deadline, jobResyncInterval)
}

// awaitJobPhase is the operator's side of a phase the handoff Job owns. It never
// executes the phase: it reports the completion the Job recorded, and turns a
// Job that ended without recording one into a terminal failure rather than a
// wait. A missing record is never read as completion.
func (r *HubTeardownReconciler) awaitJobPhase(phase operatorv1.TeardownPhase) func(context.Context, logr.Logger, *operatorv1.HubTeardown) (bool, error) {
	return func(ctx context.Context, log logr.Logger, td *operatorv1.HubTeardown) (bool, error) {
		progress, err := r.loadJobProgress(ctx, td)
		if err != nil {
			return false, err
		}
		switch jobPhaseState(progress, phase) {
		case operatorv1.PhaseStateComplete:
			return true, nil
		case operatorv1.PhaseStateFailed:
			return false, fmt.Errorf("%w: phase %s recorded Failed; see the progress record", errHandoffJobFailed, phase)
		}

		exists, terminal, err := r.jobState(ctx, td, handoffJobName)
		if err != nil {
			return false, err
		}
		if exists && terminal != nil {
			return false, fmt.Errorf("%w: Job condition %s but phase %s is not recorded complete", errHandoffJobFailed, *terminal, phase)
		}

		log.Info("Waiting for the handoff Job to finish a phase", "phase", phase)
		return false, nil
	}
}
