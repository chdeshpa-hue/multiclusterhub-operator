// Copyright Contributors to the Open Cluster Management project

package controllers

import (
	"context"
	"fmt"
	"time"

	"github.com/go-logr/logr"
	operatorv1 "github.com/stolostron/multiclusterhub-operator/api/v1"
	"github.com/stolostron/multiclusterhub-operator/pkg/verify"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
)

// terminalFailure is the record a terminal run leaves behind. Both ways out of a
// run — the deadline and a dead handoff Job — use it, so a failure always looks
// the same to whoever reads the CR afterwards.
type terminalFailure struct {
	conditionReason string
	residueReason   string
	phaseMessage    string
	message         string
}

// inProgressPhase names the phase whose work was interrupted, if any.
func inProgressPhase(td *operatorv1.HubTeardown) operatorv1.TeardownPhase {
	for _, phaseStatus := range td.Status.Phases {
		if phaseStatus.State == operatorv1.PhaseStateInProgress {
			return phaseStatus.Phase
		}
	}
	return operatorv1.TeardownPhase("")
}

// failOnTimeout ends the run at spec.maxDuration. The deadline is a bound, not
// an alarm: the phase that was running is marked Failed, the residue observed at
// that moment is recorded, and the plan is not executed further. Failed is
// terminal, so raising maxDuration afterwards does not resume the same CR.
func (r *HubTeardownReconciler) failOnTimeout(ctx context.Context, log logr.Logger, td *operatorv1.HubTeardown, elapsed, limit time.Duration) (ctrl.Result, error) {
	stuckPhase := inProgressPhase(td)
	message := fmt.Sprintf("Teardown ran for %s (limit: %s) and stopped at phase %s.",
		elapsed.Round(time.Second), limit.Round(time.Second), td.Status.Phase)

	if !td.Spec.ApprovedDestructiveActions {
		switch stuckPhase {
		case operatorv1.TeardownPhaseDisableAddons, operatorv1.TeardownPhaseDetachManagedClusters,
			operatorv1.TeardownPhaseDeleteMCH:
			message += " Consider setting spec.approvedDestructiveActions=true to resolve stuck Tier 2 finalizers."
		}
	}
	if !td.Spec.AcknowledgeCloudResourceRisk {
		if stuckPhase == operatorv1.TeardownPhaseDeleteInfrastructureCRs {
			message += " Consider setting spec.acknowledgeCloudResourceRisk=true to resolve stuck Tier 1 (cloud) finalizers."
		}
	}

	log.Info("Teardown failed: deadline exceeded", "elapsed", elapsed.Round(time.Second), "limit", limit.Round(time.Second), "phase", stuckPhase)
	return r.failTerminal(ctx, log, td, stuckPhase, terminalFailure{
		conditionReason: "DeadlineExceeded",
		residueReason:   "Deadline exceeded before verification",
		phaseMessage:    "Deadline exceeded",
		message:         message,
	})
}

// failOnHandoffJob ends the run when the handoff Job stopped without recording
// completion. Without it the CR would wait out spec.maxDuration for a Job that is
// already dead (D9b-3).
func (r *HubTeardownReconciler) failOnHandoffJob(ctx context.Context, log logr.Logger, td *operatorv1.HubTeardown, cause error) (ctrl.Result, error) {
	log.Info("Teardown failed: handoff Job", "cause", cause.Error())
	return r.failTerminal(ctx, log, td, inProgressPhase(td), terminalFailure{
		conditionReason: "HandoffJobFailed",
		residueReason:   "Handoff Job failed before verification",
		phaseMessage:    "Handoff Job failed",
		message:         fmt.Sprintf("Teardown stopped: %v.", cause),
	})
}

// failTerminal is the single writer of a terminal Failed status.
func (r *HubTeardownReconciler) failTerminal(ctx context.Context, log logr.Logger, td *operatorv1.HubTeardown, stuckPhase operatorv1.TeardownPhase, failure terminalFailure) (ctrl.Result, error) {
	now := metav1.Now()
	message := failure.message
	for _, phaseStatus := range td.Status.Phases {
		if phaseStatus.Phase == stuckPhase && phaseStatus.Message != "" {
			message += " " + phaseStatus.Message
			break
		}
	}

	// Record what is left, using the same classifier Verify uses. A failed scan
	// must not turn into a silent empty report.
	residueReason := failure.residueReason
	if input, err := r.observeForVerification(ctx, log, td); err != nil {
		log.Error(err, "Could not inventory residue at the terminal phase")
		residueReason += "; residue inventory incomplete: " + err.Error()
		td.Status.Residue = &operatorv1.ResidueReport{
			ObservedAt:                now,
			Complete:                  false,
			CompleteWithResidueReason: residueReason,
		}
	} else {
		report := verify.Evaluate(input, verifyPolicy(td))
		td.Status.Residue = &operatorv1.ResidueReport{
			ObservedAt:                now,
			Complete:                  false,
			CompleteWithResidueReason: residueReason,
			ExpectedSurvivors:         report.Expected,
			Residue:                   report.Residue,
		}
	}

	if stuckPhase != "" {
		r.setPhaseStatus(td, stuckPhase, operatorv1.PhaseStateFailed, failure.phaseMessage)
	}
	td.Status.Phase = operatorv1.TeardownPhaseFailed

	// A terminal run must stop working. Leaving the Job alive would let it keep
	// deleting with the handoff identity after the controller has given up.
	if err := r.cleanupHandoff(ctx, log, td); err != nil {
		log.Error(err, "Failed to stop the handoff Job after a terminal failure")
	}

	r.setCondition(td, conditionTypeTeardownStalled, metav1.ConditionTrue, failure.conditionReason, message)
	r.setCondition(td, conditionTypeTeardownProgressing, metav1.ConditionFalse, "TeardownFailed", message)
	r.Recorder.Event(td, corev1.EventTypeWarning, "TeardownFailed", message)

	if err := r.Client.Status().Update(ctx, td); err != nil {
		return ctrl.Result{}, fmt.Errorf("persisting failed terminal status: %w", err)
	}
	return ctrl.Result{}, nil
}
