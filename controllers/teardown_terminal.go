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

// failOnTimeout ends the run at spec.maxDuration. The deadline is a bound, not
// an alarm: the phase that was running is marked Failed, the residue observed at
// that moment is recorded, and the plan is not executed further. Failed is
// terminal, so raising maxDuration afterwards does not resume the same CR.
func (r *HubTeardownReconciler) failOnTimeout(ctx context.Context, log logr.Logger, td *operatorv1.HubTeardown, elapsed, limit time.Duration) (ctrl.Result, error) {
	now := metav1.Now()
	message := fmt.Sprintf("Teardown ran for %s (limit: %s) and stopped at phase %s.",
		elapsed.Round(time.Second), limit.Round(time.Second), td.Status.Phase)

	stuckPhase := operatorv1.TeardownPhase("")
	for _, phaseStatus := range td.Status.Phases {
		if phaseStatus.State == operatorv1.PhaseStateInProgress {
			stuckPhase = phaseStatus.Phase
			if phaseStatus.Message != "" {
				message += " " + phaseStatus.Message
			}
			break
		}
	}

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

	// Record what is left, using the same classifier Verify uses. A failed scan
	// must not turn into a silent empty report.
	reason := "Deadline exceeded before verification"
	if input, err := r.observeForVerification(ctx, log, td); err != nil {
		log.Error(err, "Could not inventory residue at the deadline")
		reason = "Deadline exceeded; residue inventory incomplete: " + err.Error()
		td.Status.Residue = &operatorv1.ResidueReport{
			ObservedAt:                now,
			Complete:                  false,
			CompleteWithResidueReason: reason,
		}
	} else {
		report := verify.Evaluate(input, verifyPolicy(td))
		td.Status.Residue = &operatorv1.ResidueReport{
			ObservedAt:                now,
			Complete:                  false,
			CompleteWithResidueReason: reason,
			ExpectedSurvivors:         report.Expected,
			Residue:                   report.Residue,
		}
	}

	if stuckPhase != "" {
		r.setPhaseStatus(td, stuckPhase, operatorv1.PhaseStateFailed, "Deadline exceeded")
	}
	td.Status.Phase = operatorv1.TeardownPhaseFailed

	// A terminal run must stop working. Leaving the Job alive would let it keep
	// deleting with the escalation identity after the controller has given up.
	if err := r.cleanupEscalation(ctx, log, td); err != nil {
		log.Error(err, "Failed to stop the escalation Job after the deadline")
	}

	r.setCondition(td, conditionTypeTeardownStalled, metav1.ConditionTrue, "DeadlineExceeded", message)
	r.setCondition(td, conditionTypeTeardownProgressing, metav1.ConditionFalse, "TeardownFailed", message)
	r.Recorder.Event(td, corev1.EventTypeWarning, "TeardownFailed", message)

	if err := r.Client.Status().Update(ctx, td); err != nil {
		return ctrl.Result{}, fmt.Errorf("persisting failed terminal status: %w", err)
	}
	log.Info("Teardown failed: deadline exceeded", "elapsed", elapsed.Round(time.Second), "limit", limit.Round(time.Second), "phase", stuckPhase)
	return ctrl.Result{}, nil
}
