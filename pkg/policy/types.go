// Package policy evaluates teardown policy over typed Blockers. Rules are
// expressions over the Blocker model, not branches in a deletion path.
//
// The Blocker model lives in api/v1 because it is part of the HubTeardown
// status and report schema. This package owns only the evaluation.
package policy

import (
	operatorv1 "github.com/stolostron/multiclusterhub-operator/api/v1"
)

// Classification names the failure mode a Blocker represents.
type Classification = operatorv1.Classification

const (
	ClassRecreateLoop         = operatorv1.ClassRecreateLoop
	ClassDeadController       = operatorv1.ClassDeadController
	ClassUnreachableSpoke     = operatorv1.ClassUnreachableSpoke
	ClassDanglingRegistration = operatorv1.ClassDanglingRegistration
	ClassCloudProtecting      = operatorv1.ClassCloudProtecting
	ClassExternalReconciler   = operatorv1.ClassExternalReconciler
	ClassUnknown              = operatorv1.ClassUnknown
)

// Action is the policy's suggestion. A human or a later automation performs it.
type Action = operatorv1.SuggestedAction

const (
	ActionReportOnly                  = operatorv1.ActionReportOnly
	ActionWaitForController           = operatorv1.ActionWaitForController
	ActionResolveFinalizer            = operatorv1.ActionResolveFinalizer
	ActionResolveFinalizerWithConsent = operatorv1.ActionResolveFinalizerWithConsent
	ActionFixOperand                  = operatorv1.ActionFixOperand
	ActionStopExternalReconciler      = operatorv1.ActionStopExternalReconciler
	ActionSweepDanglingRegistration   = operatorv1.ActionSweepDanglingRegistration
)

// ControllerRef identifies the workload that should process a finalizer.
type ControllerRef = operatorv1.ControllerRef

// CloudRisk records the consequence of acting outside the cluster.
type CloudRisk = operatorv1.CloudRisk

// Blocker is the single record consumed by the report, status, MCH mirror and
// verification. Fields mirror codex-handoff-spec-20261103.md section 4.1.
type Blocker = operatorv1.Blocker

// ConsentState is the administrator's approval, bound to the report they read.
type ConsentState struct {
	CloudRiskAcknowledged bool
	AcknowledgedReport    string
	ReportName            string
}

// Valid reports whether consent covers the current report. If the inspection
// regenerates the report, the name changes and consent lapses automatically.
func (c ConsentState) Valid() bool {
	return c.CloudRiskAcknowledged && c.AcknowledgedReport != "" && c.AcknowledgedReport == c.ReportName
}

// Decision is the policy outcome for one Blocker.
type Decision struct {
	Action          Action
	RequiresConsent bool
	ConsentReason   string
	Reason          string
}
