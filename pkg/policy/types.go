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
//
// The two flags say *what* the approval covers; the digest says *which report*
// it was given for. A digest is used instead of the report name because names
// are reused after retention and because two reports with the same digest are
// the same content - consenting to one is consenting to the other.
type ConsentState struct {
	// ApprovedDestructiveActions authorises Tier 2 (non-cloud) finalizer work.
	ApprovedDestructiveActions bool

	// CloudRiskAcknowledged authorises Tier 1 (cloud-protecting) finalizer work.
	CloudRiskAcknowledged bool

	// AcknowledgedDigest is the digest of the report the administrator read.
	AcknowledgedDigest string

	// ReportDigest is the digest of the report that exists now.
	ReportDigest string
}

// Valid reports whether the administrator approved work against the report that
// currently exists. An empty or stale digest means no consent, never implicit
// consent: that is the G2 defect, where two booleans authorised any report.
func (c ConsentState) Valid() bool {
	return (c.ApprovedDestructiveActions || c.CloudRiskAcknowledged) && c.coversReport()
}

// CloudRiskValid is the narrower rule for acting on a cloud-protecting
// finalizer: the risk has to be acknowledged specifically, not merely some
// destructive work approved.
func (c ConsentState) CloudRiskValid() bool {
	return c.CloudRiskAcknowledged && c.coversReport()
}

func (c ConsentState) coversReport() bool {
	return c.AcknowledgedDigest != "" && c.AcknowledgedDigest == c.ReportDigest
}

// Decision is the policy outcome for one Blocker.
type Decision struct {
	Action          Action
	RequiresConsent bool
	ConsentReason   string
	Reason          string
}
