// Package policy evaluates teardown policy over typed Blockers. Rules are
// expressions over the Blocker model, not branches in a deletion path.
package policy

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/stolostron/multiclusterhub-operator/pkg/ownership"
)

// Classification names the failure mode a Blocker represents.
type Classification string

const (
	ClassRecreateLoop         Classification = "RecreateLoop"
	ClassDeadController       Classification = "DeadController"
	ClassUnreachableSpoke     Classification = "UnreachableSpoke"
	ClassDanglingRegistration Classification = "DanglingRegistration"
	ClassCloudProtecting      Classification = "CloudProtecting"
	ClassExternalReconciler   Classification = "ExternalReconciler"
	ClassUnknown              Classification = "Unknown"
)

// Action is the policy's suggestion. A human or a later automation performs it.
type Action string

const (
	ActionReportOnly                  Action = "ReportOnly"
	ActionWaitForController           Action = "WaitForController"
	ActionResolveFinalizer            Action = "ResolveFinalizer"
	ActionResolveFinalizerWithConsent Action = "ResolveFinalizerWithConsent"
	ActionFixOperand                  Action = "FixOperand"
	ActionStopExternalReconciler      Action = "StopExternalReconciler"
	ActionSweepDanglingRegistration   Action = "SweepDanglingRegistration"
)

// ControllerRef identifies the workload that should process a finalizer.
type ControllerRef struct {
	FinalizerDomain string `json:"finalizerDomain"`
	Kind            string `json:"kind"`
	Name            string `json:"name"`
	Namespace       string `json:"namespace,omitempty"`
	Resolution      string `json:"resolution"`
}

// CloudRisk records the consequence of acting outside the cluster.
type CloudRisk struct {
	Platform     string `json:"platform"`
	ResourceType string `json:"resourceType"`
	MayOrphan    bool   `json:"mayOrphan"`
}

// Blocker is the single record consumed by the report, status, MCH mirror and
// verification. Fields mirror codex-handoff-spec-20261103.md section 4.1.
type Blocker struct {
	APIVersion string    `json:"apiVersion"`
	Kind       string    `json:"kind"`
	Name       string    `json:"name"`
	Namespace  string    `json:"namespace,omitempty"`
	UID        types.UID `json:"uid"`

	Finalizers        []string     `json:"finalizers,omitempty"`
	DeletionTimestamp *metav1.Time `json:"deletionTimestamp,omitempty"`
	BlockedSince      metav1.Time  `json:"blockedSince"`
	ObservedInPhase   string       `json:"observedInPhase"`

	OwningController   *ControllerRef       `json:"owningController,omitempty"`
	ControllerAlive    *bool                `json:"controllerAlive,omitempty"`
	ControllerEvidence []ownership.Evidence `json:"controllerEvidence,omitempty"`

	Ownership ownership.OwnershipVerdict `json:"ownership"`
	CloudRisk *CloudRisk                 `json:"cloudRisk,omitempty"`

	Classification Classification `json:"classification"`

	SuggestedAction Action `json:"suggestedAction"`
	RequiresConsent bool   `json:"requiresConsent"`
	ConsentReason   string `json:"consentReason,omitempty"`
	RelatedFix      string `json:"relatedFix,omitempty"`
}

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
