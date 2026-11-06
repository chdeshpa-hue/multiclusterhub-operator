// Copyright Contributors to the Open Cluster Management project

package v1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// EvidenceKind identifies one observation that bears on ownership.
type EvidenceKind string

const (
	// EvidenceInstallerLabel is the installer.name + installer.namespace
	// pair applied by utils.AddInstallerLabel to every rendered template and
	// every CRD.
	EvidenceInstallerLabel EvidenceKind = "InstallerLabel"
	// EvidenceOwnerRefChain is an ownerReference chain terminating at an
	// object that carries the installer label.
	EvidenceOwnerRefChain EvidenceKind = "OwnerRefChain"
	// EvidenceDefiningCRDLabelled means the CRD defining this object's kind
	// carries the installer label; the object is an instance of an
	// ACM-defined type.
	EvidenceDefiningCRDLabelled EvidenceKind = "DefiningCRDLabelled"
	// EvidenceAPIServiceBacking means an aggregated API is served by a
	// Service in a namespace this MCH owns.
	EvidenceAPIServiceBacking EvidenceKind = "APIServiceBackingService"
	// EvidenceControllerAbsent is evidence of STALENESS, not ownership.
	// It must never participate in the confidence calculation.
	EvidenceControllerAbsent EvidenceKind = "ControllerAbsent"
	// EvidenceControllerLiveness records what the owner probe observed: a
	// workload with ready replicas, a workload with none, or an unreadable
	// workload. Like ControllerAbsent it bears on liveness, never ownership.
	EvidenceControllerLiveness EvidenceKind = "ControllerLiveness"
	// EvidenceNameMatch is report-only. It is never sufficient for a
	// destructive action.
	EvidenceNameMatch EvidenceKind = "NameMatch"
	// EvidenceMCHIdentity means the object is the MultiClusterHub itself. The
	// hub cannot be a tenant of anything on its own cluster, so this is
	// Definitive without needing a label.
	EvidenceMCHIdentity EvidenceKind = "MCHIdentity"
	// EvidenceACMGroupCRD means the CRD defining this object's kind belongs to
	// the ACM/MCE product surface. On 5.0 the installer.* labels are no longer
	// applied to the MCH, MCE or the ACM namespaces, so group membership is the
	// structural signal that survives.
	EvidenceACMGroupCRD EvidenceKind = "ACMGroupCRD"
	// EvidenceManagedByLabel is a hub-applied marker that names ACM or MCE as
	// the managing operator ("multiclusterhubs.operator.../managed-by" or
	// "backplaneconfig.name").
	EvidenceManagedByLabel EvidenceKind = "ManagedByLabel"
	// EvidenceOLMOwner means OLM recorded an ACM or MCE ClusterServiceVersion
	// as the owner of this object.
	EvidenceOLMOwner EvidenceKind = "OLMOwner"
)

// Evidence is one observation, with where it was read from.
type Evidence struct {
	Kind     EvidenceKind `json:"kind"`
	Detail   string       `json:"detail"`
	Source   string       `json:"source"`
	Observed metav1.Time  `json:"observed"`
}

// Confidence is the maximum confidence over the evidence list. It is never a
// sum: two Weak observations do not make a Strong one.
type Confidence string

const (
	ConfidenceDefinitive Confidence = "Definitive"
	ConfidenceStrong     Confidence = "Strong"
	ConfidenceWeak       Confidence = "Weak"
	ConfidenceNone       Confidence = "None"
)

// OwnershipVerdict is the answer plus the evidence that produced it.
type OwnershipVerdict struct {
	Owned      bool       `json:"owned"`
	Confidence Confidence `json:"confidence"`
	Evidence   []Evidence `json:"evidence"`
}

// ControllerResolution records how the controller responsible for a finalizer
// was identified.
type ControllerResolution string

const (
	ResolvedByFinalizerDomain ControllerResolution = "FinalizerDomain"
	ResolvedByOwnerRefChain   ControllerResolution = "OwnerRefChain"
	ResolvedByAPIServiceRef   ControllerResolution = "APIServiceBackingService"
	Unresolved                ControllerResolution = "Unresolved"
)

// ControllerRef identifies the workload that should process a finalizer.
type ControllerRef struct {
	FinalizerDomain string               `json:"finalizerDomain"`
	Kind            string               `json:"kind"`
	Name            string               `json:"name"`
	Namespace       string               `json:"namespace,omitempty"`
	Resolution      ControllerResolution `json:"resolution"`
}

// CloudRisk records the consequence of acting outside the cluster.
type CloudRisk struct {
	Platform string `json:"platform"`
	// PlatformKnown distinguishes a recognised platform (cloud or KubeVirt)
	// from an absent or unfamiliar string. It is evidence quality, not a gate:
	// only MayOrphan authorises a consent requirement.
	PlatformKnown bool   `json:"platformKnown"`
	ResourceType  string `json:"resourceType"`
	MayOrphan     bool   `json:"mayOrphan"`
}

// CloudRiskSummary is the aggregated, never-truncated view of cloud risk that
// status carries. One entry exists per (platform, resourceType) pair; the
// per-object detail stays in the report ConfigMap.
type CloudRiskSummary struct {
	Platform     string   `json:"platform"`
	ResourceType string   `json:"resourceType"`
	Count        int32    `json:"count"`
	Namespaces   []string `json:"namespaces,omitempty"`
	ConsoleHint  string   `json:"consoleHint,omitempty"`
}

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

// SuggestedAction is the policy's suggestion. A human or a later automation
// performs it.
type SuggestedAction string

const (
	ActionReportOnly                  SuggestedAction = "ReportOnly"
	ActionWaitForController           SuggestedAction = "WaitForController"
	ActionResolveFinalizer            SuggestedAction = "ResolveFinalizer"
	ActionResolveFinalizerWithConsent SuggestedAction = "ResolveFinalizerWithConsent"
	ActionFixOperand                  SuggestedAction = "FixOperand"
	ActionStopExternalReconciler      SuggestedAction = "StopExternalReconciler"
	ActionSweepDanglingRegistration   SuggestedAction = "SweepDanglingRegistration"
)

// Blocker is the single record consumed by the report, status, MCH mirror and
// verification. UID is the identity and the join key; name is report-only.
type Blocker struct {
	// --- identity ---------------------------------------------------------
	APIVersion string    `json:"apiVersion"`
	Kind       string    `json:"kind"`
	Name       string    `json:"name"`
	Namespace  string    `json:"namespace,omitempty"`
	UID        types.UID `json:"uid"`

	// --- why it blocks ----------------------------------------------------
	Finalizers        []string      `json:"finalizers,omitempty"`
	DeletionTimestamp *metav1.Time  `json:"deletionTimestamp,omitempty"`
	BlockedSince      metav1.Time   `json:"blockedSince"`
	ObservedInPhase   TeardownPhase `json:"observedInPhase"`

	// --- who should be processing it --------------------------------------
	OwningController   *ControllerRef `json:"owningController,omitempty"`
	ControllerAlive    *bool          `json:"controllerAlive,omitempty"`
	ControllerEvidence []Evidence     `json:"controllerEvidence,omitempty"`

	// --- who owns it ------------------------------------------------------
	Ownership OwnershipVerdict `json:"ownership"`

	// --- consequence of acting --------------------------------------------
	CloudRisk *CloudRisk `json:"cloudRisk,omitempty"`

	// --- what it is -------------------------------------------------------
	Classification Classification `json:"classification"`

	// --- what to do -------------------------------------------------------
	SuggestedAction SuggestedAction `json:"suggestedAction"`
	RequiresConsent bool            `json:"requiresConsent"`
	ConsentReason   string          `json:"consentReason,omitempty"`
	RelatedFix      string          `json:"relatedFix,omitempty"`
}

// BlockerSummary is the never-truncated aggregate over the complete blocker
// list. It is safe to mirror onto MultiClusterHub status because it carries no
// per-resource detail.
type BlockerSummary struct {
	// Total is the number of blockers in the complete report.
	Total int32 `json:"total"`

	// RequiringConsent is the number of blockers whose SuggestedAction needs
	// explicit administrator approval.
	RequiringConsent int32 `json:"requiringConsent"`

	// ByClassification counts blockers per classification.
	// +optional
	ByClassification map[string]int32 `json:"byClassification,omitempty"`
}
