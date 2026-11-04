// Copyright Contributors to the Open Cluster Management project

package v1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// TeardownPhase represents the current high-level phase of the teardown process.
//
// Both the legacy and the prototype values are accepted while the controller
// cutover is pending. Removing the legacy values is part of that cutover, not
// of the additive API change.
//
// +kubebuilder:validation:Enum=DryRun;RemoveBlockingCRs;DisableAddons;DeleteInfrastructureCRs;DetachManagedClusters;DeleteMCH;MonitorMCEChain;DeleteACMCRDs;Complete;Failed;NotStarted;Inspecting;ReportReady;ConsentRequired;Preparing;HandedOff;Stalled;Verifying;CompleteWithResidue
type TeardownPhase string

const (
	TeardownPhaseDryRun                  TeardownPhase = "DryRun"
	TeardownPhaseRemoveBlockingCRs       TeardownPhase = "RemoveBlockingCRs"
	TeardownPhaseDisableAddons           TeardownPhase = "DisableAddons"
	TeardownPhaseDeleteInfrastructureCRs TeardownPhase = "DeleteInfrastructureCRs"
	TeardownPhaseDetachManagedClusters   TeardownPhase = "DetachManagedClusters"
	TeardownPhaseDeleteMCH               TeardownPhase = "DeleteMCH"
	TeardownPhaseMonitorMCEChain         TeardownPhase = "MonitorMCEChain"
	TeardownPhaseDeleteACMCRDs           TeardownPhase = "DeleteACMCRDs"
	TeardownPhaseComplete                TeardownPhase = "Complete"
	TeardownPhaseFailed                  TeardownPhase = "Failed"
)

// Prototype phase model (RFE-9332 §2.1). These values are additive: the
// legacy phase constants above stay until the controller cutover, because
// they carry 60+ references across six controller files.
//
// NotStarted, ConsentRequired, Preparing, HandedOff and Stalled have no
// producer yet. The same information is carried by conditions
// (WaitingForApproval, HandoffReady) and by status.phases, so they are kept as
// reserved values rather than deleted: a future producer must not have to churn
// the CRD enum. Inspecting, ReportReady, Verifying and CompleteWithResidue are
// produced today.
const (
	TeardownPhaseNotStarted          TeardownPhase = "NotStarted"
	TeardownPhaseInspecting          TeardownPhase = "Inspecting"
	TeardownPhaseReportReady         TeardownPhase = "ReportReady"
	TeardownPhaseConsentRequired     TeardownPhase = "ConsentRequired"
	TeardownPhasePreparing           TeardownPhase = "Preparing"
	TeardownPhaseHandedOff           TeardownPhase = "HandedOff"
	TeardownPhaseStalled             TeardownPhase = "Stalled"
	TeardownPhaseVerifying           TeardownPhase = "Verifying"
	TeardownPhaseCompleteWithResidue TeardownPhase = "CompleteWithResidue"
)

// TeardownPhaseState tracks whether an individual phase has completed.
type TeardownPhaseState string

const (
	PhaseStatePending    TeardownPhaseState = "Pending"
	PhaseStateInProgress TeardownPhaseState = "InProgress"
	PhaseStateComplete   TeardownPhaseState = "Complete"
	PhaseStateFailed     TeardownPhaseState = "Failed"
	PhaseStateSkipped    TeardownPhaseState = "Skipped"
)

// HubTeardownSpec defines the desired state of HubTeardown.
type HubTeardownSpec struct {
	// DryRun when true (the default) causes the controller to scan and report
	// without mutating any cluster resources. Set to false to begin teardown.
	// Can be toggled back to true at any time to pause and re-scan.
	// +kubebuilder:default=true
	DryRun bool `json:"dryRun"`

	// ApprovedDestructiveActions enables Tier 2 (non-cloud) finalizer
	// patching for stuck resources. Only evaluated when dryRun is false.
	ApprovedDestructiveActions bool `json:"approvedDestructiveActions,omitempty"`

	// AcknowledgeCloudResourceRisk enables Tier 1 (cloud-protecting) finalizer
	// patching. Setting this to true means the admin accepts that cloud
	// infrastructure (VMs, LBs, storage, DNS) may be orphaned and will
	// require manual cleanup in the cloud provider console.
	AcknowledgeCloudResourceRisk bool `json:"acknowledgeCloudResourceRisk,omitempty"`

	// AcknowledgedReportDigest is the digest of the inspection report the
	// administrator read before approving destructive work. Consent is bound to
	// this digest: when the report changes, the digest changes and consent
	// lapses. Required in practice for any destructive phase; an empty value
	// keeps the teardown parked at ConsentRequired.
	// +optional
	AcknowledgedReportDigest string `json:"acknowledgedReportDigest,omitempty"`

	// ForceFinalizerTimeout is the duration to wait for a controller to
	// process its own finalizer before the teardown controller intervenes.
	// Defaults to 5m.
	// +optional
	ForceFinalizerTimeout *metav1.Duration `json:"forceFinalizerTimeout,omitempty"`

	// MaxDuration is the maximum wall-clock time allowed for the entire
	// teardown. If exceeded, the controller sets a Stalled condition with
	// guidance on what is blocking. Defaults to 2h.
	// +optional
	MaxDuration *metav1.Duration `json:"maxDuration,omitempty"`
}

// HubTeardownStatus defines the observed state of HubTeardown.
type HubTeardownStatus struct {
	// Phase is the current high-level phase of the teardown process.
	Phase TeardownPhase `json:"phase,omitempty"`

	// DryRunReport contains the full teardown preview generated during
	// dry-run mode. Updated on each reconcile while dryRun is true.
	// +optional
	DryRunReport *DryRunReport `json:"dryRunReport,omitempty"`

	// BlockingResources lists resources that prevent MCH deletion via
	// the validating webhook.
	// +optional
	BlockingResources []BlockingResource `json:"blockingResources,omitempty"`

	// CloudResourceWarnings lists resources with cloud-protecting finalizers
	// and their platform-specific risk descriptions.
	// +optional
	CloudResourceWarnings []CloudResourceWarning `json:"cloudResourceWarnings,omitempty"`

	// Summary is the never-truncated aggregate over the complete blocker list.
	// +optional
	Summary *BlockerSummary `json:"summary,omitempty"`

	// Blockers is a capped view of the complete blocker list. The cap keeps
	// status small enough that admission and etcd never reject the update: 32
	// leaves room for roughly 8KB of Blocker JSON, well inside the 1.5MB
	// object limit when combined with the existing status arrays.
	// The complete list always lives in the report ConfigMap.
	// +kubebuilder:validation:MaxItems=32
	// +optional
	Blockers []Blocker `json:"blockers,omitempty"`

	// BlockersTruncated is the number of blockers omitted from Blockers.
	// +optional
	BlockersTruncated int32 `json:"blockersTruncated,omitempty"`

	// LastInspection is when the inspector last completed a full scan. It is
	// set even when the scan found nothing, so an empty blocker list can be
	// read as an observation rather than as a missing scan.
	// +optional
	LastInspection *metav1.Time `json:"lastInspection,omitempty"`

	// ReportDigest is the digest of the report named by ReportRef, as recorded
	// in the report's meta.json. It is what spec.acknowledgedReportDigest is
	// compared against, so consent never depends on a name that could be reused.
	// +optional
	ReportDigest string `json:"reportDigest,omitempty"`

	// ReportRef names the ConfigMap holding the complete inspection report.
	// The report carries no ownerReferences, so it outlives the HubTeardown CR
	// and remains available after the teardown finishes.
	// +optional
	ReportRef *corev1.LocalObjectReference `json:"reportRef,omitempty"`

	// Residue is the Verify phase's postcondition record. It is written only
	// in a terminal phase, because it is the one field that must be written
	// after the MultiClusterHub no longer exists.
	// +optional
	Residue *ResidueReport `json:"residue,omitempty"`

	// ActiveSince is the timestamp when the teardown transitioned from
	// dry-run to active execution. Used to calculate overall stall timeout.
	// +optional
	ActiveSince *metav1.Time `json:"activeSince,omitempty"`

	// PausedAt records when the admin paused an active teardown by setting
	// dryRun back to true. The deadline does not accrue while paused: on
	// resume ActiveSince is shifted forward by the paused interval.
	// +optional
	PausedAt *metav1.Time `json:"pausedAt,omitempty"`

	// Phases tracks the status of each teardown phase.
	// +optional
	Phases []TeardownPhaseStatus `json:"phases,omitempty"`

	// Conditions reflect the current state of the teardown process.
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// DryRunReport contains the full teardown preview.
type DryRunReport struct {
	// ScanTime is when the last scan was performed.
	ScanTime metav1.Time `json:"scanTime"`

	// Summary is a human-readable overview of the teardown plan.
	Summary string `json:"summary"`

	// PlannedPhases is an ordered list of phases with per-phase detail.
	PlannedPhases []PlannedPhase `json:"plannedPhases"`

	// TotalBlockingResources is the count of resources blocking uninstall.
	TotalBlockingResources int `json:"totalBlockingResources"`

	// TotalCloudRiskResources is the count of resources with Tier 1 finalizers.
	TotalCloudRiskResources int `json:"totalCloudRiskResources"`
}

// ResidueReport is the observable postcondition of a teardown. Expected
// survivors are listed separately: under D-G the operator and its namespace
// are preserved on purpose, and a report that called them residue would make
// every successful teardown look failed.
type ResidueReport struct {
	// ObservedAt is when Verify ran.
	ObservedAt metav1.Time `json:"observedAt"`

	// Complete is true only when nothing outside ExpectedSurvivors remains AND
	// the inventory was complete. An incomplete inventory cannot prove
	// absence, so it can never produce Complete.
	Complete bool `json:"complete"`

	// CompleteWithResidueReason names why Complete was refused when the
	// inventory was incomplete. Empty when Complete is true or when ordinary
	// residue is the reason.
	// +optional
	CompleteWithResidueReason string `json:"completeWithResidueReason,omitempty"`

	// ExpectedSurvivors are objects the teardown deliberately leaves behind.
	// +optional
	ExpectedSurvivors []ResidueItem `json:"expectedSurvivors,omitempty"`

	// Residue is everything else that Verify observed.
	// +optional
	Residue []ResidueItem `json:"residue,omitempty"`
}

// ResidueItem identifies one object seen at Verify time.
type ResidueItem struct {
	// APIVersion is group/version when known.
	// +optional
	APIVersion string `json:"apiVersion,omitempty"`

	Kind      string `json:"kind"`
	Name      string `json:"name"`
	Namespace string `json:"namespace,omitempty"`

	// Reason explains why the item is expected, or why it is residue.
	Reason string `json:"reason"`
}

// PlannedPhase describes what a single teardown phase will do.
type PlannedPhase struct {
	// Phase name (matches TeardownPhase enum).
	Phase TeardownPhase `json:"phase"`

	// Action is a short description of what this phase will do.
	Action string `json:"action"`

	// ResourcesAffected is the count of resources this phase touches.
	ResourcesAffected int `json:"resourcesAffected"`

	// CloudRiskResources is the count of Tier 1 resources in this phase.
	// +optional
	CloudRiskResources int `json:"cloudRiskResources,omitempty"`

	// Details is a per-resource breakdown of what will happen.
	// +optional
	Details []string `json:"details,omitempty"`
}

// BlockingResource identifies a resource that prevents MCH deletion.
type BlockingResource struct {
	// Group is the API group of the resource.
	Group string `json:"group"`

	// Kind is the resource kind.
	Kind string `json:"kind"`

	// Name is the resource name.
	Name string `json:"name"`

	// Namespace is the resource namespace (empty for cluster-scoped).
	// +optional
	Namespace string `json:"namespace,omitempty"`

	// Reason explains why this resource blocks uninstall.
	Reason string `json:"reason"`
}

// ResourceRef identifies a Kubernetes resource by GVK + namespace/name.
type ResourceRef struct {
	// Group is the API group of the resource.
	Group string `json:"group"`

	// Kind is the resource kind.
	Kind string `json:"kind"`

	// Namespace is the resource namespace (empty for cluster-scoped).
	// +optional
	Namespace string `json:"namespace,omitempty"`

	// Name is the resource name.
	Name string `json:"name"`
}

// CloudResourceWarning describes a resource whose finalizer protects cloud infrastructure.
type CloudResourceWarning struct {
	// Resource identifies the Kubernetes resource.
	Resource ResourceRef `json:"resource"`

	// Finalizer is the specific finalizer string that protects cloud resources.
	Finalizer string `json:"finalizer"`

	// RiskSummary is a human-readable description of what cloud resources
	// will be orphaned if this finalizer is removed.
	RiskSummary string `json:"riskSummary"`

	// Platform is the cloud platform (AWS, Azure, GCP, etc.) if detectable.
	// +optional
	Platform string `json:"platform,omitempty"`

	// Acknowledged indicates whether the admin has reviewed this warning.
	Acknowledged bool `json:"acknowledged"`
}

// TeardownPhaseStatus tracks the execution state of one teardown phase.
type TeardownPhaseStatus struct {
	// Phase name.
	Phase TeardownPhase `json:"phase"`

	// State is the current execution state of this phase.
	State TeardownPhaseState `json:"state"`

	// Message provides human-readable detail about the phase state.
	// +optional
	Message string `json:"message,omitempty"`

	// StartTime is when this phase began executing.
	// +optional
	StartTime *metav1.Time `json:"startTime,omitempty"`

	// CompletionTime is when this phase finished.
	// +optional
	CompletionTime *metav1.Time `json:"completionTime,omitempty"`
}

//+kubebuilder:object:root=true
//+kubebuilder:subresource:status
//+kubebuilder:resource:path=hubteardowns,scope=Namespaced,shortName=htd
//+kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`,description="Current teardown phase"
//+kubebuilder:printcolumn:name="Dry-Run",type=boolean,JSONPath=`.spec.dryRun`,description="Whether dry-run mode is active"
//+kubebuilder:printcolumn:name="Blocking",type=integer,JSONPath=`.status.dryRunReport.totalBlockingResources`,description="Number of blocking resources"
//+kubebuilder:printcolumn:name="Cloud-Risk",type=integer,JSONPath=`.status.dryRunReport.totalCloudRiskResources`,description="Number of cloud-risk resources"
//+kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// HubTeardown is the Schema for the hubteardowns API. It provides orchestrated,
// observable, and safe RHACM uninstallation with dependency graph scanning,
// ordered phase execution, and explicit cloud resource leak warnings.
type HubTeardown struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   HubTeardownSpec   `json:"spec,omitempty"`
	Status HubTeardownStatus `json:"status,omitempty"`
}

//+kubebuilder:object:root=true

// HubTeardownList contains a list of HubTeardown.
type HubTeardownList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []HubTeardown `json:"items"`
}

func init() {
	SchemeBuilder.Register(&HubTeardown{}, &HubTeardownList{})
}
