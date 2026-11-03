// Package ownership answers "does this MCH own this object?" from evidence.
// Name matching is recorded for the report but can never raise confidence
// above Weak, and Weak never authorises a destructive action.
package ownership

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
	// EvidenceNameMatch is report-only. It is never sufficient for a
	// destructive action.
	EvidenceNameMatch EvidenceKind = "NameMatch"
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
