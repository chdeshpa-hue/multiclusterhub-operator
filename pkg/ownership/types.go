// Package ownership answers "does this MCH own this object?" from evidence.
// Name matching is recorded for the report but can never raise confidence
// above Weak, and Weak never authorises a destructive action.
//
// The data types live in api/v1 so that they can be serialised into the
// HubTeardown status and the report ConfigMap. This package owns only the
// reasoning over them; aliases are kept so call sites read naturally.
package ownership

import (
	operatorv1 "github.com/stolostron/multiclusterhub-operator/api/v1"
)

// EvidenceKind identifies one observation that bears on ownership.
type EvidenceKind = operatorv1.EvidenceKind

const (
	EvidenceInstallerLabel      = operatorv1.EvidenceInstallerLabel
	EvidenceOwnerRefChain       = operatorv1.EvidenceOwnerRefChain
	EvidenceDefiningCRDLabelled = operatorv1.EvidenceDefiningCRDLabelled
	EvidenceAPIServiceBacking   = operatorv1.EvidenceAPIServiceBacking
	EvidenceControllerAbsent    = operatorv1.EvidenceControllerAbsent
	EvidenceNameMatch           = operatorv1.EvidenceNameMatch
)

// Evidence is one observation, with where it was read from.
type Evidence = operatorv1.Evidence

// Confidence is the maximum confidence over the evidence list. It is never a
// sum: two Weak observations do not make a Strong one.
type Confidence = operatorv1.Confidence

const (
	ConfidenceDefinitive = operatorv1.ConfidenceDefinitive
	ConfidenceStrong     = operatorv1.ConfidenceStrong
	ConfidenceWeak       = operatorv1.ConfidenceWeak
	ConfidenceNone       = operatorv1.ConfidenceNone
)

// OwnershipVerdict is the answer plus the evidence that produced it.
type OwnershipVerdict = operatorv1.OwnershipVerdict
