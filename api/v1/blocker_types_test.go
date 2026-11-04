// Copyright Contributors to the Open Cluster Management project

package v1

import (
	"encoding/json"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func fullyPopulatedBlocker() Blocker {
	alive := false
	ts := metav1.NewTime(metav1.Now().UTC().Truncate(time.Second))
	return Blocker{
		APIVersion:        "hypershift.openshift.io/v1beta1",
		Kind:              "HostedCluster",
		Name:              "example",
		Namespace:         "clusters-example",
		UID:               types.UID("11111111-2222-3333-4444-555555555555"),
		Finalizers:        []string{"hypershift.openshift.io/finalizer"},
		DeletionTimestamp: &ts,
		BlockedSince:      ts,
		ObservedInPhase:   TeardownPhaseInspecting,
		OwningController: &ControllerRef{
			FinalizerDomain: "hypershift.openshift.io",
			Kind:            "Deployment",
			Name:            "operator",
			Namespace:       "hypershift",
			Resolution:      ResolvedByFinalizerDomain,
		},
		ControllerAlive: &alive,
		ControllerEvidence: []Evidence{
			{Kind: EvidenceControllerAbsent, Detail: "0 ready replicas", Source: "deployment.status", Observed: ts},
		},
		Ownership: OwnershipVerdict{
			Owned:      true,
			Confidence: ConfidenceStrong,
			Evidence: []Evidence{
				{Kind: EvidenceInstallerLabel, Detail: "installer.name=multiclusterhub", Source: "metadata.labels", Observed: ts},
				{Kind: EvidenceNameMatch, Detail: "name matches pattern", Source: "metadata.name", Observed: ts},
			},
		},
		CloudRisk: &CloudRisk{
			Platform:     "AWS",
			ResourceType: "EC2 instance",
			MayOrphan:    true,
		},
		Classification:  ClassDeadController,
		SuggestedAction: ActionResolveFinalizerWithConsent,
		RequiresConsent: true,
		ConsentReason:   "Removing this finalizer may orphan 2 EC2 instances.",
		RelatedFix:      "DF-4",
	}
}

// A-1: the Blocker model survives a status round-trip without losing any
// field. Status is persisted as JSON, so a field that does not round-trip is a
// field the report cannot carry.
//
// Times are compared with Equal rather than DeepEqual: metav1.Time marshals at
// second precision and unmarshals into the local zone, so DeepEqual reports a
// difference that carries no information.
func TestBlockerJSONRoundTrip(t *testing.T) {
	in := fullyPopulatedBlocker()

	raw, err := json.Marshal(in)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var out Blocker
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if out.APIVersion != in.APIVersion || out.Kind != in.Kind || out.Name != in.Name ||
		out.Namespace != in.Namespace || out.UID != in.UID {
		t.Fatalf("identity fields lost: %#v", out)
	}
	if len(out.Finalizers) != 1 || out.Finalizers[0] != in.Finalizers[0] {
		t.Fatalf("finalizers lost: %v", out.Finalizers)
	}
	if out.DeletionTimestamp == nil || !out.DeletionTimestamp.Equal(in.DeletionTimestamp) {
		t.Fatalf("deletionTimestamp lost")
	}
	if !out.BlockedSince.Equal(&in.BlockedSince) {
		t.Fatalf("blockedSince lost")
	}
	if out.ObservedInPhase != in.ObservedInPhase {
		t.Fatalf("observedInPhase not preserved: %q", out.ObservedInPhase)
	}
	if out.OwningController == nil || *out.OwningController != *in.OwningController {
		t.Fatalf("owningController lost")
	}
	if out.ControllerAlive == nil || *out.ControllerAlive != *in.ControllerAlive {
		t.Fatalf("controllerAlive lost")
	}
	if len(out.ControllerEvidence) != 1 || out.ControllerEvidence[0].Kind != EvidenceControllerAbsent {
		t.Fatalf("controllerEvidence lost")
	}
	if !out.Ownership.Owned || out.Ownership.Confidence != ConfidenceStrong {
		t.Fatalf("ownership verdict lost: %#v", out.Ownership)
	}
	if len(out.Ownership.Evidence) != 2 {
		t.Fatalf("expected 2 ownership evidence entries, got %d", len(out.Ownership.Evidence))
	}
	if out.CloudRisk == nil || out.CloudRisk.Platform != "AWS" || !out.CloudRisk.MayOrphan {
		t.Fatalf("cloudRisk lost")
	}
	if out.Classification != ClassDeadController ||
		out.SuggestedAction != ActionResolveFinalizerWithConsent ||
		!out.RequiresConsent || out.ConsentReason == "" || out.RelatedFix != "DF-4" {
		t.Fatalf("policy fields lost: %#v", out)
	}
}

// A-12: UID is the identity. Two objects that share a name and namespace but
// differ in UID are different blockers and must both survive.
func TestBlockerIdentityIsUID(t *testing.T) {
	first := fullyPopulatedBlocker()
	first.UID = types.UID("aaaaaaaa-0000-0000-0000-000000000001")
	second := fullyPopulatedBlocker()
	second.UID = types.UID("bbbbbbbb-0000-0000-0000-000000000002")

	raw, err := json.Marshal([]Blocker{first, second})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var out []Blocker
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(out) != 2 {
		t.Fatalf("name-collision collapsed blockers: got %d, want 2", len(out))
	}
	if out[0].UID == out[1].UID {
		t.Fatalf("expected distinct UIDs, got %q twice", out[0].UID)
	}
}

// Generated deepcopy must not alias the slices and pointers it copies, or one
// blocker's report edit mutates another's.
func TestBlockerDeepCopyIsIndependent(t *testing.T) {
	original := fullyPopulatedBlocker()
	copied := original.DeepCopy()

	copied.Finalizers[0] = "mutated"
	*copied.ControllerAlive = true
	copied.Ownership.Evidence[0].Detail = "mutated"
	copied.CloudRisk.Platform = "Azure"

	if original.Finalizers[0] != "hypershift.openshift.io/finalizer" {
		t.Fatalf("Finalizers aliased between copies")
	}
	if *original.ControllerAlive {
		t.Fatalf("ControllerAlive pointer aliased between copies")
	}
	if original.Ownership.Evidence[0].Detail == "mutated" {
		t.Fatalf("Ownership.Evidence aliased between copies")
	}
	if original.CloudRisk.Platform != "AWS" {
		t.Fatalf("CloudRisk aliased between copies")
	}
}

// The legacy and prototype phase values must coexist without collision: a
// duplicated string value would make the generated enum list ambiguous.
func TestTeardownPhaseValuesAreUnique(t *testing.T) {
	values := []TeardownPhase{
		TeardownPhaseDryRun, TeardownPhaseGateOLMSubscription, TeardownPhaseRemoveBlockingCRs,
		TeardownPhaseDisableAddons, TeardownPhaseDeleteInfrastructureCRs, TeardownPhaseDetachManagedClusters,
		TeardownPhaseDeleteMCH, TeardownPhaseMonitorMCEChain, TeardownPhaseCleanOrphans,
		TeardownPhaseDeleteACMCRDs, TeardownPhaseRemoveOLMOperator, TeardownPhaseComplete,
		TeardownPhaseFailed, TeardownPhaseNotStarted, TeardownPhaseInspecting,
		TeardownPhaseReportReady, TeardownPhaseConsentRequired, TeardownPhasePreparing,
		TeardownPhaseHandedOff, TeardownPhaseStalled, TeardownPhaseEscalating,
		TeardownPhaseVerifying, TeardownPhaseCompleteWithResidue,
	}
	seen := make(map[TeardownPhase]bool, len(values))
	for _, v := range values {
		if seen[v] {
			t.Fatalf("duplicate TeardownPhase value %q", v)
		}
		seen[v] = true
	}
	if len(seen) != 23 {
		t.Fatalf("expected 23 distinct phase values, got %d", len(seen))
	}
	if TeardownPhaseComplete == TeardownPhaseCompleteWithResidue {
		t.Fatalf("Complete and CompleteWithResidue must be distinct terminal states")
	}
}

// The mirror carries counts only. A blocker list on the mirror would duplicate
// the authoritative report and invite readers to act on stale detail.
func TestUninstallMirrorCarriesSummaryNotBlockers(t *testing.T) {
	mirror := UninstallMirror{
		TeardownRef:    corev1.LocalObjectReference{Name: "teardown-1"},
		Phase:          TeardownPhaseVerifying,
		LastInspection: metav1.NewTime(metav1.Now().UTC()),
		Summary: BlockerSummary{
			Total:            7,
			RequiringConsent: 2,
			ByClassification: map[string]int32{string(ClassDeadController): 3},
		},
	}

	raw, err := json.Marshal(mirror)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var generic map[string]interface{}
	if err := json.Unmarshal(raw, &generic); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if _, found := generic["blockers"]; found {
		t.Fatalf("UninstallMirror must not carry a blocker list")
	}
	if generic["phase"] != string(TeardownPhaseVerifying) {
		t.Fatalf("phase not serialised: %v", generic["phase"])
	}
}
