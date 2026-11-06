package policy

import (
	"testing"

	"k8s.io/apimachinery/pkg/types"

	"github.com/stolostron/multiclusterhub-operator/pkg/ownership"
)

func boolPtr(v bool) *bool { return &v }

func definitiveBlocker() Blocker {
	return Blocker{
		APIVersion:      "hypershift.openshift.io/v1beta1",
		Kind:            "HostedCluster",
		Name:            "hcp1",
		Namespace:       "clusters",
		UID:             types.UID("uid-hcp1"),
		Finalizers:      []string{"hypershift.openshift.io/finalizer"},
		ControllerAlive: boolPtr(false),
		Ownership:       ownership.OwnershipVerdict{Owned: true, Confidence: ownership.ConfidenceDefinitive},
		Classification:  ClassCloudProtecting,
		ObservedInPhase: "Inspecting",
	}
}

func TestEvaluate(t *testing.T) {
	validConsent := ConsentState{CloudRiskAcknowledged: true, AcknowledgedDigest: "digest-1", ReportDigest: "digest-1"}
	staleConsent := ConsentState{CloudRiskAcknowledged: true, AcknowledgedDigest: "digest-1", ReportDigest: "digest-2"}

	tests := []struct {
		name            string
		mutate          func(*Blocker)
		consent         ConsentState
		wantAction      Action
		wantConsent     bool
		wantConsentNote bool
	}{
		{
			name:       "P-1 definitive, controller dead, no cloud risk",
			mutate:     func(b *Blocker) {},
			wantAction: ActionResolveFinalizer,
		},
		{
			name: "P-2 cloud risk without consent",
			mutate: func(b *Blocker) {
				b.CloudRisk = &CloudRisk{Platform: "AWS", ResourceType: "EC2 instance", MayOrphan: true}
			},
			wantAction:      ActionResolveFinalizerWithConsent,
			wantConsent:     true,
			wantConsentNote: true,
		},
		{
			name: "P-2 cloud risk with valid consent",
			mutate: func(b *Blocker) {
				b.CloudRisk = &CloudRisk{Platform: "AWS", ResourceType: "EC2 instance", MayOrphan: true}
			},
			consent:    validConsent,
			wantAction: ActionResolveFinalizer,
		},
		{
			name: "P-2 consent lapses when the report changes",
			mutate: func(b *Blocker) {
				b.CloudRisk = &CloudRisk{Platform: "AWS", ResourceType: "EC2 instance", MayOrphan: true}
			},
			consent:         staleConsent,
			wantAction:      ActionResolveFinalizerWithConsent,
			wantConsent:     true,
			wantConsentNote: true,
		},
		{
			name: "P-3 weak ownership is report only",
			mutate: func(b *Blocker) {
				b.Ownership = ownership.OwnershipVerdict{Owned: false, Confidence: ownership.ConfidenceWeak}
			},
			wantAction: ActionReportOnly,
		},
		{
			name: "P-3b unset ownership is report only",
			mutate: func(b *Blocker) {
				b.Ownership = ownership.OwnershipVerdict{}
			},
			wantAction: ActionReportOnly,
		},
		{
			name: "P-4 unknown liveness waits",
			mutate: func(b *Blocker) {
				b.ControllerAlive = nil
			},
			wantAction: ActionWaitForController,
		},
		{
			name: "P-5 live controller waits",
			mutate: func(b *Blocker) {
				b.ControllerAlive = boolPtr(true)
			},
			wantAction: ActionWaitForController,
		},
		{
			name: "P-6 recreate loop is an operand fix",
			mutate: func(b *Blocker) {
				b.Classification = ClassRecreateLoop
				b.RelatedFix = "OF-1"
			},
			wantAction: ActionFixOperand,
		},
		{
			name: "P-7 external reconciler is a human step",
			mutate: func(b *Blocker) {
				b.Classification = ClassExternalReconciler
			},
			wantAction: ActionStopExternalReconciler,
		},
		{
			name: "P-8 dangling registration is swept",
			mutate: func(b *Blocker) {
				b.Classification = ClassDanglingRegistration
			},
			wantAction: ActionSweepDanglingRegistration,
		},
		{
			name: "P-9 unreachable spoke overrides a live hub controller",
			mutate: func(b *Blocker) {
				b.Classification = ClassUnreachableSpoke
				b.ControllerAlive = boolPtr(true)
			},
			wantAction: ActionResolveFinalizer,
		},
		{
			name: "no finalizers is report only even when the controller is dead",
			mutate: func(b *Blocker) {
				b.Finalizers = nil
			},
			wantAction: ActionReportOnly,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			blocker := definitiveBlocker()
			tt.mutate(&blocker)
			got := Evaluate(blocker, tt.consent)
			if got.Action != tt.wantAction {
				t.Fatalf("action = %s, want %s (reason %q)", got.Action, tt.wantAction, got.Reason)
			}
			if got.RequiresConsent != tt.wantConsent {
				t.Fatalf("requiresConsent = %v, want %v", got.RequiresConsent, tt.wantConsent)
			}
			if tt.wantConsentNote && got.ConsentReason == "" {
				t.Fatal("expected a consent reason")
			}
		})
	}
}

func TestConsentValid(t *testing.T) {
	tests := []struct {
		name    string
		consent ConsentState
		want    bool
	}{
		{name: "matching digest", consent: ConsentState{CloudRiskAcknowledged: true, AcknowledgedDigest: "d1", ReportDigest: "d1"}, want: true},
		{name: "matching digest without cloud-risk acknowledgement", consent: ConsentState{AcknowledgedDigest: "d1", ReportDigest: "d1"}, want: false},
		{name: "not acknowledged", consent: ConsentState{AcknowledgedDigest: "d1", ReportDigest: "d1"}, want: false},
		{name: "report changed", consent: ConsentState{CloudRiskAcknowledged: true, AcknowledgedDigest: "d1", ReportDigest: "d2"}, want: false},
		{name: "empty acknowledgement", consent: ConsentState{CloudRiskAcknowledged: true, ReportDigest: "d1"}, want: false},
		{name: "no report yet", consent: ConsentState{CloudRiskAcknowledged: true, AcknowledgedDigest: "d1"}, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.consent.Valid(); got != tt.want {
				t.Fatalf("valid = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestConsentCloudRiskValid(t *testing.T) {
	tests := []struct {
		name    string
		consent ConsentState
		want    bool
	}{
		{name: "cloud risk acknowledged", consent: ConsentState{CloudRiskAcknowledged: true, AcknowledgedDigest: "d1", ReportDigest: "d1"}, want: true},
		{name: "empty acknowledgement", consent: ConsentState{CloudRiskAcknowledged: true, ReportDigest: "d1"}, want: false},
		{name: "stale digest", consent: ConsentState{CloudRiskAcknowledged: true, AcknowledgedDigest: "d1", ReportDigest: "d2"}, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.consent.CloudRiskValid(); got != tt.want {
				t.Fatalf("cloudRiskValid = %v, want %v", got, tt.want)
			}
		})
	}
}
