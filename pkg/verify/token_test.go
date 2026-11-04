// Copyright Contributors to the Open Cluster Management project

package verify

import (
	"strings"
	"testing"
)

func tokenObject(name, serviceAccount string) ObjectRef {
	return ObjectRef{
		Kind:           "Secret",
		Namespace:      "open-cluster-management",
		Name:           name,
		SecretType:     "kubernetes.io/service-account-token",
		ServiceAccount: serviceAccount,
	}
}

// P-1 / E-1: a token present before and after is an expected survivor, and the
// reason names the account it belongs to.
func TestEvaluateServiceAccountTokenIsExpected(t *testing.T) {
	obj := tokenObject("builder-token-abcde", "builder")
	report := Evaluate(Input{
		OperatorNamespaceObjects:  []ObjectRef{obj},
		OperatorNamespaceSnapshot: []ObjectRef{obj},
		SnapshotPresent:           true,
	}, testPolicy)

	if len(report.Expected) != 1 {
		t.Fatalf("expected = %+v, want one entry", report.Expected)
	}
	if !strings.Contains(report.Expected[0].Reason, "ServiceAccount builder") {
		t.Errorf("reason = %q, want the account name", report.Expected[0].Reason)
	}
	if len(report.Unclassified) != 0 {
		t.Errorf("unclassified = %+v, want none", report.Unclassified)
	}
}

// P-2: the type is the evidence; a missing annotation is detail, not a reason
// to leave the object unclassified.
func TestEvaluateServiceAccountTokenWithoutAnnotation(t *testing.T) {
	report := Evaluate(Input{
		OperatorNamespaceObjects: []ObjectRef{tokenObject("token-no-annotation", "")},
		SnapshotPresent:          true,
	}, testPolicy)

	if len(report.Expected) != 1 {
		t.Fatalf("expected = %+v, want one entry", report.Expected)
	}
	if strings.Contains(report.Expected[0].Reason, "ServiceAccount") {
		t.Errorf("reason = %q, want no account claim", report.Expected[0].Reason)
	}
}

// P-3: a token created during the teardown is still platform-managed, so it is
// expected and not counted as something the teardown invented.
func TestEvaluateServiceAccountTokenAppearedAfterSnapshot(t *testing.T) {
	report := Evaluate(Input{
		OperatorNamespaceObjects: []ObjectRef{tokenObject("job-token-xyz", "hubteardown-handoff")},
		SnapshotPresent:          true,
	}, testPolicy)

	if len(report.Expected) != 1 {
		t.Fatalf("expected = %+v, want one entry", report.Expected)
	}
	if report.NewSinceInspect != 0 {
		t.Errorf("newSinceInspect = %d, want 0", report.NewSinceInspect)
	}
	if len(report.Unclassified) != 0 {
		t.Errorf("unclassified = %+v, want none", report.Unclassified)
	}
}

// E-2: a snapshot written before the type fields existed still classifies the
// token, because the rule reads the live observation, not the snapshot entry.
func TestEvaluateServiceAccountTokenLegacySnapshot(t *testing.T) {
	report := Evaluate(Input{
		OperatorNamespaceObjects: []ObjectRef{tokenObject("builder-token-abcde", "builder")},
		OperatorNamespaceSnapshot: []ObjectRef{{
			Kind: "Secret", Namespace: "open-cluster-management", Name: "builder-token-abcde",
		}},
		SnapshotPresent: true,
	}, testPolicy)

	if len(report.Expected) != 1 || len(report.Unclassified) != 0 {
		t.Fatalf("expected = %+v, unclassified = %+v", report.Expected, report.Unclassified)
	}
}

// N-1..N-3: everything that is not a service-account token keeps the old
// unclassified treatment.
func TestEvaluateNonTokenSecretsStayUnclassified(t *testing.T) {
	objects := []ObjectRef{
		{Kind: "Secret", Namespace: "open-cluster-management", Name: "pull-secret",
			SecretType: "kubernetes.io/dockercfg"},
		{Kind: "Secret", Namespace: "open-cluster-management", Name: "config-secret",
			SecretType: "Opaque"},
		{Kind: "ConfigMap", Namespace: "open-cluster-management", Name: "operator-config"},
	}
	report := Evaluate(Input{
		OperatorNamespaceObjects:  objects,
		OperatorNamespaceSnapshot: objects,
		SnapshotPresent:           true,
	}, testPolicy)

	if len(report.Expected) != 0 {
		t.Errorf("expected = %+v, want none", report.Expected)
	}
	if len(report.Unclassified) != len(objects) {
		t.Errorf("unclassified = %d, want %d", len(report.Unclassified), len(objects))
	}
}
