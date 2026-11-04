// Copyright Contributors to the Open Cluster Management project

package verify

import (
	"strings"
	"testing"
)

func snapshotObject() ObjectRef {
	return ObjectRef{Kind: "ConfigMap", Namespace: "open-cluster-management", Name: "multicluster-integrations-config"}
}

func TestEvaluateOperatorNamespaceObjectPersisted(t *testing.T) {
	obj := snapshotObject()
	in := Input{
		Namespaces:                []string{"open-cluster-management"},
		OperatorNamespaceObjects:  []ObjectRef{obj},
		OperatorNamespaceSnapshot: []ObjectRef{obj},
		SnapshotPresent:           true,
	}

	report := Evaluate(in, testPolicy)

	if len(report.Unclassified) != 1 {
		t.Fatalf("want 1 unclassified, got %+v", report.Unclassified)
	}
	if report.NewSinceInspect != 0 {
		t.Errorf("NewSinceInspect = %d, want 0 for an object that was already there", report.NewSinceInspect)
	}
	if !strings.Contains(report.Unclassified[0].Reason, "present before teardown") {
		t.Errorf("reason = %q", report.Unclassified[0].Reason)
	}
	if !report.Complete {
		t.Error("unclassified objects must not refuse Complete (D13-5)")
	}
}

func TestEvaluateOperatorNamespaceObjectAppeared(t *testing.T) {
	in := Input{
		Namespaces:                []string{"open-cluster-management"},
		OperatorNamespaceObjects:  []ObjectRef{snapshotObject()},
		OperatorNamespaceSnapshot: nil,
		SnapshotPresent:           true,
	}

	report := Evaluate(in, testPolicy)

	if len(report.Unclassified) != 1 {
		t.Fatalf("want 1 unclassified, got %+v", report.Unclassified)
	}
	if report.NewSinceInspect != 1 {
		t.Errorf("NewSinceInspect = %d, want 1", report.NewSinceInspect)
	}
	if !strings.Contains(report.Unclassified[0].Reason, "appeared after the pre-teardown snapshot") {
		t.Errorf("reason = %q", report.Unclassified[0].Reason)
	}
}

func TestEvaluateOperatorNamespaceObjectRemoved(t *testing.T) {
	removed := ObjectRef{Kind: "Secret", Namespace: "open-cluster-management", Name: "removed-by-teardown"}
	in := Input{
		Namespaces:                []string{"open-cluster-management"},
		OperatorNamespaceObjects:  []ObjectRef{snapshotObject()},
		OperatorNamespaceSnapshot: []ObjectRef{snapshotObject(), removed},
		SnapshotPresent:           true,
	}

	report := Evaluate(in, testPolicy)

	if len(report.Unclassified) != 1 {
		t.Fatalf("want only the surviving object reported, got %+v", report.Unclassified)
	}
	if report.Unclassified[0].Name != snapshotObject().Name {
		t.Errorf("removed object was reported: %+v", report.Unclassified)
	}
	for _, item := range report.Residue {
		if item.Name == removed.Name {
			t.Errorf("object the teardown removed must not be residue: %+v", item)
		}
	}
}

func TestEvaluateOperatorNamespaceObjectWithoutSnapshot(t *testing.T) {
	in := Input{
		Namespaces:               []string{"open-cluster-management"},
		OperatorNamespaceObjects: []ObjectRef{snapshotObject()},
		SnapshotPresent:          false,
	}

	report := Evaluate(in, testPolicy)

	if len(report.Unclassified) != 1 {
		t.Fatalf("want 1 unclassified, got %+v", report.Unclassified)
	}
	if !strings.Contains(report.Unclassified[0].Reason, "no pre-teardown snapshot") {
		t.Errorf("reason = %q", report.Unclassified[0].Reason)
	}
	if report.NewSinceInspect != 0 {
		t.Errorf("NewSinceInspect = %d, want 0 without a baseline", report.NewSinceInspect)
	}
}

func TestEvaluateOperatorNamespaceKeepObject(t *testing.T) {
	platformCM := ObjectRef{Kind: "ConfigMap", Namespace: "open-cluster-management", Name: "kube-root-ca.crt"}
	policy := testPolicy
	policy.KeepObjects = []ObjectRef{platformCM}

	in := Input{
		Namespaces:                []string{"open-cluster-management"},
		OperatorNamespaceObjects:  []ObjectRef{platformCM},
		OperatorNamespaceSnapshot: []ObjectRef{platformCM},
		SnapshotPresent:           true,
	}

	report := Evaluate(in, policy)

	if len(report.Unclassified) != 0 {
		t.Fatalf("platform object must be classified, got %+v", report.Unclassified)
	}
	found := false
	for _, item := range report.Expected {
		if item.Kind == "ConfigMap" && item.Name == platformCM.Name {
			found = true
		}
	}
	if !found {
		t.Fatalf("platform object missing from expected survivors: %+v", report.Expected)
	}
}
