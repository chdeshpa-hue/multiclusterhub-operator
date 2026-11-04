// Copyright Contributors to the Open Cluster Management project

package verify

import (
	"testing"

	operatorv1 "github.com/stolostron/multiclusterhub-operator/api/v1"
)

var testPolicy = Policy{
	OperatorNamespace: "open-cluster-management",
	ACMGroupSuffixes:  []string{"open-cluster-management.io", "hive.openshift.io", "hypershift.openshift.io"},
	ACMNamespaces:     []string{"clusters", "multicluster-engine", "hypershift"},
	KeepCRDs:          []string{"multiclusterhubs.operator.open-cluster-management.io"},
	OperatorAPIGroup:  "operator.open-cluster-management.io",
}

func TestEvaluateCleanHub(t *testing.T) {
	in := Input{
		Namespaces: []string{"open-cluster-management", "kube-system", "default"},
		CRDs: []ObjectRef{
			{Group: "operator.open-cluster-management.io", Kind: "MultiClusterHub", Name: "multiclusterhubs.operator.open-cluster-management.io"},
			{Group: "", Kind: "Pod", Name: "pods"},
		},
		Objects: []ObjectRef{
			{Group: "operator.open-cluster-management.io", Kind: "InternalHubComponent", Namespace: "open-cluster-management", Name: "console"},
		},
		Webhooks: []WebhookRef{
			{Kind: "ValidatingWebhookConfiguration", Name: "mch-validator", ServiceNamespace: "open-cluster-management"},
		},
	}

	report := Evaluate(in, testPolicy)

	if !report.Complete {
		t.Fatalf("clean hub must be Complete, residue=%+v incomplete=%v", report.Residue, report.Incomplete)
	}
	if len(report.Residue) != 0 {
		t.Fatalf("residue = %+v", report.Residue)
	}
	if len(report.Expected) != 3 {
		t.Fatalf("expected survivors = %d, want 3: %+v", len(report.Expected), report.Expected)
	}
}

func TestEvaluateResidueNamespace(t *testing.T) {
	report := Evaluate(Input{Namespaces: []string{"open-cluster-management", "clusters"}}, testPolicy)

	if report.Complete {
		t.Fatal("a residual ACM namespace must not be Complete")
	}
	if len(report.Residue) != 1 || report.Residue[0].Kind != "Namespace" || report.Residue[0].Name != "clusters" {
		t.Fatalf("residue = %+v", report.Residue)
	}
}

func TestEvaluateResidueCRD(t *testing.T) {
	report := Evaluate(Input{CRDs: []ObjectRef{
		{Group: "cluster.open-cluster-management.io", Kind: "ManagedCluster", Name: "managedclusters.cluster.open-cluster-management.io"},
	}}, testPolicy)

	if report.Complete || len(report.Residue) != 1 || report.Residue[0].Kind != "CustomResourceDefinition" {
		t.Fatalf("report = %+v", report)
	}
}

func TestEvaluateResidueObject(t *testing.T) {
	report := Evaluate(Input{Objects: []ObjectRef{
		{Group: "cluster.open-cluster-management.io", Kind: "ManagedCluster", Name: "mc1"},
	}}, testPolicy)

	if report.Complete || len(report.Residue) != 1 || report.Residue[0].Kind != "ManagedCluster" {
		t.Fatalf("report = %+v", report)
	}
}

func TestEvaluateIncompleteNeverComplete(t *testing.T) {
	report := Evaluate(Input{Incomplete: []string{"bad.example.com/widgets: discovery failed"}}, testPolicy)

	if report.Complete {
		t.Fatal("an incomplete inventory must never produce Complete")
	}
	if report.RefusedReason == "" {
		t.Fatal("RefusedReason must explain why Complete was refused")
	}
}

func TestEvaluateWebhooks(t *testing.T) {
	report := Evaluate(Input{Webhooks: []WebhookRef{
		{Kind: "ValidatingWebhookConfiguration", Name: "no-service"},
		{Kind: "MutatingWebhookConfiguration", Name: "removed-ns", ServiceNamespace: "multicluster-engine"},
	}}, testPolicy)

	if report.Complete {
		t.Fatal("dangling webhooks are residue")
	}
	if len(report.Residue) != 2 {
		t.Fatalf("residue = %+v", report.Residue)
	}
}

func TestEvaluateRBAC(t *testing.T) {
	report := Evaluate(Input{RBAC: []ObjectRef{
		{Group: "rbac.authorization.k8s.io", Kind: "ClusterRole", Name: "acm-role"},
	}}, testPolicy)

	if report.Complete || len(report.Residue) != 1 {
		t.Fatalf("report = %+v", report)
	}
}

func TestEvaluateDoesNotUseNamePrefixes(t *testing.T) {
	// "clusters-hcp1" looks like an ACM namespace but is not declared, so the
	// classifier must not infer it from the name. Prefix matching is the defect
	// DF-5 removes from the detach path; it must not reappear here.
	report := Evaluate(Input{Namespaces: []string{"clusters-hcp1"}}, testPolicy)

	if !report.Complete {
		t.Fatalf("undeclared name must not be residue: %+v", report.Residue)
	}
}

func TestReportSortsDeterministically(t *testing.T) {
	report := Evaluate(Input{
		Namespaces: []string{"hypershift", "clusters", "multicluster-engine"},
	}, testPolicy)

	if len(report.Residue) != 3 {
		t.Fatalf("residue = %+v", report.Residue)
	}
	order := []string{report.Residue[0].Name, report.Residue[1].Name, report.Residue[2].Name}
	want := []string{"clusters", "hypershift", "multicluster-engine"}
	for i := range want {
		if order[i] != want[i] {
			t.Fatalf("order = %v, want %v", order, want)
		}
	}
}

var _ operatorv1.ResidueItem

// P-1/E-2: core-group objects in the preserved namespace are counted, not
// classified as residue, and they do not change the verdict.
func TestEvaluateOperatorNamespaceObjectsAreUnclassified(t *testing.T) {
	in := Input{
		Namespaces: []string{"open-cluster-management", "kube-system"},
		OperatorNamespaceObjects: []ObjectRef{
			{Kind: "ConfigMap", Namespace: "open-cluster-management", Name: "leftover-config"},
			{Kind: "Secret", Namespace: "open-cluster-management", Name: "leftover-secret"},
		},
	}

	report := Evaluate(in, testPolicy)

	if len(report.Unclassified) != 2 {
		t.Fatalf("unclassified = %+v, want 2", report.Unclassified)
	}
	if len(report.Residue) != 0 {
		t.Fatalf("core objects must not be residue: %+v", report.Residue)
	}
	if !report.Complete {
		t.Fatalf("disclosure must not change the verdict: %+v", report)
	}
}

// P-3: a declared keep-object is an expected survivor, not unclassified.
func TestEvaluateDeclaredKeepObjectIsExpected(t *testing.T) {
	policy := testPolicy
	policy.KeepObjects = []ObjectRef{
		{Group: "", Kind: "ConfigMap", Namespace: "open-cluster-management", Name: "kept"},
	}
	in := Input{
		Namespaces: []string{"open-cluster-management"},
		OperatorNamespaceObjects: []ObjectRef{
			{Kind: "ConfigMap", Namespace: "open-cluster-management", Name: "kept"},
		},
	}

	report := Evaluate(in, policy)

	if len(report.Unclassified) != 0 {
		t.Fatalf("declared object was classified unclassified: %+v", report.Unclassified)
	}
	found := false
	for _, item := range report.Expected {
		if item.Name == "kept" {
			found = true
		}
	}
	if !found {
		t.Fatalf("declared object missing from expected survivors: %+v", report.Expected)
	}
}

// P-2: a namespace with nothing to disclose stays silent.
func TestEvaluateNoOperatorNamespaceObjectsStaySilent(t *testing.T) {
	in := Input{Namespaces: []string{"open-cluster-management"}}

	report := Evaluate(in, testPolicy)

	if len(report.Unclassified) != 0 {
		t.Fatalf("unclassified = %+v, want none", report.Unclassified)
	}
}
