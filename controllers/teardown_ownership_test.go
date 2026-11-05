// Copyright Contributors to the Open Cluster Management project

package controllers

import (
	"testing"

	operatorv1 "github.com/stolostron/multiclusterhub-operator/api/v1"
	"github.com/stolostron/multiclusterhub-operator/pkg/inventory"
	"github.com/stolostron/multiclusterhub-operator/pkg/policy"
	apixv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
)

var testInstallerLabels = map[string]string{
	"installer.name":      "multiclusterhub",
	"installer.namespace": "open-cluster-management",
}

func testMCH() *DiscoveredResource {
	return &DiscoveredResource{
		Ref: operatorv1.ResourceRef{
			Group:     "operator.open-cluster-management.io",
			Kind:      "MultiClusterHub",
			Namespace: "open-cluster-management",
			Name:      "multiclusterhub",
		},
		UID:    "mch-uid",
		Labels: testInstallerLabels,
	}
}

func classifyTestGraph(graph *DependencyGraph) *DependencyGraph {
	(&HubTeardownReconciler{}).classifyOwnership(graph)
	return graph
}

// O-1: the object carries the installation labels itself.
func TestClassifyOwnershipInstallerLabels(t *testing.T) {
	graph := &DependencyGraph{DiscoveredResources: []DiscoveredResource{{
		Ref:     operatorv1.ResourceRef{Group: "agent-install.openshift.io", Kind: "AgentServiceConfig", Namespace: "multicluster-engine", Name: "agent"},
		Version: "v1",
		UID:     "asc-uid",
		Labels:  testInstallerLabels,
	}}}
	graph.MCH = testMCH()

	classifyTestGraph(graph)
	verdict := graph.DiscoveredResources[0].Ownership
	if !verdict.Owned || verdict.Confidence != operatorv1.ConfidenceDefinitive {
		t.Fatalf("verdict = %+v, want Definitive/Owned", verdict)
	}
}

// O-2: the object is unlabelled but its defining CRD belongs to the install.
func TestClassifyOwnershipDefiningCRD(t *testing.T) {
	graph := &DependencyGraph{
		DiscoveredResources: []DiscoveredResource{{
			Ref:     operatorv1.ResourceRef{Group: "agent-install.openshift.io", Kind: "AgentServiceConfig", Namespace: "multicluster-engine", Name: "agent"},
			Version: "v1",
			UID:     "asc-uid",
		}},
		CRDRefs: []inventory.CRDRef{{
			Group:  "agent-install.openshift.io",
			Kind:   "AgentServiceConfig",
			Labels: testInstallerLabels,
		}},
	}
	graph.MCH = testMCH()

	classifyTestGraph(graph)
	verdict := graph.DiscoveredResources[0].Ownership
	if !verdict.Owned || verdict.Confidence != operatorv1.ConfidenceStrong {
		t.Fatalf("verdict = %+v, want Strong/Owned", verdict)
	}
}

// O-3: a name that looks like ACM is weak evidence and never authorises action.
func TestClassifyOwnershipNameMatchIsWeak(t *testing.T) {
	graph := &DependencyGraph{DiscoveredResources: []DiscoveredResource{{
		Ref:     operatorv1.ResourceRef{Group: "cluster.open-cluster-management.io", Kind: "ManagedCluster", Name: "open-cluster-management-spoke"},
		Version: "v1",
		UID:     "mc-uid",
	}}}
	graph.MCH = testMCH()

	classifyTestGraph(graph)
	verdict := graph.DiscoveredResources[0].Ownership
	if verdict.Owned || verdict.Confidence != operatorv1.ConfidenceWeak {
		t.Fatalf("verdict = %+v, want Weak/not owned", verdict)
	}
}

// O-4: no evidence at all.
func TestClassifyOwnershipNoEvidence(t *testing.T) {
	graph := &DependencyGraph{DiscoveredResources: []DiscoveredResource{{
		Ref:     operatorv1.ResourceRef{Group: "cluster.open-cluster-management.io", Kind: "ManagedCluster", Name: "spoke"},
		Version: "v1",
		UID:     "mc-uid",
	}}}
	graph.MCH = testMCH()

	classifyTestGraph(graph)
	verdict := graph.DiscoveredResources[0].Ownership
	if verdict.Owned || verdict.Confidence != operatorv1.ConfidenceNone {
		t.Fatalf("verdict = %+v, want None", verdict)
	}
}

// O-5: the owner chain reaches an installer-labelled object.
func TestClassifyOwnershipOwnerChain(t *testing.T) {
	controller := true
	graph := &DependencyGraph{DiscoveredResources: []DiscoveredResource{
		{
			Ref: operatorv1.ResourceRef{Group: "operator.open-cluster-management.io", Kind: "MultiClusterHub", Namespace: "open-cluster-management", Name: "multiclusterhub"},
			UID: "mch-uid", Labels: testInstallerLabels,
		},
		{
			Ref: operatorv1.ResourceRef{Group: "multicluster.openshift.io", Kind: "MultiClusterEngine", Namespace: "multicluster-engine", Name: "multiclusterengine"},
			UID: "mce-uid",
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "operator.open-cluster-management.io/v1",
				Kind:       "MultiClusterHub",
				Name:       "multiclusterhub",
				UID:        "mch-uid",
				Controller: &controller,
			}},
		},
	}}
	graph.MCH = &graph.DiscoveredResources[0]

	classifyTestGraph(graph)
	verdict := graph.DiscoveredResources[1].Ownership
	if !verdict.Owned || verdict.Confidence != operatorv1.ConfidenceDefinitive {
		t.Fatalf("verdict = %+v, want Definitive/Owned via owner chain", verdict)
	}
}

// O-6: with no MCH object the installation identity cannot be matched by name;
// the result degrades, and it never becomes Definitive from the labels alone.
func TestClassifyOwnershipWithoutMCH(t *testing.T) {
	graph := &DependencyGraph{DiscoveredResources: []DiscoveredResource{{
		Ref:     operatorv1.ResourceRef{Group: "agent-install.openshift.io", Kind: "AgentServiceConfig", Namespace: "multicluster-engine", Name: "agent"},
		Version: "v1",
		UID:     "asc-uid",
		Labels:  testInstallerLabels,
	}}}

	classifyTestGraph(graph)
	verdict := graph.DiscoveredResources[0].Ownership
	if verdict.Owned || verdict.Confidence == operatorv1.ConfidenceDefinitive {
		t.Fatalf("verdict = %+v, want degraded and not owned", verdict)
	}
}

// The scan must capture the ownership inputs; classification cannot re-read.
func TestNewDiscoveredResourceCapturesOwnershipInputs(t *testing.T) {
	controller := true
	item := &unstructured.Unstructured{Object: map[string]interface{}{}}
	item.SetAPIVersion("agent-install.openshift.io/v1")
	item.SetKind("AgentServiceConfig")
	item.SetNamespace("multicluster-engine")
	item.SetName("agent")
	item.SetUID(types.UID("asc-uid"))
	item.SetLabels(map[string]string{"installer.namespace": "open-cluster-management"})
	item.SetFinalizers([]string{"agentserviceconfig.agent-install.openshift.io/ai-deprovision"})
	item.SetOwnerReferences([]metav1.OwnerReference{{
		APIVersion: "operator.open-cluster-management.io/v1",
		Kind:       "MultiClusterHub",
		Name:       "multiclusterhub",
		UID:        "mch-uid",
		Controller: &controller,
	}})

	crd := &apixv1.CustomResourceDefinition{
		Spec: apixv1.CustomResourceDefinitionSpec{
			Group: "agent-install.openshift.io",
			Names: apixv1.CustomResourceDefinitionNames{Kind: "AgentServiceConfig", Plural: "agentserviceconfigs"},
		},
	}

	dr := newDiscoveredResource(item, crd, "v1")
	if dr.Labels["installer.namespace"] != "open-cluster-management" {
		t.Errorf("labels not captured: %+v", dr.Labels)
	}
	if len(dr.OwnerReferences) != 1 || dr.OwnerReferences[0].UID != "mch-uid" {
		t.Errorf("owner references not captured: %+v", dr.OwnerReferences)
	}
	if dr.Ref.Name != "agent" || dr.UID != "asc-uid" || len(dr.Finalizers) != 1 {
		t.Errorf("identity not captured: %+v", dr)
	}
}

// R-2: the report no longer invents a recommendation. The action on every
// blocker is exactly what the policy returns for that blocker.
func TestBlockersUsePolicyDecision(t *testing.T) {
	graph := &DependencyGraph{DiscoveredResources: []DiscoveredResource{
		{
			Ref:        operatorv1.ResourceRef{Group: "cluster.open-cluster-management.io", Kind: "ManagedCluster", Name: "mc1"},
			Version:    "v1",
			UID:        "uid-mc",
			Finalizers: []string{testNonCloudFinalizer},
			Labels:     testInstallerLabels,
		},
		{
			Ref:        operatorv1.ResourceRef{Group: "hypershift.openshift.io", Kind: "HostedCluster", Namespace: "clusters", Name: "hcp1"},
			Version:    "v1beta1",
			UID:        "uid-hc",
			Finalizers: []string{testCloudFinalizer},
			Labels:     testInstallerLabels,
			Platform:   "AWS",
		},
	}}
	graph.MCH = testMCH()
	classifyTestGraph(graph)

	consent := policy.ConsentState{}
	blockers := blockersFromGraph(graph, testScanTime, consent)
	if len(blockers) != 2 {
		t.Fatalf("blockers = %+v, want 2", blockers)
	}
	for i := range blockers {
		want := policy.Evaluate(blockers[i], consent).Action
		if blockers[i].SuggestedAction != want {
			t.Errorf("%s action = %s, policy says %s", blockers[i].Kind, blockers[i].SuggestedAction, want)
		}
		// Liveness has no verified producer yet, so the owned blockers wait.
		if blockers[i].SuggestedAction != operatorv1.ActionWaitForController {
			t.Errorf("%s action = %s, want WaitForController while liveness is unknown", blockers[i].Kind, blockers[i].SuggestedAction)
		}
	}

	cloud := blockers[1]
	if !cloud.RequiresConsent || cloud.CloudRisk == nil {
		t.Errorf("cloud blocker lost its consent property: %+v", cloud)
	}
}

// R-1: the report says the classification ran, instead of "deferred".
func TestBuildInspectReportMarksOwnershipClassified(t *testing.T) {
	graph := &DependencyGraph{DiscoveredResources: []DiscoveredResource{{
		Ref:        operatorv1.ResourceRef{Group: "cluster.open-cluster-management.io", Kind: "ManagedCluster", Name: "mc1"},
		Version:    "v1",
		UID:        "uid-mc",
		Finalizers: []string{testNonCloudFinalizer},
	}}}

	report, meta := buildInspectReport(testTeardown(), graph, testScanTime, "")
	if report.Inventory.Ownership != reportOwnershipClassified {
		t.Errorf("inventory ownership = %q", report.Inventory.Ownership)
	}
	if meta.Ownership != reportOwnershipClassified {
		t.Errorf("meta ownership = %q", meta.Ownership)
	}
}

// O-6: metal3 is scanned but is not an ACM product surface, so group
// membership must not authorise ownership of its instances.
func TestOwnsACMGroups(t *testing.T) {
	tests := []struct {
		group string
		want  bool
	}{
		{"cluster.open-cluster-management.io", true},
		{"operator.open-cluster-management.io", true},
		{"hive.openshift.io", true},
		{"agent-install.openshift.io", true},
		{"hypershift.openshift.io", true},
		{"multicluster.openshift.io", true},
		{"metal3.io", false},
		{"apps", false},
		{"", false},
	}
	for _, tt := range tests {
		t.Run(tt.group, func(t *testing.T) {
			if got := ownsACMGroups(tt.group); got != tt.want {
				t.Errorf("ownsACMGroups(%q) = %v, want %v", tt.group, got, tt.want)
			}
		})
	}
}

// O-7: the 5.0 hub, where nothing carries installer.* labels. Every object a
// teardown must remove has to still be recognised as owned.
func TestClassifyOwnershipWithoutInstallerLabels(t *testing.T) {
	mch := DiscoveredResource{
		Ref: operatorv1.ResourceRef{
			Group:     "operator.open-cluster-management.io",
			Kind:      "MultiClusterHub",
			Namespace: "open-cluster-management",
			Name:      "multiclusterhub",
		},
		UID: "mch-uid",
	}
	graph := &DependencyGraph{
		DiscoveredResources: []DiscoveredResource{
			mch,
			{
				Ref:     operatorv1.ResourceRef{Group: "multicluster.openshift.io", Kind: "MultiClusterEngine", Namespace: "multicluster-engine", Name: "multiclusterengine"},
				UID:     "mce-uid",
				Version: "v1",
				Labels: map[string]string{
					"multiclusterhubs.operator.open-cluster-management.io/managed-by": "true",
				},
			},
			{
				Ref:     operatorv1.ResourceRef{Group: "operator.open-cluster-management.io", Kind: "ClusterManager", Name: "cluster-manager"},
				UID:     "cm-uid",
				Version: "v1",
				Labels:  map[string]string{"backplaneconfig.name": "multiclusterengine"},
			},
			{
				Ref:     operatorv1.ResourceRef{Group: "cluster.open-cluster-management.io", Kind: "ManagedCluster", Name: "unreachable-1"},
				UID:     "mc-uid",
				Version: "v1",
			},
			{
				Ref:     operatorv1.ResourceRef{Group: "agent-install.openshift.io", Kind: "AgentServiceConfig", Name: "agent"},
				UID:     "asc-uid",
				Version: "v1",
			},
		},
		CRDRefs: []inventory.CRDRef{
			{Group: "operator.open-cluster-management.io", Kind: "MultiClusterHub", ACMOwned: true},
			{Group: "multicluster.openshift.io", Kind: "MultiClusterEngine", ACMOwned: true},
			{Group: "operator.open-cluster-management.io", Kind: "ClusterManager", ACMOwned: true},
			{Group: "cluster.open-cluster-management.io", Kind: "ManagedCluster", ACMOwned: true},
			{Group: "agent-install.openshift.io", Kind: "AgentServiceConfig", ACMOwned: true},
		},
	}
	graph.MCH = &graph.DiscoveredResources[0]

	classifyTestGraph(graph)

	for _, dr := range graph.DiscoveredResources {
		if !dr.Ownership.Owned {
			t.Fatalf("%s/%s is not owned on a 5.0 hub: %+v", dr.Ref.Kind, dr.Ref.Name, dr.Ownership)
		}
	}
	if got := graph.DiscoveredResources[0].Ownership.Confidence; got != operatorv1.ConfidenceDefinitive {
		t.Fatalf("MCH confidence = %s, want Definitive", got)
	}
}
