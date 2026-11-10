// Copyright Contributors to the Open Cluster Management project

package controllers

import (
	"context"
	"testing"

	"github.com/go-logr/logr"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	fakediscovery "k8s.io/client-go/discovery/fake"
	metadatafake "k8s.io/client-go/metadata/fake"
	clienttesting "k8s.io/client-go/testing"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	operatorv1 "github.com/stolostron/multiclusterhub-operator/api/v1"
	"github.com/stolostron/multiclusterhub-operator/pkg/ownership"
	"github.com/stolostron/multiclusterhub-operator/pkg/policy"
)

type preferredResourcesDiscovery struct {
	*fakediscovery.FakeDiscovery
	resources []*metav1.APIResourceList
}

func (d *preferredResourcesDiscovery) ServerPreferredResources() ([]*metav1.APIResourceList, error) {
	return d.resources, nil
}

func TestClassifyFinalizerResourcesUsesEvidenceNotKind(t *testing.T) {
	graph := &DependencyGraph{DiscoveredResources: []DiscoveredResource{
		testDiscovered("agent-install.openshift.io", "v1beta1", "AgentClusterInstall", "clusters", "aci1", "uid-aci",
			"agentclusterinstall.agent-install.openshift.io/ai-deprovision"),
		testDiscovered("hypershift.openshift.io", "v1beta1", "HostedCluster", "clusters", "kv", "uid-kv",
			"hypershift.openshift.io/finalizer"),
		testDiscovered("hive.openshift.io", "v1", "ClusterPool", "clusters", "pool-without-finalizer", "uid-pool"),
		testDiscovered("cluster.open-cluster-management.io", "v1", "ManagedCluster", "", "mc1", "uid-mc",
			testNonCloudFinalizer),
		testDiscovered("example.com", "v1", "Widget", "default", "unknown", "uid-widget", "example.com/finalizer"),
	}}
	graph.DiscoveredResources[0].Platform = "AWS"
	graph.DiscoveredResources[1].Platform = "KUBEVIRT"

	classifyFinalizerResources(graph)
	classifyFinalizerResources(graph)

	if len(graph.InfrastructureResources) != 2 {
		t.Fatalf("infrastructure resources = %d, want 2: %+v", len(graph.InfrastructureResources), graph.InfrastructureResources)
	}
	if got := graph.InfrastructureResources[0]; got.Ref.Kind != "AgentClusterInstall" || got.Ref.Name != "aci1" {
		t.Errorf("first infrastructure resource = %s/%s, want AgentClusterInstall/aci1", got.Ref.Kind, got.Ref.Name)
	}
	if got := graph.InfrastructureResources[1]; got.Ref.Kind != "HostedCluster" || got.Ref.Name != "kv" {
		t.Errorf("second infrastructure resource = %s/%s, want HostedCluster/kv", got.Ref.Kind, got.Ref.Name)
	}
	if len(graph.CloudRiskResources) != 1 || graph.CloudRiskResources[0].Ref.Name != "aci1" {
		t.Fatalf("cloud-risk resources = %+v, want only aci1", graph.CloudRiskResources)
	}
	if len(graph.NonCloudFinalizerResources) != 1 || graph.NonCloudFinalizerResources[0].Ref.Name != "mc1" {
		t.Fatalf("non-cloud resources = %+v, want only mc1", graph.NonCloudFinalizerResources)
	}
}

func TestBuildDependencyGraphProductionWalkPopulatesCloudRisk(t *testing.T) {
	const (
		group      = "hypershift.openshift.io"
		version    = "v1beta1"
		kind       = "HostedCluster"
		resource   = "hostedclusters"
		namespace  = "clusters"
		name       = "aws-hcp"
		finalizer  = "hypershift.openshift.io/finalizer"
		observedID = types.UID("aws-hcp-observed-uid")
	)
	gvk := schema.GroupVersionKind{Group: group, Version: version, Kind: kind}

	metadataScheme := runtime.NewScheme()
	metav1.AddMetaToScheme(metadataScheme)
	metadataClient := metadatafake.NewSimpleMetadataClient(metadataScheme, &metav1.PartialObjectMetadata{
		TypeMeta: metav1.TypeMeta{APIVersion: group + "/" + version, Kind: kind},
		ObjectMeta: metav1.ObjectMeta{
			Namespace:  namespace,
			Name:       name,
			UID:        observedID,
			Finalizers: []string{finalizer},
		},
	})
	discoveryClient := &preferredResourcesDiscovery{
		FakeDiscovery: &fakediscovery.FakeDiscovery{Fake: &clienttesting.Fake{}},
		resources: []*metav1.APIResourceList{{
			GroupVersion: group + "/" + version,
			APIResources: []metav1.APIResource{{
				Name:       resource,
				Kind:       kind,
				Namespaced: true,
				Verbs:      metav1.Verbs{"get", "list"},
			}},
		}},
	}
	scheme := newTestScheme()
	registerUnstructured(t, scheme, gvk)
	fullObject := withPlatform(newUnstructured(gvk, namespace, name, finalizer), "AWS")
	fullObject.SetUID(observedID)
	baseClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(
		newTestCRD(group, version, kind, resource),
		fullObject,
	).Build()
	reconciler := &HubTeardownReconciler{
		Client:          baseClient,
		DiscoveryClient: discoveryClient,
		MetadataClient:  metadataClient,
	}

	graph, err := reconciler.buildDependencyGraph(context.Background(), testTeardown(), logr.Discard())
	if err != nil {
		t.Fatalf("buildDependencyGraph error: %v", err)
	}
	if graph.ObjectsSeen != 1 {
		t.Fatalf("objects seen = %d, want 1 (the production metadata walk must be used)", graph.ObjectsSeen)
	}
	if len(graph.InfrastructureResources) != 1 || graph.InfrastructureResources[0].Ref.Kind != kind {
		t.Fatalf("infrastructure resources = %+v, want %s", graph.InfrastructureResources, kind)
	}
	if len(graph.CloudRiskResources) != 1 || graph.CloudRiskResources[0].UID != observedID {
		t.Fatalf("cloud-risk resources = %+v, want the walked %s with UID %s", graph.CloudRiskResources, kind, observedID)
	}
	if graph.CloudRiskResources[0].Platform != "AWS" {
		t.Fatalf("cloud-risk platform = %q, want AWS", graph.CloudRiskResources[0].Platform)
	}
}

func TestBuildDryRunReportIncludesDiscoveredTier1Kind(t *testing.T) {
	discovered := testDiscovered("agent-install.openshift.io", "v1beta1", "AgentClusterInstall", "clusters", "aci1", "uid-aci",
		"agentclusterinstall.agent-install.openshift.io/ai-deprovision")
	discovered.Platform = "AWS"
	graph := &DependencyGraph{DiscoveredResources: []DiscoveredResource{discovered}}
	classifyFinalizerResources(graph)

	report := (&HubTeardownReconciler{}).buildDryRunReport(graph)
	if report == nil || len(report.PlannedPhases) <= 2 {
		t.Fatalf("dry-run report has no infrastructure phase: %+v", report)
	}
	phase := report.PlannedPhases[2]
	if phase.ResourcesAffected != 1 || phase.CloudRiskResources != 1 {
		t.Fatalf("infrastructure phase counts = %d/%d, want 1/1", phase.ResourcesAffected, phase.CloudRiskResources)
	}
	if len(phase.Details) != 1 || phase.Details[0] != "AgentClusterInstall/clusters/aci1 (platform: AWS)" {
		t.Fatalf("infrastructure phase details = %v, want the discovered AgentClusterInstall", phase.Details)
	}
}

// Cloud risk is evidence, not a property of the finalizer alone. The same
// Tier-1 finalizer is a cloud-orphan risk on AWS and not on KubeVirt.
func TestCloudRiskDerivedFromPlatform(t *testing.T) {
	notAlive := false
	tests := []struct {
		name           string
		group          string
		kind           string
		finalizer      string
		platform       string
		wantMayOrphan  bool
		wantKnown      bool
		wantConsent    bool
		wantAction     operatorv1.SuggestedAction
		wantCloudClass bool
	}{
		{
			name: "22-P1 KubeVirt HostedCluster", group: "hypershift.openshift.io", kind: "HostedCluster",
			finalizer: "hypershift.openshift.io/finalizer", platform: "KUBEVIRT",
			wantMayOrphan: false, wantKnown: true, wantConsent: false,
			wantAction: operatorv1.ActionResolveFinalizer, wantCloudClass: false,
		},
		{
			name: "22-P2 AWS ClusterDeployment", group: "hive.openshift.io", kind: "ClusterDeployment",
			finalizer: "hive.openshift.io/deprovision", platform: "AWS",
			wantMayOrphan: true, wantKnown: true, wantConsent: true,
			wantAction: operatorv1.ActionResolveFinalizerWithConsent, wantCloudClass: true,
		},
		{
			name: "22-N1 empty platform AgentServiceConfig", group: "agent-install.openshift.io", kind: "AgentServiceConfig",
			finalizer: "agentserviceconfig.agent-install.openshift.io/ai-deprovision", platform: "",
			wantMayOrphan: true, wantKnown: false, wantConsent: true,
			wantAction: operatorv1.ActionResolveFinalizerWithConsent, wantCloudClass: true,
		},
		{
			name: "22-N2 KubeVirt NodePool", group: "hypershift.openshift.io", kind: "NodePool",
			finalizer: "hypershift.openshift.io/finalizer", platform: "KUBEVIRT",
			wantMayOrphan: false, wantKnown: true, wantConsent: false,
			wantAction: operatorv1.ActionResolveFinalizer, wantCloudClass: false,
		},
		{
			name: "22-E1 unknown platform", group: "hypershift.openshift.io", kind: "HostedCluster",
			finalizer: "hypershift.openshift.io/finalizer", platform: "FOO",
			wantMayOrphan: true, wantKnown: false, wantConsent: true,
			wantAction: operatorv1.ActionResolveFinalizerWithConsent, wantCloudClass: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dr := testDiscovered(tt.group, "v1", tt.kind, "ns", "obj", "uid-1", tt.finalizer)
			dr.Platform = tt.platform
			dr.ControllerAlive = &notAlive
			dr.Ownership = ownership.OwnershipVerdict{Owned: true, Confidence: operatorv1.ConfidenceStrong}
			graph := &DependencyGraph{DiscoveredResources: []DiscoveredResource{dr}, ScanTime: testScanTime}

			blockers := blockersFromGraph(graph, testScanTime, policy.ConsentState{})
			if len(blockers) != 1 {
				t.Fatalf("blockers = %d, want 1", len(blockers))
			}
			blocker := blockers[0]
			if blocker.CloudRisk == nil {
				t.Fatal("Tier-1 finalizer must carry cloud risk evidence even when MayOrphan is false")
			}
			if blocker.CloudRisk.MayOrphan != tt.wantMayOrphan {
				t.Errorf("mayOrphan = %t, want %t", blocker.CloudRisk.MayOrphan, tt.wantMayOrphan)
			}
			if blocker.CloudRisk.PlatformKnown != tt.wantKnown {
				t.Errorf("platformKnown = %t, want %t", blocker.CloudRisk.PlatformKnown, tt.wantKnown)
			}
			if blocker.RequiresConsent != tt.wantConsent {
				t.Errorf("requiresConsent = %t, want %t", blocker.RequiresConsent, tt.wantConsent)
			}
			if got := blocker.Classification == operatorv1.ClassCloudProtecting; got != tt.wantCloudClass {
				t.Errorf("cloudProtecting = %t, want %t (classification %s)", got, tt.wantCloudClass, blocker.Classification)
			}
			if blocker.SuggestedAction != tt.wantAction {
				t.Errorf("action = %s, want %s", blocker.SuggestedAction, tt.wantAction)
			}
		})
	}
}

// Cloud warnings follow the same predicate: a KubeVirt HCP must not appear in
// the "may orphan cloud resources" list that drives acknowledgeCloudResourceRisk.
func TestCloudWarningsExcludeKubeVirt(t *testing.T) {
	kubevirt := testDiscovered("hypershift.openshift.io", "v1beta1", "HostedCluster", "clusters", "kv", "uid-kv", "hypershift.openshift.io/finalizer")
	kubevirt.Platform = "KUBEVIRT"
	aws := testDiscovered("hive.openshift.io", "v1", "ClusterDeployment", "clusters", "aws", "uid-aws", "hive.openshift.io/deprovision")
	aws.Platform = "AWS"
	graph := &DependencyGraph{
		CloudRiskResources: []DiscoveredResource{kubevirt, aws},
		ScanTime:           testScanTime,
	}

	warnings := (&HubTeardownReconciler{}).buildCloudWarnings(context.Background(), logr.Discard(), graph)
	if len(warnings) != 1 {
		t.Fatalf("warnings = %d, want 1 (AWS only): %+v", len(warnings), warnings)
	}
	if warnings[0].Platform != "AWS" {
		t.Fatalf("warning platform = %s, want AWS", warnings[0].Platform)
	}
}
