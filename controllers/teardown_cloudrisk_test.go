// Copyright Contributors to the Open Cluster Management project

package controllers

import (
	"context"
	"testing"

	"github.com/go-logr/logr"

	operatorv1 "github.com/stolostron/multiclusterhub-operator/api/v1"
	"github.com/stolostron/multiclusterhub-operator/pkg/ownership"
	"github.com/stolostron/multiclusterhub-operator/pkg/policy"
)

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
			wantMayOrphan: false, wantKnown: false, wantConsent: false,
			wantAction: operatorv1.ActionResolveFinalizer, wantCloudClass: false,
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
			wantMayOrphan: false, wantKnown: false, wantConsent: false,
			wantAction: operatorv1.ActionResolveFinalizer, wantCloudClass: false,
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
