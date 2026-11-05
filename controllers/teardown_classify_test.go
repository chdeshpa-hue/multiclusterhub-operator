// Copyright Contributors to the Open Cluster Management project

package controllers

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/go-logr/logr"
	operatorv1 "github.com/stolostron/multiclusterhub-operator/api/v1"
	"github.com/stolostron/multiclusterhub-operator/pkg/ownership"
	"github.com/stolostron/multiclusterhub-operator/pkg/policy"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func classifierReconciler(objects ...client.Object) *HubTeardownReconciler {
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = operatorv1.AddToScheme(scheme)
	return &HubTeardownReconciler{
		Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).Build(),
	}
}

func ownedResource(dr DiscoveredResource, confidence operatorv1.Confidence) DiscoveredResource {
	dr.Ownership = ownership.OwnershipVerdict{Owned: true, Confidence: confidence}
	return dr
}

func classifyGraph(t *testing.T, reconciler *HubTeardownReconciler, graph *DependencyGraph, baseline map[string]types.UID) *DiscoveredResource {
	t.Helper()
	reconciler.classifyBlockerBehaviour(context.Background(), graph, baseline)
	return &graph.DiscoveredResources[0]
}

// 21-P1: an addon label reporting unreachable is enough to classify the spoke,
// and policy must act on it instead of waiting for a controller that cannot
// reach the spoke.
func TestClassifyUnreachableSpoke(t *testing.T) {
	mc := ownedResource(testDiscovered("cluster.open-cluster-management.io", "v1", "ManagedCluster", "", "mc1", "uid-mc", "cluster.open-cluster-management.io/api-resource-cleanup"), operatorv1.ConfidenceStrong)
	mc.Labels = map[string]string{"feature.open-cluster-management.io/addon-submariner": "unreachable"}
	graph := &DependencyGraph{DiscoveredResources: []DiscoveredResource{mc}, ScanTime: testScanTime}

	dr := classifyGraph(t, classifierReconciler(), graph, nil)
	if dr.Classification != operatorv1.ClassUnreachableSpoke {
		t.Fatalf("classification = %s, want UnreachableSpoke", dr.Classification)
	}

	blockers := blockersFromGraph(graph, testScanTime, policy.ConsentState{})
	if len(blockers) != 1 {
		t.Fatalf("blockers = %d, want 1", len(blockers))
	}
	if blockers[0].SuggestedAction != operatorv1.ActionResolveFinalizer {
		t.Fatalf("action = %s, want ResolveFinalizer", blockers[0].SuggestedAction)
	}
}

// 21-N1: a healthy spoke is never classified unreachable.
func TestClassifyHealthySpoke(t *testing.T) {
	mc := ownedResource(testDiscovered("cluster.open-cluster-management.io", "v1", "ManagedCluster", "", "mc1", "uid-mc", "cluster.open-cluster-management.io/api-resource-cleanup"), operatorv1.ConfidenceStrong)
	mc.Labels = map[string]string{"feature.open-cluster-management.io/addon-submariner": "available"}
	mc.AvailableStatus = "True"
	graph := &DependencyGraph{DiscoveredResources: []DiscoveredResource{mc}, ScanTime: testScanTime}

	dr := classifyGraph(t, classifierReconciler(), graph, nil)
	if dr.Classification == operatorv1.ClassUnreachableSpoke {
		t.Fatalf("healthy spoke classified UnreachableSpoke")
	}
}

// 21-E2: local-cluster is the hub itself and can never be an unreachable spoke.
func TestClassifyLocalClusterNeverUnreachable(t *testing.T) {
	mc := ownedResource(testDiscovered("cluster.open-cluster-management.io", "v1", "ManagedCluster", "", "local-cluster", "uid-local", "cluster.open-cluster-management.io/api-resource-cleanup"), operatorv1.ConfidenceStrong)
	mc.Labels = map[string]string{"feature.open-cluster-management.io/addon-submariner": "unreachable"}
	graph := &DependencyGraph{DiscoveredResources: []DiscoveredResource{mc}, ScanTime: testScanTime}

	dr := classifyGraph(t, classifierReconciler(), graph, nil)
	if dr.Classification == operatorv1.ClassUnreachableSpoke {
		t.Fatalf("local-cluster classified UnreachableSpoke")
	}
}

// 21-E1: an available condition that has not aged past the grace window is a
// registering spoke, not an unreachable one.
func TestClassifyUnknownWithinGrace(t *testing.T) {
	mc := ownedResource(testDiscovered("cluster.open-cluster-management.io", "v1", "ManagedCluster", "", "mc1", "uid-mc", "cluster.open-cluster-management.io/api-resource-cleanup"), operatorv1.ConfidenceStrong)
	mc.AvailableStatus = "Unknown"
	justNow := metav1.NewTime(testScanTime.Add(-1 * 60 * 1e9))
	mc.AvailableSince = &justNow
	graph := &DependencyGraph{DiscoveredResources: []DiscoveredResource{mc}, ScanTime: testScanTime}

	dr := classifyGraph(t, classifierReconciler(), graph, nil)
	if dr.Classification == operatorv1.ClassUnreachableSpoke {
		t.Fatalf("spoke within the registration grace window classified UnreachableSpoke")
	}
}

// 21-P2: a finalizer whose operand Deployment is gone is a dead controller,
// which is actionable. This is F-73's submariner case.
func TestClassifyDeadController(t *testing.T) {
	drIn := ownedResource(testDiscovered("cluster.open-cluster-management.io", "v1", "ManagedClusterAddOn", "mc1", "submariner", "uid-addon", "cluster.open-cluster-management.io/submariner-cleanup"), operatorv1.ConfidenceStrong)
	graph := &DependencyGraph{DiscoveredResources: []DiscoveredResource{drIn}, ScanTime: testScanTime}

	dr := classifyGraph(t, classifierReconciler(), graph, nil)
	if dr.Classification != operatorv1.ClassDeadController {
		t.Fatalf("classification = %s, want DeadController", dr.Classification)
	}
	if dr.ControllerAlive == nil || *dr.ControllerAlive {
		t.Fatalf("controllerAlive = %v, want false", dr.ControllerAlive)
	}
	if dr.OwningController == nil || dr.OwningController.Name != "submariner-addon" || dr.OwningController.Resolution != operatorv1.ResolvedByFinalizerDomain {
		t.Fatalf("owningController = %+v", dr.OwningController)
	}

	blockers := blockersFromGraph(graph, testScanTime, policy.ConsentState{})
	if blockers[0].SuggestedAction != operatorv1.ActionResolveFinalizer {
		t.Fatalf("action = %s, want ResolveFinalizer", blockers[0].SuggestedAction)
	}
}

// 21-N2: a live controller keeps the blocker fail-closed.
func TestClassifyLiveController(t *testing.T) {
	deployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: "submariner-addon", Namespace: "open-cluster-management"},
		Status:     appsv1.DeploymentStatus{ReadyReplicas: 1},
	}
	drIn := ownedResource(testDiscovered("cluster.open-cluster-management.io", "v1", "ManagedClusterAddOn", "mc1", "submariner", "uid-addon", "cluster.open-cluster-management.io/submariner-cleanup"), operatorv1.ConfidenceStrong)
	graph := &DependencyGraph{DiscoveredResources: []DiscoveredResource{drIn}, ScanTime: testScanTime}

	dr := classifyGraph(t, classifierReconciler(deployment), graph, nil)
	if dr.ControllerAlive == nil || !*dr.ControllerAlive {
		t.Fatalf("controllerAlive = %v, want true", dr.ControllerAlive)
	}
	if dr.Classification == operatorv1.ClassDeadController {
		t.Fatalf("live controller classified DeadController")
	}

	blockers := blockersFromGraph(graph, testScanTime, policy.ConsentState{})
	if blockers[0].SuggestedAction != operatorv1.ActionWaitForController {
		t.Fatalf("action = %s, want WaitForController", blockers[0].SuggestedAction)
	}
}

// 21-P3: the same identity with a different UID means the object was deleted
// and recreated between scans.
func TestClassifyRecreateLoop(t *testing.T) {
	drIn := ownedResource(testDiscovered("work.open-cluster-management.io", "v1", "ManifestWork", "mc1", "addon", "new-uid", "work.open-cluster-management.io/manifest-work-cleanup"), operatorv1.ConfidenceStrong)
	graph := &DependencyGraph{DiscoveredResources: []DiscoveredResource{drIn}, ScanTime: testScanTime}
	baseline := map[string]types.UID{
		blockerKey("work.open-cluster-management.io/v1", "ManifestWork", "mc1", "addon"): "old-uid",
	}

	dr := classifyGraph(t, classifierReconciler(), graph, baseline)
	if dr.Classification != operatorv1.ClassRecreateLoop {
		t.Fatalf("classification = %s, want RecreateLoop", dr.Classification)
	}

	blockers := blockersFromGraph(graph, testScanTime, policy.ConsentState{})
	if blockers[0].SuggestedAction != operatorv1.ActionFixOperand {
		t.Fatalf("action = %s, want FixOperand", blockers[0].SuggestedAction)
	}
}

// 21-N3: a stable UID is not a recreate loop.
func TestClassifySameUIDIsNotRecreateLoop(t *testing.T) {
	drIn := ownedResource(testDiscovered("work.open-cluster-management.io", "v1", "ManifestWork", "mc1", "addon", "same-uid", "work.open-cluster-management.io/manifest-work-cleanup"), operatorv1.ConfidenceStrong)
	graph := &DependencyGraph{DiscoveredResources: []DiscoveredResource{drIn}, ScanTime: testScanTime}
	baseline := map[string]types.UID{
		blockerKey("work.open-cluster-management.io/v1", "ManifestWork", "mc1", "addon"): "same-uid",
	}

	dr := classifyGraph(t, classifierReconciler(), graph, baseline)
	if dr.Classification == operatorv1.ClassRecreateLoop {
		t.Fatalf("stable object classified RecreateLoop")
	}
}

// 21-E1 (MCH missing): the classifier never guesses a controller and never
// panics when the hub identity is absent.
func TestClassifyWithoutMCH(t *testing.T) {
	drIn := testDiscovered("example.com", "v1", "Widget", "ns", "w1", "uid-w", "example.com/finalizer")
	graph := &DependencyGraph{DiscoveredResources: []DiscoveredResource{drIn}, ScanTime: testScanTime}

	dr := classifyGraph(t, classifierReconciler(), graph, nil)
	if dr.OwningController != nil {
		t.Fatalf("owningController = %+v, want nil for an unknown finalizer", dr.OwningController)
	}
	if dr.ControllerAlive != nil {
		t.Fatalf("controllerAlive = %v, want nil", dr.ControllerAlive)
	}
	if dr.Classification != operatorv1.ClassUnknown {
		t.Fatalf("classification = %s, want Unknown", dr.Classification)
	}
}

// External reconciler: a controller owner the scan did not recognise as
// ACM-owned is someone else's controller.
func TestClassifyExternalReconciler(t *testing.T) {
	parent := testDiscovered("apps", "v1", "Deployment", "default", "gitops", "uid-parent")
	child := ownedResource(testDiscovered("cluster.open-cluster-management.io", "v1", "ManagedCluster", "", "mc1", "uid-mc", "cluster.open-cluster-management.io/api-resource-cleanup"), operatorv1.ConfidenceStrong)
	controller := true
	child.OwnerReferences = []metav1.OwnerReference{{
		APIVersion: "apps/v1",
		Kind:       "Deployment",
		Name:       "gitops",
		UID:        "uid-parent",
		Controller: &controller,
	}}
	graph := &DependencyGraph{DiscoveredResources: []DiscoveredResource{parent, child}, ScanTime: testScanTime}

	reconciler := classifierReconciler()
	reconciler.classifyBlockerBehaviour(context.Background(), graph, nil)
	if graph.DiscoveredResources[1].Classification != operatorv1.ClassExternalReconciler {
		t.Fatalf("classification = %s, want ExternalReconciler", graph.DiscoveredResources[1].Classification)
	}
}

// The recreate-loop rule needs the previous report's UIDs. Reading it is part
// of the production path, so it is tested through a real ConfigMap.
func TestPreviousBlockerUIDs(t *testing.T) {
	body, err := json.Marshal(inspectReport{Blockers: []operatorv1.Blocker{{
		APIVersion: "work.open-cluster-management.io/v1",
		Kind:       "ManifestWork",
		Namespace:  "mc1",
		Name:       "addon",
		UID:        "old-uid",
	}}})
	if err != nil {
		t.Fatal(err)
	}
	report := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "report", Namespace: "open-cluster-management"},
		Data:       map[string]string{reportKeyJSON: string(body)},
	}
	td := &operatorv1.HubTeardown{
		ObjectMeta: metav1.ObjectMeta{Name: "teardown", Namespace: "open-cluster-management"},
		Status:     operatorv1.HubTeardownStatus{ReportRef: &corev1.LocalObjectReference{Name: "report"}},
	}

	baseline := classifierReconciler(report).previousBlockerUIDs(context.Background(), td, logr.Discard())
	got := baseline[blockerKey("work.open-cluster-management.io/v1", "ManifestWork", "mc1", "addon")]
	if got != "old-uid" {
		t.Fatalf("baseline UID = %q, want old-uid", got)
	}
}

// The MCE operator shipped as backplane-operator before 5.0. A rename must not
// read as "the controller is gone", which would authorise finalizer removal
// while the operand is alive.
func TestProbeControllerWorkloadResolvesMCEAlias(t *testing.T) {
	deployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: "multicluster-engine-operator", Namespace: "multicluster-engine"},
		Status:     appsv1.DeploymentStatus{ReadyReplicas: 2},
	}
	reconciler := classifierReconciler(deployment)
	workload := finalizerController("finalizer.multicluster.openshift.io")
	if workload == nil {
		t.Fatalf("finalizer must resolve to a controller")
	}

	alive, evidence := reconciler.probeControllerWorkload(context.Background(), workload, testScanTime)
	if alive == nil || !*alive {
		t.Fatalf("controllerAlive = %v, want true", alive)
	}
	if want := "Deployment multicluster-engine/multicluster-engine-operator readyReplicas>0"; evidence.Detail != want {
		t.Fatalf("evidence = %q, want %q", evidence.Detail, want)
	}
}

func TestProbeControllerWorkloadDeadWhenEveryNameIsAbsent(t *testing.T) {
	reconciler := classifierReconciler()
	workload := finalizerController("finalizer.multicluster.openshift.io")
	if workload == nil {
		t.Fatalf("finalizer must resolve to a controller")
	}

	alive, _ := reconciler.probeControllerWorkload(context.Background(), workload, testScanTime)
	if alive == nil || *alive {
		t.Fatalf("controllerAlive = %v, want false when every candidate name is absent", alive)
	}
}
