// Copyright Contributors to the Open Cluster Management project

package controllers

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	operatorv1 "github.com/stolostron/multiclusterhub-operator/api/v1"
	"github.com/stolostron/multiclusterhub-operator/pkg/ownership"
	"github.com/stolostron/multiclusterhub-operator/pkg/policy"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	ctrllog "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"sigs.k8s.io/yaml"
)

const (
	testCloudFinalizer    = "hypershift.openshift.io/finalizer"
	testNonCloudFinalizer = "cluster.open-cluster-management.io/api-resource-cleanup"
)

var testScanTime = metav1.NewTime(metav1.Now().Add(-1 * 60 * 1e9))

func testDiscovered(group, version, kind, namespace, name, uid string, finalizers ...string) DiscoveredResource {
	return DiscoveredResource{
		Ref: operatorv1.ResourceRef{
			Group:     group,
			Kind:      kind,
			Namespace: namespace,
			Name:      name,
		},
		Version:    version,
		UID:        types.UID(uid),
		Finalizers: finalizers,
	}
}

func testTeardown() *operatorv1.HubTeardown {
	return &operatorv1.HubTeardown{
		ObjectMeta: metav1.ObjectMeta{Name: "teardown", Namespace: "open-cluster-management"},
	}
}

func TestBlockersFromGraph(t *testing.T) {
	graph := &DependencyGraph{
		DiscoveredResources: []DiscoveredResource{
			testDiscovered("hypershift.openshift.io", "v1beta1", "HostedCluster", "clusters-hcp1", "hcp1", "uid-hc", testCloudFinalizer),
			testDiscovered("cluster.open-cluster-management.io", "v1", "ManagedCluster", "", "mc1", "uid-mc", testNonCloudFinalizer),
			testDiscovered("v1", "", "Pod", "ns", "ignored", "uid-p", "example.com/unrelated"),
			testDiscovered("v1", "", "ConfigMap", "ns", "nofin", "uid-cm"),
		},
	}
	// A HostedCluster always carries a platform; the cloud classification
	// needs it as evidence.
	graph.DiscoveredResources[0].Platform = "AWS"

	blockers := blockersFromGraph(graph, testScanTime, policy.ConsentState{})

	if len(blockers) != 2 {
		t.Fatalf("expected 2 blockers, got %d: %+v", len(blockers), blockers)
	}
	if blockers[0].Kind != "ManagedCluster" || blockers[1].Kind != "HostedCluster" {
		t.Fatalf("expected namespace-first ordering, got %s then %s", blockers[0].Kind, blockers[1].Kind)
	}
	if blockers[1].Classification != operatorv1.ClassCloudProtecting {
		t.Errorf("HostedCluster should be ClassCloudProtecting, got %s", blockers[1].Classification)
	}
	if !blockers[1].RequiresConsent || blockers[1].CloudRisk == nil {
		t.Errorf("cloud blocker must require consent and carry cloud risk")
	}
	// Ownership is None in this graph, so the policy refuses to recommend
	// anything beyond reporting; the cloud risk is still disclosed.
	if blockers[0].SuggestedAction != operatorv1.ActionReportOnly {
		t.Errorf("non-cloud blocker action = %s", blockers[0].SuggestedAction)
	}
	if blockers[1].APIVersion != "hypershift.openshift.io/v1beta1" {
		t.Errorf("apiVersion = %q", blockers[1].APIVersion)
	}
}

func TestBlockersFromGraphIdentityIsUID(t *testing.T) {
	graph := &DependencyGraph{
		DiscoveredResources: []DiscoveredResource{
			testDiscovered("g", "v1", "Kind", "ns-a", "same-name", "uid-1", testNonCloudFinalizer),
			testDiscovered("g", "v1", "Kind", "ns-b", "same-name", "uid-2", testNonCloudFinalizer),
			testDiscovered("g", "v1", "Kind", "ns-a", "same-name", "uid-1", testNonCloudFinalizer),
		},
	}

	blockers := blockersFromGraph(graph, testScanTime, policy.ConsentState{})

	if len(blockers) != 2 {
		t.Fatalf("expected 2 blockers after UID dedupe, got %d", len(blockers))
	}
	if blockers[0].UID == blockers[1].UID {
		t.Fatalf("blockers must not share a UID")
	}
}

func TestCapBlockers(t *testing.T) {
	blockers := make([]operatorv1.Blocker, 40)
	for i := range blockers {
		blockers[i] = operatorv1.Blocker{UID: types.UID(fmt.Sprintf("uid-%d", i))}
	}

	capped, truncated := capBlockers(blockers, reportStatusBlockerCap)
	if len(capped) != 32 || truncated != 8 {
		t.Fatalf("cap = %d/%d, want 32/8", len(capped), truncated)
	}

	small := blockers[:5]
	capped, truncated = capBlockers(small, reportStatusBlockerCap)
	if len(capped) != 5 || truncated != 0 {
		t.Fatalf("small cap = %d/%d, want 5/0", len(capped), truncated)
	}
}

func TestBuildInspectReportDigestIgnoresScanTime(t *testing.T) {
	graph := &DependencyGraph{
		DiscoveredResources: []DiscoveredResource{
			testDiscovered("g", "v1", "Kind", "ns", "name", "uid-1", testNonCloudFinalizer),
		},
	}
	td := testTeardown()

	first, firstMeta := buildInspectReport(td, graph, metav1.Now(), "")
	_, secondMeta := buildInspectReport(td, graph, metav1.NewTime(metav1.Now().Add(60*time.Second)), "")

	if firstMeta.Digest != secondMeta.Digest {
		t.Fatalf("digest must ignore scanTime: %s != %s", firstMeta.Digest, secondMeta.Digest)
	}

	graph.DiscoveredResources[0].Finalizers = append(graph.DiscoveredResources[0].Finalizers, "extra.example.com/f")
	third, thirdMeta := buildInspectReport(td, graph, metav1.Now(), "")
	if thirdMeta.Digest == firstMeta.Digest {
		t.Fatalf("digest must change when the blocker set changes")
	}
	if first.Summary.Total != third.Summary.Total {
		t.Fatalf("restricting to allowlisted finalizers should keep the blocker count at 1, got %d", third.Summary.Total)
	}
}

// graphWithEvidence builds a one-blocker graph whose ownership verdict carries
// evidence observed at the supplied time.
func graphWithEvidence(observed metav1.Time) *DependencyGraph {
	dr := testDiscovered("g", "v1", "Kind", "ns", "name", "uid-1", testNonCloudFinalizer)
	dr.Ownership = ownership.OwnershipVerdict{
		Owned:      true,
		Confidence: ownership.ConfidenceDefinitive,
		Evidence: []ownership.Evidence{{
			Kind:     ownership.EvidenceInstallerLabel,
			Detail:   "installer.name=multiclusterhub installer.namespace=open-cluster-management",
			Source:   "metadata.labels",
			Observed: observed,
		}},
	}
	return &DependencyGraph{DiscoveredResources: []DiscoveredResource{dr}}
}

func TestBuildInspectReportDigestIgnoresEvidenceObserved(t *testing.T) {
	td := testTeardown()
	later := metav1.NewTime(testScanTime.Add(60 * time.Second))

	first, firstMeta := buildInspectReport(td, graphWithEvidence(testScanTime), testScanTime, "")
	_, secondMeta := buildInspectReport(td, graphWithEvidence(later), later, "")

	if firstMeta.Digest != secondMeta.Digest {
		t.Fatalf("digest must ignore ownership evidence timestamps: %s != %s", firstMeta.Digest, secondMeta.Digest)
	}
	if got := first.Blockers[0].Ownership.Evidence[0].Observed; !got.Equal(&testScanTime) {
		t.Fatalf("canonicalising the digest must not mutate the report: observed = %v, want %v", got, testScanTime)
	}
}

func TestBlockerEvidenceSerialisesObserved(t *testing.T) {
	report, _ := buildInspectReport(testTeardown(), graphWithEvidence(testScanTime), testScanTime, "")

	body, err := json.Marshal(report.Blockers[0].Ownership)
	if err != nil {
		t.Fatalf("marshal ownership: %v", err)
	}
	if strings.Contains(string(body), `"observed":null`) {
		t.Fatalf("evidence observed must never marshal as null (the CRD marks it required): %s", body)
	}
	if !strings.Contains(string(body), testScanTime.UTC().Format("2006-01-02T15:04:05Z")) {
		t.Fatalf("evidence observed is missing from %s", body)
	}
}

func TestApplyInspectionWritesReport(t *testing.T) {
	reconciler := newGateTestReconciler(t)
	td := testTeardown()
	graph := &DependencyGraph{
		DiscoveredResources: []DiscoveredResource{
			testDiscovered("hypershift.openshift.io", "v1beta1", "HostedCluster", "clusters-hcp1", "hcp1", "uid-hc", testCloudFinalizer),
			testDiscovered("cluster.open-cluster-management.io", "v1", "ManagedCluster", "", "mc1", "uid-mc", testNonCloudFinalizer),
			testDiscovered("cluster.open-cluster-management.io", "v1", "ManagedCluster", "", "mc2", "uid-mc2", testNonCloudFinalizer),
		},
	}

	if err := reconciler.applyInspection(context.Background(), ctrllog.Log, td, graph); err != nil {
		t.Fatalf("applyInspection: %v", err)
	}

	if td.Status.ReportRef == nil {
		t.Fatal("status.reportRef is nil")
	}
	if td.Status.LastInspection == nil {
		t.Fatal("status.lastInspection is nil")
	}
	if td.Status.Summary == nil || td.Status.Summary.Total != 3 {
		t.Fatalf("summary = %+v, want total 3", td.Status.Summary)
	}
	if len(td.Status.Blockers) != 3 {
		t.Fatalf("status blockers = %d", len(td.Status.Blockers))
	}

	configMap := &corev1.ConfigMap{}
	if err := reconciler.Client.Get(context.Background(), types.NamespacedName{
		Name: td.Status.ReportRef.Name, Namespace: td.Namespace,
	}, configMap); err != nil {
		t.Fatalf("reading report: %v", err)
	}
	if len(configMap.OwnerReferences) != 0 {
		t.Fatalf("report must have no ownerReferences, got %d", len(configMap.OwnerReferences))
	}
	if configMap.Labels[reportLabelUninstall] != "true" || configMap.Labels[reportLabelTeardown] != td.Name {
		t.Fatalf("labels = %v", configMap.Labels)
	}
	for _, key := range []string{reportKeyJSON, reportKeyText, reportKeyMeta} {
		if configMap.Data[key] == "" {
			t.Fatalf("missing %s", key)
		}
	}

	var report inspectReport
	if err := json.Unmarshal([]byte(configMap.Data[reportKeyJSON]), &report); err != nil {
		t.Fatalf("report.json: %v", err)
	}
	if len(report.Blockers) != 3 || report.Summary.Total != 3 {
		t.Fatalf("report blockers = %d, summary = %+v", len(report.Blockers), report.Summary)
	}
	if !report.Inventory.InventoryComplete {
		t.Fatalf("inventory should be complete when no scan errors were recorded")
	}
}

func TestApplyInspectionReusesUnchangedReport(t *testing.T) {
	reconciler := newGateTestReconciler(t)
	td := testTeardown()
	graph := &DependencyGraph{
		DiscoveredResources: []DiscoveredResource{
			testDiscovered("g", "v1", "Kind", "ns", "name", "uid-1", testNonCloudFinalizer),
		},
	}

	for i := 0; i < 2; i++ {
		if err := reconciler.applyInspection(context.Background(), ctrllog.Log, td, graph); err != nil {
			t.Fatalf("applyInspection %d: %v", i, err)
		}
	}

	list := &corev1.ConfigMapList{}
	// Scoped to reports: the namespace also holds the pre-teardown snapshot,
	// which is deliberately not a report.
	if err := reconciler.Client.List(context.Background(), list,
		client.InNamespace(td.Namespace), client.MatchingLabels{reportLabelTeardown: td.Name}); err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(list.Items) != 1 {
		t.Fatalf("expected 1 report after an unchanged resync, got %d", len(list.Items))
	}
}

func TestApplyInspectionCreatesNewReportOnChange(t *testing.T) {
	reconciler := newGateTestReconciler(t)
	td := testTeardown()
	graph := &DependencyGraph{
		DiscoveredResources: []DiscoveredResource{
			testDiscovered("g", "v1", "Kind", "ns", "name", "uid-1", testNonCloudFinalizer),
		},
	}
	if err := reconciler.applyInspection(context.Background(), ctrllog.Log, td, graph); err != nil {
		t.Fatalf("first applyInspection: %v", err)
	}
	first := td.Status.ReportRef.Name

	graph.DiscoveredResources = append(graph.DiscoveredResources,
		testDiscovered("g", "v1", "Kind", "ns", "second", "uid-2", testNonCloudFinalizer))
	if err := reconciler.applyInspection(context.Background(), ctrllog.Log, td, graph); err != nil {
		t.Fatalf("second applyInspection: %v", err)
	}

	if td.Status.ReportRef.Name == first {
		t.Fatalf("report name must change when content changes; consent is keyed by name")
	}
	if td.Status.Summary.Total != 2 {
		t.Fatalf("summary total = %d, want 2", td.Status.Summary.Total)
	}
}

func TestApplyInspectionIncompleteInventory(t *testing.T) {
	reconciler := newGateTestReconciler(t)
	td := testTeardown()
	graph := &DependencyGraph{
		DiscoveredResources: []DiscoveredResource{
			testDiscovered("g", "v1", "Kind", "ns", "name", "uid-1", testNonCloudFinalizer),
		},
		Incomplete: []ScanIncomplete{
			{Group: "bad.example.com", Resource: "widgets", Reason: "discovery failed"},
			{Group: "slow.example.com", Resource: "gadgets", Reason: "context deadline exceeded"},
		},
	}

	if err := reconciler.applyInspection(context.Background(), ctrllog.Log, td, graph); err != nil {
		t.Fatalf("applyInspection: %v", err)
	}
	if reason := conditionReason(td, conditionTypeInspectIncomplete); reason != "InventoryIncomplete" {
		t.Fatalf("InspectIncomplete reason = %q", reason)
	}

	configMap := &corev1.ConfigMap{}
	if err := reconciler.Client.Get(context.Background(), types.NamespacedName{
		Name: td.Status.ReportRef.Name, Namespace: td.Namespace,
	}, configMap); err != nil {
		t.Fatalf("reading report: %v", err)
	}
	var report inspectReport
	if err := json.Unmarshal([]byte(configMap.Data[reportKeyJSON]), &report); err != nil {
		t.Fatalf("report.json: %v", err)
	}
	if report.Inventory.InventoryComplete {
		t.Fatalf("inventory must be marked incomplete")
	}
	if len(report.Inventory.Incomplete) != 2 {
		t.Fatalf("incomplete entries = %d", len(report.Inventory.Incomplete))
	}
	if report.Summary.Total != 1 {
		t.Fatalf("summary must still count seen blockers, got %d", report.Summary.Total)
	}
}

func TestApplyInspectionReportWriteDenied(t *testing.T) {
	s := newTestScheme()
	registerUnstructured(t, s, hostedClusterGVK, managedClusterGVK)
	reconciler := &HubTeardownReconciler{
		Client: fake.NewClientBuilder().WithScheme(s).WithInterceptorFuncs(interceptor.Funcs{
			Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
				if _, ok := obj.(*corev1.ConfigMap); ok {
					return apierrors.NewForbidden(schema.GroupResource{Resource: "configmaps"}, obj.GetName(), fmt.Errorf("denied"))
				}
				return c.Create(ctx, obj, opts...)
			},
		}).Build(),
		Scheme:   s,
		Log:      ctrllog.Log,
		Recorder: nil,
	}
	td := testTeardown()
	graph := &DependencyGraph{}

	if err := reconciler.applyInspection(context.Background(), ctrllog.Log, td, graph); err == nil {
		t.Fatal("expected an error when the report cannot be written")
	}
	if td.Status.ReportRef != nil {
		t.Fatalf("reportRef must not be written when the report failed: %+v", td.Status.ReportRef)
	}
	if td.Status.Phase != operatorv1.TeardownPhaseInspecting {
		t.Fatalf("phase = %s, want Inspecting", td.Status.Phase)
	}
	if reason := conditionReason(td, conditionTypeReportWriteFailed); reason != "ReportWriteFailed" {
		t.Fatalf("ReportWriteFailed reason = %q", reason)
	}
}

func TestApplyInspectionMCHAbsent(t *testing.T) {
	reconciler := newGateTestReconciler(t)
	td := testTeardown()

	if err := reconciler.applyInspection(context.Background(), ctrllog.Log, td, &DependencyGraph{}); err != nil {
		t.Fatalf("applyInspection: %v", err)
	}

	configMap := &corev1.ConfigMap{}
	if err := reconciler.Client.Get(context.Background(), types.NamespacedName{
		Name: td.Status.ReportRef.Name, Namespace: td.Namespace,
	}, configMap); err != nil {
		t.Fatalf("reading report: %v", err)
	}
	var meta reportMeta
	if err := json.Unmarshal([]byte(configMap.Data[reportKeyMeta]), &meta); err != nil {
		t.Fatalf("meta.json: %v", err)
	}
	if meta.InventoryComplete != true {
		t.Fatalf("an empty hub is a complete scan, got complete=%t", meta.InventoryComplete)
	}
	var report inspectReport
	if err := json.Unmarshal([]byte(configMap.Data[reportKeyJSON]), &report); err != nil {
		t.Fatalf("report.json: %v", err)
	}
	if report.Inventory.MCHPresent {
		t.Fatalf("mchPresent must be false when the graph has no MCH")
	}
	if report.Summary.Total != 0 {
		t.Fatalf("summary total = %d", report.Summary.Total)
	}
}

func TestPruneInspectReportsKeepsNewestFive(t *testing.T) {
	reconciler := newGateTestReconciler(t)
	td := testTeardown()

	for i := 0; i < 6; i++ {
		cm := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      fmt.Sprintf("hubteardown-report-%s-2020010%dt000000z", td.Name, i+1),
				Namespace: td.Namespace,
				Labels:    map[string]string{reportLabelTeardown: td.Name},
			},
		}
		if err := reconciler.Client.Create(context.Background(), cm); err != nil {
			t.Fatalf("seed report %d: %v", i, err)
		}
	}
	other := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "hubteardown-report-other-20200101t000000z",
			Namespace: td.Namespace,
			Labels:    map[string]string{reportLabelTeardown: "other"},
		},
	}
	if err := reconciler.Client.Create(context.Background(), other); err != nil {
		t.Fatalf("seed other report: %v", err)
	}

	if err := reconciler.applyInspection(context.Background(), ctrllog.Log, td, &DependencyGraph{}); err != nil {
		t.Fatalf("applyInspection: %v", err)
	}

	list := &corev1.ConfigMapList{}
	if err := reconciler.Client.List(context.Background(), list, client.InNamespace(td.Namespace),
		client.MatchingLabels{reportLabelTeardown: td.Name}); err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(list.Items) != reportRetention {
		names := make([]string, 0, len(list.Items))
		for i := range list.Items {
			names = append(names, list.Items[i].Name)
		}
		t.Fatalf("retention kept %d reports, want %d: %v", len(list.Items), reportRetention, names)
	}
	for i := 0; i < 2; i++ {
		old := &corev1.ConfigMap{}
		name := fmt.Sprintf("hubteardown-report-%s-2020010%dt000000z", td.Name, i+1)
		if err := reconciler.Client.Get(context.Background(), types.NamespacedName{Name: name, Namespace: td.Namespace}, old); !apierrors.IsNotFound(err) {
			t.Fatalf("expected %s to be pruned, err=%v", name, err)
		}
	}
	kept := &corev1.ConfigMap{}
	if err := reconciler.Client.Get(context.Background(), types.NamespacedName{Name: other.Name, Namespace: td.Namespace}, kept); err != nil {
		t.Fatalf("another teardown's report must survive: %v", err)
	}
}

func TestBuildReportObjectsSplitsOversizedBlockers(t *testing.T) {
	reconciler := newGateTestReconciler(t)
	td := testTeardown()

	blockers := make([]operatorv1.Blocker, 3000)
	for i := range blockers {
		blockers[i] = operatorv1.Blocker{
			APIVersion:      "example.com/v1",
			Kind:            "Widget",
			Name:            fmt.Sprintf("widget-%04d", i),
			UID:             types.UID(fmt.Sprintf("uid-%04d", i)),
			Finalizers:      []string{strings.Repeat("f", 400)},
			BlockedSince:    testScanTime,
			ObservedInPhase: operatorv1.TeardownPhaseInspecting,
			Classification:  operatorv1.ClassUnknown,
			SuggestedAction: operatorv1.ActionResolveFinalizer,
		}
	}
	report := inspectReport{
		Teardown: td.Name,
		ScanTime: testScanTime,
		Summary:  summarizeBlockers(blockers),
		Blockers: blockers,
	}
	meta := reportMeta{Teardown: td.Name, ScanTime: testScanTime, Digest: reportDigest(report)}

	objects, err := reconciler.buildReportObjects(td, "hubteardown-report-teardown-20260101t000000z", report, meta)
	if err != nil {
		t.Fatalf("buildReportObjects: %v", err)
	}
	if len(objects) < 2 {
		t.Fatalf("expected a split report, got %d objects", len(objects))
	}

	var index reportMeta
	main := objects[len(objects)-1]
	if err := json.Unmarshal([]byte(main.Data[reportKeyMeta]), &index); err != nil {
		t.Fatalf("index meta: %v", err)
	}
	if len(index.Parts) != len(objects)-1 {
		t.Fatalf("meta.parts = %d, objects = %d", len(index.Parts), len(objects))
	}
	if !index.InventoryOmitted {
		t.Fatalf("meta.json must record inventoryOmitted=true when the payload is split")
	}

	total := 0
	seen := map[types.UID]bool{}
	for _, object := range objects[:len(objects)-1] {
		var part inspectReport
		if err := json.Unmarshal([]byte(object.Data[reportKeyJSON]), &part); err != nil {
			t.Fatalf("part json: %v", err)
		}
		if len(object.Data[reportKeyJSON]) > reportMaxBytes {
			t.Fatalf("part %s exceeds the payload budget: %d", object.Name, len(object.Data[reportKeyJSON]))
		}
		for _, blocker := range part.Blockers {
			if seen[blocker.UID] {
				t.Fatalf("blocker %s appears in more than one part", blocker.UID)
			}
			seen[blocker.UID] = true
			total++
		}
	}
	if total != len(blockers) {
		t.Fatalf("parts contain %d blockers, want %d (blockers must never be dropped)", total, len(blockers))
	}
}

func TestReportObjectNameLengthGuard(t *testing.T) {
	long := strings.Repeat("a", 250)
	name := reportObjectName(long, testScanTime)
	if len(name) > reportMaxNameLength {
		t.Fatalf("name length = %d, want <= %d", len(name), reportMaxNameLength)
	}
	if !strings.HasPrefix(name, "hubteardown-report-") {
		t.Fatalf("name = %q", name)
	}
}

func TestRenderInspectReportText(t *testing.T) {
	report := inspectReport{
		Teardown: "teardown",
		ScanTime: testScanTime,
		Inventory: inventorySection{
			ObjectsSeen:       2,
			DiscoveryPath:     "Legacy",
			InventoryComplete: true,
		},
		Summary: operatorv1.BlockerSummary{Total: 1, RequiringConsent: 1},
		Blockers: []operatorv1.Blocker{{
			Kind:       "HostedCluster",
			Name:       "hcp1",
			Namespace:  "clusters-hcp1",
			UID:        "uid-hc",
			Finalizers: []string{testCloudFinalizer, "second.example.com/f"},
			Ownership:  operatorv1.OwnershipVerdict{Owned: false, Confidence: operatorv1.ConfidenceNone},
			CloudRisk:  &operatorv1.CloudRisk{Platform: "AWS", MayOrphan: true},
		}},
	}
	text := renderInspectReportText(report, reportMeta{Digest: "deadbeef"})

	for _, want := range []string{"uid-hc", "HostedCluster", "clusters-hcp1", testCloudFinalizer, "second.example.com/f", "deadbeef"} {
		if !strings.Contains(text, want) {
			t.Fatalf("report.txt missing %q:\n%s", want, text)
		}
	}
}

func TestHubTeardownCRDStatusBlockerMaxItems(t *testing.T) {
	raw, err := os.ReadFile("../config/crd/bases/operator.open-cluster-management.io_hubteardowns.yaml")
	if err != nil {
		t.Fatalf("reading CRD: %v", err)
	}
	var crd map[string]interface{}
	if err := yaml.Unmarshal(raw, &crd); err != nil {
		t.Fatalf("parsing CRD: %v", err)
	}
	versions, _ := crd["spec"].(map[string]interface{})["versions"].([]interface{})
	if len(versions) == 0 {
		t.Fatal("CRD has no versions")
	}
	schemaMap := versions[0].(map[string]interface{})["schema"].(map[string]interface{})
	status := schemaMap["openAPIV3Schema"].(map[string]interface{})["properties"].(map[string]interface{})["status"].(map[string]interface{})
	blockers := status["properties"].(map[string]interface{})["blockers"].(map[string]interface{})
	if got := blockers["maxItems"]; got != float64(reportStatusBlockerCap) {
		t.Fatalf("status.blockers maxItems = %v, want %d", got, reportStatusBlockerCap)
	}
}

func init() {
	ctrllog.SetLogger(zap.New(zap.WriteTo(os.Stderr), zap.UseDevMode(true)))
}
