// Copyright Contributors to the Open Cluster Management project

package controllers

import (
	"context"
	"strings"
	"testing"

	operatorv1 "github.com/stolostron/multiclusterhub-operator/api/v1"
	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	ctrllog "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
)

var (
	mchGVK = schema.GroupVersionKind{Group: "operator.open-cluster-management.io", Version: "v1", Kind: "MultiClusterHub"}
	mcoGVK = schema.GroupVersionKind{Group: "observability.open-cluster-management.io", Version: "v1beta2", Kind: "MultiClusterObservability"}
	dcGVK  = schema.GroupVersionKind{Group: "discovery.open-cluster-management.io", Version: "v1", Kind: "DiscoveryConfig"}
	ascGVK = schema.GroupVersionKind{Group: "agent-install.openshift.io", Version: "v1beta1", Kind: "AgentServiceConfig"}
)

var (
	hostedClusterGVK  = schema.GroupVersionKind{Group: "hypershift.openshift.io", Version: "v1beta1", Kind: "HostedCluster"}
	managedClusterGVK = schema.GroupVersionKind{Group: "cluster.open-cluster-management.io", Version: "v1", Kind: "ManagedCluster"}
)

func registerUnstructured(t *testing.T, s *runtime.Scheme, gvks ...schema.GroupVersionKind) {
	t.Helper()
	for _, gvk := range gvks {
		s.AddKnownTypeWithName(gvk, &unstructured.Unstructured{})
		s.AddKnownTypeWithName(gvk.GroupVersion().WithKind(gvk.Kind+"List"), &unstructured.UnstructuredList{})
	}
}

func newGateTestReconciler(t *testing.T, objs ...client.Object) *HubTeardownReconciler {
	t.Helper()
	s := newTestScheme()
	registerUnstructured(t, s, hostedClusterGVK, managedClusterGVK)
	return &HubTeardownReconciler{
		Client:   fake.NewClientBuilder().WithScheme(s).WithObjects(objs...).Build(),
		Scheme:   s,
		Log:      ctrllog.Log,
		Recorder: record.NewFakeRecorder(50),
	}
}

func newUnstructured(gvk schema.GroupVersionKind, namespace, name string, finalizers ...string) *unstructured.Unstructured {
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(gvk)
	u.SetNamespace(namespace)
	u.SetName(name)
	u.SetFinalizers(finalizers)
	return u
}

// withPlatform gives a HostedCluster/NodePool the spec.platform.type every
// real object carries. The cloud gate now keys on platform evidence, so a
// synthetic object without one is not a cloud-provisioned object.
func withPlatform(u *unstructured.Unstructured, platform string) *unstructured.Unstructured {
	_ = unstructured.SetNestedField(u.Object, platform, "spec", "platform", "type")
	return u
}

func conditionReason(td *operatorv1.HubTeardown, condType string) string {
	for _, c := range td.Status.Conditions {
		if c.Type == condType {
			return c.Reason
		}
	}
	return ""
}

func hasFinalizer(u *unstructured.Unstructured, finalizer string) bool {
	for _, f := range u.GetFinalizers() {
		if f == finalizer {
			return true
		}
	}
	return false
}

func drainEvents(r *HubTeardownReconciler) []string {
	var events []string
	for {
		select {
		case ev := <-r.Recorder.(*record.FakeRecorder).Events:
			events = append(events, ev)
		default:
			return events
		}
	}
}

func newTeardownCR() *operatorv1.HubTeardown {
	return &operatorv1.HubTeardown{
		ObjectMeta: metav1.ObjectMeta{Name: "teardown", Namespace: "open-cluster-management"},
	}
}

// Cloud-risk gate: infrastructure CRs exist and acknowledgeCloudResourceRisk is false.
// The phase must not delete anything and must surface why it is blocked.
func TestPhaseDeleteInfrastructureCRs_CloudRiskGateBlocks(t *testing.T) {
	ctrllog.SetLogger(zap.New(zap.UseDevMode(true)))
	hcp := withPlatform(newUnstructured(hostedClusterGVK, "clusters", "hcp1"), "AWS")
	r := newGateTestReconciler(t, hcp)
	td := newTeardownCR()

	done, err := r.phaseDeleteInfrastructureCRs(context.Background(), ctrllog.Log.WithName("test"), td)
	if err != nil {
		t.Fatalf("phaseDeleteInfrastructureCRs error: %v", err)
	}
	if done {
		t.Fatal("expected cloud-risk gate to block the phase, but it completed")
	}

	got := &unstructured.Unstructured{}
	got.SetGroupVersionKind(hostedClusterGVK)
	if err := r.Client.Get(context.Background(), types.NamespacedName{Namespace: "clusters", Name: "hcp1"}, got); err != nil {
		t.Fatalf("infrastructure CR must not be deleted while approval is missing: %v", err)
	}

	events := strings.Join(drainEvents(r), "\n")
	if !strings.Contains(events, "CloudRiskGate") || !strings.Contains(events, "acknowledgeCloudResourceRisk") {
		t.Fatalf("expected CloudRiskGate approval event, got:\n%s", events)
	}
}

// Positive counterpart: the same phase must proceed once acknowledgment is set.
func TestPhaseDeleteInfrastructureCRs_CloudRiskAckAllowsDeletion(t *testing.T) {
	ctrllog.SetLogger(zap.New(zap.UseDevMode(true)))
	hcp := withPlatform(newUnstructured(hostedClusterGVK, "clusters", "hcp1"), "AWS")
	r := newGateTestReconciler(t, hcp)
	td := newTeardownCR()
	td.Spec.AcknowledgeCloudResourceRisk = true

	done, err := r.phaseDeleteInfrastructureCRs(context.Background(), ctrllog.Log.WithName("test"), td)
	if err != nil {
		t.Fatalf("phaseDeleteInfrastructureCRs error: %v", err)
	}
	if done {
		t.Log("phase completed immediately; verifying deletion was still issued")
	}

	got := &unstructured.Unstructured{}
	got.SetGroupVersionKind(hostedClusterGVK)
	err = r.Client.Get(context.Background(), types.NamespacedName{Namespace: "clusters", Name: "hcp1"}, got)
	if err == nil && got.GetDeletionTimestamp() == nil {
		t.Fatal("expected deletion to be requested after acknowledgment")
	}

	events := strings.Join(drainEvents(r), "\n")
	if strings.Contains(events, "CloudRiskGate") {
		t.Fatalf("cloud-risk gate must not fire after acknowledgment, got:\n%s", events)
	}
}

// Tier 1 finalizers protect cloud resources and require acknowledgeCloudResourceRisk.
func TestPatchFinalizer_Tier1BlockedWithoutAck(t *testing.T) {
	ctrllog.SetLogger(zap.New(zap.UseDevMode(true)))
	const finalizer = "hypershift.openshift.io/finalizer"
	hcp := newUnstructured(hostedClusterGVK, "clusters", "hcp1", finalizer)
	r := newGateTestReconciler(t, hcp)
	td := newTeardownCR()

	removed, err := r.patchFinalizerOffResource(context.Background(), ctrllog.Log.WithName("test"), td,
		operatorv1.ResourceRef{Group: hostedClusterGVK.Group, Kind: hostedClusterGVK.Kind, Namespace: "clusters", Name: "hcp1"},
		finalizer, hostedClusterGVK, "AWS")
	if err != nil {
		t.Fatalf("patchFinalizerOffResource error: %v", err)
	}
	if removed {
		t.Fatal("Tier 1 finalizer must not be removed without acknowledgeCloudResourceRisk")
	}
	if reason := conditionReason(td, conditionTypeWaitingForApproval); reason != "Tier1CloudFinalizersBlocked" {
		t.Fatalf("expected Tier1CloudFinalizersBlocked condition, got %q", reason)
	}

	got := &unstructured.Unstructured{}
	got.SetGroupVersionKind(hostedClusterGVK)
	if err := r.Client.Get(context.Background(), types.NamespacedName{Namespace: "clusters", Name: "hcp1"}, got); err != nil {
		t.Fatalf("get hosted cluster: %v", err)
	}
	if !hasFinalizer(got, finalizer) {
		t.Fatal("Tier 1 finalizer was removed despite the missing acknowledgment")
	}
}

func TestPatchFinalizer_Tier1AllowedWithAck(t *testing.T) {
	ctrllog.SetLogger(zap.New(zap.UseDevMode(true)))
	const finalizer = "hypershift.openshift.io/finalizer"
	hcp := newUnstructured(hostedClusterGVK, "clusters", "hcp1", finalizer)
	r := newGateTestReconciler(t, hcp)
	td := newTeardownCR()
	td.Spec.AcknowledgeCloudResourceRisk = true

	removed, err := r.patchFinalizerOffResource(context.Background(), ctrllog.Log.WithName("test"), td,
		operatorv1.ResourceRef{Group: hostedClusterGVK.Group, Kind: hostedClusterGVK.Kind, Namespace: "clusters", Name: "hcp1"},
		finalizer, hostedClusterGVK, "AWS")
	if err != nil {
		t.Fatalf("patchFinalizerOffResource error: %v", err)
	}
	if !removed {
		t.Fatal("Tier 1 finalizer must be removed after acknowledgment")
	}

	got := &unstructured.Unstructured{}
	got.SetGroupVersionKind(hostedClusterGVK)
	if err := r.Client.Get(context.Background(), types.NamespacedName{Namespace: "clusters", Name: "hcp1"}, got); err != nil {
		t.Fatalf("get hosted cluster: %v", err)
	}
	if hasFinalizer(got, finalizer) {
		t.Fatal("Tier 1 finalizer still present after acknowledgment")
	}

	events := strings.Join(drainEvents(r), "\n")
	if !strings.Contains(events, "FinalizerRemovalWarning") {
		t.Fatalf("expected FinalizerRemovalWarning event, got:\n%s", events)
	}
}

// 22-E4: a KubeVirt HostedCluster finalizer protects VMs inside the hub, not
// a cloud account. Destructive approval is enough; cloud acknowledgment is not.
func TestPatchFinalizer_KubeVirtNeedsNoCloudAck(t *testing.T) {
	ctrllog.SetLogger(zap.New(zap.UseDevMode(true)))
	const finalizer = "hypershift.openshift.io/finalizer"
	hcp := withPlatform(newUnstructured(hostedClusterGVK, "clusters", "hcp1", finalizer), "KUBEVIRT")
	r := newGateTestReconciler(t, hcp)
	td := newTeardownCR()
	td.Spec.ApprovedDestructiveActions = true

	removed, err := r.patchFinalizerOffResource(context.Background(), ctrllog.Log.WithName("test"), td,
		operatorv1.ResourceRef{Group: hostedClusterGVK.Group, Kind: hostedClusterGVK.Kind, Namespace: "clusters", Name: "hcp1"},
		finalizer, hostedClusterGVK, "KUBEVIRT")
	if err != nil {
		t.Fatalf("patchFinalizerOffResource error: %v", err)
	}
	if !removed {
		t.Fatal("KubeVirt HCP finalizer must be removable with destructive approval alone")
	}

	got := &unstructured.Unstructured{}
	got.SetGroupVersionKind(hostedClusterGVK)
	if err := r.Client.Get(context.Background(), types.NamespacedName{Namespace: "clusters", Name: "hcp1"}, got); err != nil {
		t.Fatalf("get hosted cluster: %v", err)
	}
	if hasFinalizer(got, finalizer) {
		t.Fatal("KubeVirt HCP finalizer still present")
	}
	if events := strings.Join(drainEvents(r), "\n"); strings.Contains(events, "FinalizerRemovalWarning") {
		t.Fatalf("KubeVirt removal must not claim cloud orphan risk:\n%s", events)
	}
}

// 22-E5: dropping the cloud gate does not drop the destructive gate.
func TestPatchFinalizer_KubeVirtRequiresDestructiveApproval(t *testing.T) {
	ctrllog.SetLogger(zap.New(zap.UseDevMode(true)))
	const finalizer = "hypershift.openshift.io/finalizer"
	hcp := withPlatform(newUnstructured(hostedClusterGVK, "clusters", "hcp1", finalizer), "KUBEVIRT")
	r := newGateTestReconciler(t, hcp)
	td := newTeardownCR()

	removed, err := r.patchFinalizerOffResource(context.Background(), ctrllog.Log.WithName("test"), td,
		operatorv1.ResourceRef{Group: hostedClusterGVK.Group, Kind: hostedClusterGVK.Kind, Namespace: "clusters", Name: "hcp1"},
		finalizer, hostedClusterGVK, "KUBEVIRT")
	if err != nil {
		t.Fatalf("patchFinalizerOffResource error: %v", err)
	}
	if removed {
		t.Fatal("KubeVirt HCP finalizer must not be removed without any approval")
	}
	if reason := conditionReason(td, conditionTypeWaitingForApproval); reason != "Tier2FinalizersBlocked" {
		t.Fatalf("expected Tier2FinalizersBlocked condition, got %q", reason)
	}
}

// Tier 2 finalizers protect Kubernetes state and require approvedDestructiveActions.
func TestPatchFinalizer_Tier2BlockedWithoutApproval(t *testing.T) {
	ctrllog.SetLogger(zap.New(zap.UseDevMode(true)))
	const finalizer = "addon.open-cluster-management.io/addon-pre-delete"
	mc := newUnstructured(managedClusterGVK, "clusters", "mc1", finalizer)
	r := newGateTestReconciler(t, mc)
	td := newTeardownCR()

	removed, err := r.patchFinalizerOffResource(context.Background(), ctrllog.Log.WithName("test"), td,
		operatorv1.ResourceRef{Group: managedClusterGVK.Group, Kind: managedClusterGVK.Kind, Namespace: "clusters", Name: "mc1"},
		finalizer, managedClusterGVK, "")
	if err != nil {
		t.Fatalf("patchFinalizerOffResource error: %v", err)
	}
	if removed {
		t.Fatal("Tier 2 finalizer must not be removed without approvedDestructiveActions")
	}
	if reason := conditionReason(td, conditionTypeWaitingForApproval); reason != "Tier2FinalizersBlocked" {
		t.Fatalf("expected Tier2FinalizersBlocked condition, got %q", reason)
	}

	got := &unstructured.Unstructured{}
	got.SetGroupVersionKind(managedClusterGVK)
	if err := r.Client.Get(context.Background(), types.NamespacedName{Namespace: "clusters", Name: "mc1"}, got); err != nil {
		t.Fatalf("get managed cluster: %v", err)
	}
	if !hasFinalizer(got, finalizer) {
		t.Fatal("Tier 2 finalizer was removed despite the missing approval")
	}
}

func TestPatchFinalizer_Tier2AllowedWithApproval(t *testing.T) {
	ctrllog.SetLogger(zap.New(zap.UseDevMode(true)))
	const finalizer = "addon.open-cluster-management.io/addon-pre-delete"
	mc := newUnstructured(managedClusterGVK, "clusters", "mc1", finalizer)
	r := newGateTestReconciler(t, mc)
	td := newTeardownCR()
	td.Spec.ApprovedDestructiveActions = true

	removed, err := r.patchFinalizerOffResource(context.Background(), ctrllog.Log.WithName("test"), td,
		operatorv1.ResourceRef{Group: managedClusterGVK.Group, Kind: managedClusterGVK.Kind, Namespace: "clusters", Name: "mc1"},
		finalizer, managedClusterGVK, "")
	if err != nil {
		t.Fatalf("patchFinalizerOffResource error: %v", err)
	}
	if !removed {
		t.Fatal("Tier 2 finalizer must be removed after approval")
	}
}

// Unknown finalizers are never removed, regardless of approvals.
func TestPatchFinalizer_UnknownNotRemoved(t *testing.T) {
	ctrllog.SetLogger(zap.New(zap.UseDevMode(true)))
	const finalizer = "example.com/unknown"
	mc := newUnstructured(managedClusterGVK, "clusters", "mc1", finalizer)
	r := newGateTestReconciler(t, mc)
	td := newTeardownCR()
	td.Spec.ApprovedDestructiveActions = true
	td.Spec.AcknowledgeCloudResourceRisk = true

	removed, err := r.patchFinalizerOffResource(context.Background(), ctrllog.Log.WithName("test"), td,
		operatorv1.ResourceRef{Group: managedClusterGVK.Group, Kind: managedClusterGVK.Kind, Namespace: "clusters", Name: "mc1"},
		finalizer, managedClusterGVK, "")
	if err != nil {
		t.Fatalf("patchFinalizerOffResource error: %v", err)
	}
	if removed {
		t.Fatal("unknown finalizer must never be removed")
	}
	if reason := conditionReason(td, conditionTypeWaitingForApproval); reason != "" {
		t.Fatalf("unknown finalizer must not raise an approval condition, got %q", reason)
	}
}

func TestPhaseRemoveBlockingCRs_DeletesBlockingCRsWithoutApproval(t *testing.T) {
	s := runtime.NewScheme()
	registerUnstructured(t, s, mchGVK, mcoGVK, dcGVK, ascGVK)

	mco := newUnstructured(mcoGVK, "", "observability")
	dc := newUnstructured(dcGVK, "", "discovery")
	asc := newUnstructured(ascGVK, "", "agent")

	r := &HubTeardownReconciler{
		Client:   fake.NewClientBuilder().WithScheme(s).WithObjects(mco, dc, asc).Build(),
		Scheme:   s,
		Log:      ctrllog.Log,
		Recorder: record.NewFakeRecorder(50),
	}

	td := newTeardownCR()
	td.Spec.DryRun = false
	td.Spec.ApprovedDestructiveActions = false
	td.Spec.AcknowledgeCloudResourceRisk = true

	done, err := r.phaseRemoveBlockingCRs(context.Background(), ctrllog.Log, td)
	if err != nil {
		t.Fatalf("phaseRemoveBlockingCRs error: %v", err)
	}
	if done {
		t.Fatal("phase must not report complete on the pass that requests blocking-CR deletion")
	}

	for _, obj := range []*unstructured.Unstructured{mco, dc, asc} {
		got := &unstructured.Unstructured{}
		got.SetGroupVersionKind(obj.GroupVersionKind())
		err := r.Client.Get(context.Background(), types.NamespacedName{Name: obj.GetName()}, got)
		if err == nil {
			t.Errorf("%s %q still present after RemoveBlockingCRs", obj.GetKind(), obj.GetName())
			continue
		}
		if !apierrors.IsNotFound(err) {
			t.Fatalf("unexpected error reading %s %q: %v", obj.GetKind(), obj.GetName(), err)
		}
	}
}

func TestPhaseRemoveBlockingCRs_CompletesAndDeletesAgentInstallWebhook(t *testing.T) {
	s := runtime.NewScheme()
	registerUnstructured(t, s, mchGVK)
	s.AddKnownTypes(admissionregistrationv1.SchemeGroupVersion,
		&admissionregistrationv1.ValidatingWebhookConfiguration{},
		&admissionregistrationv1.MutatingWebhookConfiguration{})

	vwc := &admissionregistrationv1.ValidatingWebhookConfiguration{
		ObjectMeta: metav1.ObjectMeta{Name: agentInstallWebhookNames[0]},
	}

	r := &HubTeardownReconciler{
		Client:   fake.NewClientBuilder().WithScheme(s).WithObjects(vwc).Build(),
		Scheme:   s,
		Log:      ctrllog.Log,
		Recorder: record.NewFakeRecorder(50),
	}

	td := newTeardownCR()
	td.Spec.DryRun = false

	done, err := r.phaseRemoveBlockingCRs(context.Background(), ctrllog.Log, td)
	if err != nil {
		t.Fatalf("phaseRemoveBlockingCRs error: %v", err)
	}
	if !done {
		t.Fatal("phase must complete once no blocking CRs remain")
	}

	got := &admissionregistrationv1.ValidatingWebhookConfiguration{}
	err = r.Client.Get(context.Background(), types.NamespacedName{Name: vwc.Name}, got)
	if err == nil {
		t.Fatalf("agent-install webhook %q still present after phase completion", vwc.Name)
	}
	if !apierrors.IsNotFound(err) {
		t.Fatalf("unexpected error reading webhook: %v", err)
	}
}

func TestSetTeardownActiveAnnotation_SetsAndIsIdempotent(t *testing.T) {
	s := runtime.NewScheme()
	registerUnstructured(t, s, mchGVK)

	mch := newUnstructured(mchGVK, "open-cluster-management", "multiclusterhub")
	r := &HubTeardownReconciler{
		Client:   fake.NewClientBuilder().WithScheme(s).WithObjects(mch).Build(),
		Scheme:   s,
		Log:      ctrllog.Log,
		Recorder: record.NewFakeRecorder(50),
	}

	for attempt := 1; attempt <= 2; attempt++ {
		if err := r.setTeardownActiveAnnotation(context.Background(), ctrllog.Log); err != nil {
			t.Fatalf("setTeardownActiveAnnotation attempt %d error: %v", attempt, err)
		}
		got := &unstructured.Unstructured{}
		got.SetGroupVersionKind(mchGVK)
		if err := r.Client.Get(context.Background(), types.NamespacedName{
			Namespace: mch.GetNamespace(),
			Name:      mch.GetName(),
		}, got); err != nil {
			t.Fatalf("reading MCH after attempt %d: %v", attempt, err)
		}
		if got.GetAnnotations()[annotationTeardownActive] != "true" {
			t.Fatalf("attempt %d: teardown-active annotation not set, got %v", attempt, got.GetAnnotations())
		}
	}
}
