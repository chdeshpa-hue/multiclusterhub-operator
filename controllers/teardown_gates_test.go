// Copyright Contributors to the Open Cluster Management project

package controllers

import (
	"context"
	"strings"
	"testing"

	operatorv1 "github.com/stolostron/multiclusterhub-operator/api/v1"
	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	corev1 "k8s.io/api/core/v1"
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
	registerUnstructured(t, s, hostedClusterGVK, managedClusterGVK, mcoGVK, dcGVK, ascGVK)
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

// ownedByTestHub gives a fixture the installer identity the evidence classifier
// needs. Name matching alone must never authorize a destructive action.
func ownedByTestHub(u *unstructured.Unstructured) *unstructured.Unstructured {
	labels := u.GetLabels()
	if labels == nil {
		labels = map[string]string{}
	}
	labels["installer.name"] = testHubRef().Name
	labels["installer.namespace"] = testHubRef().Namespace
	u.SetLabels(labels)
	return u
}

// grantTestCloudConsent binds the acknowledgement to the report digest, which
// is the only consent model the execution path honors.
func grantTestCloudConsent(td *operatorv1.HubTeardown) {
	td.Spec.AcknowledgeCloudResourceRisk = true
	td.Spec.AcknowledgedReportDigest = "test-digest"
	td.Status.ReportDigest = "test-digest"
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
		Spec:       operatorv1.HubTeardownSpec{HubRef: testHubRef()},
	}
}

// Cloud-risk gate: infrastructure CRs exist and acknowledgeCloudResourceRisk is false.
// The phase must not delete anything and must surface why it is blocked.
func TestPhaseDeleteInfrastructureCRs_CloudRiskConsentBlocks(t *testing.T) {
	ctrllog.SetLogger(zap.New(zap.UseDevMode(true)))
	hcp := ownedByTestHub(withPlatform(newUnstructured(hostedClusterGVK, "clusters", "hcp1",
		"hypershift.openshift.io/finalizer"), "AWS"))
	r := newGateTestReconciler(t, hcp)
	td := newTeardownCR()

	done, err := r.phaseDeleteInfrastructureCRs(context.Background(), ctrllog.Log.WithName("test"), td)
	if err != nil {
		t.Fatalf("phaseDeleteInfrastructureCRs error: %v", err)
	}
	if done {
		t.Fatal("expected the missing cloud-risk consent to block the phase, but it completed")
	}

	got := &unstructured.Unstructured{}
	got.SetGroupVersionKind(hostedClusterGVK)
	if err := r.Client.Get(context.Background(), types.NamespacedName{Namespace: "clusters", Name: "hcp1"}, got); err != nil {
		t.Fatalf("infrastructure CR must not be deleted while approval is missing: %v", err)
	}

	events := strings.Join(drainEvents(r), "\n")
	if !strings.Contains(events, "InfrastructureCRRetained") || !strings.Contains(events, "digest-bound consent") {
		t.Fatalf("expected a retained-infrastructure event naming the consent gate, got:\n%s", events)
	}
}

// Positive counterpart: the same phase must proceed once acknowledgment is set.
func TestPhaseDeleteInfrastructureCRs_CloudRiskConsentAllowsDeletion(t *testing.T) {
	ctrllog.SetLogger(zap.New(zap.UseDevMode(true)))
	hcp := ownedByTestHub(withPlatform(newUnstructured(hostedClusterGVK, "clusters", "hcp1",
		"hypershift.openshift.io/finalizer"), "AWS"))
	r := newGateTestReconciler(t, hcp)
	td := newTeardownCR()
	grantTestCloudConsent(td)

	done, err := r.phaseDeleteInfrastructureCRs(context.Background(), ctrllog.Log.WithName("test"), td)
	if err != nil {
		t.Fatalf("phaseDeleteInfrastructureCRs error: %v", err)
	}
	if done {
		t.Fatal("phase must wait for the finalizer-bearing infrastructure CR to terminate")
	}

	got := &unstructured.Unstructured{}
	got.SetGroupVersionKind(hostedClusterGVK)
	err = r.Client.Get(context.Background(), types.NamespacedName{Namespace: "clusters", Name: "hcp1"}, got)
	if err != nil {
		t.Fatalf("infrastructure CR disappeared without a finalizer: %v", err)
	}
	if got.GetDeletionTimestamp() == nil {
		t.Fatal("expected deletion to be requested after digest-bound consent")
	}

	events := strings.Join(drainEvents(r), "\n")
	if strings.Contains(events, "InfrastructureCRRetained") {
		t.Fatalf("cloud-risk gate must not retain the CR after consent, got:\n%s", events)
	}
}

// Tier 1 finalizers protect cloud resources and require acknowledgeCloudResourceRisk.
func TestPatchFinalizer_Tier1BlockedWithoutAck(t *testing.T) {
	ctrllog.SetLogger(zap.New(zap.UseDevMode(true)))
	const finalizer = "hypershift.openshift.io/finalizer"
	hcp := ownedByTestHub(newUnstructured(hostedClusterGVK, "clusters", "hcp1", finalizer))
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
	if reason := conditionReason(td, conditionTypeWaitingForApproval); reason != "CloudRiskConsentRequired" {
		t.Fatalf("expected CloudRiskConsentRequired condition, got %q", reason)
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
	hcp := ownedByTestHub(newUnstructured(hostedClusterGVK, "clusters", "hcp1", finalizer))
	r := newGateTestReconciler(t, hcp)
	td := newTeardownCR()
	grantTestCloudConsent(td)

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
	hcp := ownedByTestHub(withPlatform(newUnstructured(hostedClusterGVK, "clusters", "hcp1", finalizer), "KUBEVIRT"))
	r := newGateTestReconciler(t, hcp)
	td := newTeardownCR()

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

// A KubeVirt finalizer is not a cloud-risk action, but it still requires
// evidence-backed ownership before the shared authorizer permits the patch.
func TestPatchFinalizer_KubeVirtRequiresOwnershipEvidence(t *testing.T) {
	ctrllog.SetLogger(zap.New(zap.UseDevMode(true)))
	const finalizer = "hypershift.openshift.io/finalizer"
	hcp := withPlatform(newUnstructured(hostedClusterGVK, "clusters", "hcp1", finalizer), "KUBEVIRT")
	r := newGateTestReconciler(t, hcp)
	td := newTeardownCR()
	grantTestCloudConsent(td)

	removed, err := r.patchFinalizerOffResource(context.Background(), ctrllog.Log.WithName("test"), td,
		operatorv1.ResourceRef{Group: hostedClusterGVK.Group, Kind: hostedClusterGVK.Kind, Namespace: "clusters", Name: "hcp1"},
		finalizer, hostedClusterGVK, "KUBEVIRT")
	if err != nil {
		t.Fatalf("patchFinalizerOffResource error: %v", err)
	}
	if removed {
		t.Fatal("KubeVirt HCP finalizer must not be removed without ownership evidence")
	}
	if reason := conditionReason(td, conditionTypeWaitingForApproval); reason != "" {
		t.Fatalf("ownership failure must not masquerade as a consent request, got %q", reason)
	}
}

// Ownership is the first gate for every action, including non-cloud finalizers.
func TestPatchFinalizer_NonCloudBlockedWithoutOwnership(t *testing.T) {
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
		t.Fatal("non-cloud finalizer must not be removed without ownership evidence")
	}
	if reason := conditionReason(td, conditionTypeWaitingForApproval); reason != "" {
		t.Fatalf("ownership failure must not raise an approval condition, got %q", reason)
	}

	got := &unstructured.Unstructured{}
	got.SetGroupVersionKind(managedClusterGVK)
	if err := r.Client.Get(context.Background(), types.NamespacedName{Namespace: "clusters", Name: "mc1"}, got); err != nil {
		t.Fatalf("get managed cluster: %v", err)
	}
	if !hasFinalizer(got, finalizer) {
		t.Fatal("non-cloud finalizer was removed despite missing ownership evidence")
	}
}

func TestPatchFinalizer_NonCloudAllowedWithOwnership(t *testing.T) {
	ctrllog.SetLogger(zap.New(zap.UseDevMode(true)))
	const finalizer = "addon.open-cluster-management.io/addon-pre-delete"
	mc := ownedByTestHub(newUnstructured(managedClusterGVK, "clusters", "mc1", finalizer))
	r := newGateTestReconciler(t, mc)
	td := newTeardownCR()

	removed, err := r.patchFinalizerOffResource(context.Background(), ctrllog.Log.WithName("test"), td,
		operatorv1.ResourceRef{Group: managedClusterGVK.Group, Kind: managedClusterGVK.Kind, Namespace: "clusters", Name: "mc1"},
		finalizer, managedClusterGVK, "")
	if err != nil {
		t.Fatalf("patchFinalizerOffResource error: %v", err)
	}
	if !removed {
		t.Fatal("owned non-cloud finalizer must be removed when its controller is gone")
	}
}

// Unknown finalizers are never removed, regardless of approvals.
func TestPatchFinalizer_UnknownNotRemoved(t *testing.T) {
	ctrllog.SetLogger(zap.New(zap.UseDevMode(true)))
	const finalizer = "example.com/unknown"
	mc := ownedByTestHub(newUnstructured(managedClusterGVK, "clusters", "mc1", finalizer))
	r := newGateTestReconciler(t, mc)
	td := newTeardownCR()
	grantTestCloudConsent(td)

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

func TestPhaseRemoveBlockingCRs_DeletesOwnedBlockingCRs(t *testing.T) {
	mco := ownedByTestHub(newUnstructured(mcoGVK, "", "observability"))
	dc := ownedByTestHub(newUnstructured(dcGVK, "", "discovery"))
	asc := ownedByTestHub(newUnstructured(ascGVK, "", "agent"))

	r := newGateTestReconciler(t, mco, dc, asc)

	td := newTeardownCR()
	td.Spec.DryRun = false

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

func TestPhaseRemoveBlockingCRs_DeletesOwnedDanglingRegistration(t *testing.T) {
	vwc := &admissionregistrationv1.ValidatingWebhookConfiguration{
		ObjectMeta: metav1.ObjectMeta{Name: "acm-dangling.example.com"},
		Webhooks: []admissionregistrationv1.ValidatingWebhook{{
			Name: "validate.example.com",
			ClientConfig: admissionregistrationv1.WebhookClientConfig{
				Service: &admissionregistrationv1.ServiceReference{
					Namespace: "open-cluster-management",
					Name:      "missing-webhook-service",
				},
			},
		}},
	}

	r := newGateTestReconciler(t, vwc)

	td := newTeardownCR()
	td.Spec.DryRun = false

	done, err := r.phaseRemoveBlockingCRs(context.Background(), ctrllog.Log, td)
	if err != nil {
		t.Fatalf("phaseRemoveBlockingCRs error: %v", err)
	}
	if !done {
		t.Fatal("phase must complete once the dangling registration is settled")
	}

	got := &admissionregistrationv1.ValidatingWebhookConfiguration{}
	err = r.Client.Get(context.Background(), types.NamespacedName{Name: vwc.Name}, got)
	if err == nil {
		t.Fatalf("owned dangling webhook %q still present after phase completion", vwc.Name)
	}
	if !apierrors.IsNotFound(err) {
		t.Fatalf("unexpected error reading webhook: %v", err)
	}
}

func TestPhaseRemoveBlockingCRs_RetainsHealthyRegistration(t *testing.T) {
	vwc := &admissionregistrationv1.ValidatingWebhookConfiguration{
		ObjectMeta: metav1.ObjectMeta{Name: "acm-healthy.example.com"},
		Webhooks: []admissionregistrationv1.ValidatingWebhook{{
			Name: "validate.example.com",
			ClientConfig: admissionregistrationv1.WebhookClientConfig{
				Service: &admissionregistrationv1.ServiceReference{
					Namespace: "open-cluster-management",
					Name:      "healthy-webhook-service",
				},
			},
		}},
	}
	svc := &corev1.Service{ObjectMeta: metav1.ObjectMeta{
		Name:      "healthy-webhook-service",
		Namespace: "open-cluster-management",
	}}
	r := newGateTestReconciler(t, vwc, svc)
	td := newTeardownCR()
	td.Spec.DryRun = false

	done, err := r.phaseRemoveBlockingCRs(context.Background(), ctrllog.Log, td)
	if err != nil {
		t.Fatalf("phaseRemoveBlockingCRs error: %v", err)
	}
	if !done {
		t.Fatal("phase must complete when the webhook is healthy")
	}
	got := &admissionregistrationv1.ValidatingWebhookConfiguration{}
	if err := r.Client.Get(context.Background(), types.NamespacedName{Name: vwc.Name}, got); err != nil {
		t.Fatalf("healthy webhook must be retained: %v", err)
	}
}

func TestPhaseRemoveBlockingCRs_RetainsUnownedDanglingRegistration(t *testing.T) {
	vwc := &admissionregistrationv1.ValidatingWebhookConfiguration{
		ObjectMeta: metav1.ObjectMeta{Name: "platform-dangling.example.com"},
		Webhooks: []admissionregistrationv1.ValidatingWebhook{{
			Name: "validate.example.com",
			ClientConfig: admissionregistrationv1.WebhookClientConfig{
				Service: &admissionregistrationv1.ServiceReference{
					Namespace: "platform-system",
					Name:      "missing-webhook-service",
				},
			},
		}},
	}
	r := newGateTestReconciler(t, vwc)
	td := newTeardownCR()
	td.Spec.DryRun = false

	done, err := r.phaseRemoveBlockingCRs(context.Background(), ctrllog.Log, td)
	if err != nil {
		t.Fatalf("phaseRemoveBlockingCRs error: %v", err)
	}
	if !done {
		t.Fatal("report-only registration must be a terminal disposition, not an infinite wait")
	}
	got := &admissionregistrationv1.ValidatingWebhookConfiguration{}
	if err := r.Client.Get(context.Background(), types.NamespacedName{Name: vwc.Name}, got); err != nil {
		t.Fatalf("unowned webhook must be retained: %v", err)
	}
	if events := strings.Join(drainEvents(r), "\n"); !strings.Contains(events, "DanglingRegistrationRetained") {
		t.Fatalf("expected a retained-registration event, got:\n%s", events)
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
		td := newTeardownCR()
		if err := r.setTeardownActiveAnnotation(context.Background(), ctrllog.Log, td); err != nil {
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
