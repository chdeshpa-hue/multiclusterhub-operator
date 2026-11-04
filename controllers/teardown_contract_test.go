// Copyright Contributors to the Open Cluster Management project

package controllers

import (
	"context"
	"errors"
	"testing"
	"time"

	operatorv1 "github.com/stolostron/multiclusterhub-operator/api/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	ctrllog "sigs.k8s.io/controller-runtime/pkg/log"
)

func contractScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := corev1.AddToScheme(s); err != nil {
		t.Fatalf("corev1 AddToScheme: %v", err)
	}
	// Only HubTeardown is needed; registering the typed MultiClusterHub would
	// collide with the unstructured one the teardown code lists.
	s.AddKnownTypes(operatorv1.GroupVersion, &operatorv1.HubTeardown{}, &operatorv1.HubTeardownList{})
	metav1.AddToGroupVersion(s, operatorv1.GroupVersion)
	registerUnstructured(t, s, mchGVK)
	return s
}

func contractReconciler(t *testing.T, objs ...client.Object) *HubTeardownReconciler {
	t.Helper()
	s := contractScheme(t)
	return &HubTeardownReconciler{
		Client: fake.NewClientBuilder().WithScheme(s).WithObjects(objs...).Build(),
		Scheme: s,
	}
}

func readMCHAnnotations(t *testing.T, r *HubTeardownReconciler, namespace, name string) map[string]string {
	t.Helper()
	u := newUnstructured(mchGVK, namespace, name)
	if err := r.Client.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: name}, u); err != nil {
		t.Fatalf("reading MCH %s/%s: %v", namespace, name, err)
	}
	return u.GetAnnotations()
}

// P-1: the writer sets both the confirmed value and the contract version.
func TestSetTeardownActiveAnnotation_WritesValueAndVersion(t *testing.T) {
	mch := newUnstructured(mchGVK, "open-cluster-management", "multiclusterhub")
	r := contractReconciler(t, mch)

	if err := r.setTeardownActiveAnnotation(context.Background(), ctrllog.Log); err != nil {
		t.Fatalf("setTeardownActiveAnnotation: %v", err)
	}

	annotations := readMCHAnnotations(t, r, mch.GetNamespace(), mch.GetName())
	if annotations[annotationTeardownActive] != "true" {
		t.Fatalf("teardown-active = %q, want true", annotations[annotationTeardownActive])
	}
	if annotations[annotationTeardownActiveVersion] != teardownContractVersion {
		t.Fatalf("teardown-active-version = %q, want %q",
			annotations[annotationTeardownActiveVersion], teardownContractVersion)
	}
}

// N-1: a hub that cannot signal the contract must fail the phase, not continue.
func TestSetTeardownActiveAnnotation_PatchFailureIsFatal(t *testing.T) {
	mch := newUnstructured(mchGVK, "open-cluster-management", "multiclusterhub")
	s := contractScheme(t)
	r := &HubTeardownReconciler{
		Scheme: s,
		Client: fake.NewClientBuilder().WithScheme(s).WithObjects(mch).WithInterceptorFuncs(interceptor.Funcs{
			Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
				return apierrors.NewForbidden(
					schema.GroupResource{Group: mchGVK.Group, Resource: "multiclusterhubs"},
					obj.GetName(), errors.New("denied by test"))
			},
		}).Build(),
	}

	if err := r.setTeardownActiveAnnotation(context.Background(), ctrllog.Log); err == nil {
		t.Fatal("expected an error when the MCH cannot be annotated")
	}
}

// E-1..E-3: a nil map is created, a wrong value is corrected, a missing version is added.
func TestSetTeardownActiveAnnotation_RepairsPartialContract(t *testing.T) {
	mch := newUnstructured(mchGVK, "open-cluster-management", "multiclusterhub")
	mch.SetAnnotations(map[string]string{annotationTeardownActive: "false"})
	r := contractReconciler(t, mch)

	if err := r.setTeardownActiveAnnotation(context.Background(), ctrllog.Log); err != nil {
		t.Fatalf("setTeardownActiveAnnotation: %v", err)
	}

	annotations := readMCHAnnotations(t, r, mch.GetNamespace(), mch.GetName())
	if annotations[annotationTeardownActive] != "true" {
		t.Fatalf("wrong value was not corrected: %v", annotations)
	}
	if annotations[annotationTeardownActiveVersion] != teardownContractVersion {
		t.Fatalf("missing version was not added: %v", annotations)
	}
}

func TestSetTeardownActiveAnnotation_CreatesNilMap(t *testing.T) {
	mch := newUnstructured(mchGVK, "open-cluster-management", "multiclusterhub")
	if mch.GetAnnotations() != nil {
		t.Fatal("fixture must start without annotations")
	}
	r := contractReconciler(t, mch)

	if err := r.setTeardownActiveAnnotation(context.Background(), ctrllog.Log); err != nil {
		t.Fatalf("setTeardownActiveAnnotation: %v", err)
	}
	if got := readMCHAnnotations(t, r, mch.GetNamespace(), mch.GetName()); len(got) != 2 {
		t.Fatalf("expected exactly the two contract keys, got %v", got)
	}
}

// E-6: every MCH is annotated; the contract is cluster-wide by design.
func TestSetTeardownActiveAnnotation_MultipleMCH(t *testing.T) {
	first := newUnstructured(mchGVK, "open-cluster-management", "hub-a")
	second := newUnstructured(mchGVK, "open-cluster-management", "hub-b")
	r := contractReconciler(t, first, second)

	if err := r.setTeardownActiveAnnotation(context.Background(), ctrllog.Log); err != nil {
		t.Fatalf("setTeardownActiveAnnotation: %v", err)
	}
	for _, name := range []string{"hub-a", "hub-b"} {
		if got := readMCHAnnotations(t, r, "open-cluster-management", name); got[annotationTeardownActive] != "true" {
			t.Fatalf("%s not annotated: %v", name, got)
		}
	}
}

// E-4: no MCH is a satisfied contract, not a failure.
func TestReadContract_NoMCHIsSatisfied(t *testing.T) {
	r := contractReconciler(t)

	record := r.readContract(context.Background(), ctrllog.Log)
	if record == nil || !record.Signalled {
		t.Fatalf("no-MCH contract must be satisfied, got %+v", record)
	}
	if record.Reason != "NoMCH" {
		t.Fatalf("reason = %q, want NoMCH", record.Reason)
	}
}

func TestReadContract_ReportsEveryMCH(t *testing.T) {
	mch := newUnstructured(mchGVK, "open-cluster-management", "multiclusterhub")
	mch.SetAnnotations(map[string]string{
		annotationTeardownActive:        "true",
		annotationTeardownActiveVersion: teardownContractVersion,
	})
	r := contractReconciler(t, mch)

	record := r.readContract(context.Background(), ctrllog.Log)
	if !record.Signalled || record.Reason != "Signalled" {
		t.Fatalf("contract should be signalled, got %+v", record)
	}
	if len(record.MCHs) != 1 || record.MCHs[0] != "multiclusterhub" {
		t.Fatalf("MCH list = %v", record.MCHs)
	}
}

func TestReadContract_MissingVersionIsIncomplete(t *testing.T) {
	mch := newUnstructured(mchGVK, "open-cluster-management", "multiclusterhub")
	mch.SetAnnotations(map[string]string{annotationTeardownActive: "true"})
	r := contractReconciler(t, mch)

	record := r.readContract(context.Background(), ctrllog.Log)
	if record.Signalled {
		t.Fatalf("a contract without the version must not be reported as signalled: %+v", record)
	}
}

// E-5: an absent MCH CRD is not an error.
func TestReadContract_NoMatchIsSatisfied(t *testing.T) {
	s := contractScheme(t)
	r := &HubTeardownReconciler{
		Scheme: s,
		Client: fake.NewClientBuilder().WithScheme(s).WithInterceptorFuncs(interceptor.Funcs{
			List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
				return &apimeta.NoKindMatchError{GroupKind: schema.GroupKind{Group: mchGVK.Group, Kind: mchGVK.Kind}}
			},
		}).Build(),
	}

	record := r.readContract(context.Background(), ctrllog.Log)
	if !record.Signalled || record.Reason != "NoMCH" {
		t.Fatalf("no-match must be treated as no MCH, got %+v", record)
	}
}

// E-7: cancelling the CR clears both keys.
func TestClearTeardownActiveAnnotation_RemovesBoth(t *testing.T) {
	mch := newUnstructured(mchGVK, "open-cluster-management", "multiclusterhub")
	mch.SetAnnotations(map[string]string{
		annotationTeardownActive:        "true",
		annotationTeardownActiveVersion: teardownContractVersion,
		"unrelated":                     "kept",
	})
	r := contractReconciler(t, mch)

	if err := r.clearTeardownActiveAnnotation(context.Background(), ctrllog.Log); err != nil {
		t.Fatalf("clearTeardownActiveAnnotation: %v", err)
	}

	annotations := readMCHAnnotations(t, r, mch.GetNamespace(), mch.GetName())
	if _, ok := annotations[annotationTeardownActive]; ok {
		t.Fatalf("teardown-active still present: %v", annotations)
	}
	if _, ok := annotations[annotationTeardownActiveVersion]; ok {
		t.Fatalf("teardown-active-version still present: %v", annotations)
	}
	if annotations["unrelated"] != "kept" {
		t.Fatalf("unrelated annotation was removed: %v", annotations)
	}
}

// P-3: the operator mirrors the Job's contract record into a condition.
func TestMirrorJobProgress_SetsContractCondition(t *testing.T) {
	r := &HubTeardownReconciler{}
	td := handoffTestCR()
	progress := &jobProgress{Contract: &contractRecord{
		Version:   teardownContractVersion,
		Signalled: true,
		MCHs:      []string{"multiclusterhub"},
		Reason:    "Signalled",
		Message:   "Signalled 1 MultiClusterHub(s) at contract version 1.",
	}}

	r.mirrorJobProgress(td, progress, r.preparePhasePlan(), "prepare")

	condition := apimeta.FindStatusCondition(td.Status.Conditions, conditionTypeContractReady)
	if condition == nil {
		t.Fatal("ContractReady condition was not written")
	}
	if condition.Status != metav1.ConditionTrue || condition.Reason != "Signalled" {
		t.Fatalf("condition = %+v", condition)
	}
}

func TestMirrorJobProgress_IncompleteContractIsFalse(t *testing.T) {
	r := &HubTeardownReconciler{}
	td := handoffTestCR()
	progress := &jobProgress{Contract: &contractRecord{
		Version: teardownContractVersion,
		Reason:  "SignallingIncomplete",
		Message: "missing",
	}}

	r.mirrorJobProgress(td, progress, r.preparePhasePlan(), "prepare")

	condition := apimeta.FindStatusCondition(td.Status.Conditions, conditionTypeContractReady)
	if condition == nil || condition.Status != metav1.ConditionFalse {
		t.Fatalf("condition = %+v", condition)
	}
}

// P-4/E: the Job loop records the contract after RemoveBlockingCRs so the
// operator can mirror it.
func TestRunJobLoop_RecordsContract(t *testing.T) {
	td := handoffTestCR()
	mch := newUnstructured(mchGVK, "open-cluster-management", "multiclusterhub")
	mch.SetAnnotations(map[string]string{
		annotationTeardownActive:        "true",
		annotationTeardownActiveVersion: teardownContractVersion,
	})
	r := contractReconciler(t, td, mch)

	plan := []teardownPhaseStep{fixedStep(operatorv1.TeardownPhaseRemoveBlockingCRs, true, nil)}
	if err := r.runJobLoop(context.Background(), ctrllog.Log, td, plan, time.Now().Add(time.Minute), time.Millisecond); err != nil {
		t.Fatalf("runJobLoop: %v", err)
	}

	progress, err := r.loadJobProgress(context.Background(), td)
	if err != nil {
		t.Fatalf("loadJobProgress: %v", err)
	}
	if progress.Contract == nil {
		t.Fatal("contract record was not written to progress")
	}
	if !progress.Contract.Signalled || progress.Contract.Reason != "Signalled" {
		t.Fatalf("contract = %+v", progress.Contract)
	}
}

// P-2: a second call must not issue another patch.
func TestSetTeardownActiveAnnotation_SecondCallDoesNotPatch(t *testing.T) {
	mch := newUnstructured(mchGVK, "open-cluster-management", "multiclusterhub")
	mch.SetAnnotations(map[string]string{
		annotationTeardownActive:        "true",
		annotationTeardownActiveVersion: teardownContractVersion,
	})
	s := contractScheme(t)
	patches := 0
	r := &HubTeardownReconciler{
		Scheme: s,
		Client: fake.NewClientBuilder().WithScheme(s).WithObjects(mch).WithInterceptorFuncs(interceptor.Funcs{
			Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
				patches++
				return c.Patch(ctx, obj, patch, opts...)
			},
		}).Build(),
	}

	if err := r.setTeardownActiveAnnotation(context.Background(), ctrllog.Log); err != nil {
		t.Fatalf("setTeardownActiveAnnotation: %v", err)
	}
	if patches != 0 {
		t.Fatalf("idempotent call issued %d patch(es)", patches)
	}
}
