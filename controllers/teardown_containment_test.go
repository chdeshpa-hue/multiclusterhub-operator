// Copyright Contributors to the Open Cluster Management project

package controllers

import (
	"context"
	"testing"
	"time"

	apixv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	ctrllog "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
)

func containmentCRD(name, group, kind, plural string) *apixv1.CustomResourceDefinition {
	return &apixv1.CustomResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: apixv1.CustomResourceDefinitionSpec{
			Group: group,
			Names: apixv1.CustomResourceDefinitionNames{Kind: kind, Plural: plural},
			Versions: []apixv1.CustomResourceDefinitionVersion{
				{Name: "v1", Served: true, Storage: true},
			},
		},
	}
}

func TestPhaseDeleteACMCRDsRetainsMetal3CRD(t *testing.T) {
	ctrllog.SetLogger(zap.New(zap.UseDevMode(true)))
	s := newTestScheme()
	acmCRD := containmentCRD("managedclusters.cluster.open-cluster-management.io",
		"cluster.open-cluster-management.io", "ManagedCluster", "managedclusters")
	bmoCRD := containmentCRD("baremetalhosts.metal3.io", "metal3.io", "BareMetalHost", "baremetalhosts")
	// Even a platform CRD that somehow carries the installer labels is not
	// eligible: group ownership is a hard boundary, not a confidence input.
	bmoCRD.Labels = map[string]string{
		"installer.name":      testHubRef().Name,
		"installer.namespace": testHubRef().Namespace,
	}
	r := &HubTeardownReconciler{
		Client:   fake.NewClientBuilder().WithScheme(s).WithObjects(acmCRD, bmoCRD).Build(),
		Scheme:   s,
		Log:      ctrllog.Log,
		Recorder: record.NewFakeRecorder(20),
	}
	td := phaseDeleteACMCRDsTestTD()
	ctx := context.Background()

	if _, err := r.phaseDeleteACMCRDs(ctx, ctrllog.Log.WithName("test"), td); err != nil {
		t.Fatalf("phaseDeleteACMCRDs error: %v", err)
	}

	got := &apixv1.CustomResourceDefinition{}
	if err := r.Client.Get(ctx, types.NamespacedName{Name: acmCRD.Name}, got); !apierrors.IsNotFound(err) {
		t.Fatalf("ACM CRD get error = %v, want NotFound", err)
	}
	if err := r.Client.Get(ctx, types.NamespacedName{Name: bmoCRD.Name}, got); err != nil {
		t.Fatalf("metal3 CRD must be retained: %v", err)
	}
}

func TestDisableConversionWebhookBeforeDelete(t *testing.T) {
	ctrllog.SetLogger(zap.New(zap.UseDevMode(true)))
	s := newTestScheme()
	crd := containmentCRD("managedclusters.cluster.open-cluster-management.io",
		"cluster.open-cluster-management.io", "ManagedCluster", "managedclusters")
	crd.Spec.Conversion = &apixv1.CustomResourceConversion{
		Strategy: apixv1.WebhookConverter,
		Webhook:  &apixv1.WebhookConversion{ConversionReviewVersions: []string{"v1"}},
	}
	r := &HubTeardownReconciler{
		Client:   fake.NewClientBuilder().WithScheme(s).WithObjects(crd).Build(),
		Scheme:   s,
		Log:      ctrllog.Log,
		Recorder: record.NewFakeRecorder(20),
	}
	ctx := context.Background()

	if err := r.disableConversionWebhookBeforeDelete(ctx, ctrllog.Log.WithName("test"), crd); err != nil {
		t.Fatalf("disableConversionWebhookBeforeDelete error: %v", err)
	}

	got := &apixv1.CustomResourceDefinition{}
	if err := r.Client.Get(ctx, types.NamespacedName{Name: crd.Name}, got); err != nil {
		t.Fatalf("getting CRD: %v", err)
	}
	if got.Spec.Conversion == nil || got.Spec.Conversion.Strategy != apixv1.NoneConverter {
		t.Fatalf("conversion = %+v, want strategy None", got.Spec.Conversion)
	}
	if got.Spec.Conversion.Webhook != nil {
		t.Fatalf("conversion webhook = %+v, want nil", got.Spec.Conversion.Webhook)
	}
}

func TestPhaseDetachManagedClustersRetainsUnknownFinalizer(t *testing.T) {
	ctrllog.SetLogger(zap.New(zap.UseDevMode(true)))
	td := phaseDeleteACMCRDsTestTD()
	deletedAt := metav1.NewTime(time.Now().Add(-10 * time.Minute))
	mc := newUnstructured(managedClusterGVK, "", "spoke", "example.com/third-party")
	mc.SetDeletionTimestamp(&deletedAt)
	r := newGateTestReconciler(t, mc)
	ctx := context.Background()

	done, err := r.phaseDetachManagedClusters(ctx, ctrllog.Log.WithName("test"), td)
	if err != nil {
		t.Fatalf("phaseDetachManagedClusters error: %v", err)
	}
	if done {
		t.Fatal("phase reported done while the ManagedCluster still terminates")
	}

	got := &unstructured.Unstructured{}
	got.SetGroupVersionKind(managedClusterGVK)
	if err := r.Client.Get(ctx, types.NamespacedName{Name: mc.GetName()}, got); err != nil {
		t.Fatalf("getting ManagedCluster: %v", err)
	}
	if len(got.GetFinalizers()) != 1 || got.GetFinalizers()[0] != "example.com/third-party" {
		t.Fatalf("finalizers = %v, want the unknown finalizer retained", got.GetFinalizers())
	}
}
