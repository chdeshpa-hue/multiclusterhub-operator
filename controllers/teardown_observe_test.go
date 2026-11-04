// Copyright Contributors to the Open Cluster Management project

package controllers

import (
	"context"
	"errors"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	ctrllog "sigs.k8s.io/controller-runtime/pkg/log"
)

// P-1/N-1: only core-group objects in the preserved namespace are collected.
func TestObserveForVerification_CollectsOperatorNamespaceObjects(t *testing.T) {
	td := handoffTestCR()
	inNamespace := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "leftover", Namespace: td.Namespace}}
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "tls", Namespace: td.Namespace}}
	elsewhere := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "other", Namespace: "default"}}
	r := newHandoffReconciler(t, td, inNamespace, secret, elsewhere)

	input, err := r.observeForVerification(context.Background(), ctrllog.Log, td)
	if err != nil {
		t.Fatalf("observeForVerification: %v", err)
	}

	if len(input.OperatorNamespaceObjects) != 2 {
		t.Fatalf("collected %d objects, want 2: %+v", len(input.OperatorNamespaceObjects), input.OperatorNamespaceObjects)
	}
	for _, obj := range input.OperatorNamespaceObjects {
		if obj.Namespace != td.Namespace {
			t.Fatalf("collected an object outside the operator namespace: %+v", obj)
		}
	}
}

// N-2: a forbidden list must be recorded as incomplete, not treated as absence.
func TestObserveForVerification_ForbiddenCoreListIsIncomplete(t *testing.T) {
	s := newTestScheme()
	r := &HubTeardownReconciler{
		Scheme: s,
		Client: fake.NewClientBuilder().WithScheme(s).WithInterceptorFuncs(interceptor.Funcs{
			List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
				if _, ok := list.(*corev1.ConfigMapList); ok {
					return apierrors.NewForbidden(
						schema.GroupResource{Resource: "configmaps"}, "", errors.New("denied by test"))
				}
				return c.List(ctx, list, opts...)
			},
		}).Build(),
	}

	input, err := r.observeForVerification(context.Background(), ctrllog.Log, handoffTestCR())
	if err != nil {
		t.Fatalf("observeForVerification: %v", err)
	}

	found := false
	for _, incomplete := range input.Incomplete {
		if strings.HasPrefix(incomplete, "configmaps:") {
			found = true
		}
	}
	if !found {
		t.Fatalf("forbidden ConfigMap list was not recorded as incomplete: %+v", input.Incomplete)
	}
}
