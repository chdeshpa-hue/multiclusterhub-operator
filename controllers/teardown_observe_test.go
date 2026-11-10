// Copyright Contributors to the Open Cluster Management project

package controllers

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/stolostron/multiclusterhub-operator/pkg/verify"
	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
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

// F-v30-LIVE-1: ownership and backing-Service absence are separate facts. This
// webhook is unlabelled and lives in the preserved namespace, which used to
// drop it from verification entirely.
func TestObserveForVerification_UnlabelledDanglingWebhookIsDangling(t *testing.T) {
	td := handoffTestCR()
	vwc := validatingWebhookNamed("application-webhook-validator",
		admissionregistrationv1.ServiceReference{
			Namespace: td.Namespace,
			Name:      "multicluster-operators-application-svc",
		})
	r := newHandoffReconciler(t, td, vwc)

	input, err := r.observeForVerification(context.Background(), ctrllog.Log, td)
	if err != nil {
		t.Fatalf("observeForVerification: %v", err)
	}

	hook := webhookRefByName(t, input.Webhooks, vwc.Name)
	if hook.ACMOwned {
		t.Fatal("the fixture is unlabelled; ACMOwned must be false")
	}
	if !hook.ServicesAbsent {
		t.Fatalf("absent backing Service was not observed: %+v", hook)
	}
	if len(hook.BackingServices) != 1 ||
		hook.BackingServices[0].Namespace != td.Namespace ||
		hook.BackingServices[0].Name != "multicluster-operators-application-svc" {
		t.Fatalf("backing Services = %+v", hook.BackingServices)
	}
}

// One live backing Service is enough to disprove dangling; the registration is
// not residue and must not be reported as such.
func TestObserveForVerification_OnePresentBackingServiceIsNotDangling(t *testing.T) {
	td := handoffTestCR()
	vwc := validatingWebhookNamed("mixed-backing",
		admissionregistrationv1.ServiceReference{Namespace: td.Namespace, Name: "absent-svc"},
		admissionregistrationv1.ServiceReference{Namespace: td.Namespace, Name: "present-svc"},
	)
	present := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: "present-svc", Namespace: td.Namespace}}
	r := newHandoffReconciler(t, td, vwc, present)

	input, err := r.observeForVerification(context.Background(), ctrllog.Log, td)
	if err != nil {
		t.Fatalf("observeForVerification: %v", err)
	}

	hook := webhookRefByName(t, input.Webhooks, vwc.Name)
	if hook.ServicesAbsent {
		t.Fatalf("one present backing Service disproves dangling: %+v", hook)
	}
	if len(hook.BackingServices) != 2 {
		t.Fatalf("backing Services = %+v, want both", hook.BackingServices)
	}
}

// A URL-based platform webhook has no Service reference, so its backing cannot
// be proven absent and it stays ignored.
func TestObserveForVerification_URLWebhookIsNotDangling(t *testing.T) {
	td := handoffTestCR()
	vwc := urlValidatingWebhookNamed("network-node-identity.openshift.io")
	r := newHandoffReconciler(t, td, vwc)

	input, err := r.observeForVerification(context.Background(), ctrllog.Log, td)
	if err != nil {
		t.Fatalf("observeForVerification: %v", err)
	}

	hook := webhookRefByName(t, input.Webhooks, vwc.Name)
	if hook.ServicesAbsent || len(hook.BackingServices) != 0 {
		t.Fatalf("URL-based webhook must not be dangling: %+v", hook)
	}
}

// A read failure is not absence. It must invalidate the absence claim rather
// than silently classify the registration as healthy.
func TestObserveForVerification_ServiceReadErrorIsIncomplete(t *testing.T) {
	td := handoffTestCR()
	vwc := validatingWebhookNamed("read-error",
		admissionregistrationv1.ServiceReference{Namespace: td.Namespace, Name: "some-svc"})
	s := newTestScheme()
	r := &HubTeardownReconciler{
		Scheme: s,
		Client: fake.NewClientBuilder().WithScheme(s).WithObjects(td, vwc).WithInterceptorFuncs(interceptor.Funcs{
			Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				if _, ok := obj.(*corev1.Service); ok {
					return apierrors.NewForbidden(
						schema.GroupResource{Resource: "services"}, key.Name, errors.New("denied by test"))
				}
				return c.Get(ctx, key, obj, opts...)
			},
		}).Build(),
	}

	input, err := r.observeForVerification(context.Background(), ctrllog.Log, td)
	if err != nil {
		t.Fatalf("observeForVerification: %v", err)
	}

	found := false
	for _, incomplete := range input.Incomplete {
		if strings.HasPrefix(incomplete, "validatingwebhookconfigurations/read-error:") {
			found = true
		}
	}
	if !found {
		t.Fatalf("Service read error was not recorded as incomplete: %+v", input.Incomplete)
	}
	if webhookRefByName(t, input.Webhooks, vwc.Name).ServicesAbsent {
		t.Fatal("an unreadable Service must not be reported as absent")
	}
}

func validatingWebhookNamed(name string, services ...admissionregistrationv1.ServiceReference) *admissionregistrationv1.ValidatingWebhookConfiguration {
	hooks := make([]admissionregistrationv1.ValidatingWebhook, 0, len(services))
	for i := range services {
		service := services[i]
		hooks = append(hooks, admissionregistrationv1.ValidatingWebhook{
			Name:         fmt.Sprintf("%s-%d.rfe9332.example.com", name, i),
			ClientConfig: admissionregistrationv1.WebhookClientConfig{Service: &service},
		})
	}
	return &admissionregistrationv1.ValidatingWebhookConfiguration{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Webhooks:   hooks,
	}
}

func urlValidatingWebhookNamed(name string) *admissionregistrationv1.ValidatingWebhookConfiguration {
	url := "https://platform.example.com/validate"
	return &admissionregistrationv1.ValidatingWebhookConfiguration{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Webhooks: []admissionregistrationv1.ValidatingWebhook{{
			Name:         name + ".rfe9332.example.com",
			ClientConfig: admissionregistrationv1.WebhookClientConfig{URL: &url},
		}},
	}
}

func webhookRefByName(t *testing.T, hooks []verify.WebhookRef, name string) verify.WebhookRef {
	t.Helper()
	for _, hook := range hooks {
		if hook.Name == name {
			return hook
		}
	}
	t.Fatalf("webhook %q was not observed: %+v", name, hooks)
	return verify.WebhookRef{}
}
