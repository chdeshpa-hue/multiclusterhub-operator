// Copyright Contributors to the Open Cluster Management project

package v1

import (
	"strings"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	admissionregistration "k8s.io/api/admissionregistration/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func duration(d time.Duration) *metav1.Duration {
	return &metav1.Duration{Duration: d}
}

// W-8..W-12: the duration rules are pure, so they are tested without a cluster.
func TestValidateHubTeardownDurations(t *testing.T) {
	cases := []struct {
		name        string
		maxDuration *metav1.Duration
		force       *metav1.Duration
		wantErr     string
	}{
		{name: "both unset is valid", wantErr: ""},
		{name: "zero maxDuration", maxDuration: duration(0), wantErr: "greater than zero"},
		{name: "negative maxDuration", maxDuration: duration(-5 * time.Minute), wantErr: "greater than zero"},
		{name: "maxDuration above ceiling", maxDuration: duration(30 * time.Hour), wantErr: "must not exceed"},
		{name: "maxDuration at ceiling", maxDuration: duration(24 * time.Hour), wantErr: ""},
		{name: "zero forceFinalizerTimeout", force: duration(0), wantErr: "greater than zero"},
		{name: "negative forceFinalizerTimeout", force: duration(-time.Minute), wantErr: "greater than zero"},
		{name: "force equals max", maxDuration: duration(2 * time.Hour), force: duration(2 * time.Hour), wantErr: "must be shorter"},
		{name: "force greater than max", maxDuration: duration(30 * time.Minute), force: duration(time.Hour), wantErr: "must be shorter"},
		{name: "force shorter than max", maxDuration: duration(2 * time.Hour), force: duration(5 * time.Minute), wantErr: ""},
		{name: "force only", force: duration(5 * time.Minute), wantErr: ""},
		{name: "max only", maxDuration: duration(45 * time.Minute), wantErr: ""},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			obj := &HubTeardown{
				Spec: HubTeardownSpec{
					MaxDuration:           tc.maxDuration,
					ForceFinalizerTimeout: tc.force,
				},
			}
			err := validateHubTeardownDurations(obj)
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("expected no error, got %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("expected an error containing %q, got nil", tc.wantErr)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("expected an error containing %q, got %q", tc.wantErr, err.Error())
			}
		})
	}
}

// W-7 and W-16: the rule must not cover the status subresource, and the
// configuration must be its own object so the MCH CRD's garbage collection
// does not remove it.
func TestHubTeardownValidatingWebhookShape(t *testing.T) {
	cfg := HubTeardownValidatingWebhook("test-ns")

	if cfg.Name != hubTeardownWebhookConfigName {
		t.Fatalf("config name = %q, want %q", cfg.Name, hubTeardownWebhookConfigName)
	}
	if cfg.Name == ValidatingWebhook("test-ns").Name {
		t.Fatalf("HubTeardown webhook must not share the MultiClusterHub configuration")
	}
	if len(cfg.Webhooks) != 1 {
		t.Fatalf("expected 1 webhook entry, got %d", len(cfg.Webhooks))
	}

	wh := cfg.Webhooks[0]
	if wh.Name != hubTeardownWebhookName {
		t.Fatalf("webhook name = %q", wh.Name)
	}
	if wh.ClientConfig.Service == nil {
		t.Fatalf("expected a service client config")
	}
	if wh.ClientConfig.Service.Name != "multiclusterhub-operator-webhook" ||
		wh.ClientConfig.Service.Namespace != "test-ns" ||
		wh.ClientConfig.Service.Path == nil ||
		*wh.ClientConfig.Service.Path != hubTeardownWebhookPath {
		t.Fatalf("unexpected client config: %#v", wh.ClientConfig.Service)
	}
	if wh.FailurePolicy == nil || *wh.FailurePolicy != admissionregistration.Fail {
		t.Fatalf("failurePolicy must be Fail")
	}
	if len(wh.Rules) != 1 {
		t.Fatalf("expected 1 rule, got %d", len(wh.Rules))
	}
	for _, resource := range wh.Rules[0].Resources {
		if resource != "hubteardowns" {
			t.Fatalf("rule must name exactly \"hubteardowns\", got %q", resource)
		}
	}
	if len(wh.Rules[0].Resources) != 1 {
		t.Fatalf("rule must name exactly one resource, got %v", wh.Rules[0].Resources)
	}
	wantOps := []admissionregistration.OperationType{
		admissionregistration.Create,
		admissionregistration.Update,
	}
	if len(wh.Rules[0].Operations) != len(wantOps) {
		t.Fatalf("operations = %v, want %v", wh.Rules[0].Operations, wantOps)
	}
	for i := range wantOps {
		if wh.Rules[0].Operations[i] != wantOps[i] {
			t.Fatalf("operations = %v, want %v", wh.Rules[0].Operations, wantOps)
		}
	}
}

func deleteAllHubTeardowns() {
	list := &HubTeardownList{}
	Expect(k8sClient.List(ctx, list)).To(Succeed())
	for i := range list.Items {
		Expect(k8sClient.Delete(ctx, &list.Items[i])).To(Succeed())
	}
}

var _ = Describe("HubTeardown admission", func() {
	AfterEach(func() {
		deleteAllHubTeardowns()
	})

	newTeardown := func(name, namespace string) *HubTeardown {
		return &HubTeardown{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
			Spec:       HubTeardownSpec{DryRun: true},
		}
	}

	It("admits the first teardown (W-1, W-12)", func() {
		Expect(k8sClient.Create(ctx, newTeardown("admit-first", "default"))).To(Succeed())
	})

	It("rejects a second teardown in another namespace (W-4)", func() {
		// envtest has no open-cluster-management namespace; kube-system is an
		// equally unrelated second namespace for this check.
		Expect(k8sClient.Create(ctx, newTeardown("admit-other", "kube-system"))).To(Succeed())

		err := k8sClient.Create(ctx, newTeardown("admit-default", "default"))
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("only one teardown may run per hub"))
	})

	It("rejects a second teardown in the same namespace (W-5)", func() {
		Expect(k8sClient.Create(ctx, newTeardown("admit-a", "default"))).To(Succeed())

		err := k8sClient.Create(ctx, newTeardown("admit-b", "default"))
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("only one teardown may run per hub"))
	})

	It("admits an update to the existing teardown (W-3, W-6)", func() {
		td := newTeardown("admit-update", "default")
		Expect(k8sClient.Create(ctx, td)).To(Succeed())

		var current HubTeardown
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(td), &current)).To(Succeed())
		current.Spec.MaxDuration = duration(45 * time.Minute)
		Expect(k8sClient.Update(ctx, &current)).To(Succeed())
	})

	It("rejects a zero maxDuration at admission (W-8)", func() {
		td := newTeardown("admit-zero", "default")
		td.Spec.MaxDuration = duration(0)

		err := k8sClient.Create(ctx, td)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("greater than zero"))
	})

	It("rejects forceFinalizerTimeout longer than maxDuration (W-10)", func() {
		td := newTeardown("admit-order", "default")
		td.Spec.MaxDuration = duration(time.Hour)
		td.Spec.ForceFinalizerTimeout = duration(2 * time.Hour)

		err := k8sClient.Create(ctx, td)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("must be shorter"))
	})
})
