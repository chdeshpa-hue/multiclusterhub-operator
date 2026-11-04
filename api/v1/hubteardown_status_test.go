// Copyright Contributors to the Open Cluster Management project

package v1

import (
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// These specs exercise the generated CRD schema through a real API server.
// They are the only place where "the field survived admission" can be proven;
// a Go round-trip test cannot detect a schema that prunes.
var _ = Describe("HubTeardown status schema", func() {
	newTeardown := func(name string) *HubTeardown {
		return &HubTeardown{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
			Spec:       HubTeardownSpec{DryRun: true},
		}
	}

	It("persists a blocker with its ownership verdict (A-2)", func() {
		ht := newTeardown("schema-blockers")
		Expect(k8sClient.Create(ctx, ht)).To(Succeed())

		ht.Status.Blockers = []Blocker{fullyPopulatedBlocker()}
		ht.Status.Summary = &BlockerSummary{Total: 1, RequiringConsent: 1}
		Expect(k8sClient.Status().Update(ctx, ht)).To(Succeed())

		var got HubTeardown
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: ht.Name, Namespace: ht.Namespace}, &got)).To(Succeed())
		Expect(got.Status.Blockers).To(HaveLen(1))
		Expect(got.Status.Blockers[0].UID).To(Equal(ht.Status.Blockers[0].UID))
		Expect(got.Status.Blockers[0].Ownership.Confidence).To(Equal(ConfidenceStrong))
		Expect(got.Status.Blockers[0].OwningController).NotTo(BeNil())
		Expect(got.Status.Summary).NotTo(BeNil())
		Expect(got.Status.Summary.Total).To(Equal(int32(1)))
	})

	It("accepts the prototype phase and rejects a near-miss (A-3, A-4, A-6)", func() {
		ht := newTeardown("schema-phases")
		Expect(k8sClient.Create(ctx, ht)).To(Succeed())

		for _, phase := range []TeardownPhase{
			TeardownPhaseVerifying,
			TeardownPhaseCompleteWithResidue,
			TeardownPhaseCleanOrphans,
		} {
			ht.Status.Phase = phase
			Expect(k8sClient.Status().Update(ctx, ht)).To(Succeed(), "phase %q should be accepted", phase)
		}

		ht.Status.Phase = TeardownPhase("Verify")
		err := k8sClient.Status().Update(ctx, ht)
		Expect(err).To(HaveOccurred(), "near-miss phase must be rejected by the enum")
	})

	It("caps the blocker list at 32 (A-9)", func() {
		ht := newTeardown("schema-cap")
		Expect(k8sClient.Create(ctx, ht)).To(Succeed())

		blockers := make([]Blocker, 33)
		for i := range blockers {
			blockers[i] = fullyPopulatedBlocker()
			blockers[i].Name = fmt.Sprintf("blocker-%d", i)
		}
		ht.Status.Blockers = blockers
		Expect(k8sClient.Status().Update(ctx, ht)).NotTo(Succeed(), "33 blockers must exceed the cap")
	})

	It("mirrors counts onto MultiClusterHub status (A-5)", func() {
		mch := &MultiClusterHub{
			ObjectMeta: metav1.ObjectMeta{Name: "schema-mch", Namespace: "default"},
			Spec:       MultiClusterHubSpec{},
		}
		Expect(k8sClient.Create(ctx, mch)).To(Succeed())

		mch.Status.Uninstall = &UninstallMirror{
			TeardownRef:    corev1.LocalObjectReference{Name: "schema-mch-teardown"},
			Phase:          TeardownPhaseVerifying,
			Summary:        BlockerSummary{Total: 3, RequiringConsent: 1},
			LastInspection: metav1.NewTime(metav1.Now().UTC().Truncate(time.Second)),
		}
		Expect(k8sClient.Status().Update(ctx, mch)).To(Succeed(), "non-fatal mirror write must be accepted")

		var got MultiClusterHub
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: mch.Name, Namespace: mch.Namespace}, &got)).To(Succeed())
		Expect(got.Status.Uninstall).NotTo(BeNil())
		Expect(got.Status.Uninstall.Phase).To(Equal(TeardownPhaseVerifying))
		Expect(got.Status.Uninstall.Summary.Total).To(Equal(int32(3)))

		// The hub admits one MultiClusterHub at a time; leaving this object
		// behind would break every later spec in the shared suite.
		Expect(k8sClient.Delete(ctx, mch)).To(Succeed())
		Eventually(func() bool {
			err := k8sClient.Get(ctx, types.NamespacedName{Name: mch.Name, Namespace: mch.Namespace}, &MultiClusterHub{})
			return err != nil
		}).Should(BeTrue(), "test MCH must be gone before the next spec")
	})
})
