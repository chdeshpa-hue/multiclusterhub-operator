// Copyright Contributors to the Open Cluster Management project

package controllers

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"time"

	operatorv1 "github.com/stolostron/multiclusterhub-operator/api/v1"
	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apixv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/config"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	subv1alpha1 "github.com/operator-framework/api/pkg/operators/v1alpha1"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("HubTeardown controller", func() {
	const (
		// The controller requeues active work every 15s, so a 30s window
		// allowed only two attempts and failed intermittently when the suite
		// was under load.
		tdTimeout  = time.Second * 90
		tdInterval = time.Millisecond * 250
		tdName     = "teardown"
		tdNs       = "open-cluster-management"
	)

	var (
		testEnv      *envtest.Environment
		k8sClient    client.Client
		clientScheme = runtime.NewScheme()
		ctxTD        context.Context
		cancelTD     context.CancelFunc
	)

	BeforeEach(func() {
		testEnv = &envtest.Environment{
			CRDDirectoryPaths: []string{
				filepath.Join("..", "config", "crd", "bases"),
			},
			CRDInstallOptions: envtest.CRDInstallOptions{
				CleanUpAfterUse: true,
			},
			ErrorIfCRDPathMissing: true,
		}

		By("bootstrapping HubTeardown test environment")
		var err error
		cfg, err := testEnv.Start()
		Expect(err).NotTo(HaveOccurred())
		Expect(cfg).NotTo(BeNil())

		Expect(operatorv1.AddToScheme(clientScheme)).Should(Succeed())
		Expect(corev1.AddToScheme(clientScheme)).Should(Succeed())
		Expect(batchv1.AddToScheme(clientScheme)).Should(Succeed())
		Expect(apixv1.AddToScheme(clientScheme)).Should(Succeed())
		Expect(subv1alpha1.AddToScheme(clientScheme)).Should(Succeed())
		Expect(admissionregistrationv1.AddToScheme(clientScheme)).Should(Succeed())
		Expect(rbacv1.AddToScheme(clientScheme)).Should(Succeed())

		k8sManager, err := ctrl.NewManager(cfg, ctrl.Options{
			Scheme: clientScheme,
			Controller: config.Controller{
				SkipNameValidation: ptr.To(true),
			},
			Metrics: metricsserver.Options{
				BindAddress: "0",
			},
			HealthProbeBindAddress: "0",
		})
		Expect(err).ToNot(HaveOccurred())

		k8sClient = k8sManager.GetClient()
		Expect(k8sClient).NotTo(BeNil())

		uncachedClient, err := client.New(cfg, client.Options{Scheme: clientScheme})
		Expect(err).NotTo(HaveOccurred())

		tdReconciler := &HubTeardownReconciler{
			Client:         k8sClient,
			UncachedClient: uncachedClient,
			Scheme:         k8sManager.GetScheme(),
			Log:            ctrl.Log.WithName("test").WithName("HubTeardown"),
		}
		Expect(tdReconciler.SetupWithManager(k8sManager)).Should(Succeed())

		ctxTD, cancelTD = context.WithCancel(context.Background())
		Expect(client.IgnoreAlreadyExists(uncachedClient.Create(ctxTD, &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{Name: tdNs},
		}))).To(Succeed())
		go func() {
			defer GinkgoRecover()
			Expect(k8sManager.Start(ctxTD)).Should(Succeed())
		}()
	})

	AfterEach(func() {
		cancelTD()
		Expect(testEnv.Stop()).Should(Succeed())
	})

	It("should set DryRunComplete condition in dry-run mode", func() {
		td := &operatorv1.HubTeardown{
			ObjectMeta: metav1.ObjectMeta{
				Name:      tdName,
				Namespace: tdNs,
			},
			Spec: operatorv1.HubTeardownSpec{
				DryRun: true,
			},
		}

		Expect(k8sClient.Create(ctxTD, td)).Should(Succeed())

		Eventually(func() bool {
			fetched := &operatorv1.HubTeardown{}
			if err := k8sClient.Get(ctxTD, types.NamespacedName{Name: tdName, Namespace: tdNs}, fetched); err != nil {
				return false
			}
			for _, c := range fetched.Status.Conditions {
				if c.Type == conditionTypeDryRunComplete && c.Status == metav1.ConditionTrue {
					return true
				}
			}
			return false
		}, tdTimeout, tdInterval).Should(BeTrue(), "DryRunComplete condition should be True")

		fetched := &operatorv1.HubTeardown{}
		Expect(k8sClient.Get(ctxTD, types.NamespacedName{Name: tdName, Namespace: tdNs}, fetched)).Should(Succeed())
		Expect(fetched.Status.Phase).To(Equal(operatorv1.TeardownPhaseReportReady))
		Expect(fetched.Status.DryRunReport).NotTo(BeNil())
	})

	It("should add the teardown finalizer on the CR", func() {
		td := &operatorv1.HubTeardown{
			ObjectMeta: metav1.ObjectMeta{
				Name:      tdName + "-fin",
				Namespace: tdNs,
			},
			Spec: operatorv1.HubTeardownSpec{
				DryRun: true,
			},
		}

		Expect(k8sClient.Create(ctxTD, td)).Should(Succeed())

		Eventually(func() bool {
			fetched := &operatorv1.HubTeardown{}
			if err := k8sClient.Get(ctxTD, types.NamespacedName{Name: td.Name, Namespace: tdNs}, fetched); err != nil {
				return false
			}
			for _, f := range fetched.GetFinalizers() {
				if f == teardownJobFinalizer {
					return true
				}
			}
			return false
		}, tdTimeout, tdInterval).Should(BeTrue(), "teardown finalizer should be added to the CR")
	})

	It("should hold at the approval boundary when dryRun is disabled without consent", func() {
		os.Setenv("OPERATOR_IMAGE", "quay.io/test/operator:latest")
		defer os.Unsetenv("OPERATOR_IMAGE")

		td := &operatorv1.HubTeardown{
			ObjectMeta: metav1.ObjectMeta{
				Name:      tdName + "-noapproval",
				Namespace: tdNs,
			},
			Spec: operatorv1.HubTeardownSpec{
				DryRun: false,
			},
		}

		Expect(k8sClient.Create(ctxTD, td)).Should(Succeed())

		// Destructive phases moved into the Job (increment 8). Without an
		// approval flag the operator must not start the Job and must not run
		// the phases itself.
		Eventually(func() string {
			fetched := &operatorv1.HubTeardown{}
			if err := k8sClient.Get(ctxTD, types.NamespacedName{Name: td.Name, Namespace: tdNs}, fetched); err != nil {
				return ""
			}
			for _, c := range fetched.Status.Conditions {
				if c.Type == conditionTypeTeardownProgressing && c.Reason == "WaitingForApproval" {
					return c.Reason
				}
			}
			return ""
		}, tdTimeout, tdInterval).Should(Equal("WaitingForApproval"))

		Consistently(func() bool {
			job := &batchv1.Job{}
			err := k8sClient.Get(ctxTD, types.NamespacedName{Name: prepareJobName, Namespace: tdNs}, job)
			return err == nil
		}, time.Second*2, tdInterval).Should(BeFalse(), "no Job may exist before consent")
	})

	It("should hand the destructive phases to the Job and finish once it reports progress", func() {
		os.Setenv("OPERATOR_IMAGE", "quay.io/test/operator:latest")
		defer os.Unsetenv("OPERATOR_IMAGE")

		// The role ships with the bundle; envtest installs CRDs only.
		Expect(k8sClient.Create(ctxTD, &rbacv1.ClusterRole{
			ObjectMeta: metav1.ObjectMeta{Name: handoffRoleName},
		})).Should(Succeed())

		td := &operatorv1.HubTeardown{
			ObjectMeta: metav1.ObjectMeta{
				Name:      tdName + "-active",
				Namespace: tdNs,
			},
			Spec: operatorv1.HubTeardownSpec{
				DryRun:                     false,
				ApprovedDestructiveActions: true,
			},
		}

		Expect(k8sClient.Create(ctxTD, td)).Should(Succeed())

		// Consent is bound to the inspection report (increment 16). Even a CR
		// created directly in active mode gets a report written on its first
		// reconcile; the operator then holds at ConsentLapsed until the
		// acknowledged digest matches that report.
		Eventually(func() string {
			fetched := &operatorv1.HubTeardown{}
			if err := k8sClient.Get(ctxTD, types.NamespacedName{Name: td.Name, Namespace: tdNs}, fetched); err != nil {
				return ""
			}
			for _, c := range fetched.Status.Conditions {
				if c.Type == conditionTypeTeardownProgressing && c.Reason == "ConsentLapsed" {
					return fetched.Status.ReportDigest
				}
			}
			return ""
		}, tdTimeout, tdInterval).ShouldNot(BeEmpty(), "the operator must write a report and hold for consent")

		Eventually(func() error {
			fetched := &operatorv1.HubTeardown{}
			if err := k8sClient.Get(ctxTD, types.NamespacedName{Name: td.Name, Namespace: tdNs}, fetched); err != nil {
				return err
			}
			if fetched.Status.ReportDigest == "" {
				return errors.New("no inspection report digest yet")
			}
			fetched.Spec.AcknowledgedReportDigest = fetched.Status.ReportDigest
			return k8sClient.Update(ctxTD, fetched)
		}, tdTimeout, tdInterval).Should(Succeed(), "the administrator acknowledges the report digest")

		Eventually(func() string {
			job := &batchv1.Job{}
			if err := k8sClient.Get(ctxTD, types.NamespacedName{Name: prepareJobName, Namespace: tdNs}, job); err != nil {
				return ""
			}
			return job.Spec.Template.Spec.ServiceAccountName
		}, tdTimeout, tdInterval).Should(Equal(handoffSAName), "Job must run as the handoff identity")

		Eventually(func() string {
			fetched := &operatorv1.HubTeardown{}
			if err := k8sClient.Get(ctxTD, types.NamespacedName{Name: td.Name, Namespace: tdNs}, fetched); err != nil {
				return ""
			}
			for _, c := range fetched.Status.Conditions {
				if c.Type == conditionTypeTeardownProgressing {
					return c.Reason
				}
			}
			return ""
		}, tdTimeout, tdInterval).Should(Equal("PrepareJobRunning"))

		// The Job pod cannot run in envtest, so record its progress the way the
		// Job does. The operator must then continue with its own plan.
		progress := &jobProgress{}
		for _, step := range (&HubTeardownReconciler{}).preparePhasePlan() {
			recordJobPhase(progress, step.phase, operatorv1.PhaseStateComplete, "seeded by envtest")
		}
		body, err := json.Marshal(progress)
		Expect(err).NotTo(HaveOccurred())
		Expect(k8sClient.Create(ctxTD, &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Name: progressConfigMapName(td), Namespace: tdNs},
			Data:       map[string]string{progressKeyPhases: string(body)},
		})).Should(Succeed())

		// The prepare Job must reach a terminal condition before the handoff Job
		// may start (D9b-2): the Job controller would set it, so the spec sets it.
		Eventually(func() error {
			job := &batchv1.Job{}
			if err := k8sClient.Get(ctxTD, types.NamespacedName{Name: prepareJobName, Namespace: tdNs}, job); err != nil {
				return err
			}
			if len(job.Status.Conditions) > 0 && job.Status.Conditions[0].Status == corev1.ConditionTrue {
				return nil
			}
			now := metav1.Now()
			job.Status.StartTime = &now
			job.Status.CompletionTime = &now
			// Kubernetes 1.31+ requires SuccessCriteriaMet before Complete, and
			// rejects a finished Job without startTime/completionTime.
			job.Status.Conditions = []batchv1.JobCondition{
				{Type: batchv1.JobComplete, Status: corev1.ConditionTrue},
				{Type: batchv1.JobSuccessCriteriaMet, Status: corev1.ConditionTrue},
			}
			return k8sClient.Status().Update(ctxTD, job)
		}, tdTimeout, tdInterval).Should(Succeed())

		// The handoff Job is the only executor of MonitorMCEChain and
		// DeleteACMCRDs. It must appear only after the prepare Job is terminal.
		handoffJob := &batchv1.Job{}
		Eventually(func() error {
			return k8sClient.Get(ctxTD, types.NamespacedName{Name: handoffJobName, Namespace: tdNs}, handoffJob)
		}, tdTimeout, tdInterval).Should(Succeed())
		Expect(handoffJob.Spec.Template.Spec.ServiceAccountName).Should(Equal(handoffSAName))
		Expect(handoffJob.Spec.Template.Spec.Containers[0].Args).Should(ContainElement("--teardown-plan=handoff"))

		// Record the handoff plan's completion the way the Job does. The operator
		// reads completion from the progress record, not from the Job condition,
		// so nothing else has to be simulated here.
		cm := &corev1.ConfigMap{}
		Expect(k8sClient.Get(ctxTD, types.NamespacedName{Name: progressConfigMapName(td), Namespace: tdNs}, cm)).Should(Succeed())
		all := &jobProgress{}
		Expect(json.Unmarshal([]byte(cm.Data[progressKeyPhases]), all)).Should(Succeed())
		for _, step := range (&HubTeardownReconciler{}).handoffPhasePlan() {
			recordJobPhase(all, step.phase, operatorv1.PhaseStateComplete, "seeded by envtest")
		}
		body, err = json.Marshal(all)
		Expect(err).NotTo(HaveOccurred())
		cm.Data[progressKeyPhases] = string(body)
		Expect(k8sClient.Update(ctxTD, cm)).Should(Succeed())

		Eventually(func() operatorv1.TeardownPhase {
			fetched := &operatorv1.HubTeardown{}
			if err := k8sClient.Get(ctxTD, types.NamespacedName{Name: td.Name, Namespace: tdNs}, fetched); err != nil {
				return ""
			}
			return fetched.Status.Phase
		}, tdTimeout, tdInterval).Should(Equal(operatorv1.TeardownPhaseComplete), "teardown should reach Complete after the Job reports progress")

		fetched := &operatorv1.HubTeardown{}
		Expect(k8sClient.Get(ctxTD, types.NamespacedName{Name: td.Name, Namespace: tdNs}, fetched)).Should(Succeed())
		foundComplete := false
		for _, c := range fetched.Status.Conditions {
			if c.Type == conditionTypeTeardownComplete && c.Status == metav1.ConditionTrue {
				foundComplete = true
			}
		}
		Expect(foundComplete).To(BeTrue(), "Complete condition should be True")
	})

	It("should pause execution when dryRun is toggled back to true", func() {
		td := &operatorv1.HubTeardown{
			ObjectMeta: metav1.ObjectMeta{
				Name:      tdName + "-pause",
				Namespace: tdNs,
			},
			Spec: operatorv1.HubTeardownSpec{
				DryRun: true,
			},
		}

		Expect(k8sClient.Create(ctxTD, td)).Should(Succeed())

		// Wait for dry-run to complete
		Eventually(func() bool {
			fetched := &operatorv1.HubTeardown{}
			if err := k8sClient.Get(ctxTD, types.NamespacedName{Name: td.Name, Namespace: tdNs}, fetched); err != nil {
				return false
			}
			return fetched.Status.Phase == operatorv1.TeardownPhaseReportReady
		}, tdTimeout, tdInterval).Should(BeTrue())

		// Switch to active
		fetched := &operatorv1.HubTeardown{}
		Expect(k8sClient.Get(ctxTD, types.NamespacedName{Name: td.Name, Namespace: tdNs}, fetched)).Should(Succeed())
		fetched.Spec.DryRun = false
		Expect(k8sClient.Update(ctxTD, fetched)).Should(Succeed())

		// Wait for it to be in progress or complete, then toggle back
		time.Sleep(500 * time.Millisecond)
		Expect(k8sClient.Get(ctxTD, types.NamespacedName{Name: td.Name, Namespace: tdNs}, fetched)).Should(Succeed())
		fetched.Spec.DryRun = true
		Expect(k8sClient.Update(ctxTD, fetched)).Should(Succeed())

		// Verify it goes back to dry-run
		Eventually(func() operatorv1.TeardownPhase {
			latest := &operatorv1.HubTeardown{}
			if err := k8sClient.Get(ctxTD, types.NamespacedName{Name: td.Name, Namespace: tdNs}, latest); err != nil {
				return ""
			}
			return latest.Status.Phase
		}, tdTimeout, tdInterval).Should(Equal(operatorv1.TeardownPhaseReportReady))
	})
})
