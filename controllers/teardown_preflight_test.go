// Copyright Contributors to the Open Cluster Management project

package controllers

import (
	"context"
	"strings"
	"testing"
	"time"

	operatorv1 "github.com/stolostron/multiclusterhub-operator/api/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	apiregistrationv1 "k8s.io/kube-aggregator/pkg/apis/apiregistration/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	ctrllog "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
)

const (
	staleAPIServiceName = "v1alpha1.t8unowned.rfe9332.example.com"
	staleServiceNS      = "default"
	staleServiceName    = "t8-unowned-svc"
)

func newPreflightReconciler(t *testing.T, objs ...client.Object) *HubTeardownReconciler {
	t.Helper()
	ctrllog.SetLogger(zap.New(zap.UseDevMode(true)))
	s := newTestScheme()
	return &HubTeardownReconciler{
		Client: fake.NewClientBuilder().WithScheme(s).
			WithObjects(objs...).
			WithStatusSubresource(&operatorv1.HubTeardown{}).
			Build(),
		Scheme:   s,
		Log:      ctrllog.Log,
		Recorder: record.NewFakeRecorder(50),
	}
}

func preflightAPIService(name, serviceNamespace, serviceName string, labels map[string]string) *apiregistrationv1.APIService {
	apiService := &apiregistrationv1.APIService{
		ObjectMeta: metav1.ObjectMeta{Name: name, Labels: labels},
		Spec: apiregistrationv1.APIServiceSpec{
			Group:                "rfe9332.example.com",
			Version:              "v1alpha1",
			GroupPriorityMinimum: 100,
			VersionPriority:      100,
		},
	}
	if serviceNamespace != "" || serviceName != "" {
		apiService.Spec.Service = &apiregistrationv1.ServiceReference{
			Namespace: serviceNamespace,
			Name:      serviceName,
		}
	}
	return apiService
}

func preflightTestTeardown(dryRun bool) *operatorv1.HubTeardown {
	activeSince := metav1.NewTime(time.Now().Add(-time.Minute))
	td := &operatorv1.HubTeardown{
		ObjectMeta: metav1.ObjectMeta{Name: "teardown-preflight", Namespace: "open-cluster-management"},
		Spec: operatorv1.HubTeardownSpec{
			HubRef:                       testHubRef(),
			DryRun:                       dryRun,
			AcknowledgeCloudResourceRisk: !dryRun,
			AcknowledgedReportDigest:     "test-digest",
		},
		Status: operatorv1.HubTeardownStatus{
			ReportDigest: "test-digest",
		},
	}
	if !dryRun {
		td.Status.ActiveSince = &activeSince
	}
	return td
}

func reconcilePreflightOnce(t *testing.T, r *HubTeardownReconciler, td *operatorv1.HubTeardown) ctrl.Result {
	t.Helper()
	result, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: td.Name, Namespace: td.Namespace},
	})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	return result
}

func fetchPreflightTeardown(t *testing.T, r *HubTeardownReconciler, td *operatorv1.HubTeardown) *operatorv1.HubTeardown {
	t.Helper()
	fetched := &operatorv1.HubTeardown{}
	if err := r.Client.Get(context.Background(),
		types.NamespacedName{Name: td.Name, Namespace: td.Namespace}, fetched); err != nil {
		t.Fatalf("fetching CR: %v", err)
	}
	return fetched
}

func TestPreflightStaleAPIServices_UnownedDanglingIsReported(t *testing.T) {
	td := preflightTestTeardown(false)
	apiService := preflightAPIService(staleAPIServiceName, staleServiceNS, staleServiceName, nil)
	r := newPreflightReconciler(t, td, apiService)

	stale, err := r.preflightStaleAPIServices(context.Background(), ctrllog.Log, td)
	if err != nil {
		t.Fatalf("preflightStaleAPIServices: %v", err)
	}
	if len(stale) != 1 {
		t.Fatalf("stale count = %d, want 1: %+v", len(stale), stale)
	}
	if stale[0].Name != staleAPIServiceName || stale[0].ServiceNamespace != staleServiceNS || stale[0].ServiceName != staleServiceName {
		t.Errorf("stale entry = %+v", stale[0])
	}
	if stale[0].Reason == "" {
		t.Error("stale entry carries no ownership reason")
	}
	if !exists(t, r.Client, types.NamespacedName{Name: staleAPIServiceName}, &apiregistrationv1.APIService{}) {
		t.Error("preflight deleted the APIService")
	}
}

func TestPreflightStaleAPIServices_OwnedDanglingIsAllowedForSweep(t *testing.T) {
	td := preflightTestTeardown(false)
	labels := map[string]string{
		"installer.name":      testHubRef().Name,
		"installer.namespace": testHubRef().Namespace,
	}
	apiService := preflightAPIService(staleAPIServiceName, staleServiceNS, staleServiceName, labels)
	r := newPreflightReconciler(t, td, apiService)

	stale, err := r.preflightStaleAPIServices(context.Background(), ctrllog.Log, td)
	if err != nil {
		t.Fatalf("preflightStaleAPIServices: %v", err)
	}
	if len(stale) != 0 {
		t.Fatalf("owned dangling APIService blocked the start: %+v", stale)
	}
	if !exists(t, r.Client, types.NamespacedName{Name: staleAPIServiceName}, &apiregistrationv1.APIService{}) {
		t.Error("preflight deleted an owned APIService; deletion belongs to the Job sweep")
	}
}

func TestPreflightStaleAPIServices_HealthyUnownedIsNotBlocked(t *testing.T) {
	td := preflightTestTeardown(false)
	apiService := preflightAPIService(staleAPIServiceName, staleServiceNS, staleServiceName, nil)
	service := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Namespace: staleServiceNS, Name: staleServiceName}}
	r := newPreflightReconciler(t, td, apiService, service)

	stale, err := r.preflightStaleAPIServices(context.Background(), ctrllog.Log, td)
	if err != nil {
		t.Fatalf("preflightStaleAPIServices: %v", err)
	}
	if len(stale) != 0 {
		t.Fatalf("healthy APIService blocked the start: %+v", stale)
	}
}

func TestPreflightStaleAPIServices_LocalAPIServiceIsIgnored(t *testing.T) {
	td := preflightTestTeardown(false)
	apiService := preflightAPIService("v1beta1.local.rfe9332.example.com", "", "", nil)
	r := newPreflightReconciler(t, td, apiService)

	stale, err := r.preflightStaleAPIServices(context.Background(), ctrllog.Log, td)
	if err != nil {
		t.Fatalf("preflightStaleAPIServices: %v", err)
	}
	if len(stale) != 0 {
		t.Fatalf("local APIService blocked the start: %+v", stale)
	}
}

func TestPreflightStaleAPIServices_OwnedNamespaceEvidenceIsAllowed(t *testing.T) {
	td := preflightTestTeardown(false)
	apiService := preflightAPIService(staleAPIServiceName, "open-cluster-management-hub", staleServiceName, nil)
	r := newPreflightReconciler(t, td, apiService)

	stale, err := r.preflightStaleAPIServices(context.Background(), ctrllog.Log, td)
	if err != nil {
		t.Fatalf("preflightStaleAPIServices: %v", err)
	}
	if len(stale) != 0 {
		t.Fatalf("ACM-owned backing namespace was not treated as evidence: %+v", stale)
	}
}

func TestPreflightStaleAPIServices_DeterministicOrder(t *testing.T) {
	td := preflightTestTeardown(false)
	r := newPreflightReconciler(t,
		td,
		preflightAPIService("v1beta1.zeta.rfe9332.example.com", staleServiceNS, "zeta", nil),
		preflightAPIService("v1alpha1.alpha.rfe9332.example.com", staleServiceNS, "alpha", nil),
	)

	stale, err := r.preflightStaleAPIServices(context.Background(), ctrllog.Log, td)
	if err != nil {
		t.Fatalf("preflightStaleAPIServices: %v", err)
	}
	if len(stale) != 2 {
		t.Fatalf("stale count = %d, want 2: %+v", len(stale), stale)
	}
	if stale[0].Name != "v1alpha1.alpha.rfe9332.example.com" || stale[1].Name != "v1beta1.zeta.rfe9332.example.com" {
		t.Errorf("stale order = %q, %q", stale[0].Name, stale[1].Name)
	}
}

func TestReconcileActive_StaleAPIServiceBlocksBeforeAnyJob(t *testing.T) {
	t.Setenv("OPERATOR_IMAGE", "quay.io/test/operator:latest")
	td := preflightTestTeardown(false)
	apiService := preflightAPIService(staleAPIServiceName, staleServiceNS, staleServiceName, nil)
	r := newPreflightReconciler(t, td, handoffRole(), apiService)

	result := reconcilePreflightOnce(t, r, td)
	if result.RequeueAfter != teardownResyncActive {
		t.Errorf("RequeueAfter = %s, want %s", result.RequeueAfter, teardownResyncActive)
	}

	for _, name := range []string{prepareJobName, handoffJobName} {
		if exists(t, r.Client, types.NamespacedName{Name: name, Namespace: td.Namespace}, &batchv1.Job{}) {
			t.Errorf("Job %s was created while the preflight was blocked", name)
		}
	}
	if exists(t, r.Client, types.NamespacedName{Name: handoffSAName, Namespace: td.Namespace}, &corev1.ServiceAccount{}) {
		t.Error("handoff ServiceAccount was created while the preflight was blocked")
	}
	if exists(t, r.Client, types.NamespacedName{Name: handoffCRBName}, &rbacv1.ClusterRoleBinding{}) {
		t.Error("handoff ClusterRoleBinding was created while the preflight was blocked")
	}
	if !exists(t, r.Client, types.NamespacedName{Name: staleAPIServiceName}, &apiregistrationv1.APIService{}) {
		t.Error("operator deleted the unowned APIService")
	}

	fetched := fetchPreflightTeardown(t, r, td)
	blocked := metaFind(fetched, conditionTypePreflightBlocked)
	if blocked == nil || blocked.Status != metav1.ConditionTrue || blocked.Reason != preflightReasonStaleAPIService {
		t.Fatalf("PreflightBlocked = %+v, want True/%s", blocked, preflightReasonStaleAPIService)
	}
	if !strings.Contains(blocked.Message, staleAPIServiceName) {
		t.Errorf("block message does not name the APIService: %q", blocked.Message)
	}
	if !strings.Contains(blocked.Message, staleServiceNS+"/"+staleServiceName) {
		t.Errorf("block message does not name the absent Service: %q", blocked.Message)
	}
	if !strings.Contains(blocked.Message, "will not delete an unowned registration") {
		t.Errorf("block message does not state the no-delete policy: %q", blocked.Message)
	}
	progressing := metaFind(fetched, conditionTypeTeardownProgressing)
	if progressing == nil || progressing.Reason != preflightReasonStaleAPIService {
		t.Errorf("Progressing = %+v, want reason %s", progressing, preflightReasonStaleAPIService)
	}

	select {
	case event := <-r.Recorder.(*record.FakeRecorder).Events:
		if !strings.Contains(event, preflightReasonStaleAPIService) {
			t.Errorf("event = %q, want the stale-APIService reason", event)
		}
	default:
		t.Error("no preflight block event was recorded")
	}
}

func TestReconcileActive_HealthyAPIServiceAllowsJobStart(t *testing.T) {
	t.Setenv("OPERATOR_IMAGE", "quay.io/test/operator:latest")
	td := preflightTestTeardown(false)
	apiService := preflightAPIService(staleAPIServiceName, staleServiceNS, staleServiceName, nil)
	service := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Namespace: staleServiceNS, Name: staleServiceName}}
	r := newPreflightReconciler(t, td, handoffRole(), apiService, service)

	reconcilePreflightOnce(t, r, td)

	if !exists(t, r.Client, types.NamespacedName{Name: prepareJobName, Namespace: td.Namespace}, &batchv1.Job{}) {
		t.Error("prepare Job was not created for a healthy APIService")
	}
	fetched := fetchPreflightTeardown(t, r, td)
	if blocked := metaFind(fetched, conditionTypePreflightBlocked); blocked != nil && blocked.Status == metav1.ConditionTrue {
		t.Errorf("PreflightBlocked = %+v, want no active block", blocked)
	}
}

func TestReconcileActive_OwnedDanglingAPIServiceAllowsJobStart(t *testing.T) {
	t.Setenv("OPERATOR_IMAGE", "quay.io/test/operator:latest")
	td := preflightTestTeardown(false)
	labels := map[string]string{
		"installer.name":      testHubRef().Name,
		"installer.namespace": testHubRef().Namespace,
	}
	apiService := preflightAPIService(staleAPIServiceName, staleServiceNS, staleServiceName, labels)
	r := newPreflightReconciler(t, td, handoffRole(), apiService)

	reconcilePreflightOnce(t, r, td)

	if !exists(t, r.Client, types.NamespacedName{Name: prepareJobName, Namespace: td.Namespace}, &batchv1.Job{}) {
		t.Error("prepare Job was not created for an owned dangling APIService")
	}
	if !exists(t, r.Client, types.NamespacedName{Name: staleAPIServiceName}, &apiregistrationv1.APIService{}) {
		t.Error("preflight deleted the owned APIService instead of leaving it to the Job sweep")
	}
}

func TestReconcileDryRun_StaleAPIServiceIsReportOnly(t *testing.T) {
	td := preflightTestTeardown(true)
	apiService := preflightAPIService(staleAPIServiceName, staleServiceNS, staleServiceName, nil)
	r := newPreflightReconciler(t, td, apiService)

	reconcilePreflightOnce(t, r, td)

	fetched := fetchPreflightTeardown(t, r, td)
	complete := metaFind(fetched, conditionTypeDryRunComplete)
	if complete == nil || complete.Status != metav1.ConditionTrue {
		t.Fatalf("DryRunComplete = %+v, want True", complete)
	}
	if blocked := metaFind(fetched, conditionTypePreflightBlocked); blocked != nil && blocked.Status == metav1.ConditionTrue {
		t.Errorf("PreflightBlocked = %+v, want dry-run to remain report-only", blocked)
	}
	for _, name := range []string{prepareJobName, handoffJobName} {
		if exists(t, r.Client, types.NamespacedName{Name: name, Namespace: td.Namespace}, &batchv1.Job{}) {
			t.Errorf("dry-run created Job %s", name)
		}
	}
	if !exists(t, r.Client, types.NamespacedName{Name: staleAPIServiceName}, &apiregistrationv1.APIService{}) {
		t.Error("dry-run deleted the APIService")
	}
}

func TestReconcileActive_PreflightRecoversAfterAPIServiceRemoval(t *testing.T) {
	t.Setenv("OPERATOR_IMAGE", "quay.io/test/operator:latest")
	td := preflightTestTeardown(false)
	apiService := preflightAPIService(staleAPIServiceName, staleServiceNS, staleServiceName, nil)
	r := newPreflightReconciler(t, td, handoffRole(), apiService)

	reconcilePreflightOnce(t, r, td)
	if exists(t, r.Client, types.NamespacedName{Name: prepareJobName, Namespace: td.Namespace}, &batchv1.Job{}) {
		t.Fatal("prepare Job created while blocked")
	}

	if err := r.Client.Delete(context.Background(), apiService); err != nil {
		t.Fatalf("deleting APIService: %v", err)
	}
	reconcilePreflightOnce(t, r, td)

	if !exists(t, r.Client, types.NamespacedName{Name: prepareJobName, Namespace: td.Namespace}, &batchv1.Job{}) {
		t.Error("prepare Job was not created after the APIService was removed")
	}
	fetched := fetchPreflightTeardown(t, r, td)
	blocked := metaFind(fetched, conditionTypePreflightBlocked)
	if blocked == nil || blocked.Status != metav1.ConditionFalse || blocked.Reason != preflightReasonPassed {
		t.Errorf("PreflightBlocked = %+v, want False/%s", blocked, preflightReasonPassed)
	}
}

func TestReconcileActive_ExistingJobIsNotReGated(t *testing.T) {
	td := preflightTestTeardown(false)
	apiService := preflightAPIService(staleAPIServiceName, staleServiceNS, staleServiceName, nil)
	job := jobWithCondition(prepareJobName, "")
	job.Namespace = td.Namespace
	r := newPreflightReconciler(t, td, handoffRole(), apiService, job)

	reconcilePreflightOnce(t, r, td)

	fetched := fetchPreflightTeardown(t, r, td)
	if blocked := metaFind(fetched, conditionTypePreflightBlocked); blocked != nil && blocked.Status == metav1.ConditionTrue {
		t.Fatalf("PreflightBlocked = %+v, want no re-gate of a run in flight", blocked)
	}
	if !exists(t, r.Client, types.NamespacedName{Name: prepareJobName, Namespace: td.Namespace}, &batchv1.Job{}) {
		t.Error("existing prepare Job disappeared")
	}
}

func TestReconcileActive_NoConsentDoesNotEvaluatePreflight(t *testing.T) {
	td := preflightTestTeardown(false)
	td.Spec.AcknowledgeCloudResourceRisk = false
	td.Spec.AcknowledgedReportDigest = ""
	apiService := preflightAPIService(staleAPIServiceName, staleServiceNS, staleServiceName, nil)
	r := newPreflightReconciler(t, td, apiService)

	reconcilePreflightOnce(t, r, td)

	fetched := fetchPreflightTeardown(t, r, td)
	progressing := metaFind(fetched, conditionTypeTeardownProgressing)
	if progressing == nil || progressing.Reason != "WaitingForApproval" {
		t.Errorf("Progressing = %+v, want WaitingForApproval", progressing)
	}
	if blocked := metaFind(fetched, conditionTypePreflightBlocked); blocked != nil {
		t.Errorf("PreflightBlocked = %+v, want the preflight unevaluated without consent", blocked)
	}
}
