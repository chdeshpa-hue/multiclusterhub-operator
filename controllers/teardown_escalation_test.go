// Copyright Contributors to the Open Cluster Management project

package controllers

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	operatorv1 "github.com/stolostron/multiclusterhub-operator/api/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	ctrllog "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
)

func newEscalationReconciler(t *testing.T, objs ...client.Object) *HubTeardownReconciler {
	t.Helper()
	ctrllog.SetLogger(zap.New(zap.UseDevMode(true)))
	s := newTestScheme()
	return &HubTeardownReconciler{
		Client:   fake.NewClientBuilder().WithScheme(s).WithObjects(objs...).Build(),
		Scheme:   s,
		Log:      ctrllog.Log,
		Recorder: record.NewFakeRecorder(20),
	}
}

func escalationTestCR() *operatorv1.HubTeardown {
	return &operatorv1.HubTeardown{
		ObjectMeta: metav1.ObjectMeta{Name: "teardown", Namespace: "open-cluster-management"},
		Spec: operatorv1.HubTeardownSpec{
			DryRun:                     false,
			ApprovedDestructiveActions: true,
		},
	}
}

func escalationRole() *rbacv1.ClusterRole {
	return &rbacv1.ClusterRole{ObjectMeta: metav1.ObjectMeta{Name: escalationRoleName}}
}

func exists(t *testing.T, c client.Client, key types.NamespacedName, obj client.Object) bool {
	t.Helper()
	err := c.Get(context.Background(), key, obj)
	if err == nil {
		return true
	}
	if apierrors.IsNotFound(err) {
		return false
	}
	t.Fatalf("unexpected error checking %s: %v", key.String(), err)
	return false
}

func TestTeardownConsentGiven(t *testing.T) {
	cases := []struct {
		name string
		spec operatorv1.HubTeardownSpec
		want bool
	}{
		{"dry-run with approvals", operatorv1.HubTeardownSpec{DryRun: true, ApprovedDestructiveActions: true}, false},
		{"active without approvals", operatorv1.HubTeardownSpec{DryRun: false}, false},
		{"active with destructive approval", operatorv1.HubTeardownSpec{DryRun: false, ApprovedDestructiveActions: true}, true},
		{"active with cloud acknowledgement", operatorv1.HubTeardownSpec{DryRun: false, AcknowledgeCloudResourceRisk: true}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			td := &operatorv1.HubTeardown{Spec: tc.spec}
			if got := teardownConsentGiven(td); got != tc.want {
				t.Errorf("teardownConsentGiven = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestEnsureEscalation_NoConsentCreatesNothing(t *testing.T) {
	t.Setenv("OPERATOR_IMAGE", "quay.io/test/operator:latest")
	td := escalationTestCR()
	td.Spec.ApprovedDestructiveActions = false
	r := newEscalationReconciler(t, escalationRole())

	if err := r.ensureEscalation(context.Background(), ctrllog.Log, td); err != nil {
		t.Fatalf("ensureEscalation: %v", err)
	}

	ns := types.NamespacedName{Name: escalationSAName, Namespace: td.Namespace}
	if exists(t, r.Client, ns, &corev1.ServiceAccount{}) {
		t.Error("ServiceAccount created without consent")
	}
	if exists(t, r.Client, types.NamespacedName{Name: escalationCRBName}, &rbacv1.ClusterRoleBinding{}) {
		t.Error("ClusterRoleBinding created without consent")
	}
	if exists(t, r.Client, types.NamespacedName{Name: teardownJobName, Namespace: td.Namespace}, &batchv1.Job{}) {
		t.Error("Job created without consent")
	}
}

func TestEnsureEscalation_DryRunCreatesNothing(t *testing.T) {
	t.Setenv("OPERATOR_IMAGE", "quay.io/test/operator:latest")
	td := escalationTestCR()
	td.Spec.DryRun = true
	r := newEscalationReconciler(t, escalationRole())

	if err := r.ensureEscalation(context.Background(), ctrllog.Log, td); err != nil {
		t.Fatalf("ensureEscalation: %v", err)
	}
	if exists(t, r.Client, types.NamespacedName{Name: teardownJobName, Namespace: td.Namespace}, &batchv1.Job{}) {
		t.Error("Job created during dry-run")
	}
	if exists(t, r.Client, types.NamespacedName{Name: escalationSAName, Namespace: td.Namespace}, &corev1.ServiceAccount{}) {
		t.Error("ServiceAccount created during dry-run")
	}
}

func TestEnsureEscalation_CreatesIdentityAndJob(t *testing.T) {
	t.Setenv("OPERATOR_IMAGE", "quay.io/test/operator:latest")
	td := escalationTestCR()
	r := newEscalationReconciler(t, escalationRole())

	if err := r.ensureEscalation(context.Background(), ctrllog.Log, td); err != nil {
		t.Fatalf("ensureEscalation: %v", err)
	}

	sa := &corev1.ServiceAccount{}
	if !exists(t, r.Client, types.NamespacedName{Name: escalationSAName, Namespace: td.Namespace}, sa) {
		t.Fatal("expected escalation ServiceAccount")
	}

	crb := &rbacv1.ClusterRoleBinding{}
	if !exists(t, r.Client, types.NamespacedName{Name: escalationCRBName}, crb) {
		t.Fatal("expected escalation ClusterRoleBinding")
	}
	if crb.RoleRef.Kind != "ClusterRole" || crb.RoleRef.Name != escalationRoleName {
		t.Errorf("roleRef = %s/%s, want ClusterRole/%s", crb.RoleRef.Kind, crb.RoleRef.Name, escalationRoleName)
	}
	if len(crb.Subjects) != 1 || crb.Subjects[0].Name != escalationSAName || crb.Subjects[0].Namespace != td.Namespace {
		t.Errorf("subjects = %+v, want %s in %s", crb.Subjects, escalationSAName, td.Namespace)
	}

	job := &batchv1.Job{}
	if !exists(t, r.Client, types.NamespacedName{Name: teardownJobName, Namespace: td.Namespace}, job) {
		t.Fatal("expected teardown Job")
	}
	if job.Spec.Template.Spec.ServiceAccountName != escalationSAName {
		t.Errorf("Job ServiceAccountName = %q, want %q", job.Spec.Template.Spec.ServiceAccountName, escalationSAName)
	}
	if job.Spec.ActiveDeadlineSeconds == nil {
		t.Error("Job has no activeDeadlineSeconds")
	}
	if job.Spec.BackoffLimit == nil || *job.Spec.BackoffLimit != 2 {
		t.Errorf("Job backoffLimit = %v, want 2", job.Spec.BackoffLimit)
	}
}

func TestEnsureEscalation_MissingRoleBlocks(t *testing.T) {
	t.Setenv("OPERATOR_IMAGE", "quay.io/test/operator:latest")
	td := escalationTestCR()
	r := newEscalationReconciler(t)

	err := r.ensureEscalation(context.Background(), ctrllog.Log, td)
	if err == nil || !strings.Contains(err.Error(), escalationRoleName) {
		t.Fatalf("expected error naming %s, got %v", escalationRoleName, err)
	}
	if exists(t, r.Client, types.NamespacedName{Name: escalationCRBName}, &rbacv1.ClusterRoleBinding{}) {
		t.Error("ClusterRoleBinding created without the role existing")
	}
	if exists(t, r.Client, types.NamespacedName{Name: teardownJobName, Namespace: td.Namespace}, &batchv1.Job{}) {
		t.Error("Job created without the role existing")
	}
}

func TestEnsureEscalation_Idempotent(t *testing.T) {
	t.Setenv("OPERATOR_IMAGE", "quay.io/test/operator:latest")
	td := escalationTestCR()
	r := newEscalationReconciler(t, escalationRole())

	for i := 0; i < 3; i++ {
		if err := r.ensureEscalation(context.Background(), ctrllog.Log, td); err != nil {
			t.Fatalf("ensureEscalation attempt %d: %v", i, err)
		}
	}

	jobs := &batchv1.JobList{}
	if err := r.Client.List(context.Background(), jobs, client.InNamespace(td.Namespace)); err != nil {
		t.Fatalf("list jobs: %v", err)
	}
	if len(jobs.Items) != 1 {
		t.Errorf("expected 1 Job, got %d", len(jobs.Items))
	}
}

type recordingClient struct {
	client.Client
	calls           *[]string
	forbidCRBCreate bool
}

func (c recordingClient) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	*c.calls = append(*c.calls, fmt.Sprintf("get:%T:%s", obj, key.Name))
	return c.Client.Get(ctx, key, obj, opts...)
}

func (c recordingClient) Create(ctx context.Context, obj client.Object, opts ...client.CreateOption) error {
	*c.calls = append(*c.calls, fmt.Sprintf("create:%T:%s", obj, obj.GetName()))
	if c.forbidCRBCreate {
		if _, ok := obj.(*rbacv1.ClusterRoleBinding); ok {
			return apierrors.NewForbidden(
				schema.GroupResource{Group: "rbac.authorization.k8s.io", Resource: "clusterrolebindings"},
				obj.GetName(), fmt.Errorf("bind not permitted"))
		}
	}
	return c.Client.Create(ctx, obj, opts...)
}

func (c recordingClient) Delete(ctx context.Context, obj client.Object, opts ...client.DeleteOption) error {
	*c.calls = append(*c.calls, fmt.Sprintf("delete:%T:%s", obj, obj.GetName()))
	return c.Client.Delete(ctx, obj, opts...)
}

func TestEnsureEscalation_BindForbiddenSurfaces(t *testing.T) {
	t.Setenv("OPERATOR_IMAGE", "quay.io/test/operator:latest")
	td := escalationTestCR()
	base := newEscalationReconciler(t, escalationRole())
	calls := []string{}
	r := &HubTeardownReconciler{
		Client:   &recordingClient{Client: base.Client, calls: &calls, forbidCRBCreate: true},
		Scheme:   base.Scheme,
		Log:      base.Log,
		Recorder: record.NewFakeRecorder(20),
	}

	err := r.ensureEscalation(context.Background(), ctrllog.Log, td)
	if err == nil {
		t.Fatal("expected a forbidden error to surface")
	}
	if !apierrors.IsForbidden(err) && !strings.Contains(strings.ToLower(err.Error()), "forbidden") {
		t.Errorf("expected forbidden error, got %v", err)
	}
	if exists(t, base.Client, types.NamespacedName{Name: teardownJobName, Namespace: td.Namespace}, &batchv1.Job{}) {
		t.Error("Job created after the binding was denied")
	}
}

func finishedJob(condType batchv1.JobConditionType) *batchv1.Job {
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: teardownJobName, Namespace: "open-cluster-management"},
		Status: batchv1.JobStatus{Conditions: []batchv1.JobCondition{
			{Type: condType, Status: corev1.ConditionTrue},
		}},
	}
}

func runningJob() *batchv1.Job {
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: teardownJobName, Namespace: "open-cluster-management"},
		Status: batchv1.JobStatus{Conditions: []batchv1.JobCondition{
			{Type: batchv1.JobComplete, Status: corev1.ConditionFalse},
		}},
	}
}

func escalationIdentityObjects() []client.Object {
	return []client.Object{
		escalationServiceAccount("open-cluster-management"),
		escalationClusterRoleBinding("open-cluster-management"),
	}
}

func TestFinishEscalation_DeletesInSpecOrder(t *testing.T) {
	td := escalationTestCR()
	objs := append(escalationIdentityObjects(), finishedJob(batchv1.JobComplete))
	base := newEscalationReconciler(t, objs...)
	calls := []string{}
	r := &HubTeardownReconciler{
		Client:   &recordingClient{Client: base.Client, calls: &calls},
		Scheme:   base.Scheme,
		Log:      base.Log,
		Recorder: record.NewFakeRecorder(20),
	}

	if err := r.finishEscalation(context.Background(), ctrllog.Log, td); err != nil {
		t.Fatalf("finishEscalation: %v", err)
	}

	crbIdx, saIdx, jobIdx := -1, -1, -1
	for i, call := range calls {
		switch {
		case strings.HasPrefix(call, "delete:*v1.ClusterRoleBinding"):
			crbIdx = i
		case strings.HasPrefix(call, "delete:*v1.ServiceAccount"):
			saIdx = i
		case strings.HasPrefix(call, "delete:*v1.Job"):
			jobIdx = i
		}
	}
	if crbIdx < 0 || saIdx < 0 || jobIdx < 0 {
		t.Fatalf("missing deletions, calls: %v", calls)
	}
	if !(crbIdx < saIdx && saIdx < jobIdx) {
		t.Errorf("deletion order CRB(%d) SA(%d) Job(%d) is not the required order; calls: %v", crbIdx, saIdx, jobIdx, calls)
	}

	for _, obj := range append(escalationIdentityObjects(), &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: teardownJobName, Namespace: td.Namespace}}) {
		if exists(t, base.Client, types.NamespacedName{Name: obj.GetName(), Namespace: obj.GetNamespace()}, obj) {
			t.Errorf("%T %s still present after finishEscalation", obj, obj.GetName())
		}
	}

	found := false
	for _, c := range td.Status.Conditions {
		if c.Type == conditionTypeEscalationReady && c.Reason == string(batchv1.JobComplete) {
			found = true
		}
	}
	if !found {
		t.Errorf("terminal condition not recorded on the CR: %+v", td.Status.Conditions)
	}
}

func TestFinishEscalation_RunningJobLeavesEverything(t *testing.T) {
	td := escalationTestCR()
	objs := append(escalationIdentityObjects(), runningJob())
	base := newEscalationReconciler(t, objs...)
	calls := []string{}
	r := &HubTeardownReconciler{
		Client:   &recordingClient{Client: base.Client, calls: &calls},
		Scheme:   base.Scheme,
		Log:      base.Log,
		Recorder: record.NewFakeRecorder(20),
	}

	if err := r.finishEscalation(context.Background(), ctrllog.Log, td); err != nil {
		t.Fatalf("finishEscalation: %v", err)
	}
	for _, call := range calls {
		if strings.HasPrefix(call, "delete:") {
			t.Errorf("running Job triggered a deletion: %s", call)
		}
	}
	if len(td.Status.Conditions) != 0 {
		t.Errorf("running Job wrote a terminal condition: %+v", td.Status.Conditions)
	}
	if !exists(t, base.Client, types.NamespacedName{Name: escalationSAName, Namespace: td.Namespace}, &corev1.ServiceAccount{}) {
		t.Error("ServiceAccount removed while the Job was still running")
	}
}

func TestFinishEscalation_FailedJobRecordsAndCleans(t *testing.T) {
	td := escalationTestCR()
	objs := append(escalationIdentityObjects(), finishedJob(batchv1.JobFailed))
	r := newEscalationReconciler(t, objs...)

	if err := r.finishEscalation(context.Background(), ctrllog.Log, td); err != nil {
		t.Fatalf("finishEscalation: %v", err)
	}

	found := false
	for _, c := range td.Status.Conditions {
		if c.Type == conditionTypeEscalationReady && c.Reason == string(batchv1.JobFailed) {
			found = true
		}
	}
	if !found {
		t.Errorf("failed Job not recorded: %+v", td.Status.Conditions)
	}
	if exists(t, r.Client, types.NamespacedName{Name: escalationCRBName}, &rbacv1.ClusterRoleBinding{}) {
		t.Error("ClusterRoleBinding left behind after a failed Job")
	}
}

func TestCleanupEscalation_RemovesAll(t *testing.T) {
	td := escalationTestCR()
	objs := append(escalationIdentityObjects(), runningJob())
	r := newEscalationReconciler(t, objs...)

	if err := r.cleanupEscalation(context.Background(), ctrllog.Log, td); err != nil {
		t.Fatalf("cleanupEscalation: %v", err)
	}
	if exists(t, r.Client, types.NamespacedName{Name: escalationSAName, Namespace: td.Namespace}, &corev1.ServiceAccount{}) {
		t.Error("ServiceAccount left behind")
	}
	if exists(t, r.Client, types.NamespacedName{Name: escalationCRBName}, &rbacv1.ClusterRoleBinding{}) {
		t.Error("ClusterRoleBinding left behind")
	}
	if exists(t, r.Client, types.NamespacedName{Name: teardownJobName, Namespace: td.Namespace}, &batchv1.Job{}) {
		t.Error("Job left behind")
	}
}

func TestTeardownJobDeadline(t *testing.T) {
	td := escalationTestCR()
	if got := teardownJobDeadline(td); got == nil || *got != int64(defaultMaxDuration.Seconds()) {
		t.Errorf("default deadline = %v, want %v", got, defaultMaxDuration.Seconds())
	}

	td.Spec.MaxDuration = &metav1.Duration{Duration: 45 * time.Minute}
	if got := teardownJobDeadline(td); got == nil || *got != 2700 {
		t.Errorf("45m deadline = %v, want 2700", got)
	}

	td.Spec.MaxDuration = &metav1.Duration{Duration: 10 * time.Second}
	if got := teardownJobDeadline(td); got == nil || *got != 60 {
		t.Errorf("floor deadline = %v, want 60", got)
	}
}
