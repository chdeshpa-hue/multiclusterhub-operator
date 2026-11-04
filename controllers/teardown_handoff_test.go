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

func newHandoffReconciler(t *testing.T, objs ...client.Object) *HubTeardownReconciler {
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

func handoffTestCR() *operatorv1.HubTeardown {
	return &operatorv1.HubTeardown{
		ObjectMeta: metav1.ObjectMeta{Name: "teardown", Namespace: "open-cluster-management"},
		Spec: operatorv1.HubTeardownSpec{
			DryRun:                     false,
			ApprovedDestructiveActions: true,
		},
	}
}

func handoffRole() *rbacv1.ClusterRole {
	return &rbacv1.ClusterRole{ObjectMeta: metav1.ObjectMeta{Name: handoffRoleName}}
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

func TestEnsureHandoff_NoConsentCreatesNothing(t *testing.T) {
	t.Setenv("OPERATOR_IMAGE", "quay.io/test/operator:latest")
	td := handoffTestCR()
	td.Spec.ApprovedDestructiveActions = false
	r := newHandoffReconciler(t, handoffRole())

	if err := r.ensureHandoff(context.Background(), ctrllog.Log, td); err != nil {
		t.Fatalf("ensureHandoff: %v", err)
	}

	ns := types.NamespacedName{Name: handoffSAName, Namespace: td.Namespace}
	if exists(t, r.Client, ns, &corev1.ServiceAccount{}) {
		t.Error("ServiceAccount created without consent")
	}
	if exists(t, r.Client, types.NamespacedName{Name: handoffCRBName}, &rbacv1.ClusterRoleBinding{}) {
		t.Error("ClusterRoleBinding created without consent")
	}
	if exists(t, r.Client, types.NamespacedName{Name: prepareJobName, Namespace: td.Namespace}, &batchv1.Job{}) {
		t.Error("Job created without consent")
	}
}

func TestEnsureHandoff_DryRunCreatesNothing(t *testing.T) {
	t.Setenv("OPERATOR_IMAGE", "quay.io/test/operator:latest")
	td := handoffTestCR()
	td.Spec.DryRun = true
	r := newHandoffReconciler(t, handoffRole())

	if err := r.ensureHandoff(context.Background(), ctrllog.Log, td); err != nil {
		t.Fatalf("ensureHandoff: %v", err)
	}
	if exists(t, r.Client, types.NamespacedName{Name: prepareJobName, Namespace: td.Namespace}, &batchv1.Job{}) {
		t.Error("Job created during dry-run")
	}
	if exists(t, r.Client, types.NamespacedName{Name: handoffSAName, Namespace: td.Namespace}, &corev1.ServiceAccount{}) {
		t.Error("ServiceAccount created during dry-run")
	}
}

func TestEnsureHandoff_CreatesIdentityAndJob(t *testing.T) {
	t.Setenv("OPERATOR_IMAGE", "quay.io/test/operator:latest")
	td := handoffTestCR()
	r := newHandoffReconciler(t, handoffRole())

	if err := r.ensureHandoff(context.Background(), ctrllog.Log, td); err != nil {
		t.Fatalf("ensureHandoff: %v", err)
	}

	sa := &corev1.ServiceAccount{}
	if !exists(t, r.Client, types.NamespacedName{Name: handoffSAName, Namespace: td.Namespace}, sa) {
		t.Fatal("expected handoff ServiceAccount")
	}

	crb := &rbacv1.ClusterRoleBinding{}
	if !exists(t, r.Client, types.NamespacedName{Name: handoffCRBName}, crb) {
		t.Fatal("expected handoff ClusterRoleBinding")
	}
	if crb.RoleRef.Kind != "ClusterRole" || crb.RoleRef.Name != handoffRoleName {
		t.Errorf("roleRef = %s/%s, want ClusterRole/%s", crb.RoleRef.Kind, crb.RoleRef.Name, handoffRoleName)
	}
	if len(crb.Subjects) != 1 || crb.Subjects[0].Name != handoffSAName || crb.Subjects[0].Namespace != td.Namespace {
		t.Errorf("subjects = %+v, want %s in %s", crb.Subjects, handoffSAName, td.Namespace)
	}

	job := &batchv1.Job{}
	if !exists(t, r.Client, types.NamespacedName{Name: prepareJobName, Namespace: td.Namespace}, job) {
		t.Fatal("expected teardown Job")
	}
	if job.Spec.Template.Spec.ServiceAccountName != handoffSAName {
		t.Errorf("Job ServiceAccountName = %q, want %q", job.Spec.Template.Spec.ServiceAccountName, handoffSAName)
	}
	if job.Spec.ActiveDeadlineSeconds == nil {
		t.Error("Job has no activeDeadlineSeconds")
	}
	if job.Spec.BackoffLimit == nil || *job.Spec.BackoffLimit != 2 {
		t.Errorf("Job backoffLimit = %v, want 2", job.Spec.BackoffLimit)
	}
}

func TestEnsureHandoff_MissingRoleBlocks(t *testing.T) {
	t.Setenv("OPERATOR_IMAGE", "quay.io/test/operator:latest")
	td := handoffTestCR()
	r := newHandoffReconciler(t)

	err := r.ensureHandoff(context.Background(), ctrllog.Log, td)
	if err == nil || !strings.Contains(err.Error(), handoffRoleName) {
		t.Fatalf("expected error naming %s, got %v", handoffRoleName, err)
	}
	if exists(t, r.Client, types.NamespacedName{Name: handoffCRBName}, &rbacv1.ClusterRoleBinding{}) {
		t.Error("ClusterRoleBinding created without the role existing")
	}
	if exists(t, r.Client, types.NamespacedName{Name: prepareJobName, Namespace: td.Namespace}, &batchv1.Job{}) {
		t.Error("Job created without the role existing")
	}
}

func TestEnsureHandoff_Idempotent(t *testing.T) {
	t.Setenv("OPERATOR_IMAGE", "quay.io/test/operator:latest")
	td := handoffTestCR()
	r := newHandoffReconciler(t, handoffRole())

	for i := 0; i < 3; i++ {
		if err := r.ensureHandoff(context.Background(), ctrllog.Log, td); err != nil {
			t.Fatalf("ensureHandoff attempt %d: %v", i, err)
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

func TestEnsureHandoff_BindForbiddenSurfaces(t *testing.T) {
	t.Setenv("OPERATOR_IMAGE", "quay.io/test/operator:latest")
	td := handoffTestCR()
	base := newHandoffReconciler(t, handoffRole())
	calls := []string{}
	r := &HubTeardownReconciler{
		Client:   &recordingClient{Client: base.Client, calls: &calls, forbidCRBCreate: true},
		Scheme:   base.Scheme,
		Log:      base.Log,
		Recorder: record.NewFakeRecorder(20),
	}

	err := r.ensureHandoff(context.Background(), ctrllog.Log, td)
	if err == nil {
		t.Fatal("expected a forbidden error to surface")
	}
	if !apierrors.IsForbidden(err) && !strings.Contains(strings.ToLower(err.Error()), "forbidden") {
		t.Errorf("expected forbidden error, got %v", err)
	}
	if exists(t, base.Client, types.NamespacedName{Name: prepareJobName, Namespace: td.Namespace}, &batchv1.Job{}) {
		t.Error("Job created after the binding was denied")
	}
}

func finishedJob(condType batchv1.JobConditionType) *batchv1.Job {
	return jobWithCondition(prepareJobName, condType)
}

func runningJob() *batchv1.Job {
	return jobWithCondition(prepareJobName, "")
}

// jobWithCondition builds a Job in the given terminal state. An empty condition
// means "still running": the Job exists, has no true condition, and must never be
// read as finished.
func jobWithCondition(name string, condType batchv1.JobConditionType) *batchv1.Job {
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "open-cluster-management"},
	}
	job.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionFalse}}
	if condType != "" {
		job.Status.Conditions = []batchv1.JobCondition{{Type: condType, Status: corev1.ConditionTrue}}
	}
	return job
}

func handoffIdentityObjects() []client.Object {
	return []client.Object{
		handoffServiceAccount("open-cluster-management"),
		handoffClusterRoleBinding("open-cluster-management"),
	}
}

// The prepare Job's terminal record is an event, not a condition: the handoff
// condition describes the identity and the second Job, and must not be rewritten
// by the first one finishing.
func TestFinishPrepareJob_RemovesJobAndKeepsIdentity(t *testing.T) {
	td := handoffTestCR()
	objs := append(handoffIdentityObjects(), finishedJob(batchv1.JobComplete))
	base := newHandoffReconciler(t, objs...)
	calls := []string{}
	recorder := record.NewFakeRecorder(20)
	r := &HubTeardownReconciler{
		Client:   &recordingClient{Client: base.Client, calls: &calls},
		Scheme:   base.Scheme,
		Log:      base.Log,
		Recorder: recorder,
	}

	if err := r.finishPrepareJob(context.Background(), ctrllog.Log, td); err != nil {
		t.Fatalf("finishPrepareJob: %v", err)
	}

	deletedJob := false
	for _, call := range calls {
		switch {
		case strings.HasPrefix(call, "delete:*v1.ClusterRoleBinding"), strings.HasPrefix(call, "delete:*v1.ServiceAccount"):
			t.Errorf("prepare Job completion removed the identity: %s", call)
		case strings.HasPrefix(call, "delete:*v1.Job"):
			deletedJob = true
		}
	}
	if !deletedJob {
		t.Errorf("prepare Job not removed, calls: %v", calls)
	}
	if exists(t, base.Client, types.NamespacedName{Name: prepareJobName, Namespace: td.Namespace}, &batchv1.Job{}) {
		t.Error("prepare Job still present")
	}
	if !exists(t, base.Client, types.NamespacedName{Name: handoffSAName, Namespace: td.Namespace}, &corev1.ServiceAccount{}) {
		t.Error("ServiceAccount removed before the handoff Job could use it")
	}
	if len(td.Status.Conditions) != 0 {
		t.Errorf("prepare Job completion wrote a condition: %+v", td.Status.Conditions)
	}

	event := <-recorder.Events
	if !strings.Contains(event, "PrepareJobFinished") {
		t.Errorf("expected a PrepareJobFinished event, got %q", event)
	}
}

func TestFinishPrepareJob_RunningJobLeavesEverything(t *testing.T) {
	td := handoffTestCR()
	objs := append(handoffIdentityObjects(), runningJob())
	base := newHandoffReconciler(t, objs...)
	calls := []string{}
	r := &HubTeardownReconciler{
		Client:   &recordingClient{Client: base.Client, calls: &calls},
		Scheme:   base.Scheme,
		Log:      base.Log,
		Recorder: record.NewFakeRecorder(20),
	}

	if err := r.finishPrepareJob(context.Background(), ctrllog.Log, td); err != nil {
		t.Fatalf("finishPrepareJob: %v", err)
	}
	for _, call := range calls {
		if strings.HasPrefix(call, "delete:") {
			t.Errorf("running Job triggered a deletion: %s", call)
		}
	}
	if len(td.Status.Conditions) != 0 {
		t.Errorf("running Job wrote a condition: %+v", td.Status.Conditions)
	}
	if !exists(t, base.Client, types.NamespacedName{Name: handoffSAName, Namespace: td.Namespace}, &corev1.ServiceAccount{}) {
		t.Error("ServiceAccount removed while the Job was still running")
	}
}

func TestFinishPrepareJob_FailedJobIsRemovedIdentityKept(t *testing.T) {
	td := handoffTestCR()
	objs := append(handoffIdentityObjects(), finishedJob(batchv1.JobFailed))
	r := newHandoffReconciler(t, objs...)

	if err := r.finishPrepareJob(context.Background(), ctrllog.Log, td); err != nil {
		t.Fatalf("finishPrepareJob: %v", err)
	}
	if exists(t, r.Client, types.NamespacedName{Name: prepareJobName, Namespace: td.Namespace}, &batchv1.Job{}) {
		t.Error("failed prepare Job left in place")
	}
	if !exists(t, r.Client, types.NamespacedName{Name: handoffCRBName}, &rbacv1.ClusterRoleBinding{}) {
		t.Error("identity removed after a failed prepare Job; cleanupHandoff owns that")
	}
}

func TestFinishHandoffJob_RemovesEverything(t *testing.T) {
	td := handoffTestCR()
	objs := append(handoffIdentityObjects(),
		jobWithCondition(prepareJobName, batchv1.JobComplete),
		jobWithCondition(handoffJobName, batchv1.JobComplete),
	)
	base := newHandoffReconciler(t, objs...)
	calls := []string{}
	r := &HubTeardownReconciler{
		Client:   &recordingClient{Client: base.Client, calls: &calls},
		Scheme:   base.Scheme,
		Log:      base.Log,
		Recorder: record.NewFakeRecorder(20),
	}

	if err := r.finishHandoffJob(context.Background(), ctrllog.Log, td); err != nil {
		t.Fatalf("finishHandoffJob: %v", err)
	}

	crbIdx, saIdx := -1, -1
	jobDeletes := 0
	for i, call := range calls {
		switch {
		case strings.HasPrefix(call, "delete:*v1.ClusterRoleBinding"):
			crbIdx = i
		case strings.HasPrefix(call, "delete:*v1.ServiceAccount"):
			saIdx = i
		case strings.HasPrefix(call, "delete:*v1.Job"):
			jobDeletes++
		}
	}
	if crbIdx < 0 || saIdx < 0 || jobDeletes != 2 {
		t.Fatalf("expected CRB, SA and both Jobs to be deleted; calls: %v", calls)
	}
	if crbIdx > saIdx {
		t.Errorf("ClusterRoleBinding deleted after the ServiceAccount; calls: %v", calls)
	}

	for _, obj := range handoffIdentityObjects() {
		if exists(t, base.Client, types.NamespacedName{Name: obj.GetName(), Namespace: obj.GetNamespace()}, obj) {
			t.Errorf("%T %s still present after a terminal handoff", obj, obj.GetName())
		}
	}
	for _, name := range []string{prepareJobName, handoffJobName} {
		if exists(t, base.Client, types.NamespacedName{Name: name, Namespace: td.Namespace}, &batchv1.Job{}) {
			t.Errorf("Job %s still present after a terminal handoff", name)
		}
	}

	found := false
	for _, c := range td.Status.Conditions {
		if c.Type == conditionTypeHandoffReady && c.Status == metav1.ConditionFalse && c.Reason == string(batchv1.JobComplete) {
			found = true
		}
	}
	if !found {
		t.Errorf("handoff terminal condition not recorded: %+v", td.Status.Conditions)
	}
}

func TestFinishHandoffJob_RunningJobLeavesEverything(t *testing.T) {
	td := handoffTestCR()
	objs := append(handoffIdentityObjects(), jobWithCondition(handoffJobName, ""))
	r := newHandoffReconciler(t, objs...)

	if err := r.finishHandoffJob(context.Background(), ctrllog.Log, td); err != nil {
		t.Fatalf("finishHandoffJob: %v", err)
	}
	if !exists(t, r.Client, types.NamespacedName{Name: handoffSAName, Namespace: td.Namespace}, &corev1.ServiceAccount{}) {
		t.Error("identity removed while the handoff Job was still running")
	}
	if !exists(t, r.Client, types.NamespacedName{Name: handoffJobName, Namespace: td.Namespace}, &batchv1.Job{}) {
		t.Error("running handoff Job removed")
	}
}

func TestCleanupHandoff_RemovesAll(t *testing.T) {
	td := handoffTestCR()
	objs := append(handoffIdentityObjects(),
		jobWithCondition(prepareJobName, ""),
		jobWithCondition(handoffJobName, ""),
	)
	r := newHandoffReconciler(t, objs...)

	if err := r.cleanupHandoff(context.Background(), ctrllog.Log, td); err != nil {
		t.Fatalf("cleanupHandoff: %v", err)
	}
	if exists(t, r.Client, types.NamespacedName{Name: handoffSAName, Namespace: td.Namespace}, &corev1.ServiceAccount{}) {
		t.Error("ServiceAccount left behind")
	}
	if exists(t, r.Client, types.NamespacedName{Name: handoffCRBName}, &rbacv1.ClusterRoleBinding{}) {
		t.Error("ClusterRoleBinding left behind")
	}
	for _, name := range []string{prepareJobName, handoffJobName} {
		if exists(t, r.Client, types.NamespacedName{Name: name, Namespace: td.Namespace}, &batchv1.Job{}) {
			t.Errorf("Job %s left behind", name)
		}
	}
}

func TestTeardownJobDeadline(t *testing.T) {
	td := handoffTestCR()
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
