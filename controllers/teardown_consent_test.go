// Copyright Contributors to the Open Cluster Management project

package controllers

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	operatorv1 "github.com/stolostron/multiclusterhub-operator/api/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	ctrllog "sigs.k8s.io/controller-runtime/pkg/log"
)

// S-6: the digest the consent comparison uses is recorded from the report the
// operator actually wrote, not from anything the administrator typed.
func TestApplyInspectionRecordsReportDigest(t *testing.T) {
	reconciler := newGateTestReconciler(t)
	td := testTeardown()
	graph := &DependencyGraph{
		DiscoveredResources: []DiscoveredResource{
			testDiscovered("g", "v1", "Kind", "ns", "name", "uid-1", testNonCloudFinalizer),
		},
	}

	if err := reconciler.applyInspection(context.Background(), ctrllog.Log, td, graph); err != nil {
		t.Fatalf("applyInspection: %v", err)
	}
	if td.Status.ReportDigest == "" {
		t.Fatal("applyInspection recorded no report digest")
	}

	cm := &corev1.ConfigMap{}
	if err := reconciler.Client.Get(context.Background(),
		types.NamespacedName{Name: td.Status.ReportRef.Name, Namespace: td.Namespace}, cm); err != nil {
		t.Fatalf("reading report: %v", err)
	}
	var meta reportMeta
	if err := json.Unmarshal([]byte(cm.Data[reportKeyMeta]), &meta); err != nil {
		t.Fatalf("decoding report meta: %v", err)
	}
	if td.Status.ReportDigest != meta.Digest {
		t.Errorf("status.reportDigest = %q, want the report meta digest %q", td.Status.ReportDigest, meta.Digest)
	}
}

// S-7: approval flags survive a report change, but consent does not. The CR
// parks at ConsentRequired with a ConsentLapsed reason instead of proceeding.
func TestReconcileLapsesConsentWhenReportChanged(t *testing.T) {
	s := newTestScheme()
	td := &operatorv1.HubTeardown{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "teardown-consent",
			Namespace: "open-cluster-management",
		},
		Spec: operatorv1.HubTeardownSpec{HubRef: testHubRef(),
			DryRun:                       false,
			AcknowledgeCloudResourceRisk: true,
			// Approved against a report that no longer exists.
			AcknowledgedReportDigest: "sha256:approved-earlier",
		},
	}
	recorder := record.NewFakeRecorder(20)
	r := &HubTeardownReconciler{
		Client: fake.NewClientBuilder().WithScheme(s).
			WithObjects(td).WithStatusSubresource(td).Build(),
		Scheme:   s,
		Log:      ctrllog.Log,
		Recorder: recorder,
	}

	if _, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: td.Name, Namespace: td.Namespace},
	}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	fetched := &operatorv1.HubTeardown{}
	if err := r.Client.Get(context.Background(),
		types.NamespacedName{Name: td.Name, Namespace: td.Namespace}, fetched); err != nil {
		t.Fatalf("fetching CR: %v", err)
	}
	if teardownConsentGiven(fetched) {
		t.Fatal("consent survived a report change")
	}
	condition := metaFind(fetched, conditionTypeTeardownProgressing)
	if condition == nil || condition.Reason != "ConsentLapsed" {
		t.Fatalf("Progressing condition = %+v, want reason ConsentLapsed", condition)
	}
	if !strings.Contains(condition.Message, "acknowledgedReportDigest") {
		t.Errorf("message does not name the field to set: %q", condition.Message)
	}

	select {
	case event := <-recorder.Events:
		if !strings.Contains(event, "ConsentLapsed") {
			t.Errorf("event = %q", event)
		}
	default:
		t.Error("no ConsentLapsed event was recorded")
	}
}

// S-8: a re-created CR carries the spec flags but no status, so it is not
// consented until an inspection runs and the digest matches.
func TestConsentDoesNotSurviveRecreatedCR(t *testing.T) {
	recreated := &operatorv1.HubTeardown{
		ObjectMeta: metav1.ObjectMeta{Name: "teardown", Namespace: "open-cluster-management"},
		Spec: operatorv1.HubTeardownSpec{HubRef: testHubRef(),
			DryRun:                       false,
			AcknowledgeCloudResourceRisk: true,
			AcknowledgedReportDigest:     "sha256:from-a-previous-life",
		},
	}
	if teardownConsentGiven(recreated) {
		t.Fatal("a fresh CR inherited consent from the spec alone")
	}
}

func metaFind(td *operatorv1.HubTeardown, condType string) *metav1.Condition {
	for i := range td.Status.Conditions {
		if td.Status.Conditions[i].Type == condType {
			return &td.Status.Conditions[i]
		}
	}
	return nil
}
