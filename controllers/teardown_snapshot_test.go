// Copyright Contributors to the Open Cluster Management project

package controllers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	ctrllog "sigs.k8s.io/controller-runtime/pkg/log"
)

func snapshotTestConfigMap(namespace, name string) *corev1.ConfigMap {
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name},
	}
}

func decodeSnapshot(t *testing.T, cm *corev1.ConfigMap) operatorNamespaceSnapshot {
	t.Helper()
	raw := cm.Data[snapshotKeyObjects]
	if raw == "" {
		t.Fatalf("snapshot object %q has no %s key", cm.Name, snapshotKeyObjects)
	}
	snapshot := operatorNamespaceSnapshot{}
	if err := json.Unmarshal([]byte(raw), &snapshot); err != nil {
		t.Fatalf("decoding snapshot: %v", err)
	}
	return snapshot
}

func TestEnsureOperatorNamespaceSnapshotCreatedOnce(t *testing.T) {
	td := handoffTestCR()
	r := newHandoffReconciler(t, snapshotTestConfigMap(td.Namespace, "kube-root-ca.crt"))

	first, reused, err := r.ensureOperatorNamespaceSnapshot(context.Background(), td)
	if err != nil {
		t.Fatalf("first capture: %v", err)
	}
	if reused {
		t.Error("first capture must not report reuse")
	}
	if got := len(decodeSnapshot(t, first).Objects); got != 1 {
		t.Fatalf("captured %d objects, want 1", got)
	}

	// The namespace changes after the capture. The baseline must not move.
	if err := r.Client.Create(context.Background(),
		snapshotTestConfigMap(td.Namespace, "multicluster-integrations-config")); err != nil {
		t.Fatalf("creating late object: %v", err)
	}

	second, reused, err := r.ensureOperatorNamespaceSnapshot(context.Background(), td)
	if err != nil {
		t.Fatalf("second capture: %v", err)
	}
	if !reused {
		t.Error("second capture must reuse the first record")
	}
	if second.Data[snapshotKeyObjects] != first.Data[snapshotKeyObjects] {
		t.Error("snapshot was rewritten; a resumed run would re-baseline onto a half-destroyed namespace")
	}
	if got := len(decodeSnapshot(t, second).Objects); got != 1 {
		t.Fatalf("snapshot grew to %d objects after the second call", got)
	}
}

func TestEnsureOperatorNamespaceSnapshotRecordsIdentityOnly(t *testing.T) {
	td := handoffTestCR()
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: td.Namespace, Name: "pull-secret"},
		Data:       map[string][]byte{"token": []byte("super-secret-value")},
	}
	r := newHandoffReconciler(t, secret)

	cm, _, err := r.ensureOperatorNamespaceSnapshot(context.Background(), td)
	if err != nil {
		t.Fatalf("capture: %v", err)
	}

	body := cm.Data[snapshotKeyObjects]
	if strings.Contains(body, "super-secret-value") {
		t.Error("snapshot recorded secret material")
	}
	snapshot := decodeSnapshot(t, cm)
	if len(snapshot.Objects) != 1 || snapshot.Objects[0].Name != "pull-secret" {
		t.Fatalf("snapshot objects = %+v", snapshot.Objects)
	}
	if snapshot.Objects[0].Kind != "Secret" {
		t.Errorf("kind = %q", snapshot.Objects[0].Kind)
	}
}

func TestEnsureOperatorNamespaceSnapshotListFailure(t *testing.T) {
	td := handoffTestCR()
	scheme := newTestScheme()
	r := &HubTeardownReconciler{
		Client: fake.NewClientBuilder().WithScheme(scheme).WithInterceptorFuncs(interceptor.Funcs{
			List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
				if _, ok := list.(*corev1.ConfigMapList); ok {
					return errors.New("simulated list failure")
				}
				return c.List(ctx, list, opts...)
			},
		}).Build(),
		Scheme:   scheme,
		Log:      ctrllog.Log,
		Recorder: record.NewFakeRecorder(10),
	}

	if _, _, err := r.ensureOperatorNamespaceSnapshot(context.Background(), td); err == nil {
		t.Fatal("a partial capture must fail rather than record a baseline that is missing objects")
	}

	// Nothing was written, so verification reports "no snapshot" instead of
	// treating every live object as pre-existing.
	if _, present, err := r.loadOperatorNamespaceSnapshot(context.Background(), td); err != nil {
		t.Fatalf("load after failed capture: %v", err)
	} else if present {
		t.Error("a failed capture left a snapshot behind")
	}
}

func TestSnapshotConfigMapNameIsStable(t *testing.T) {
	td := handoffTestCR()
	first := snapshotConfigMapName(td)
	td.Status.LastInspection = &metav1.Time{Time: time.Now()}
	if second := snapshotConfigMapName(td); second != first {
		t.Errorf("snapshot name depends on status: %s != %s", second, first)
	}
	if !strings.HasPrefix(first, snapshotConfigMapPrefix) {
		t.Errorf("name %q missing prefix", first)
	}

	long := handoffTestCR()
	long.Name = strings.Repeat("a", 300)
	if got := snapshotConfigMapName(long); len(got) > reportMaxNameLength {
		t.Errorf("snapshot name too long: %d chars", len(got))
	}
}

func TestSnapshotSurvivesReportPruning(t *testing.T) {
	td := handoffTestCR()
	objects := make([]client.Object, 0, reportRetention+1)
	for i := 0; i < reportRetention+1; i++ {
		objects = append(objects, &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Namespace:         td.Namespace,
				Name:              fmt.Sprintf("hubteardown-report-%s-%d", td.Name, i),
				Labels:            map[string]string{reportLabelTeardown: td.Name},
				CreationTimestamp: metav1.NewTime(time.Now().Add(time.Duration(i) * time.Minute)),
			},
		})
	}
	r := newHandoffReconciler(t, objects...)

	cm, _, err := r.ensureOperatorNamespaceSnapshot(context.Background(), td)
	if err != nil {
		t.Fatalf("capture: %v", err)
	}

	r.pruneInspectReports(context.Background(), r.Log, td)

	remaining := &corev1.ConfigMap{}
	if err := r.Client.Get(context.Background(), client.ObjectKeyFromObject(cm), remaining); err != nil {
		t.Fatalf("snapshot was pruned with the reports: %v", err)
	}
	reports := &corev1.ConfigMapList{}
	if err := r.Client.List(context.Background(), reports, client.InNamespace(td.Namespace),
		client.MatchingLabels{reportLabelTeardown: td.Name}); err != nil {
		t.Fatalf("listing reports: %v", err)
	}
	if len(reports.Items) != reportRetention {
		t.Errorf("reports kept = %d, want %d", len(reports.Items), reportRetention)
	}
}

func TestApplyInspectionRecordsSnapshotFailure(t *testing.T) {
	td := testTeardown()
	scheme := newTestScheme()
	r := &HubTeardownReconciler{
		Client: fake.NewClientBuilder().WithScheme(scheme).WithInterceptorFuncs(interceptor.Funcs{
			List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
				if _, ok := list.(*corev1.ConfigMapList); ok {
					return errors.New("simulated list failure")
				}
				return c.List(ctx, list, opts...)
			},
		}).Build(),
		Scheme:   scheme,
		Log:      ctrllog.Log,
		Recorder: record.NewFakeRecorder(10),
	}
	graph := &DependencyGraph{
		DiscoveredResources: []DiscoveredResource{
			testDiscovered("g", "v1", "Kind", "ns", "name", "uid-1", testNonCloudFinalizer),
		},
	}

	if err := r.applyInspection(context.Background(), ctrllog.Log, td, graph); err != nil {
		t.Fatalf("inspection must survive a snapshot failure: %v", err)
	}
	if reason := conditionReason(td, conditionTypeInspectIncomplete); reason != "SnapshotFailed" {
		t.Errorf("InspectIncomplete reason = %q, want SnapshotFailed", reason)
	}
}
