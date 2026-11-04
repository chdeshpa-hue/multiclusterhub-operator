// Copyright Contributors to the Open Cluster Management project

package controllers

import (
	"context"
	"testing"
	"time"

	operatorsv1 "github.com/stolostron/multiclusterhub-operator/api/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	ctrllog "sigs.k8s.io/controller-runtime/pkg/log"
)

func mirrorTestTeardown() *operatorsv1.HubTeardown {
	inspected := metav1.NewTime(time.Date(2026, 11, 4, 9, 0, 0, 0, time.UTC))
	return &operatorsv1.HubTeardown{
		ObjectMeta: metav1.ObjectMeta{Name: "teardown", Namespace: "open-cluster-management"},
		Status: operatorsv1.HubTeardownStatus{
			Phase:          operatorsv1.TeardownPhaseRemoveBlockingCRs,
			Summary:        &operatorsv1.BlockerSummary{Total: 7, RequiringConsent: 2},
			LastInspection: &inspected,
			ReportRef:      &corev1.LocalObjectReference{Name: "hubteardown-report-teardown-20261104t090000z"},
		},
	}
}

func TestUninstallMirrorForNoTeardownIsNil(t *testing.T) {
	if got := uninstallMirrorFor(nil); got != nil {
		t.Fatalf("mirror for no teardown = %+v, want nil so the field is cleared", got)
	}
}

func TestUninstallMirrorForCopiesFields(t *testing.T) {
	td := mirrorTestTeardown()

	mirror := uninstallMirrorFor(td)
	if mirror == nil {
		t.Fatal("mirror is nil")
	}
	if mirror.TeardownRef.Name != td.Name {
		t.Errorf("teardownRef = %q, want %q", mirror.TeardownRef.Name, td.Name)
	}
	if mirror.Phase != operatorsv1.TeardownPhaseRemoveBlockingCRs {
		t.Errorf("phase = %q", mirror.Phase)
	}
	if mirror.Summary.Total != 7 || mirror.Summary.RequiringConsent != 2 {
		t.Errorf("summary = %+v", mirror.Summary)
	}
	if !mirror.LastInspection.Equal(td.Status.LastInspection) {
		t.Errorf("lastInspection = %v", mirror.LastInspection)
	}
	if mirror.ReportRef == nil || mirror.ReportRef.Name != td.Status.ReportRef.Name {
		t.Errorf("reportRef = %+v", mirror.ReportRef)
	}
}

func TestUninstallMirrorForDoesNotAlias(t *testing.T) {
	td := mirrorTestTeardown()
	mirror := uninstallMirrorFor(td)

	td.Status.Summary.Total = 99
	td.Status.ReportRef.Name = "changed-after-mirroring"

	if mirror.Summary.Total != 7 {
		t.Errorf("mirror followed the teardown summary: %d", mirror.Summary.Total)
	}
	if mirror.ReportRef.Name == "changed-after-mirroring" {
		t.Error("mirror aliases the teardown's reportRef")
	}

	mirror.ReportRef.Name = "changed-in-mirror"
	if td.Status.ReportRef.Name == "changed-in-mirror" {
		t.Error("teardown aliases the mirror's reportRef")
	}
}

func TestUninstallMirrorForToleratesUnscannedTeardown(t *testing.T) {
	td := &operatorsv1.HubTeardown{ObjectMeta: metav1.ObjectMeta{Name: "fresh"}}

	mirror := uninstallMirrorFor(td)
	if mirror == nil {
		t.Fatal("mirror is nil")
	}
	if mirror.Summary.Total != 0 || mirror.ReportRef != nil {
		t.Errorf("unscanned teardown produced %+v", mirror)
	}
}

func TestCalculateStatusKeepsUninstallMirror(t *testing.T) {
	td := mirrorTestTeardown()
	hub := &operatorsv1.MultiClusterHub{
		ObjectMeta: metav1.ObjectMeta{Name: "multiclusterhub", Namespace: "open-cluster-management"},
		Spec:       operatorsv1.MultiClusterHubSpec{DisableHubSelfManagement: true},
		Status: operatorsv1.MultiClusterHubStatus{
			Uninstall: uninstallMirrorFor(td),
		},
	}

	r := &MultiClusterHubReconciler{
		Client: fake.NewClientBuilder().WithScheme(scheme.Scheme).Build(),
		Scheme: scheme.Scheme,
		Log:    ctrllog.Log,
	}

	status := r.calculateStatus(context.Background(), hub, nil, map[string]*unstructured.Unstructured{}, false, false)

	if status.Uninstall == nil {
		t.Fatal("calculateStatus dropped the uninstall mirror")
	}
	if status.Uninstall.Phase != operatorsv1.TeardownPhaseRemoveBlockingCRs {
		t.Errorf("mirror phase = %q", status.Uninstall.Phase)
	}
}
