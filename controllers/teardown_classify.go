// Copyright Contributors to the Open Cluster Management project

package controllers

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/go-logr/logr"
	operatorv1 "github.com/stolostron/multiclusterhub-operator/api/v1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// unreachableGrace is how long a ManagedCluster may report Available=Unknown
// before it is treated as unreachable. A fresh install passes through Unknown
// while the klusterlet registers; acting on that would sweep a spoke that is
// merely starting.
const unreachableGrace = 5 * time.Minute

// externalReconcilerLabels are standard GitOps markers. They are consulted
// only when the object is not ACM-owned: operands legitimately carry
// app.kubernetes.io/instance as chart metadata, and flagging those would stop
// the teardown on its own workloads.
var externalReconcilerLabels = []string{
	"argocd.argoproj.io/instance",
	"app.kubernetes.io/instance",
}

// classifyBlockerBehaviour fills the classification and controller fields that
// pkg/policy consumes. The rule order is deliberate: a recreating object is a
// different problem from a slow controller, and an unreachable spoke is a
// different problem from a live hub controller.
func (r *HubTeardownReconciler) classifyBlockerBehaviour(ctx context.Context, graph *DependencyGraph, baseline map[string]types.UID) {
	byUID := make(map[types.UID]*DiscoveredResource, len(graph.DiscoveredResources))
	for i := range graph.DiscoveredResources {
		if graph.DiscoveredResources[i].UID != "" {
			byUID[graph.DiscoveredResources[i].UID] = &graph.DiscoveredResources[i]
		}
	}

	workloads := make(map[string]*bool)
	for i := range graph.DiscoveredResources {
		dr := &graph.DiscoveredResources[i]
		if len(dr.Finalizers) == 0 {
			continue
		}
		dr.ControllerAlive, dr.OwningController, dr.ControllerEvidence = r.resolveFinalizerController(ctx, graph.ScanTime, dr, workloads)
		dr.Classification = classifyDiscoveredResource(dr, graph.ScanTime, byUID, baseline)
	}
}

// resolveFinalizerController maps the object's finalizers to the operands that
// own them and records whether those operands are alive. When several
// finalizers map to different operands, any live operand keeps the verdict
// fail-closed (alive); only an object whose every known operand is gone is
// treated as having no controller left.
func (r *HubTeardownReconciler) resolveFinalizerController(ctx context.Context, observed metav1.Time, dr *DiscoveredResource, workloads map[string]*bool) (*bool, *operatorv1.ControllerRef, []operatorv1.Evidence) {
	var ref *operatorv1.ControllerRef
	var evidence []operatorv1.Evidence
	anyAlive := false
	anyKnown := false
	allDead := true

	probed := make(map[string]struct{})
	for _, finalizer := range dr.Finalizers {
		workload := finalizerController(finalizer)
		if workload == nil {
			continue
		}
		anyKnown = true
		if ref == nil {
			ref = &operatorv1.ControllerRef{
				FinalizerDomain: finalizerDomain(finalizer),
				Kind:            workload.Kind,
				Name:            workload.Name,
				Namespace:       workload.Namespace,
				Resolution:      operatorv1.ResolvedByFinalizerDomain,
			}
		}

		key := workload.Kind + "/" + workload.Namespace + "/" + workload.Name
		alive, ok := workloads[key]
		var proof operatorv1.Evidence
		if !ok {
			alive, proof = r.probeControllerWorkload(ctx, workload, observed)
			workloads[key] = alive
		} else {
			proof = workloadEvidence(workload, alive, observed)
		}
		if _, seen := probed[key]; !seen {
			evidence = append(evidence, proof)
			probed[key] = struct{}{}
		}

		if alive == nil {
			allDead = false
			continue
		}
		if *alive {
			anyAlive = true
			allDead = false
		}
	}

	if !anyKnown {
		return nil, nil, nil
	}
	if anyAlive {
		alive := true
		return &alive, ref, evidence
	}
	if allDead {
		dead := false
		return &dead, ref, evidence
	}
	return nil, ref, evidence
}

// probeControllerWorkload reads the resolved operand's readiness. A read
// failure other than NotFound is reported as unknown, never as dead: an
// authorization or API error must not authorise finalizer removal.
func (r *HubTeardownReconciler) probeControllerWorkload(ctx context.Context, workload *controllerWorkload, observed metav1.Time) (*bool, operatorv1.Evidence) {
	key := types.NamespacedName{Namespace: workload.Namespace, Name: workload.Name}
	ready := int32(0)

	switch workload.Kind {
	case "StatefulSet":
		obj := &appsv1.StatefulSet{}
		if err := r.Client.Get(ctx, key, obj); err != nil {
			return controllerReadFailure(workload, err, observed)
		}
		ready = obj.Status.ReadyReplicas
	case "Deployment":
		obj := &appsv1.Deployment{}
		if err := r.Client.Get(ctx, key, obj); err != nil {
			return controllerReadFailure(workload, err, observed)
		}
		ready = obj.Status.ReadyReplicas
	default:
		return nil, operatorv1.Evidence{
			Kind:     operatorv1.EvidenceControllerLiveness,
			Detail:   fmt.Sprintf("%s %s/%s has an unsupported workload kind", workload.Kind, workload.Namespace, workload.Name),
			Source:   "classifier",
			Observed: observed,
		}
	}

	alive := ready > 0
	return &alive, workloadEvidence(workload, &alive, observed)
}

func controllerReadFailure(workload *controllerWorkload, err error, observed metav1.Time) (*bool, operatorv1.Evidence) {
	if apierrors.IsNotFound(err) {
		dead := false
		return &dead, workloadEvidence(workload, &dead, observed)
	}
	return nil, operatorv1.Evidence{
		Kind:     operatorv1.EvidenceControllerLiveness,
		Detail:   fmt.Sprintf("could not read %s %s/%s: %v", workload.Kind, workload.Namespace, workload.Name, err),
		Source:   "classifier",
		Observed: observed,
	}
}

func workloadEvidence(workload *controllerWorkload, alive *bool, observed metav1.Time) operatorv1.Evidence {
	if alive == nil {
		return operatorv1.Evidence{
			Kind:     operatorv1.EvidenceControllerLiveness,
			Detail:   fmt.Sprintf("%s %s/%s liveness is unknown", workload.Kind, workload.Namespace, workload.Name),
			Source:   "classifier",
			Observed: observed,
		}
	}
	state := "absent or readyReplicas=0"
	if *alive {
		state = "readyReplicas>0"
	}
	return operatorv1.Evidence{
		Kind:     operatorv1.EvidenceControllerLiveness,
		Detail:   fmt.Sprintf("%s %s/%s %s", workload.Kind, workload.Namespace, workload.Name, state),
		Source:   "classifier",
		Observed: observed,
	}
}

// classifyDiscoveredResource applies the rule order from the design. It uses
// only evidence already gathered by the scan plus the controller probe.
func classifyDiscoveredResource(dr *DiscoveredResource, now metav1.Time, byUID map[types.UID]*DiscoveredResource, baseline map[string]types.UID) operatorv1.Classification {
	if hasExternalReconciler(dr, byUID) {
		return operatorv1.ClassExternalReconciler
	}
	if recreatedSinceBaseline(dr, baseline) {
		return operatorv1.ClassRecreateLoop
	}
	if isUnreachableSpoke(dr, now) {
		return operatorv1.ClassUnreachableSpoke
	}
	if dr.ControllerAlive != nil && !*dr.ControllerAlive {
		return operatorv1.ClassDeadController
	}
	for _, finalizer := range dr.Finalizers {
		if IsCloudProtectingFinalizer(finalizer) {
			return operatorv1.ClassCloudProtecting
		}
	}
	return operatorv1.ClassUnknown
}

// hasExternalReconciler walks the controller owner chain. An owner that the
// scan recognised as not ACM-owned means something outside this installation
// is reconciling the object. An owner outside the scan is not judged: the scan
// does not cover Deployments, and guessing there would flag ACM's own operands.
func hasExternalReconciler(dr *DiscoveredResource, byUID map[types.UID]*DiscoveredResource) bool {
	current := dr
	for depth := 0; depth < 16; depth++ {
		var controller *metav1.OwnerReference
		for i := range current.OwnerReferences {
			if current.OwnerReferences[i].Controller != nil && *current.OwnerReferences[i].Controller {
				controller = &current.OwnerReferences[i]
				break
			}
		}
		if controller == nil {
			break
		}
		owner, ok := byUID[controller.UID]
		if !ok {
			return false
		}
		if !owner.Ownership.Owned {
			return true
		}
		current = owner
	}
	if dr.Ownership.Owned {
		return false
	}
	for _, label := range externalReconcilerLabels {
		if _, ok := dr.Labels[label]; ok {
			return true
		}
	}
	return false
}

func recreatedSinceBaseline(dr *DiscoveredResource, baseline map[string]types.UID) bool {
	if len(baseline) == 0 || dr.UID == "" {
		return false
	}
	previous, ok := baseline[blockerKey(apiVersionFor(dr.Ref, dr.Version), dr.Ref.Kind, dr.Ref.Namespace, dr.Ref.Name)]
	return ok && previous != "" && previous != dr.UID
}

// isUnreachableSpoke recognises a non-local ManagedCluster whose addon
// reporting says unreachable, or whose Available condition has been Unknown
// past the registration grace window.
func isUnreachableSpoke(dr *DiscoveredResource, now metav1.Time) bool {
	if dr.Ref.Group != "cluster.open-cluster-management.io" || dr.Ref.Kind != "ManagedCluster" {
		return false
	}
	if dr.Ref.Name == "local-cluster" {
		return false
	}
	for key, value := range dr.Labels {
		if strings.HasPrefix(key, "feature.open-cluster-management.io/addon-") && strings.EqualFold(value, "unreachable") {
			return true
		}
	}
	if dr.AvailableStatus == "Unknown" && dr.AvailableSince != nil {
		return now.Sub(dr.AvailableSince.Time) >= unreachableGrace
	}
	return false
}

func finalizerDomain(finalizer string) string {
	if i := strings.Index(finalizer, "/"); i > 0 {
		return finalizer[:i]
	}
	return finalizer
}

// previousBlockerUIDs reads the last written report and returns its blocker
// identities. It is the UID baseline for the recreate-loop rule. A missing or
// unreadable baseline means no recreate claim: absence of a snapshot is not
// evidence of stability, and it is not evidence of recreation either.
func (r *HubTeardownReconciler) previousBlockerUIDs(ctx context.Context, td *operatorv1.HubTeardown, log logr.Logger) map[string]types.UID {
	if r.Client == nil || td == nil || td.Status.ReportRef == nil {
		return nil
	}
	cm := &corev1.ConfigMap{}
	key := types.NamespacedName{Name: td.Status.ReportRef.Name, Namespace: td.Namespace}
	if err := r.Client.Get(ctx, key, cm); err != nil {
		if !apierrors.IsNotFound(err) {
			log.Error(err, "Could not read the previous report for recreate-loop detection", "report", td.Status.ReportRef.Name)
		}
		return nil
	}

	baseline := make(map[string]types.UID)
	collect := func(body string) bool {
		if body == "" {
			return true
		}
		var report inspectReport
		if err := json.Unmarshal([]byte(body), &report); err != nil {
			log.Error(err, "Could not decode the previous report for recreate-loop detection", "report", cm.Name)
			return false
		}
		for i := range report.Blockers {
			blocker := &report.Blockers[i]
			baseline[blockerKey(blocker.APIVersion, blocker.Kind, blocker.Namespace, blocker.Name)] = blocker.UID
		}
		return true
	}

	if !collect(cm.Data[reportKeyJSON]) {
		return nil
	}
	for _, part := range reportParts(cm) {
		partCM := &corev1.ConfigMap{}
		if err := r.Client.Get(ctx, types.NamespacedName{Name: part, Namespace: td.Namespace}, partCM); err != nil {
			log.Error(err, "Could not read a previous report part for recreate-loop detection", "part", part)
			return nil
		}
		if !collect(partCM.Data[reportKeyJSON]) {
			return nil
		}
	}
	return baseline
}

func reportParts(cm *corev1.ConfigMap) []string {
	raw := cm.Data[reportKeyMeta]
	if raw == "" {
		return nil
	}
	var meta reportMeta
	if err := json.Unmarshal([]byte(raw), &meta); err != nil {
		return nil
	}
	return meta.Parts
}

func blockerKey(apiVersion, kind, namespace, name string) string {
	group := apiVersion
	if i := strings.Index(group, "/"); i >= 0 {
		group = group[:i]
	} else {
		group = ""
	}
	return group + "|" + kind + "|" + namespace + "|" + name
}
