// Copyright Contributors to the Open Cluster Management project

package controllers

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/go-logr/logr"
	operatorv1 "github.com/stolostron/multiclusterhub-operator/api/v1"
	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	corev1 "k8s.io/api/core/v1"
	apixv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	acmSubscriptionLabel = "operators.coreos.com/advanced-cluster-management"

	defaultFinalizerTimeout = 5 * time.Minute

	// annotationTeardownActive is the cross-repo signal set on the MCH CR
	// during active teardown. Downstream operators check this annotation
	// to activate teardown-aware behavior:
	//   DF-1: klusterlet-addon-controller skips MCA recreation
	//   DF-3: observability-operator strips ObsAddon finalizer immediately
	//   DF-4: import-controller force-cleans ManifestWork finalizers
	// Alternative trigger: MCH.DeletionTimestamp != nil (covers plain oc delete mch).
	annotationTeardownActive = "operator.open-cluster-management.io/teardown-active"

	// annotationTeardownActiveVersion carries the contract version. It is a
	// separate key so the confirmed DF-1..DF-5 check (value == "true") does not
	// change; a consumer that does not know this key ignores it.
	annotationTeardownActiveVersion = "operator.open-cluster-management.io/teardown-active-version"
	teardownContractVersion         = "1"
)

// agentInstallWebhookNames lists admission webhook configurations created by
// the assisted-installer whose backing service may be gone after
// AgentServiceConfig is deleted in phaseRemoveBlockingCRs.
var agentInstallWebhookNames = []string{
	"infraenvvalidators.admission.agentinstall.openshift.io",
	"agentvalidators.admission.agentinstall.openshift.io",
	"agentclusterinstallvalidators.admission.agentinstall.openshift.io",
	"agentclusterinstallmutators.admission.agentinstall.openshift.io",
}

// deleteAgentInstallWebhooks unconditionally deletes agent-install admission
// webhooks. Called after AgentServiceConfig is confirmed deleted — the backing
// service is gone, so these webhooks (failurePolicy: Fail) would reject PATCH
// calls in subsequent phases.
func (r *HubTeardownReconciler) deleteAgentInstallWebhooks(
	ctx context.Context,
	log logr.Logger,
	td *operatorv1.HubTeardown,
) error {
	for _, name := range agentInstallWebhookNames {
		key := types.NamespacedName{Name: name}

		vwc := &admissionregistrationv1.ValidatingWebhookConfiguration{}
		if err := r.Client.Get(ctx, key, vwc); err == nil {
			log.Info("Deleting agent-install ValidatingWebhookConfiguration", "name", name)
			if err := r.Client.Delete(ctx, vwc); err != nil && !errors.IsNotFound(err) {
				return fmt.Errorf("deleting VWC %s: %w", name, err)
			}
			r.Recorder.Eventf(td, corev1.EventTypeNormal, "AgentWebhookDeleted",
				"Deleted ValidatingWebhookConfiguration %s (agent-install service gone)", name)
			continue
		}

		mwc := &admissionregistrationv1.MutatingWebhookConfiguration{}
		if err := r.Client.Get(ctx, key, mwc); err == nil {
			log.Info("Deleting agent-install MutatingWebhookConfiguration", "name", name)
			if err := r.Client.Delete(ctx, mwc); err != nil && !errors.IsNotFound(err) {
				return fmt.Errorf("deleting MWC %s: %w", name, err)
			}
			r.Recorder.Eventf(td, corev1.EventTypeNormal, "AgentWebhookDeleted",
				"Deleted MutatingWebhookConfiguration %s (agent-install service gone)", name)
		}
	}
	return nil
}

// setTeardownActiveAnnotation annotates the MCH CR with the teardown-active
// signal. Downstream operators watch for this annotation to activate their
// own teardown-aware paths (see DF-1, DF-3, DF-4). Idempotent — skips if
// already set. Called at the start of PrepareAndDrain (phaseRemoveBlockingCRs).
func (r *HubTeardownReconciler) setTeardownActiveAnnotation(ctx context.Context, log logr.Logger) error {
	mchGVK := schema.GroupVersionKind{
		Group:   "operator.open-cluster-management.io",
		Version: "v1",
		Kind:    "MultiClusterHubList",
	}
	list := &unstructured.UnstructuredList{}
	list.SetGroupVersionKind(mchGVK)

	if err := r.Client.List(ctx, list); err != nil {
		if isNoMatchError(err) {
			return nil
		}
		return fmt.Errorf("listing MCH for teardown-active annotation: %w", err)
	}

	for i := range list.Items {
		mch := &list.Items[i]
		annotations := mch.GetAnnotations()
		if annotations[annotationTeardownActive] == "true" &&
			annotations[annotationTeardownActiveVersion] == teardownContractVersion {
			continue
		}
		if annotations == nil {
			annotations = make(map[string]string)
		}
		patch := client.MergeFrom(mch.DeepCopy())
		annotations[annotationTeardownActive] = "true"
		annotations[annotationTeardownActiveVersion] = teardownContractVersion
		mch.SetAnnotations(annotations)
		if err := r.Client.Patch(ctx, mch, patch); err != nil {
			if errors.IsNotFound(err) {
				continue
			}
			return fmt.Errorf("setting teardown-active annotation on MCH %s: %w", mch.GetName(), err)
		}
		log.Info("Set teardown-active annotation on MCH — downstream operators will activate teardown paths",
			"name", mch.GetName(), "annotation", annotationTeardownActive)
	}
	return nil
}

// readContract reports what the contract looks like on the hub. The prepare Job
// calls it after RemoveBlockingCRs so the operator can surface the result; the
// Job never writes status.
func (r *HubTeardownReconciler) readContract(ctx context.Context, log logr.Logger) *contractRecord {
	record := &contractRecord{Version: teardownContractVersion, Signalled: true}

	list := &unstructured.UnstructuredList{}
	list.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "operator.open-cluster-management.io",
		Version: "v1",
		Kind:    "MultiClusterHubList",
	})
	if err := r.Client.List(ctx, list); err != nil {
		if isNoMatchError(err) {
			record.Reason = "NoMCH"
			record.Message = "MultiClusterHub API is not installed; nothing to signal."
			return record
		}
		record.Signalled = false
		record.Reason = "ContractUnreadable"
		record.Message = err.Error()
		return record
	}

	for i := range list.Items {
		mch := &list.Items[i]
		annotations := mch.GetAnnotations()
		record.MCHs = append(record.MCHs, mch.GetName())
		if annotations[annotationTeardownActive] != "true" ||
			annotations[annotationTeardownActiveVersion] != teardownContractVersion {
			record.Signalled = false
		}
	}

	switch {
	case len(record.MCHs) == 0:
		record.Reason = "NoMCH"
		record.Message = "No MultiClusterHub present; nothing to signal."
	case record.Signalled:
		record.Reason = "Signalled"
		record.Message = fmt.Sprintf("Signalled %d MultiClusterHub(s) at contract version %s.",
			len(record.MCHs), teardownContractVersion)
	default:
		record.Reason = "SignallingIncomplete"
		record.Message = "One or more MultiClusterHub objects are missing the teardown-active contract."
	}
	log.V(1).Info("Read teardown-active contract", "signalled", record.Signalled, "reason", record.Reason)
	return record
}

// clearTeardownActiveAnnotation removes the contract from every MCH. It is
// called when the HubTeardown CR is deleted, which is the only explicit cancel.
// Best effort by design: a stuck patch must not hold the CR finalizer on a hub
// that may already be gone.
func (r *HubTeardownReconciler) clearTeardownActiveAnnotation(ctx context.Context, log logr.Logger) error {
	list := &unstructured.UnstructuredList{}
	list.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "operator.open-cluster-management.io",
		Version: "v1",
		Kind:    "MultiClusterHubList",
	})
	if err := r.Client.List(ctx, list); err != nil {
		if isNoMatchError(err) {
			return nil
		}
		return fmt.Errorf("listing MCH to clear teardown-active contract: %w", err)
	}

	for i := range list.Items {
		mch := &list.Items[i]
		annotations := mch.GetAnnotations()
		if annotations == nil {
			continue
		}
		if _, ok := annotations[annotationTeardownActive]; !ok {
			if _, ok := annotations[annotationTeardownActiveVersion]; !ok {
				continue
			}
		}

		patch := client.MergeFrom(mch.DeepCopy())
		delete(annotations, annotationTeardownActive)
		delete(annotations, annotationTeardownActiveVersion)
		mch.SetAnnotations(annotations)
		if err := r.Client.Patch(ctx, mch, patch); err != nil && !errors.IsNotFound(err) {
			return fmt.Errorf("clearing teardown-active contract on MCH %s: %w", mch.GetName(), err)
		}
		log.Info("Cleared teardown-active contract from MCH", "name", mch.GetName())
	}
	return nil
}

// phaseRemoveBlockingCRs deletes resources that block MCH webhook validation:
// MultiClusterObservability, DiscoveryConfig, AgentServiceConfig.
func (r *HubTeardownReconciler) phaseRemoveBlockingCRs(ctx context.Context, log logr.Logger, td *operatorv1.HubTeardown) (bool, error) {
	log.Info("Phase: RemoveBlockingCRs")

	// Signal downstream operators that teardown is active. Must be set before
	// deleting resources so DF-1/DF-3/DF-4 activate their teardown paths.
	// A hub that cannot signal the contract must not go on to delete
	// resources: the DF operators would recreate what this phase removes.
	if err := r.setTeardownActiveAnnotation(ctx, log); err != nil {
		return false, fmt.Errorf("signalling teardown-active contract: %w", err)
	}

	blockingTypes := []struct {
		group   string
		version string
		kind    string
	}{
		{"observability.open-cluster-management.io", "v1beta2", "MultiClusterObservability"},
		{"discovery.open-cluster-management.io", "v1", "DiscoveryConfig"},
		{"agent-install.openshift.io", "v1beta1", "AgentServiceConfig"},
	}

	allGone := true
	for _, bt := range blockingTypes {
		listGVK := schema.GroupVersionKind{
			Group:   bt.group,
			Version: bt.version,
			Kind:    bt.kind + "List",
		}
		list := &unstructured.UnstructuredList{}
		list.SetGroupVersionKind(listGVK)

		if err := r.Client.List(ctx, list); err != nil {
			if errors.IsNotFound(err) || isNoMatchError(err) {
				continue
			}
			return false, fmt.Errorf("listing %s: %w", bt.kind, err)
		}

		for i := range list.Items {
			item := &list.Items[i]
			if item.GetDeletionTimestamp() != nil {
				log.Info("Blocking CR already deleting, waiting", "kind", bt.kind, "name", item.GetName())
				allGone = false
				continue
			}
			log.Info("Deleting blocking CR", "kind", bt.kind, "name", item.GetName())
			if err := r.Client.Delete(ctx, item); err != nil && !errors.IsNotFound(err) {
				return false, fmt.Errorf("deleting %s/%s: %w", bt.kind, item.GetName(), err)
			}
			r.Recorder.Eventf(td, corev1.EventTypeNormal, "BlockingCRDeleted",
				"Deleted %s/%s to unblock MCH webhook", bt.kind, item.GetName())
			allGone = false
		}
	}

	if allGone {
		if err := r.deleteAgentInstallWebhooks(ctx, log, td); err != nil {
			log.Error(err, "Error deleting agent-install webhooks after blocking CRs removed")
			return false, nil
		}
	}
	return allGone, nil
}

// phaseDisableAddons deletes ManagedClusterAddOns for non-local ManagedClusters.
// It first disables addon-manager placements to prevent recreation, then deletes
// MCAs and resolves stuck addon finalizers on terminating MCAs.
func (r *HubTeardownReconciler) phaseDisableAddons(ctx context.Context, log logr.Logger, td *operatorv1.HubTeardown) (bool, error) {
	log.Info("Phase: DisableAddons")

	// Step 1: Disable ClusterManagementAddOn placements to break CMAO recreation loop.
	if err := r.disableAddonPlacements(ctx, log); err != nil {
		log.Error(err, "Failed to disable addon placements, continuing with MCA deletion")
	}

	// Step 2: Identify unreachable ManagedClusters (skip waiting for their addons)
	unreachableMCs := r.getUnreachableManagedClusters(ctx, log)

	addonGVK := schema.GroupVersionKind{
		Group:   "addon.open-cluster-management.io",
		Version: "v1alpha1",
		Kind:    "ManagedClusterAddOnList",
	}
	list := &unstructured.UnstructuredList{}
	list.SetGroupVersionKind(addonGVK)

	if err := r.Client.List(ctx, list); err != nil {
		if isNoMatchError(err) {
			log.Info("ManagedClusterAddOn API not available, skipping")
			return true, nil
		}
		return false, fmt.Errorf("listing ManagedClusterAddOns: %w", err)
	}

	remaining := 0
	for i := range list.Items {
		addon := &list.Items[i]
		ns := addon.GetNamespace()
		if ns == "local-cluster" {
			continue
		}

		// Addons on unreachable clusters will never terminate gracefully
		// because the klusterlet has no connectivity to process finalizers.
		// Still require approvedDestructiveActions + forceFinalizerTimeout
		// to avoid stripping finalizers on clusters with transient network issues.
		if unreachableMCs[ns] {
			if addon.GetDeletionTimestamp() == nil {
				log.Info("Deleting addon on unreachable cluster", "namespace", ns, "name", addon.GetName())
				if err := r.Client.Delete(ctx, addon); err != nil && !errors.IsNotFound(err) {
					return false, fmt.Errorf("deleting addon %s/%s: %w", ns, addon.GetName(), err)
				}
			}
			if addon.GetDeletionTimestamp() != nil && len(addon.GetFinalizers()) > 0 {
				if td.Spec.ApprovedDestructiveActions && r.shouldResolveStuckFinalizers(td, addon) {
					r.resolveAddonFinalizers(ctx, log, td, addon)
				} else {
					remaining++
				}
			}
			continue
		}

		if addon.GetDeletionTimestamp() != nil {
			// Resolve stuck addon finalizers if past timeout and approvedDestructiveActions
			if r.shouldResolveStuckFinalizers(td, addon) {
				r.resolveAddonFinalizers(ctx, log, td, addon)
			} else {
				remaining++
			}
			continue
		}
		log.Info("Deleting addon", "namespace", ns, "name", addon.GetName())
		if err := r.Client.Delete(ctx, addon); err != nil && !errors.IsNotFound(err) {
			return false, fmt.Errorf("deleting addon %s/%s: %w", ns, addon.GetName(), err)
		}
		remaining++
	}

	if remaining > 0 {
		msg := fmt.Sprintf("Waiting for %d addons to terminate.", remaining)
		needsApproval := false
		for i := range list.Items {
			addon := &list.Items[i]
			if addon.GetNamespace() == "local-cluster" || addon.GetDeletionTimestamp() == nil {
				continue
			}
			for _, f := range addon.GetFinalizers() {
				if !IsAllowlistedFinalizer(f) {
					continue
				}
				if IsCloudProtectingFinalizer(f) && !td.Spec.AcknowledgeCloudResourceRisk {
					msg += " Set spec.acknowledgeCloudResourceRisk=true to patch Tier 1 (cloud-protecting) finalizers."
					needsApproval = true
				} else if !IsCloudProtectingFinalizer(f) && !td.Spec.ApprovedDestructiveActions {
					msg += " Set spec.approvedDestructiveActions=true to patch Tier 2 (non-cloud) finalizers."
					needsApproval = true
				}
				if needsApproval {
					break
				}
			}
			if needsApproval {
				break
			}
		}
		r.setPhaseStatus(td, operatorv1.TeardownPhaseDisableAddons, operatorv1.PhaseStateInProgress, msg)
		log.Info(msg, "remaining", remaining)
		return false, nil
	}
	return true, nil
}

// disableAddonPlacements deletes Placement references from ClusterManagementAddOns
// to prevent the addon-manager from recreating MCAs after deletion.
func (r *HubTeardownReconciler) disableAddonPlacements(ctx context.Context, log logr.Logger) error {
	cmaGVK := schema.GroupVersionKind{
		Group:   "addon.open-cluster-management.io",
		Version: "v1alpha1",
		Kind:    "ClusterManagementAddOnList",
	}
	list := &unstructured.UnstructuredList{}
	list.SetGroupVersionKind(cmaGVK)

	if err := r.Client.List(ctx, list); err != nil {
		if isNoMatchError(err) {
			return nil
		}
		return fmt.Errorf("listing ClusterManagementAddOns: %w", err)
	}

	for i := range list.Items {
		cma := &list.Items[i]
		spec, ok, _ := unstructured.NestedMap(cma.Object, "spec")
		if !ok || spec == nil {
			continue
		}
		_, hasInstallStrategy, _ := unstructured.NestedSlice(cma.Object, "spec", "installStrategy", "placements")
		if !hasInstallStrategy {
			continue
		}
		patch := client.MergeFrom(cma.DeepCopy())
		unstructured.RemoveNestedField(cma.Object, "spec", "installStrategy", "placements")
		if err := r.Client.Patch(ctx, cma, patch); err != nil {
			if !errors.IsNotFound(err) {
				log.Error(err, "Failed to remove placements from ClusterManagementAddOn", "name", cma.GetName())
			}
			continue
		}
		log.Info("Disabled addon placements", "clusterManagementAddOn", cma.GetName())
	}
	return nil
}

// getUnreachableManagedClusters returns a set of MC names where the cluster
// has never been imported or has lost connectivity (Available=Unknown/False).
func (r *HubTeardownReconciler) getUnreachableManagedClusters(ctx context.Context, log logr.Logger) map[string]bool {
	result := make(map[string]bool)
	mcGVK := schema.GroupVersionKind{
		Group:   "cluster.open-cluster-management.io",
		Version: "v1",
		Kind:    "ManagedClusterList",
	}
	list := &unstructured.UnstructuredList{}
	list.SetGroupVersionKind(mcGVK)

	if err := r.Client.List(ctx, list); err != nil {
		return result
	}

	for i := range list.Items {
		mc := &list.Items[i]
		name := mc.GetName()
		if name == "local-cluster" {
			continue
		}
		conditions, _, _ := unstructured.NestedSlice(mc.Object, "status", "conditions")
		for _, c := range conditions {
			cond, ok := c.(map[string]interface{})
			if !ok {
				continue
			}
			condType, _ := cond["type"].(string)
			condStatus, _ := cond["status"].(string)
			if condType == "ManagedClusterConditionAvailable" && condStatus != "True" {
				log.Info("ManagedCluster unreachable, will force addon cleanup", "cluster", name, "status", condStatus)
				result[name] = true
				break
			}
		}
	}
	return result
}

// resolveAddonFinalizers resolves stuck addon finalizers on a single MCA.
func (r *HubTeardownReconciler) resolveAddonFinalizers(ctx context.Context, log logr.Logger, td *operatorv1.HubTeardown, addon *unstructured.Unstructured) {
	finalizers := addon.GetFinalizers()
	if len(finalizers) == 0 {
		return
	}
	dr := DiscoveredResource{
		Ref: operatorv1.ResourceRef{
			Group:     "addon.open-cluster-management.io",
			Kind:      "ManagedClusterAddOn",
			Namespace: addon.GetNamespace(),
			Name:      addon.GetName(),
		},
		Version:    "v1alpha1",
		Finalizers: finalizers,
	}
	if _, err := r.resolveStuckFinalizers(ctx, log, td, []DiscoveredResource{dr}); err != nil {
		log.Error(err, "Error resolving addon finalizers", "addon", addon.GetNamespace()+"/"+addon.GetName())
	}
}

// phaseDeleteInfrastructureCRs deletes all infrastructure provisioning CRs (HostedClusters,
// ClusterDeployments, ClusterPools, NodePools, InfraEnvs) while their controllers are still
// alive to handle graceful deprovisioning. Force-resolves finalizers after timeout.
//
// The acknowledgeCloudResourceRisk gate is only applied when infrastructure CRs
// actually exist. Hubs with no cloud-provisioned clusters skip this gate entirely.
func (r *HubTeardownReconciler) phaseDeleteInfrastructureCRs(ctx context.Context, log logr.Logger, td *operatorv1.HubTeardown) (bool, error) {
	log.Info("Phase: DeleteInfrastructureCRs")

	infraTypes := []struct {
		group   string
		version string
		kind    string
	}{
		{"hypershift.openshift.io", "v1beta1", "HostedCluster"},
		{"hypershift.openshift.io", "v1beta1", "NodePool"},
		{"hive.openshift.io", "v1", "ClusterDeployment"},
		{"hive.openshift.io", "v1", "ClusterPool"},
		{"agent-install.openshift.io", "v1beta1", "InfraEnv"},
		{"siteconfig.open-cluster-management.io", "v1alpha1", "ClusterInstance"},
	}

	// Collect all existing infrastructure CR instances first.
	type infraItem struct {
		obj     *unstructured.Unstructured
		group   string
		version string
		kind    string
	}
	var allItems []infraItem

	for _, it := range infraTypes {
		listGVK := schema.GroupVersionKind{
			Group:   it.group,
			Version: it.version,
			Kind:    it.kind + "List",
		}
		list := &unstructured.UnstructuredList{}
		list.SetGroupVersionKind(listGVK)

		if err := r.Client.List(ctx, list); err != nil {
			if isNoMatchError(err) {
				continue
			}
			return false, fmt.Errorf("listing %s: %w", it.kind, err)
		}

		for i := range list.Items {
			allItems = append(allItems, infraItem{
				obj:     &list.Items[i],
				group:   it.group,
				version: it.version,
				kind:    it.kind,
			})
		}
	}

	// No infrastructure CRs on this hub — skip the cloud risk gate entirely.
	if len(allItems) == 0 {
		log.Info("No infrastructure CRs found, skipping phase")
		return true, nil
	}

	// Gate: require explicit acknowledgment only when infra CRs actually exist.
	if !td.Spec.AcknowledgeCloudResourceRisk {
		log.Info("Waiting for spec.acknowledgeCloudResourceRisk=true before deleting infrastructure CRs",
			"infraCRCount", len(allItems))
		r.Recorder.Eventf(td, corev1.EventTypeWarning, "CloudRiskGate",
			"%d infrastructure CRs (HostedClusters, ClusterDeployments, etc.) require spec.acknowledgeCloudResourceRisk=true to proceed",
			len(allItems))
		return false, nil
	}

	remaining := 0
	for _, ii := range allItems {
		item := ii.obj

		if item.GetDeletionTimestamp() != nil {
			if r.shouldResolveStuckFinalizers(td, item) && td.Spec.AcknowledgeCloudResourceRisk {
				dr := DiscoveredResource{
					Ref: operatorv1.ResourceRef{
						Group:     ii.group,
						Kind:      ii.kind,
						Namespace: item.GetNamespace(),
						Name:      item.GetName(),
					},
					Version:    ii.version,
					Finalizers: item.GetFinalizers(),
				}
				resolved, err := r.resolveStuckFinalizers(ctx, log, td, []DiscoveredResource{dr})
				if err != nil {
					log.Error(err, "Error resolving finalizers on infrastructure CR",
						"kind", ii.kind, "name", item.GetName(), "namespace", item.GetNamespace())
				}
				if !resolved {
					remaining++
				}
			} else {
				remaining++
			}
			continue
		}

		log.Info("Deleting infrastructure CR",
			"kind", ii.kind, "name", item.GetName(), "namespace", item.GetNamespace())
		if err := r.Client.Delete(ctx, item); err != nil && !errors.IsNotFound(err) {
			return false, fmt.Errorf("deleting %s %s/%s: %w", ii.kind, item.GetNamespace(), item.GetName(), err)
		}
		r.Recorder.Eventf(td, corev1.EventTypeNormal, "InfrastructureCRDeleted",
			"Deleted %s %s/%s", ii.kind, item.GetNamespace(), item.GetName())
		remaining++
	}

	if remaining > 0 {
		msg := fmt.Sprintf("Waiting for %d infrastructure CRs to terminate.", remaining)
		needsApproval := false
		for _, ii := range allItems {
			item := ii.obj
			if item.GetDeletionTimestamp() == nil {
				continue
			}
			for _, f := range item.GetFinalizers() {
				if !IsAllowlistedFinalizer(f) {
					continue
				}
				if IsCloudProtectingFinalizer(f) && !td.Spec.AcknowledgeCloudResourceRisk {
					msg += " Set spec.acknowledgeCloudResourceRisk=true to patch Tier 1 (cloud-protecting) finalizers."
					needsApproval = true
				} else if !IsCloudProtectingFinalizer(f) && !td.Spec.ApprovedDestructiveActions {
					msg += " Set spec.approvedDestructiveActions=true to patch Tier 2 (non-cloud) finalizers."
					needsApproval = true
				}
				if needsApproval {
					break
				}
			}
			if needsApproval {
				break
			}
		}
		r.setPhaseStatus(td, operatorv1.TeardownPhaseDeleteInfrastructureCRs, operatorv1.PhaseStateInProgress, msg)
		log.Info(msg, "remaining", remaining)
		return false, nil
	}

	log.Info("All infrastructure CRs deleted")
	return true, nil
}

// phaseDetachManagedClusters deletes non-local ManagedClusters so the MCH webhook passes.
// With DF-4 (import-controller ManifestWork finalizer sweep), ManifestWork finalizers
// are force-cleaned during teardown so ManagedClusters terminate faster. The tiered
// force-strip logic remains as a safety net for finalizers not covered by DF-4.
func (r *HubTeardownReconciler) phaseDetachManagedClusters(ctx context.Context, log logr.Logger, td *operatorv1.HubTeardown) (bool, error) {
	log.Info("Phase: DetachManagedClusters")

	mcGVK := schema.GroupVersionKind{
		Group:   "cluster.open-cluster-management.io",
		Version: "v1",
		Kind:    "ManagedClusterList",
	}
	list := &unstructured.UnstructuredList{}
	list.SetGroupVersionKind(mcGVK)

	if err := r.Client.List(ctx, list); err != nil {
		if isNoMatchError(err) {
			return true, nil
		}
		return false, fmt.Errorf("listing ManagedClusters: %w", err)
	}

	remaining := 0
	for i := range list.Items {
		mc := &list.Items[i]
		if mc.GetName() == "local-cluster" {
			continue
		}

		if mc.GetDeletionTimestamp() != nil {
			remaining++

			if r.shouldResolveStuckFinalizers(td, mc) {
				dr := DiscoveredResource{
					Ref: operatorv1.ResourceRef{
						Group: "cluster.open-cluster-management.io",
						Kind:  "ManagedCluster",
						Name:  mc.GetName(),
					},
					Version:    "v1",
					Finalizers: mc.GetFinalizers(),
				}
				resolved, err := r.resolveStuckFinalizers(ctx, log, td, []DiscoveredResource{dr})
				if err != nil {
					log.Error(err, "Error resolving stuck finalizers on ManagedCluster", "name", mc.GetName())
				}
				if resolved {
					remaining--
				} else if td.Spec.ApprovedDestructiveActions {
					// Gap 12 safety net: if allowlisted resolution did not clear all finalizers
					// (e.g. agent-side finalizers from an offline/destroyed cluster), force-strip
					// ALL remaining finalizers after the timeout. This prevents indefinite hang.
					// Require acknowledgeCloudResourceRisk if any cloud-protecting finalizers remain.
					hasCloudFinalizer := false
					for _, f := range mc.GetFinalizers() {
						if IsCloudProtectingFinalizer(f) {
							hasCloudFinalizer = true
							break
						}
					}
					if hasCloudFinalizer && !td.Spec.AcknowledgeCloudResourceRisk {
						log.Info("Skipping force-strip: ManagedCluster has cloud-protecting finalizers but acknowledgeCloudResourceRisk is not set",
							"name", mc.GetName())
					} else {
						ref := operatorv1.ResourceRef{
							Group: "cluster.open-cluster-management.io",
							Kind:  "ManagedCluster",
							Name:  mc.GetName(),
						}
						gvk := schema.GroupVersionKind{
							Group:   "cluster.open-cluster-management.io",
							Version: "v1",
							Kind:    "ManagedCluster",
						}
						if err := r.forceStripAllFinalizers(ctx, log, td, ref, gvk); err != nil {
							log.Error(err, "Failed to force-strip finalizers on ManagedCluster", "name", mc.GetName())
						} else {
							remaining--
						}
					}
				}
			}
			continue
		}

		log.Info("Deleting ManagedCluster", "name", mc.GetName())
		if err := r.Client.Delete(ctx, mc); err != nil && !errors.IsNotFound(err) {
			return false, fmt.Errorf("deleting ManagedCluster %s: %w", mc.GetName(), err)
		}
		r.Recorder.Eventf(td, corev1.EventTypeNormal, "ManagedClusterDetached",
			"Deleted ManagedCluster %s", mc.GetName())
		remaining++
	}

	if remaining > 0 {
		msg := fmt.Sprintf("Waiting for %d ManagedClusters to terminate.", remaining)
		needsApproval := false
		for i := range list.Items {
			mc := &list.Items[i]
			if mc.GetName() == "local-cluster" || mc.GetDeletionTimestamp() == nil {
				continue
			}
			for _, f := range mc.GetFinalizers() {
				if !IsAllowlistedFinalizer(f) {
					continue
				}
				if IsCloudProtectingFinalizer(f) && !td.Spec.AcknowledgeCloudResourceRisk {
					msg += " Set spec.acknowledgeCloudResourceRisk=true to patch Tier 1 (cloud-protecting) finalizers."
					needsApproval = true
				} else if !IsCloudProtectingFinalizer(f) && !td.Spec.ApprovedDestructiveActions {
					msg += " Set spec.approvedDestructiveActions=true to patch Tier 2 (non-cloud) finalizers."
					needsApproval = true
				}
				if needsApproval {
					break
				}
			}
			if needsApproval {
				break
			}
		}
		r.setPhaseStatus(td, operatorv1.TeardownPhaseDetachManagedClusters, operatorv1.PhaseStateInProgress, msg)
		log.Info(msg, "remaining", remaining)
		return false, nil
	}

	r.sweepMCSAndCMA(ctx, log, td)
	return true, nil
}

// phaseDeleteMCH deletes the MultiClusterHub CR.
//
// CONCURRENT RECONCILER NOTE: Deleting the MCH triggers the MCH reconciler's
// finalizeHub in the same operator process. finalizeHub and HubTeardown phases
// 7-9 (MonitorMCEChain, CleanOrphans, DeleteACMCRDs) overlap in scope — both
// delete operands, remove CRDs, and clean up resources. This is by design:
// HubTeardown orchestrates the preconditions so finalizeHub can run cleanly,
// then monitors and mops up what finalizeHub leaves behind. The overlap is
// safe because all delete operations are idempotent (NotFound is ignored),
// but duplicate events and log entries are expected during this window.
func (r *HubTeardownReconciler) phaseDeleteMCH(ctx context.Context, log logr.Logger, td *operatorv1.HubTeardown) (bool, error) {
	log.Info("Phase: DeleteMCH")

	mchGVK := schema.GroupVersionKind{
		Group:   "operator.open-cluster-management.io",
		Version: "v1",
		Kind:    "MultiClusterHubList",
	}
	list := &unstructured.UnstructuredList{}
	list.SetGroupVersionKind(mchGVK)

	if err := r.Client.List(ctx, list); err != nil {
		return false, fmt.Errorf("listing MultiClusterHub: %w", err)
	}

	if len(list.Items) == 0 {
		log.Info("No MultiClusterHub found, phase complete")
		return true, nil
	}

	for i := range list.Items {
		mch := &list.Items[i]
		if mch.GetDeletionTimestamp() != nil {
			if r.shouldResolveStuckFinalizers(td, mch) {
				log.Info("MCH stuck deleting past timeout, resolving blocking finalizers", "name", mch.GetName())
				if err := r.resolveMCHBlockingFinalizers(ctx, log, td); err != nil {
					log.Error(err, "Error resolving MCH-blocking finalizers")
				}
				dr := DiscoveredResource{
					Ref: operatorv1.ResourceRef{
						Group:     "operator.open-cluster-management.io",
						Kind:      "MultiClusterHub",
						Namespace: mch.GetNamespace(),
						Name:      mch.GetName(),
					},
					Version:    "v1",
					Finalizers: mch.GetFinalizers(),
				}
				if _, err := r.resolveStuckFinalizers(ctx, log, td, []DiscoveredResource{dr}); err != nil {
					log.Error(err, "Error resolving MCH finalizers")
				}
			} else {
				log.Info("MCH already deleting, waiting for finalizer to complete", "name", mch.GetName())
			}
			return false, nil
		}
		log.Info("Deleting MultiClusterHub", "name", mch.GetName(), "namespace", mch.GetNamespace())
		if err := r.Client.Delete(ctx, mch); err != nil {
			if errors.IsNotFound(err) {
				continue
			}
			return false, fmt.Errorf("deleting MCH %s: %w", mch.GetName(), err)
		}
		r.Recorder.Eventf(td, corev1.EventTypeNormal, "MCHDeleted",
			"Deleted MultiClusterHub %s/%s", mch.GetNamespace(), mch.GetName())
	}

	return false, nil
}

// resolveMCHBlockingFinalizers resolves stuck finalizers on resources that
// block the MCH's own finalizer (finalizeHub). Submariner's cleanup finalizers
// on ClusterManagementAddOns and ManagedClusterSets are the known blockers.
func (r *HubTeardownReconciler) resolveMCHBlockingFinalizers(
	ctx context.Context,
	log logr.Logger,
	td *operatorv1.HubTeardown,
) error {
	blockerTypes := []struct {
		group   string
		version string
		kind    string
	}{
		{"addon.open-cluster-management.io", "v1alpha1", "ClusterManagementAddOn"},
		{"cluster.open-cluster-management.io", "v1beta2", "ManagedClusterSet"},
	}

	for _, bt := range blockerTypes {
		listGVK := schema.GroupVersionKind{
			Group:   bt.group,
			Version: bt.version,
			Kind:    bt.kind + "List",
		}
		list := &unstructured.UnstructuredList{}
		list.SetGroupVersionKind(listGVK)

		if err := r.Client.List(ctx, list); err != nil {
			if isNoMatchError(err) || errors.IsNotFound(err) {
				continue
			}
			return fmt.Errorf("listing %s: %w", bt.kind, err)
		}

		for i := range list.Items {
			item := &list.Items[i]
			if item.GetDeletionTimestamp() == nil || len(item.GetFinalizers()) == 0 {
				continue
			}

			log.Info("Resolving stuck finalizers on MCH-blocking resource",
				"kind", bt.kind, "name", item.GetName())
			dr := DiscoveredResource{
				Ref: operatorv1.ResourceRef{
					Group:     bt.group,
					Kind:      bt.kind,
					Namespace: item.GetNamespace(),
					Name:      item.GetName(),
				},
				Version:    bt.version,
				Finalizers: item.GetFinalizers(),
			}
			if _, err := r.resolveStuckFinalizers(ctx, log, td, []DiscoveredResource{dr}); err != nil {
				log.Error(err, "Failed to resolve finalizers on MCH-blocking resource",
					"kind", bt.kind, "name", item.GetName())
			}
		}
	}
	return nil
}

// sweepMCSAndCMA proactively deletes ManagedClusterSet and ClusterManagementAddOn
// resources and strips their finalizers. Called at the end of phaseDetachManagedClusters
// to clear Submariner/addon finalizers whose processing controllers have been
// scaled down or removed, so finalizeHub runs unobstructed when MCH is deleted.
func (r *HubTeardownReconciler) sweepMCSAndCMA(
	ctx context.Context,
	log logr.Logger,
	td *operatorv1.HubTeardown,
) {
	sweepTypes := []struct {
		group   string
		version string
		kind    string
	}{
		{"addon.open-cluster-management.io", "v1alpha1", "ClusterManagementAddOn"},
		{"cluster.open-cluster-management.io", "v1beta2", "ManagedClusterSet"},
	}

	for _, st := range sweepTypes {
		listGVK := schema.GroupVersionKind{
			Group:   st.group,
			Version: st.version,
			Kind:    st.kind + "List",
		}
		list := &unstructured.UnstructuredList{}
		list.SetGroupVersionKind(listGVK)

		if err := r.Client.List(ctx, list); err != nil {
			if isNoMatchError(err) || errors.IsNotFound(err) {
				continue
			}
			log.Error(err, "Failed to list resources for MCS/CMA sweep", "kind", st.kind)
			continue
		}

		for i := range list.Items {
			item := &list.Items[i]

			if item.GetDeletionTimestamp() == nil {
				log.Info("Sweeping pre-MCH-delete resource", "kind", st.kind, "name", item.GetName())
				if err := r.Client.Delete(ctx, item); err != nil && !errors.IsNotFound(err) {
					log.Error(err, "Failed to delete in MCS/CMA sweep", "kind", st.kind, "name", item.GetName())
					continue
				}
				r.Recorder.Eventf(td, corev1.EventTypeNormal, "MCSCMASweep",
					"Deleted %s %s before MCH delete (controller scaled down)", st.kind, item.GetName())
			}

			if len(item.GetFinalizers()) > 0 && r.shouldResolveStuckFinalizers(td, item) {
				dr := drFromUnstructured(item, st.group, st.version, st.kind)
				if _, err := r.resolveStuckFinalizers(ctx, log, td, []DiscoveredResource{dr}); err != nil {
					log.Error(err, "Failed to resolve finalizers in MCS/CMA sweep",
						"kind", st.kind, "name", item.GetName())
				}
			}
		}
	}
}

// phaseMonitorMCEChain drives the MCE -> ClusterManager -> Hypershift cleanup chain.
// When the MCH finalizer cascade ran normally, MCE is already deleting and this
// phase monitors its removal. When MCH was already gone (cascade did not fire),
// this phase actively initiates MCE deletion, cleans up local-cluster, and
// resolves orphaned addon/submariner finalizers.
func (r *HubTeardownReconciler) phaseMonitorMCEChain(ctx context.Context, log logr.Logger, td *operatorv1.HubTeardown) (bool, error) {
	log.Info("Phase: MonitorMCEChain")

	mchGone, err := r.isResourceGone(ctx, "operator.open-cluster-management.io", "v1", "MultiClusterHub")
	if err != nil {
		return false, err
	}
	if !mchGone {
		log.Info("Waiting for MultiClusterHub to be fully removed")
		return false, nil
	}

	// Resolve stuck submariner/addon finalizers on ClusterManagementAddOns and
	// ManagedClusterSets. When MCH was present, resolveMCHBlockingFinalizers ran
	// inside phaseDeleteMCH. When MCH was already gone, the call never fired.
	if err := r.resolveMCHBlockingFinalizers(ctx, log, td); err != nil {
		log.Error(err, "Error resolving MCH-blocking finalizers in MonitorMCEChain")
	}

	// Clean up local-cluster addons and ManagedCluster. Earlier phases skip
	// local-cluster because the MCH webhook requires it for MCH deletion. Now
	// that MCH is gone, local-cluster can be safely removed.
	if err := r.cleanupLocalCluster(ctx, log, td); err != nil {
		log.Error(err, "Error cleaning up local-cluster")
	}

	// Ensure MCE is deleted. When the MCH finalizer cascade ran, MCE is already
	// deleting. When MCH was already gone, nobody issued the delete.
	if err := r.ensureMCEDeleted(ctx, log, td); err != nil {
		return false, err
	}

	mceGone, err := r.isResourceGone(ctx, "multicluster.openshift.io", "v1", "MultiClusterEngine")
	if err != nil {
		return false, err
	}
	if !mceGone {
		log.Info("Waiting for MultiClusterEngine to be fully removed")
		return false, nil
	}

	log.Info("MCH and MCE chain cleanup complete")
	r.Recorder.Event(td, corev1.EventTypeNormal, "MCEChainComplete",
		"MultiClusterHub and MultiClusterEngine have been fully removed")
	return true, nil
}

// ensureMCEDeleted drives MCE deletion. With DF-2 (backplane-operator
// ClusterManager delete ordering), the natural chain (MCE operator ->
// ClusterManager -> OCM hub cleanup) completes without intervention.
// If stuck, the handoff path resolves the finalizer under its own identity.
func (r *HubTeardownReconciler) ensureMCEDeleted(ctx context.Context, log logr.Logger, td *operatorv1.HubTeardown) error {
	mceGVK := schema.GroupVersionKind{
		Group:   "multicluster.openshift.io",
		Version: "v1",
		Kind:    "MultiClusterEngineList",
	}
	list := &unstructured.UnstructuredList{}
	list.SetGroupVersionKind(mceGVK)

	if err := r.Client.List(ctx, list); err != nil {
		if isNoMatchError(err) {
			return nil
		}
		return fmt.Errorf("listing MultiClusterEngine: %w", err)
	}

	for i := range list.Items {
		mce := &list.Items[i]
		if mce.GetDeletionTimestamp() != nil {
			// With DF-2, backplane waits for ClusterManager finalization before
			// deleting templates — the natural MCE->CM chain completes without
			// intervention. If stuck, the handoff path resolves the finalizer
			// under its own identity.
			log.Info("MCE is deleting, waiting for natural chain to complete", "name", mce.GetName())
			return nil
		}

		log.Info("Deleting MultiClusterEngine (MCH cascade did not fire)", "name", mce.GetName())
		if err := r.Client.Delete(ctx, mce); err != nil {
			if errors.IsNotFound(err) {
				continue
			}
			return fmt.Errorf("deleting MCE %s: %w", mce.GetName(), err)
		}
		r.Recorder.Eventf(td, corev1.EventTypeNormal, "MCEDeleted",
			"Deleted MultiClusterEngine %s", mce.GetName())
	}
	return nil
}

// cleanupLocalCluster deletes local-cluster ManagedClusterAddOns and the
// local-cluster ManagedCluster. Earlier phases skip local-cluster because the
// MCH webhook requires it for MCH deletion. This function runs after MCH is
// confirmed gone, so the webhook constraint is satisfied.
func (r *HubTeardownReconciler) cleanupLocalCluster(ctx context.Context, log logr.Logger, td *operatorv1.HubTeardown) error {
	addonGVK := schema.GroupVersionKind{
		Group:   "addon.open-cluster-management.io",
		Version: "v1alpha1",
		Kind:    "ManagedClusterAddOnList",
	}
	addonList := &unstructured.UnstructuredList{}
	addonList.SetGroupVersionKind(addonGVK)

	if err := r.Client.List(ctx, addonList, client.InNamespace("local-cluster")); err != nil {
		if !isNoMatchError(err) {
			return fmt.Errorf("listing local-cluster addons: %w", err)
		}
	} else {
		for i := range addonList.Items {
			addon := &addonList.Items[i]
			if addon.GetDeletionTimestamp() != nil {
				if r.shouldResolveStuckFinalizers(td, addon) {
					r.resolveAddonFinalizers(ctx, log, td, addon)
				}
				continue
			}
			log.Info("Deleting local-cluster addon", "name", addon.GetName())
			if err := r.Client.Delete(ctx, addon); err != nil && !errors.IsNotFound(err) {
				return fmt.Errorf("deleting local-cluster addon %s: %w", addon.GetName(), err)
			}
			r.Recorder.Eventf(td, corev1.EventTypeNormal, "LocalClusterAddonDeleted",
				"Deleted local-cluster ManagedClusterAddOn %s", addon.GetName())
		}
	}

	mcGVK := schema.GroupVersionKind{
		Group:   "cluster.open-cluster-management.io",
		Version: "v1",
		Kind:    "ManagedCluster",
	}
	mc := &unstructured.Unstructured{}
	mc.SetGroupVersionKind(mcGVK)
	mcKey := types.NamespacedName{Name: "local-cluster"}
	if err := r.Client.Get(ctx, mcKey, mc); err != nil {
		if errors.IsNotFound(err) || isNoMatchError(err) {
			return nil
		}
		return fmt.Errorf("getting local-cluster ManagedCluster: %w", err)
	}

	if mc.GetDeletionTimestamp() != nil {
		if r.shouldResolveStuckFinalizers(td, mc) {
			dr := DiscoveredResource{
				Ref: operatorv1.ResourceRef{
					Group: "cluster.open-cluster-management.io",
					Kind:  "ManagedCluster",
					Name:  "local-cluster",
				},
				Version:    "v1",
				Finalizers: mc.GetFinalizers(),
			}
			if _, err := r.resolveStuckFinalizers(ctx, log, td, []DiscoveredResource{dr}); err != nil {
				log.Error(err, "Error resolving local-cluster finalizers")
			}
		}
		return nil
	}

	log.Info("Deleting local-cluster ManagedCluster")
	if err := r.Client.Delete(ctx, mc); err != nil && !errors.IsNotFound(err) {
		return fmt.Errorf("deleting local-cluster ManagedCluster: %w", err)
	}
	r.Recorder.Eventf(td, corev1.EventTypeNormal, "LocalClusterDetached",
		"Deleted local-cluster ManagedCluster (post-MCH cleanup)")
	return nil
}

// acmNamespaces are namespaces created or managed by the ACM/MCE install.
// open-cluster-management is excluded here because it is cleaned in
// phaseRemoveOLMOperator after the operator self-destructs.
var acmNamespaces = map[string]bool{
	"multicluster-engine":                   true,
	"hive":                                  true,
	"open-cluster-management-hub":           true,
	"open-cluster-management-agent":         true,
	"open-cluster-management-agent-addon":   true,
	"open-cluster-management-observability": true,
	"open-cluster-management-backup":        true,
	"hypershift":                            true,
}

// isACMOwnedRBAC checks if a cluster-scoped RBAC resource was created by the
// ACM installer using the standard installer.name label, or belongs to known
// ACM/Submariner naming patterns.
func isACMOwnedRBAC(obj *unstructured.Unstructured) bool {
	labels := obj.GetLabels()
	if labels != nil {
		if labels["installer.name"] == "multiclusterhub" {
			return true
		}
		if labels["installer.namespace"] == "open-cluster-management" {
			return true
		}
	}
	name := obj.GetName()
	return strings.HasPrefix(name, "open-cluster-management:") ||
		strings.HasPrefix(name, "multiclusterhub") ||
		strings.Contains(name, "submariner")
}

// hubteardownCRDName is the fully-qualified CRD name for HubTeardown.
// Matching by CRD name (group + plural) is structurally safe — it cannot
// accidentally match a renamed Kind or a different resource in the same group.
const hubteardownCRDName = "hubteardowns.operator.open-cluster-management.io"

// phaseDeleteACMCRDs removes all RHACM/MCE CRDs from the cluster. Kubernetes
// garbage-collects all CR instances when their CRD is deleted, ensuring zero
// traces of Policies, Apps, Channels, and other feature CRs remain.
//
// The HubTeardown CRD itself is preserved by matching its fully-qualified CRD
// name, not the Kind string — this survives Kind renames and avoids the
// bootstrap paradox where the controller deletes the CRD it's reconciling.
func (r *HubTeardownReconciler) phaseDeleteACMCRDs(ctx context.Context, log logr.Logger, td *operatorv1.HubTeardown) (bool, error) {
	log.Info("Phase: DeleteACMCRDs")

	crdList := &apixv1.CustomResourceDefinitionList{}
	if err := r.Client.List(ctx, crdList); err != nil {
		return false, fmt.Errorf("listing CRDs: %w", err)
	}

	// Pre-check: verify no stuck-terminating CRs remain before deleting CRDs.
	// Deleting a CRD while CRs with finalizers are still terminating can wedge
	// the CRD in a Terminating state because Kubernetes cannot garbage-collect
	// CRs whose finalizers are still being processed.
	for i := range crdList.Items {
		crd := &crdList.Items[i]
		if !isACMAPIGroup(crd.Spec.Group) || crd.Name == hubteardownCRDName {
			continue
		}
		version := preferredVersion(crd)
		if version == "" {
			continue
		}
		listGVK := schema.GroupVersionKind{
			Group:   crd.Spec.Group,
			Version: version,
			Kind:    crd.Spec.Names.Kind + "List",
		}
		list := &unstructured.UnstructuredList{}
		list.SetGroupVersionKind(listGVK)
		if err := r.Client.List(ctx, list); err != nil {
			continue
		}
		for j := range list.Items {
			item := &list.Items[j]
			if item.GetDeletionTimestamp() != nil && len(item.GetFinalizers()) > 0 {
				log.Info("Stuck-terminating CR blocks CRD deletion — the handoff path must resolve this finalizer",
					"crd", crd.Name, "cr", item.GetName(), "namespace", item.GetNamespace(),
					"finalizers", item.GetFinalizers())
				return false, nil
			}
		}
	}

	remaining := 0
	for i := range crdList.Items {
		crd := &crdList.Items[i]

		if !isACMAPIGroup(crd.Spec.Group) {
			continue
		}

		// Preserve the HubTeardown CRD by fully-qualified name (group + plural).
		if crd.Name == hubteardownCRDName {
			continue
		}

		if crd.GetDeletionTimestamp() != nil {
			remaining++
			continue
		}

		log.Info("Deleting CRD", "name", crd.Name, "group", crd.Spec.Group, "kind", crd.Spec.Names.Kind)
		if err := r.Client.Delete(ctx, crd); err != nil {
			if errors.IsNotFound(err) {
				continue
			}
			log.Error(err, "Failed to delete CRD", "name", crd.Name)
			remaining++
			continue
		}
		remaining++
	}

	if remaining > 0 {
		log.Info("Waiting for ACM/MCE CRDs to be removed", "remaining", remaining)
		return false, nil
	}

	log.Info("All ACM/MCE CRDs removed")
	r.Recorder.Event(td, corev1.EventTypeNormal, "CRDsDeleted",
		"All RHACM/MCE CRDs and their instances have been garbage-collected")
	return true, nil
}

// isResourceGone checks if all instances of a resource type have been deleted.
func (r *HubTeardownReconciler) isResourceGone(ctx context.Context, group, version, kind string) (bool, error) {
	listGVK := schema.GroupVersionKind{
		Group:   group,
		Version: version,
		Kind:    kind + "List",
	}
	list := &unstructured.UnstructuredList{}
	list.SetGroupVersionKind(listGVK)

	if err := r.Client.List(ctx, list); err != nil {
		if isNoMatchError(err) || errors.IsNotFound(err) {
			return true, nil
		}
		return false, fmt.Errorf("listing %s: %w", kind, err)
	}
	return len(list.Items) == 0, nil
}

// shouldResolveStuckFinalizers determines if enough time has passed for a stuck resource.
// Uses spec.forceFinalizerTimeout if set, otherwise falls back to defaultFinalizerTimeout.
func (r *HubTeardownReconciler) shouldResolveStuckFinalizers(td *operatorv1.HubTeardown, obj *unstructured.Unstructured) bool {
	ts := obj.GetDeletionTimestamp()
	if ts == nil {
		return false
	}
	timeout := defaultFinalizerTimeout
	if td.Spec.ForceFinalizerTimeout != nil {
		timeout = td.Spec.ForceFinalizerTimeout.Duration
	}
	return time.Since(ts.Time) > timeout
}

// drFromUnstructured builds a DiscoveredResource from an unstructured item.
func drFromUnstructured(item *unstructured.Unstructured, group, version, kind string) DiscoveredResource {
	return DiscoveredResource{
		Ref: operatorv1.ResourceRef{
			Group:     group,
			Kind:      kind,
			Namespace: item.GetNamespace(),
			Name:      item.GetName(),
		},
		Version:    version,
		Finalizers: item.GetFinalizers(),
	}
}

// isNoMatchError checks if an error is a "no match" error from the API discovery.
func isNoMatchError(err error) bool {
	if err == nil {
		return false
	}
	return errors.IsNotFound(err) || apimeta.IsNoMatchError(err)
}
