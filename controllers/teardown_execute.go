// Copyright Contributors to the Open Cluster Management project

package controllers

import (
	"context"
	"fmt"
	"time"

	"github.com/go-logr/logr"
	mcev1 "github.com/stolostron/backplane-operator/api/v1"
	operatorv1 "github.com/stolostron/multiclusterhub-operator/api/v1"
	"github.com/stolostron/multiclusterhub-operator/pkg/multiclusterengineutils"
	"github.com/stolostron/multiclusterhub-operator/pkg/ownership"
	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	corev1 "k8s.io/api/core/v1"
	apixv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	apiregistrationv1 "k8s.io/kube-aggregator/pkg/apis/apiregistration/v1"
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

// setTeardownActiveAnnotation annotates the MCH CR with the teardown-active
// signal. Downstream operators watch for this annotation to activate their
// own teardown-aware paths (see DF-1, DF-3, DF-4). Idempotent — skips if
// already set. Called at the start of PrepareAndDrain (phaseRemoveBlockingCRs).
func (r *HubTeardownReconciler) setTeardownActiveAnnotation(ctx context.Context, log logr.Logger, td *operatorv1.HubTeardown) error {
	target, err := r.targetMCH(ctx, td)
	if err != nil {
		if isNoMatchError(err) || errors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("reading target MCH for teardown-active annotation: %w", err)
	}

	annotations := target.GetAnnotations()
	if annotations[annotationTeardownActive] == "true" &&
		annotations[annotationTeardownActiveVersion] == teardownContractVersion {
		return nil
	}
	if annotations == nil {
		annotations = make(map[string]string)
	}
	patch := client.MergeFrom(target.DeepCopy())
	annotations[annotationTeardownActive] = "true"
	annotations[annotationTeardownActiveVersion] = teardownContractVersion
	target.SetAnnotations(annotations)
	if err := r.Client.Patch(ctx, target, patch); err != nil {
		return fmt.Errorf("setting teardown-active annotation on MCH %s/%s: %w",
			target.GetNamespace(), target.GetName(), err)
	}
	log.Info("Set teardown-active annotation on the target MCH — downstream operators will activate teardown paths",
		"namespace", target.GetNamespace(), "name", target.GetName(), "annotation", annotationTeardownActive)
	return nil
}

// readContract reports what the contract looks like on the hub. The prepare Job
// calls it after RemoveBlockingCRs so the operator can surface the result; the
// Job never writes status.
func (r *HubTeardownReconciler) readContract(ctx context.Context, log logr.Logger, td *operatorv1.HubTeardown) *contractRecord {
	record := &contractRecord{Version: teardownContractVersion, Signalled: true}

	mch, err := r.targetMCH(ctx, td)
	if err != nil {
		if isNoMatchError(err) {
			record.Reason = "NoMCH"
			record.Message = "MultiClusterHub API is not installed; nothing to signal."
			return record
		}
		if errors.IsNotFound(err) {
			record.Reason = "NoMCH"
			record.Message = fmt.Sprintf("Target MultiClusterHub %s/%s is gone; nothing to signal.",
				td.Spec.HubRef.Namespace, td.Spec.HubRef.Name)
			return record
		}
		record.Signalled = false
		record.Reason = "ContractUnreadable"
		record.Message = err.Error()
		return record
	}

	annotations := mch.GetAnnotations()
	record.MCHs = append(record.MCHs, mch.GetName())
	if annotations[annotationTeardownActive] != "true" ||
		annotations[annotationTeardownActiveVersion] != teardownContractVersion {
		record.Signalled = false
	}

	switch {
	case len(record.MCHs) == 0:
		record.Reason = "NoMCH"
		record.Message = "No MultiClusterHub present; nothing to signal."
	case record.Signalled:
		record.Reason = "Signalled"
		record.Message = fmt.Sprintf("Signalled the target MultiClusterHub at contract version %s.",
			teardownContractVersion)
	default:
		record.Reason = "SignallingIncomplete"
		record.Message = "The target MultiClusterHub is missing the teardown-active contract."
	}
	log.V(1).Info("Read teardown-active contract", "signalled", record.Signalled, "reason", record.Reason)
	return record
}

// clearTeardownActiveAnnotation removes the contract from every MCH. It is
// called when the HubTeardown CR is deleted, which is the only explicit cancel.
// Best effort by design: a stuck patch must not hold the CR finalizer on a hub
// that may already be gone.
func (r *HubTeardownReconciler) clearTeardownActiveAnnotation(ctx context.Context, log logr.Logger, td *operatorv1.HubTeardown) error {
	mch, err := r.targetMCH(ctx, td)
	if err != nil {
		if isNoMatchError(err) || errors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("reading target MCH to clear teardown-active contract: %w", err)
	}

	annotations := mch.GetAnnotations()
	if annotations == nil {
		return nil
	}
	if _, ok := annotations[annotationTeardownActive]; !ok {
		if _, ok := annotations[annotationTeardownActiveVersion]; !ok {
			return nil
		}
	}

	patch := client.MergeFrom(mch.DeepCopy())
	delete(annotations, annotationTeardownActive)
	delete(annotations, annotationTeardownActiveVersion)
	mch.SetAnnotations(annotations)
	if err := r.Client.Patch(ctx, mch, patch); err != nil && !errors.IsNotFound(err) {
		return fmt.Errorf("clearing teardown-active contract on MCH %s/%s: %w",
			mch.GetNamespace(), mch.GetName(), err)
	}
	log.Info("Cleared teardown-active contract from the target MCH",
		"namespace", mch.GetNamespace(), "name", mch.GetName())
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
	if err := r.setTeardownActiveAnnotation(ctx, log, td); err != nil {
		return false, fmt.Errorf("signalling teardown-active contract: %w", err)
	}

	authz, err := r.authorizationFor(ctx, log, td)
	if err != nil {
		return false, err
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
			decision, authzErr := r.authorize(ctx, authz, authzRequest{
				Action:         actionDeleteObject,
				Object:         partialFromUnstructured(item),
				Finalizers:     item.GetFinalizers(),
				Classification: operatorv1.ClassDanglingRegistration,
			})
			if authzErr != nil {
				return false, authzErr
			}
			if !decision.Allowed {
				log.Info("Blocking CR deletion not authorized; retaining it",
					"kind", bt.kind, "namespace", item.GetNamespace(), "name", item.GetName(),
					"reason", decision.Reason)
				r.Recorder.Eventf(td, corev1.EventTypeWarning, "BlockingCRRetained",
					"Retained %s %s/%s: %s", bt.kind, item.GetNamespace(), item.GetName(), decision.Reason)
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
		return r.sweepDanglingRegistrations(ctx, log, td, authz)
	}
	return allGone, nil
}

// sweepDanglingRegistrations is policy P-8. A webhook or APIService whose
// backing Service is gone is a dangling registration: it cannot serve, and a
// failurePolicy=Fail webhook left behind rejects the patches later phases need.
// A registration is deleted only when every backing Service is absent and the
// shared authorizer accepts its ownership evidence. Healthy and unowned
// registrations are reported and retained.
func (r *HubTeardownReconciler) sweepDanglingRegistrations(
	ctx context.Context,
	log logr.Logger,
	td *operatorv1.HubTeardown,
	authz *authzContext,
) (bool, error) {
	servicePresent := func(namespace, name string) (bool, error) {
		return r.servicePresent(ctx, namespace, name)
	}

	allSettled := true

	vwcList := &admissionregistrationv1.ValidatingWebhookConfigurationList{}
	if err := r.Client.List(ctx, vwcList); err != nil && !isNoMatchError(err) {
		return false, fmt.Errorf("listing ValidatingWebhookConfigurations: %w", err)
	} else if err == nil {
		for i := range vwcList.Items {
			config := &vwcList.Items[i]
			services, decisive := validatingRegistrationServices(config.Webhooks)
			if !decisive {
				continue
			}
			dangling, err := allServicesAbsent(services, servicePresent)
			if err != nil {
				return false, err
			}
			if !dangling {
				continue
			}
			if !r.deleteDanglingRegistration(ctx, log, td, authz, config,
				"admissionregistration.k8s.io/v1", "ValidatingWebhookConfiguration", services[0].Namespace) {
				allSettled = false
			}
		}
	}

	mwcList := &admissionregistrationv1.MutatingWebhookConfigurationList{}
	if err := r.Client.List(ctx, mwcList); err != nil && !isNoMatchError(err) {
		return false, fmt.Errorf("listing MutatingWebhookConfigurations: %w", err)
	} else if err == nil {
		for i := range mwcList.Items {
			config := &mwcList.Items[i]
			services, decisive := mutatingRegistrationServices(config.Webhooks)
			if !decisive {
				continue
			}
			dangling, err := allServicesAbsent(services, servicePresent)
			if err != nil {
				return false, err
			}
			if !dangling {
				continue
			}
			if !r.deleteDanglingRegistration(ctx, log, td, authz, config,
				"admissionregistration.k8s.io/v1", "MutatingWebhookConfiguration", services[0].Namespace) {
				allSettled = false
			}
		}
	}

	apiServiceList := &apiregistrationv1.APIServiceList{}
	if err := r.Client.List(ctx, apiServiceList); err != nil && !isNoMatchError(err) {
		return false, fmt.Errorf("listing APIServices: %w", err)
	} else if err == nil {
		for i := range apiServiceList.Items {
			apiService := &apiServiceList.Items[i]
			service := apiService.Spec.Service
			if service == nil || service.Name == "" || service.Namespace == "" {
				continue
			}
			present, err := servicePresent(service.Namespace, service.Name)
			if err != nil {
				return false, err
			}
			if present {
				continue
			}
			if !r.deleteDanglingRegistration(ctx, log, td, authz, apiService,
				"apiregistration.k8s.io/v1", "APIService", service.Namespace) {
				allSettled = false
			}
		}
	}

	return allSettled, nil
}

type registrationService struct {
	Namespace string
	Name      string
}

// validatingRegistrationServices returns every backing Service named by a
// webhook set. decisive is false when any webhook is URL-based: without a
// Service reference its backing cannot be proven absent, so the registration
// is report-only.
func validatingRegistrationServices(webhooks []admissionregistrationv1.ValidatingWebhook) ([]registrationService, bool) {
	services := make([]registrationService, 0, len(webhooks))
	for _, webhook := range webhooks {
		if webhook.ClientConfig.Service == nil {
			return nil, false
		}
		services = append(services, registrationService{
			Namespace: webhook.ClientConfig.Service.Namespace,
			Name:      webhook.ClientConfig.Service.Name,
		})
	}
	return services, len(services) > 0
}

func mutatingRegistrationServices(webhooks []admissionregistrationv1.MutatingWebhook) ([]registrationService, bool) {
	services := make([]registrationService, 0, len(webhooks))
	for _, webhook := range webhooks {
		if webhook.ClientConfig.Service == nil {
			return nil, false
		}
		services = append(services, registrationService{
			Namespace: webhook.ClientConfig.Service.Namespace,
			Name:      webhook.ClientConfig.Service.Name,
		})
	}
	return services, len(services) > 0
}

func allServicesAbsent(
	services []registrationService,
	present func(namespace, name string) (bool, error),
) (bool, error) {
	for _, service := range services {
		found, err := present(service.Namespace, service.Name)
		if err != nil {
			return false, err
		}
		if found {
			return false, nil
		}
	}
	return len(services) > 0, nil
}

// servicePresent reports whether the named Service object exists. An empty
// reference cannot be absent, so it is reported false. Any read error other
// than NotFound is returned so the caller can refuse to claim absence.
func (r *HubTeardownReconciler) servicePresent(ctx context.Context, namespace, name string) (bool, error) {
	if namespace == "" || name == "" {
		return false, nil
	}
	svc := &corev1.Service{}
	err := r.Client.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, svc)
	if err == nil {
		return true, nil
	}
	if errors.IsNotFound(err) {
		return false, nil
	}
	return false, err
}

func (r *HubTeardownReconciler) deleteDanglingRegistration(
	ctx context.Context,
	log logr.Logger,
	td *operatorv1.HubTeardown,
	authz *authzContext,
	object client.Object,
	apiVersion string,
	kind string,
	serviceNamespace string,
) bool {
	meta := partialMetadataFor(object, apiVersion, kind)
	decision, authzErr := r.authorize(ctx, authz, authzRequest{
		Action:           actionDeleteObject,
		Object:           meta,
		Classification:   operatorv1.ClassDanglingRegistration,
		ServiceNamespace: serviceNamespace,
	})
	if authzErr != nil {
		log.Error(authzErr, "Authorization failed for dangling registration", "kind", kind, "name", object.GetName())
		return false
	}
	if !decision.Allowed {
		log.Info("Dangling registration retained: ownership is not evidence-backed",
			"kind", kind, "name", object.GetName(), "reason", decision.Reason)
		r.Recorder.Eventf(td, corev1.EventTypeWarning, "DanglingRegistrationRetained",
			"Retained %s %s: %s", kind, object.GetName(), decision.Reason)
		// Report-only is a terminal disposition for this pass. Returning false
		// here would make the phase wait forever on an object policy forbids
		// it to touch.
		return true
	}
	log.Info("Deleting dangling registration", "kind", kind, "name", object.GetName())
	if err := r.Client.Delete(ctx, object); err != nil && !errors.IsNotFound(err) {
		log.Error(err, "Failed to delete dangling registration", "kind", kind, "name", object.GetName())
		return false
	}
	r.Recorder.Eventf(td, corev1.EventTypeNormal, "DanglingRegistrationDeleted",
		"Deleted %s %s: backing Service is absent and ownership is evidence-backed", kind, object.GetName())
	return true
}

// phaseDisableAddons deletes ManagedClusterAddOns for non-local ManagedClusters.
// It first disables addon-manager placements to prevent recreation, then deletes
// MCAs and resolves stuck addon finalizers on terminating MCAs.
func (r *HubTeardownReconciler) phaseDisableAddons(ctx context.Context, log logr.Logger, td *operatorv1.HubTeardown) (bool, error) {
	log.Info("Phase: DisableAddons")

	authz, err := r.authorizationFor(ctx, log, td)
	if err != nil {
		return false, err
	}

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
		// Removal still requires ownership and a dead controller; the stall
		// timeout only says when to try.
		if unreachableMCs[ns] {
			if addon.GetDeletionTimestamp() == nil {
				log.Info("Deleting addon on unreachable cluster", "namespace", ns, "name", addon.GetName())
				decision, authzErr := r.authorize(ctx, authz, authzRequest{
					Action:     actionDeleteObject,
					Object:     partialFromUnstructured(addon),
					Finalizers: addon.GetFinalizers(),
				})
				if authzErr != nil {
					return false, authzErr
				}
				if !decision.Allowed {
					log.Info("Addon deletion not authorized", "namespace", ns, "name", addon.GetName(), "reason", decision.Reason)
					remaining++
					continue
				}
				if err := r.Client.Delete(ctx, addon); err != nil && !errors.IsNotFound(err) {
					return false, fmt.Errorf("deleting addon %s/%s: %w", ns, addon.GetName(), err)
				}
			}
			if addon.GetDeletionTimestamp() != nil && len(addon.GetFinalizers()) > 0 {
				if r.shouldResolveStuckFinalizers(td, addon) {
					r.resolveAddonFinalizers(ctx, log, td, addon)
				} else {
					remaining++
				}
			}
			continue
		}

		if addon.GetDeletionTimestamp() != nil {
			// Resolve stuck addon finalizers only after the stall timeout; the
			// shared authorizer still requires ownership, a dead controller and
			// digest-bound consent for cloud risk.
			if r.shouldResolveStuckFinalizers(td, addon) {
				r.resolveAddonFinalizers(ctx, log, td, addon)
			} else {
				remaining++
			}
			continue
		}
		decision, authzErr := r.authorize(ctx, authz, authzRequest{
			Action:     actionDeleteObject,
			Object:     partialFromUnstructured(addon),
			Finalizers: addon.GetFinalizers(),
		})
		if authzErr != nil {
			return false, authzErr
		}
		if !decision.Allowed {
			log.Info("Addon deletion not authorized", "namespace", ns, "name", addon.GetName(), "reason", decision.Reason)
			remaining++
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
		msg += " Finalizers are removed only when ownership is evidence-backed, the owning controller is not alive, and any cloud risk is covered by the digest-bound acknowledgement."
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

// phaseDeleteInfrastructureCRs deletes resources carrying Tier-1 cloud-protecting
// finalizers while their controllers are still alive to handle graceful
// deprovisioning. The candidate set comes from the inventory graph; no product
// kind or name makes an object eligible.
//
// The acknowledgeCloudResourceRisk gate is only applied when infrastructure CRs
// actually exist. Hubs with no cloud-provisioned clusters skip this gate entirely.
func (r *HubTeardownReconciler) phaseDeleteInfrastructureCRs(ctx context.Context, log logr.Logger, td *operatorv1.HubTeardown) (bool, error) {
	log.Info("Phase: DeleteInfrastructureCRs")

	authz, err := r.authorizationFor(ctx, log, td)
	if err != nil {
		return false, err
	}

	candidates := authz.graph.InfrastructureResources

	// No infrastructure CRs on this hub — skip the cloud risk gate entirely.
	if len(candidates) == 0 {
		log.Info("No Tier-1 infrastructure resources found, skipping phase")
		return true, nil
	}

	remaining := 0
	for i := range candidates {
		dr := &candidates[i]
		if dr.UID == "" {
			log.Info("Infrastructure CR has no observed UID; retaining it",
				"kind", dr.Ref.Kind, "namespace", dr.Ref.Namespace, "name", dr.Ref.Name)
			r.Recorder.Eventf(td, corev1.EventTypeWarning, "InfrastructureCRRetained",
				"Retained %s %s/%s: inventory did not record an object UID", dr.Ref.Kind, dr.Ref.Namespace, dr.Ref.Name)
			remaining++
			continue
		}
		item := unstructuredForDiscoveredResource(*dr)

		if dr.DeletionTimestamp != nil {
			if r.shouldResolveStuckFinalizersTime(td, dr.DeletionTimestamp) {
				resolved, err := r.resolveStuckFinalizers(ctx, log, td, []DiscoveredResource{*dr})
				if err != nil {
					log.Error(err, "Error resolving finalizers on infrastructure CR",
						"kind", dr.Ref.Kind, "name", dr.Ref.Name, "namespace", dr.Ref.Namespace)
				}
				if !resolved {
					remaining++
				}
			} else {
				remaining++
			}
			continue
		}

		decision, authzErr := r.authorize(ctx, authz, authzRequest{
			Action:     actionDeleteObject,
			Object:     partialMetadata(dr),
			Finalizers: dr.Finalizers,
			Platform:   dr.Platform,
		})
		if authzErr != nil {
			return false, authzErr
		}
		if !decision.Allowed {
			log.Info("Infrastructure CR deletion not authorized; retaining it",
				"kind", dr.Ref.Kind, "namespace", dr.Ref.Namespace, "name", dr.Ref.Name,
				"reason", decision.Reason)
			r.Recorder.Eventf(td, corev1.EventTypeWarning, "InfrastructureCRRetained",
				"Retained %s %s/%s: %s", dr.Ref.Kind, dr.Ref.Namespace, dr.Ref.Name, decision.Reason)
			remaining++
			continue
		}

		log.Info("Deleting infrastructure CR",
			"kind", dr.Ref.Kind, "name", dr.Ref.Name, "namespace", dr.Ref.Namespace)
		var deleteOptions []client.DeleteOption
		if dr.UID != "" {
			uid := dr.UID
			deleteOptions = append(deleteOptions, client.Preconditions{UID: &uid})
		}
		if err := r.Client.Delete(ctx, item, deleteOptions...); err != nil && !errors.IsNotFound(err) {
			return false, fmt.Errorf("deleting %s %s/%s: %w", dr.Ref.Kind, dr.Ref.Namespace, dr.Ref.Name, err)
		}
		r.Recorder.Eventf(td, corev1.EventTypeNormal, "InfrastructureCRDeleted",
			"Deleted %s %s/%s", dr.Ref.Kind, dr.Ref.Namespace, dr.Ref.Name)
		remaining++
	}

	if remaining > 0 {
		msg := fmt.Sprintf("Waiting for %d infrastructure CRs to terminate.", remaining)
		msg += " Finalizers are removed only when ownership is evidence-backed, the owning controller is not alive, and any cloud risk is covered by the digest-bound acknowledgement."
		r.setPhaseStatus(td, operatorv1.TeardownPhaseDeleteInfrastructureCRs, operatorv1.PhaseStateInProgress, msg)
		log.Info(msg, "remaining", remaining)
		return false, nil
	}

	log.Info("All infrastructure CRs deleted")
	return true, nil
}

func unstructuredForDiscoveredResource(dr DiscoveredResource) *unstructured.Unstructured {
	item := &unstructured.Unstructured{}
	item.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   dr.Ref.Group,
		Version: dr.Version,
		Kind:    dr.Ref.Kind,
	})
	item.SetNamespace(dr.Ref.Namespace)
	item.SetName(dr.Ref.Name)
	item.SetUID(dr.UID)
	return item
}

// phaseDetachManagedClusters deletes non-local ManagedClusters so the MCH webhook passes.
// With DF-4 (import-controller ManifestWork finalizer sweep), ManifestWork finalizers
// are force-cleaned during teardown so ManagedClusters terminate faster. Finalizers
// are only removed through the policy gate; unknown finalizers are reported and retained.
func (r *HubTeardownReconciler) phaseDetachManagedClusters(ctx context.Context, log logr.Logger, td *operatorv1.HubTeardown) (bool, error) {
	log.Info("Phase: DetachManagedClusters")

	authz, err := r.authorizationFor(ctx, log, td)
	if err != nil {
		return false, err
	}

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
			log.Info("ManagedCluster local-cluster is retained until MCH deletion; this is the explicit detach disposition",
				"disposition", "detach after MultiClusterHub is gone")
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
				if !resolved {
					log.Info("Finalizers remain on ManagedCluster; ownership/liveness policy does not permit removal",
						"name", mc.GetName(), "finalizers", mc.GetFinalizers())
					r.Recorder.Eventf(td, corev1.EventTypeWarning, "FinalizersRetained",
						"ManagedCluster %s still has %d finalizer(s); manual action required",
						mc.GetName(), len(mc.GetFinalizers()))
				}
			}
			continue
		}

		decision, authzErr := r.authorize(ctx, authz, authzRequest{
			Action:     actionDeleteObject,
			Object:     partialFromUnstructured(mc),
			Finalizers: mc.GetFinalizers(),
		})
		if authzErr != nil {
			return false, authzErr
		}
		if !decision.Allowed {
			log.Info("ManagedCluster deletion not authorized; retaining it",
				"name", mc.GetName(), "reason", decision.Reason)
			r.Recorder.Eventf(td, corev1.EventTypeWarning, "ManagedClusterRetained",
				"Retained ManagedCluster %s: %s", mc.GetName(), decision.Reason)
			remaining++
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
		msg += " Finalizers are removed only when ownership is evidence-backed, the owning controller is not alive, and any cloud risk is covered by the digest-bound acknowledgement. local-cluster is retained until MCH deletion."
		r.setPhaseStatus(td, operatorv1.TeardownPhaseDetachManagedClusters, operatorv1.PhaseStateInProgress, msg)
		log.Info(msg, "remaining", remaining)
		return false, nil
	}

	r.sweepMCSAndCMA(ctx, log, td, authz)
	return true, nil
}

// phaseDeleteMCH deletes exactly the MultiClusterHub named by spec.hubRef.
// Deleting it triggers the MCH reconciler's finalizeHub in the same operator
// process; the handoff Job then monitors and mops up what finalizeHub leaves
// behind. The operator itself never patches an MCH finalizer.
func (r *HubTeardownReconciler) phaseDeleteMCH(ctx context.Context, log logr.Logger, td *operatorv1.HubTeardown) (bool, error) {
	log.Info("Phase: DeleteMCH")

	mch, err := r.targetMCH(ctx, td)
	if err != nil {
		if errors.IsNotFound(err) || isNoMatchError(err) {
			log.Info("Target MultiClusterHub is gone, phase complete",
				"namespace", td.Spec.HubRef.Namespace, "name", td.Spec.HubRef.Name)
			return true, nil
		}
		return false, fmt.Errorf("reading target MultiClusterHub %s/%s: %w",
			td.Spec.HubRef.Namespace, td.Spec.HubRef.Name, err)
	}

	if mch.GetDeletionTimestamp() != nil {
		// The operator waits. Finalizer removal, when policy permits it, runs
		// in the handoff Job (PH-3); a stuck MCH is visible through status and
		// bounded by the Job deadline.
		log.Info("Target MultiClusterHub is already deleting, waiting for its finalizer to complete",
			"namespace", mch.GetNamespace(), "name", mch.GetName())
		return false, nil
	}

	authz, err := r.authorizationFor(ctx, log, td)
	if err != nil {
		return false, err
	}
	decision, authzErr := r.authorize(ctx, authz, authzRequest{
		Action:     actionDeleteObject,
		Object:     partialFromUnstructured(mch),
		Finalizers: mch.GetFinalizers(),
	})
	if authzErr != nil {
		return false, authzErr
	}
	if !decision.Allowed {
		log.Info("MultiClusterHub deletion not authorized; retaining it",
			"namespace", mch.GetNamespace(), "name", mch.GetName(), "reason", decision.Reason)
		r.Recorder.Eventf(td, corev1.EventTypeWarning, "MCHRetained",
			"Retained MultiClusterHub %s/%s: %s", mch.GetNamespace(), mch.GetName(), decision.Reason)
		return false, nil
	}

	log.Info("Deleting target MultiClusterHub", "namespace", mch.GetNamespace(), "name", mch.GetName())
	if err := r.Client.Delete(ctx, mch); err != nil {
		if errors.IsNotFound(err) {
			return true, nil
		}
		return false, fmt.Errorf("deleting MCH %s/%s: %w", mch.GetNamespace(), mch.GetName(), err)
	}
	r.Recorder.Eventf(td, corev1.EventTypeNormal, "MCHDeleted",
		"Deleted MultiClusterHub %s/%s", mch.GetNamespace(), mch.GetName())

	// Re-observe on the next pass; the API server may report NotFound then.
	return false, nil
}

// resolveMCHBlockingFinalizers resolves stuck finalizers on resources that
// block the MCH's own finalizer (finalizeHub). Submariner's cleanup finalizers
// on ClusterManagementAddOns and ManagedClusterSets are the known blockers.
// It runs only inside the handoff Job; the always-on operator never patches a
// finalizer (spec section 6.1, PH-3).
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
	authz *authzContext,
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
				decision, authzErr := r.authorize(ctx, authz, authzRequest{
					Action:     actionDeleteObject,
					Object:     partialFromUnstructured(item),
					Finalizers: item.GetFinalizers(),
				})
				if authzErr != nil {
					log.Error(authzErr, "Authorization failed in MCS/CMA sweep", "kind", st.kind, "name", item.GetName())
					continue
				}
				if !decision.Allowed {
					log.Info("MCS/CMA sweep retained an object whose ownership is not evidence-backed",
						"kind", st.kind, "name", item.GetName(), "reason", decision.Reason)
					r.Recorder.Eventf(td, corev1.EventTypeWarning, "MCSCMASweepRetained",
						"Retained %s %s: %s", st.kind, item.GetName(), decision.Reason)
					continue
				}
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

	// finalizeHub blocks MCH removal on the finalizers of its own operands.
	// Resolve those before waiting for the MCH to disappear: doing it after the
	// wait deadlocks whenever a ClusterManagementAddOn or ManagedClusterSet
	// finalizer is exactly what keeps the MCH around. This still only touches
	// objects that are already deleting, and the shared authorizer still
	// requires evidence-backed ownership, a dead controller, and cloud consent.
	if err := r.resolveMCHBlockingFinalizers(ctx, log, td); err != nil {
		log.Error(err, "Error resolving MCH-blocking finalizers in MonitorMCEChain")
	}

	mchGone, err := r.targetMCHGone(ctx, td)
	if err != nil {
		return false, err
	}
	if !mchGone {
		log.Info("Waiting for the target MultiClusterHub to be fully removed",
			"namespace", td.Spec.HubRef.Namespace, "name", td.Spec.HubRef.Name)
		return false, nil
	}

	// Clean up local-cluster addons and ManagedCluster. Earlier phases skip
	// local-cluster because the MCH webhook requires it for MCH deletion. Now
	// that MCH is gone, local-cluster can be safely removed.
	if err := r.cleanupLocalCluster(ctx, log, td); err != nil {
		log.Error(err, "Error cleaning up local-cluster")
	}

	// Ensure MCE is settled. Default removes only an MCE this MCH created;
	// Preserve never touches it. A retained pre-existing MCE is a disposition,
	// not a reason to wait forever.
	mceSettled, err := r.ensureMCEDeleted(ctx, log, td)
	if err != nil {
		return false, err
	}
	if !mceSettled {
		log.Info("Waiting for the MultiClusterEngine disposition to settle")
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
func (r *HubTeardownReconciler) ensureMCEDeleted(ctx context.Context, log logr.Logger, td *operatorv1.HubTeardown) (bool, error) {
	if td.Spec.MCE == operatorv1.MCEDispositionPreserve {
		log.Info("spec.mce=Preserve: MultiClusterEngine is never touched")
		return true, nil
	}

	list := &mcev1.MultiClusterEngineList{}
	if err := r.Client.List(ctx, list); err != nil {
		if isNoMatchError(err) {
			return true, nil
		}
		return false, fmt.Errorf("listing MultiClusterEngine: %w", err)
	}
	if len(list.Items) == 0 {
		return true, nil
	}

	authz, err := r.authorizationFor(ctx, log, td)
	if err != nil {
		return false, err
	}

	settled := true
	for i := range list.Items {
		mce := &list.Items[i]
		if !multiclusterengineutils.MCECreatedByMCH(mce, td.Spec.HubRef.Name, td.Spec.HubRef.Namespace) {
			log.Info("Retaining pre-existing MultiClusterEngine: it was not created by this MultiClusterHub",
				"name", mce.GetName())
			r.Recorder.Eventf(td, corev1.EventTypeNormal, "MCEPreserved",
				"Retained MultiClusterEngine %s: installer labels do not match %s/%s",
				mce.GetName(), td.Spec.HubRef.Namespace, td.Spec.HubRef.Name)
			continue
		}

		if mce.GetDeletionTimestamp() != nil {
			// With DF-2, backplane waits for ClusterManager finalization before
			// deleting templates. If the wait outlives the stall timeout, the
			// handoff Job resolves only finalizers policy allows.
			if r.shouldResolveStuckFinalizersTime(td, mce.GetDeletionTimestamp()) {
				dr := DiscoveredResource{
					Ref: operatorv1.ResourceRef{
						Group: "multicluster.openshift.io",
						Kind:  "MultiClusterEngine",
						Name:  mce.GetName(),
					},
					Version:    "v1",
					Finalizers: mce.GetFinalizers(),
				}
				if _, err := r.resolveStuckFinalizers(ctx, log, td, []DiscoveredResource{dr}); err != nil {
					log.Error(err, "Error resolving MultiClusterEngine finalizers", "name", mce.GetName())
				}
			}
			log.Info("MultiClusterEngine is deleting, waiting for the natural chain to complete", "name", mce.GetName())
			settled = false
			continue
		}

		meta := metav1.PartialObjectMetadata{ObjectMeta: *mce.ObjectMeta.DeepCopy()}
		meta.APIVersion = "multicluster.openshift.io/v1"
		meta.Kind = "MultiClusterEngine"
		decision, authzErr := r.authorize(ctx, authz, authzRequest{
			Action:     actionDeleteObject,
			Object:     meta,
			Finalizers: mce.GetFinalizers(),
		})
		if authzErr != nil {
			return false, authzErr
		}
		if !decision.Allowed {
			log.Info("MultiClusterEngine deletion not authorized; retaining it",
				"name", mce.GetName(), "reason", decision.Reason)
			r.Recorder.Eventf(td, corev1.EventTypeWarning, "MCERetained",
				"Retained MultiClusterEngine %s: %s", mce.GetName(), decision.Reason)
			settled = false
			continue
		}

		log.Info("Deleting MultiClusterEngine created by this MultiClusterHub", "name", mce.GetName())
		if err := r.Client.Delete(ctx, mce); err != nil && !errors.IsNotFound(err) {
			return false, fmt.Errorf("deleting MCE %s: %w", mce.GetName(), err)
		}
		r.Recorder.Eventf(td, corev1.EventTypeNormal, "MCEDeleted",
			"Deleted MultiClusterEngine %s", mce.GetName())
		settled = false
	}
	return settled, nil
}

// cleanupLocalCluster deletes local-cluster ManagedClusterAddOns and the
// local-cluster ManagedCluster. Earlier phases skip local-cluster because the
// MCH webhook requires it for MCH deletion. This function runs after MCH is
// confirmed gone, so the webhook constraint is satisfied.
func (r *HubTeardownReconciler) cleanupLocalCluster(ctx context.Context, log logr.Logger, td *operatorv1.HubTeardown) error {
	authz, err := r.authorizationFor(ctx, log, td)
	if err != nil {
		return err
	}

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
			decision, authzErr := r.authorize(ctx, authz, authzRequest{
				Action:     actionDeleteObject,
				Object:     partialFromUnstructured(addon),
				Finalizers: addon.GetFinalizers(),
			})
			if authzErr != nil {
				return authzErr
			}
			if !decision.Allowed {
				log.Info("local-cluster addon deletion not authorized; retaining it",
					"name", addon.GetName(), "reason", decision.Reason)
				r.Recorder.Eventf(td, corev1.EventTypeWarning, "LocalClusterAddonRetained",
					"Retained local-cluster ManagedClusterAddOn %s: %s", addon.GetName(), decision.Reason)
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

	decision, authzErr := r.authorize(ctx, authz, authzRequest{
		Action:     actionDeleteObject,
		Object:     partialFromUnstructured(mc),
		Finalizers: mc.GetFinalizers(),
	})
	if authzErr != nil {
		return authzErr
	}
	if !decision.Allowed {
		log.Info("local-cluster ManagedCluster deletion not authorized; retaining it", "reason", decision.Reason)
		r.Recorder.Eventf(td, corev1.EventTypeWarning, "LocalClusterRetained",
			"Retained local-cluster ManagedCluster: %s", decision.Reason)
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

	authz, err := r.authorizationFor(ctx, log, td)
	if err != nil {
		return false, err
	}
	if authz.graph == nil {
		return false, fmt.Errorf("inventory snapshot is missing; refusing to delete CRDs without ownership evidence")
	}
	// Consume the CRDs seen by the same scan that produced the ownership
	// verdicts. A fresh list here would authorize one object and delete another.
	crdList := &apixv1.CustomResourceDefinitionList{Items: append([]apixv1.CustomResourceDefinition(nil), authz.graph.CRDs...)}

	// Pre-check: verify no stuck-terminating CRs remain before deleting CRDs.
	// Deleting a CRD while CRs with finalizers are still terminating can wedge
	// the CRD in a Terminating state because Kubernetes cannot garbage-collect
	// CRs whose finalizers are still being processed.
	for i := range crdList.Items {
		crd := &crdList.Items[i]
		if !isACMAPIGroup(crd.Spec.Group) || !ownsACMGroups(crd.Spec.Group) || crd.Name == hubteardownCRDName {
			continue
		}
		meta := metav1.PartialObjectMetadata{ObjectMeta: *crd.ObjectMeta.DeepCopy()}
		decision, authzErr := r.authorize(ctx, authz, authzRequest{
			Action:        actionDeleteCRD,
			Object:        meta,
			ACMOwnedGroup: ownsACMGroups(crd.Spec.Group),
		})
		if authzErr != nil {
			return false, authzErr
		}
		if !decision.Allowed {
			log.Info("Skipping CRD: ownership not established",
				"crd", crd.Name, "group", crd.Spec.Group, "confidence", decision.Verdict.Confidence)
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
			if item.GetDeletionTimestamp() == nil || len(item.GetFinalizers()) == 0 {
				continue
			}
			// Deleting the CRD now would wedge it in Terminating behind a CR
			// nobody is left to finalize. Resolve the finalizer here through
			// the shared authorizer, and only report the CR as blocking while
			// it still carries one.
			if r.shouldResolveStuckFinalizers(td, item) {
				dr := drFromUnstructured(item, crd.Spec.Group, version, crd.Spec.Names.Kind)
				if _, err := r.resolveStuckFinalizers(ctx, log, td, []DiscoveredResource{dr}); err != nil {
					log.Error(err, "Error resolving finalizers on stuck-terminating CR",
						"crd", crd.Name, "cr", item.GetName(), "namespace", item.GetNamespace())
				}
			}
			log.Info("Stuck-terminating CR blocks CRD deletion",
				"crd", crd.Name, "cr", item.GetName(), "namespace", item.GetNamespace(),
				"finalizers", item.GetFinalizers())
			return false, nil
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

		// The scan is deliberately broader than the deletion set: metal3.io is
		// observed and reported because ACM can integrate with bare metal, but
		// it is platform API, never ACM-owned. A stray installer label must not
		// turn it into a deletion candidate.
		if !ownsACMGroups(crd.Spec.Group) {
			log.Info("Retaining CRD: API group is not ACM-owned",
				"crd", crd.Name, "group", crd.Spec.Group)
			continue
		}

		meta := metav1.PartialObjectMetadata{ObjectMeta: *crd.ObjectMeta.DeepCopy()}
		decision, authzErr := r.authorize(ctx, authz, authzRequest{
			Action:        actionDeleteCRD,
			Object:        meta,
			ACMOwnedGroup: ownsACMGroups(crd.Spec.Group),
		})
		if authzErr != nil {
			return false, authzErr
		}
		if !decision.Allowed {
			log.Info("Retaining CRD: ownership not established",
				"crd", crd.Name, "group", crd.Spec.Group, "confidence", decision.Verdict.Confidence)
			r.Recorder.Eventf(td, corev1.EventTypeWarning, "CRDRetained",
				"CRD %s retained: ownership confidence %s", crd.Name, decision.Verdict.Confidence)
			continue
		}

		if crd.GetDeletionTimestamp() != nil {
			remaining++
			continue
		}

		if err := r.disableConversionWebhookBeforeDelete(ctx, log, crd); err != nil {
			log.Error(err, "Failed to disable conversion webhook before CRD deletion", "name", crd.Name)
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

// crdOwnershipVerdict classifies a CRD from the evidence it carries. The group
// boundary lives here because pkg/ownership does not own the product list.
func crdOwnershipVerdict(crd *apixv1.CustomResourceDefinition, observedAt metav1.Time) ownership.OwnershipVerdict {
	meta := metav1.PartialObjectMetadata{ObjectMeta: *crd.ObjectMeta.DeepCopy()}
	return ownership.ClassifyCRD(meta, ownsACMGroups(crd.Spec.Group), observedAt)
}

// disableConversionWebhookBeforeDelete clears a conversion webhook before the
// CRD is deleted. A CRD whose conversion webhook points at a Service that the
// teardown already removed can otherwise stay Terminating.
func (r *HubTeardownReconciler) disableConversionWebhookBeforeDelete(ctx context.Context, log logr.Logger, crd *apixv1.CustomResourceDefinition) error {
	if crd.Spec.Conversion == nil || crd.Spec.Conversion.Strategy != apixv1.WebhookConverter {
		return nil
	}
	patch := client.MergeFrom(crd.DeepCopy())
	crd.Spec.Conversion.Strategy = apixv1.NoneConverter
	crd.Spec.Conversion.Webhook = nil
	if err := r.Client.Patch(ctx, crd, patch); err != nil {
		return fmt.Errorf("disabling conversion webhook on CRD %s: %w", crd.Name, err)
	}
	log.Info("Disabled conversion webhook before CRD deletion", "crd", crd.Name)
	return nil
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

// targetMCH resolves the exact MultiClusterHub named by spec.hubRef. Teardown
// must never infer its target from whichever MCH happens to exist.
func (r *HubTeardownReconciler) targetMCH(ctx context.Context, td *operatorv1.HubTeardown) (*unstructured.Unstructured, error) {
	mch := &unstructured.Unstructured{}
	mch.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "operator.open-cluster-management.io",
		Version: "v1",
		Kind:    "MultiClusterHub",
	})
	if err := r.Client.Get(ctx, types.NamespacedName{
		Namespace: td.Spec.HubRef.Namespace,
		Name:      td.Spec.HubRef.Name,
	}, mch); err != nil {
		return nil, err
	}
	return mch, nil
}

func (r *HubTeardownReconciler) targetMCHGone(ctx context.Context, td *operatorv1.HubTeardown) (bool, error) {
	_, err := r.targetMCH(ctx, td)
	if err == nil {
		return false, nil
	}
	if errors.IsNotFound(err) || isNoMatchError(err) {
		return true, nil
	}
	return false, err
}

func partialMetadataFor(object client.Object, apiVersion, kind string) metav1.PartialObjectMetadata {
	return metav1.PartialObjectMetadata{
		TypeMeta: metav1.TypeMeta{APIVersion: apiVersion, Kind: kind},
		ObjectMeta: metav1.ObjectMeta{
			Name:              object.GetName(),
			Namespace:         object.GetNamespace(),
			UID:               object.GetUID(),
			ResourceVersion:   object.GetResourceVersion(),
			Labels:            object.GetLabels(),
			Annotations:       object.GetAnnotations(),
			OwnerReferences:   object.GetOwnerReferences(),
			Finalizers:        object.GetFinalizers(),
			DeletionTimestamp: object.GetDeletionTimestamp(),
		},
	}
}

// effectiveMaxDuration is the terminal bound. The new escalation block wins;
// the deprecated field remains for CRs written before it existed.
func effectiveMaxDuration(td *operatorv1.HubTeardown) time.Duration {
	if td != nil && td.Spec.Escalation != nil && td.Spec.Escalation.MaxDuration != nil {
		return td.Spec.Escalation.MaxDuration.Duration
	}
	if td != nil && td.Spec.MaxDuration != nil {
		return td.Spec.MaxDuration.Duration
	}
	return defaultMaxDuration
}

// effectiveStallTimeout is the no-progress bound. Precedence is the new
// escalation block, then the deprecated forceFinalizerTimeout, then 15m.
func effectiveStallTimeout(td *operatorv1.HubTeardown) time.Duration {
	if td != nil && td.Spec.Escalation != nil && td.Spec.Escalation.StallTimeout != nil {
		return td.Spec.Escalation.StallTimeout.Duration
	}
	if td != nil && td.Spec.ForceFinalizerTimeout != nil {
		return td.Spec.ForceFinalizerTimeout.Duration
	}
	return defaultStallTimeout
}

// shouldResolveStuckFinalizers determines if enough time has passed for a stuck resource.
func (r *HubTeardownReconciler) shouldResolveStuckFinalizers(td *operatorv1.HubTeardown, obj *unstructured.Unstructured) bool {
	ts := obj.GetDeletionTimestamp()
	if ts == nil {
		return false
	}
	return r.shouldResolveStuckFinalizersTime(td, ts)
}

func (r *HubTeardownReconciler) shouldResolveStuckFinalizersTime(td *operatorv1.HubTeardown, ts *metav1.Time) bool {
	if ts == nil {
		return false
	}
	return time.Since(ts.Time) > effectiveStallTimeout(td)
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
		Platform:   detectPlatform(kind, item),
	}
}

// isNoMatchError checks if an error is a "no match" error from the API discovery.
func isNoMatchError(err error) bool {
	if err == nil {
		return false
	}
	return errors.IsNotFound(err) || apimeta.IsNoMatchError(err)
}
