// Copyright Contributors to the Open Cluster Management project

package controllers

import (
	"context"
	"fmt"

	"github.com/go-logr/logr"
	operatorv1 "github.com/stolostron/multiclusterhub-operator/api/v1"
	"github.com/stolostron/multiclusterhub-operator/pkg/verify"
	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apixv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// verifyACMNamespaces are namespaces a complete teardown must remove. The
// operator namespace is deliberately absent: it is passed as the policy's
// OperatorNamespace and is an expected survivor under D-G.
//
// This is a declared list, not a prefix match. Prefix-named namespaces are
// still covered when the scan sees their objects; a name rule would repeat the
// DF-5 defect in the report path.
var verifyACMNamespaces = []string{
	"clusters",
	"multicluster-engine",
	"hive",
	"open-cluster-management-hub",
	"open-cluster-management-agent",
	"open-cluster-management-agent-addon",
	"open-cluster-management-observability",
	"open-cluster-management-backup",
	"hypershift",
}

// verifyOperatorAPIGroup is served by the preserved MCH operator (D-G).
const verifyOperatorAPIGroup = "operator.open-cluster-management.io"

// verifyKeepCRDs are the definitions the preserved operator needs. Everything
// else in an ACM group is residue.
var verifyKeepCRDs = []string{
	"multiclusterhubs.operator.open-cluster-management.io",
	"hubteardowns.operator.open-cluster-management.io",
	"internalhubcomponents.operator.open-cluster-management.io",
}

// verifyOperatorNamespaceKeepObjects are the core objects the platform creates
// in every namespace. They are exact names, not patterns: a name rule here
// would repeat the DF-5 defect in the report path.
func verifyOperatorNamespaceKeepObjects(namespace string) []verify.ObjectRef {
	return []verify.ObjectRef{
		{Kind: "ConfigMap", Namespace: namespace, Name: "kube-root-ca.crt"},
		{Kind: "ConfigMap", Namespace: namespace, Name: "openshift-service-ca.crt"},
	}
}

// verifyPolicy is the classification policy shared by Verify and the timeout
// path, so a failed run reports residue the same way a completed one does.
func verifyPolicy(td *operatorv1.HubTeardown) verify.Policy {
	return verify.Policy{
		OperatorNamespace: td.Namespace,
		// Verification classifies ownership, not scan reach. metal3.io is
		// scanned because ACM integrates with it, but its CRDs belong to the
		// platform and must not be reported as ACM residue.
		ACMGroupSuffixes: acmOwnedGroupSuffixes,
		ACMNamespaces:    verifyACMNamespaces,
		KeepCRDs:         verifyKeepCRDs,
		KeepObjects:      verifyOperatorNamespaceKeepObjects(td.Namespace),
		OperatorAPIGroup: verifyOperatorAPIGroup,
	}
}

// phaseVerify is the postcondition phase. It observes, classifies and writes
// the terminal state; it deletes nothing.
func (r *HubTeardownReconciler) phaseVerify(ctx context.Context, log logr.Logger, td *operatorv1.HubTeardown) (bool, error) {
	log.Info("Phase: Verify")

	input, err := r.observeForVerification(ctx, log, td)
	if err != nil {
		return false, err
	}
	if len(input.Incomplete) > 0 {
		log.Info("Verification inventory is incomplete", "details", input.Incomplete)
	}

	report := verify.Evaluate(input, verifyPolicy(td))

	now := metav1.Now()
	td.Status.Residue = &operatorv1.ResidueReport{
		ObservedAt:                now,
		Complete:                  report.Complete,
		CompleteWithResidueReason: report.RefusedReason,
		ExpectedSurvivors:         report.Expected,
		Residue:                   report.Residue,
	}

	if report.Complete {
		td.Status.Phase = operatorv1.TeardownPhaseComplete
		td.Status.ObservedGeneration = td.Generation
		message := fmt.Sprintf("Teardown verified: no residue outside %d expected survivors.", len(report.Expected))
		switch n := len(report.Unclassified); {
		case n > 0 && !input.SnapshotPresent:
			message = fmt.Sprintf("Teardown verified: no residue outside %d expected survivors; "+
				"%d core-group object(s) in the operator namespace were not classified "+
				"(no pre-teardown snapshot was recorded).", len(report.Expected), n)
		case n > 0:
			message = fmt.Sprintf("Teardown verified: no residue outside %d expected survivors; "+
				"%d core-group object(s) in the operator namespace were not classified "+
				"(%d appeared after the pre-teardown snapshot).", len(report.Expected), n, report.NewSinceInspect)
		}
		r.setCondition(td, conditionTypeTeardownComplete, metav1.ConditionTrue, "Complete", message)
		r.setCondition(td, conditionTypeTeardownProgressing, metav1.ConditionFalse, "TeardownComplete",
			"All teardown phases completed and verified.")
		td.Status.BlockingResources = nil
		log.Info("Teardown verified complete", "expectedSurvivors", len(report.Expected))
	} else {
		td.Status.Phase = operatorv1.TeardownPhaseCompleteWithResidue
		td.Status.ObservedGeneration = td.Generation
		reason := "ResidueDetected"
		message := fmt.Sprintf("Teardown finished with %d residue item(s); see status.residue.", len(report.Residue))
		if report.RefusedReason != "" {
			reason = "InventoryIncomplete"
			message = fmt.Sprintf("Complete not claimed: %s. %d item(s) observed.", report.RefusedReason, len(report.Residue))
		}
		r.setCondition(td, conditionTypeTeardownComplete, metav1.ConditionFalse, reason, message)
		r.setCondition(td, conditionTypeTeardownProgressing, metav1.ConditionFalse, "TeardownCompleteWithResidue", message)
		log.Info("Teardown finished with residue", "residue", len(report.Residue), "reason", reason)
	}
	r.setCondition(td, conditionTypeTeardownStalled, metav1.ConditionFalse, "TeardownFinished",
		"Teardown reached a terminal phase; stall detection no longer applies.")

	if err := r.Client.Status().Update(ctx, td); err != nil {
		return false, fmt.Errorf("persisting terminal status: %w", err)
	}
	return true, nil
}

// observeForVerification collects everything pkg/verify classifies. A failed
// list is recorded as incomplete rather than aborting: the phase can still
// report what it saw, it just cannot claim absence.
func (r *HubTeardownReconciler) observeForVerification(ctx context.Context, log logr.Logger, td *operatorv1.HubTeardown) (verify.Input, error) {
	graph, err := r.buildDependencyGraph(ctx, td, log)
	if err != nil {
		return verify.Input{}, fmt.Errorf("scanning for verification: %w", err)
	}

	input := verify.Input{}
	for _, incomplete := range graph.Incomplete {
		input.Incomplete = append(input.Incomplete,
			fmt.Sprintf("%s/%s: %s", incomplete.Group, incomplete.Resource, incomplete.Reason))
	}
	for i := range graph.DiscoveredResources {
		dr := &graph.DiscoveredResources[i]
		input.Objects = append(input.Objects, verify.ObjectRef{
			Group:     dr.Ref.Group,
			Kind:      dr.Ref.Kind,
			Namespace: dr.Ref.Namespace,
			Name:      dr.Ref.Name,
		})
	}

	crdList := &apixv1.CustomResourceDefinitionList{}
	if err := r.Client.List(ctx, crdList); err != nil {
		input.Incomplete = append(input.Incomplete, "customresourcedefinitions: "+err.Error())
	}
	for i := range crdList.Items {
		input.CRDs = append(input.CRDs, verify.ObjectRef{
			Group: crdList.Items[i].Spec.Group,
			Kind:  crdList.Items[i].Spec.Names.Kind,
			Name:  crdList.Items[i].Name,
		})
	}

	vwcList := &admissionregistrationv1.ValidatingWebhookConfigurationList{}
	if err := r.Client.List(ctx, vwcList); err != nil {
		input.Incomplete = append(input.Incomplete, "validatingwebhookconfigurations: "+err.Error())
	}
	for i := range vwcList.Items {
		item := &vwcList.Items[i]
		ref := verify.WebhookRef{
			Kind:             "ValidatingWebhookConfiguration",
			Name:             item.Name,
			ServiceNamespace: firstValidatingWebhookServiceNamespace(item.Webhooks),
			ACMOwned:         hasInstallerLabels(item.Labels, td.Namespace),
		}
		services, decisive := validatingRegistrationServices(item.Webhooks)
		ref.BackingServices, ref.ServicesAbsent, err = r.webhookBackingServices(ctx, services, decisive)
		if err != nil {
			input.Incomplete = append(input.Incomplete,
				fmt.Sprintf("validatingwebhookconfigurations/%s: %v", item.Name, err))
		}
		input.Webhooks = append(input.Webhooks, ref)
	}

	mwcList := &admissionregistrationv1.MutatingWebhookConfigurationList{}
	if err := r.Client.List(ctx, mwcList); err != nil {
		input.Incomplete = append(input.Incomplete, "mutatingwebhookconfigurations: "+err.Error())
	}
	for i := range mwcList.Items {
		item := &mwcList.Items[i]
		ref := verify.WebhookRef{
			Kind:             "MutatingWebhookConfiguration",
			Name:             item.Name,
			ServiceNamespace: firstMutatingWebhookServiceNamespace(item.Webhooks),
			ACMOwned:         hasInstallerLabels(item.Labels, td.Namespace),
		}
		services, decisive := mutatingRegistrationServices(item.Webhooks)
		ref.BackingServices, ref.ServicesAbsent, err = r.webhookBackingServices(ctx, services, decisive)
		if err != nil {
			input.Incomplete = append(input.Incomplete,
				fmt.Sprintf("mutatingwebhookconfigurations/%s: %v", item.Name, err))
		}
		input.Webhooks = append(input.Webhooks, ref)
	}

	// Core-group objects in the preserved namespace are not residue under D-G.
	// They are compared against the snapshot taken before any destructive
	// phase, so "was always here" and "the teardown created it" are different
	// observations instead of one undifferentiated count.
	observed, incomplete := r.listOperatorNamespaceObjects(ctx, td)
	input.OperatorNamespaceObjects = append(input.OperatorNamespaceObjects, observed...)
	input.Incomplete = append(input.Incomplete, incomplete...)

	snapshot, present, err := r.loadOperatorNamespaceSnapshot(ctx, td)
	if err != nil {
		input.Incomplete = append(input.Incomplete, "operator namespace snapshot: "+err.Error())
	} else {
		input.SnapshotPresent = present
		input.OperatorNamespaceSnapshot = snapshot.Objects
	}

	nsList := &corev1.NamespaceList{}
	if err := r.Client.List(ctx, nsList); err != nil {
		input.Incomplete = append(input.Incomplete, "namespaces: "+err.Error())
	}
	for i := range nsList.Items {
		input.Namespaces = append(input.Namespaces, nsList.Items[i].Name)
	}

	labels := client.MatchingLabels{"installer.namespace": td.Namespace}
	crList := &rbacv1.ClusterRoleList{}
	if err := r.Client.List(ctx, crList, labels); err != nil {
		input.Incomplete = append(input.Incomplete, "clusterroles: "+err.Error())
	}
	for i := range crList.Items {
		input.RBAC = append(input.RBAC, verify.ObjectRef{
			Group: "rbac.authorization.k8s.io",
			Kind:  "ClusterRole",
			Name:  crList.Items[i].Name,
		})
	}
	crbList := &rbacv1.ClusterRoleBindingList{}
	if err := r.Client.List(ctx, crbList, labels); err != nil {
		input.Incomplete = append(input.Incomplete, "clusterrolebindings: "+err.Error())
	}
	for i := range crbList.Items {
		input.RBAC = append(input.RBAC, verify.ObjectRef{
			Group: "rbac.authorization.k8s.io",
			Kind:  "ClusterRoleBinding",
			Name:  crbList.Items[i].Name,
		})
	}

	return input, nil
}

// webhookBackingServices records the Service-backed client configurations and
// whether every one of them was observed absent. decisive is false when the
// configuration has a URL-based client: without a Service reference there is
// nothing to prove absent, so the registration stays report-only.
func (r *HubTeardownReconciler) webhookBackingServices(
	ctx context.Context,
	services []registrationService,
	decisive bool,
) ([]verify.ServiceRef, bool, error) {
	if !decisive {
		return nil, false, nil
	}
	refs := make([]verify.ServiceRef, 0, len(services))
	for _, service := range services {
		refs = append(refs, verify.ServiceRef{Namespace: service.Namespace, Name: service.Name})
	}
	absent, err := allServicesAbsent(services, func(namespace, name string) (bool, error) {
		return r.servicePresent(ctx, namespace, name)
	})
	if err != nil {
		return refs, false, err
	}
	return refs, absent, nil
}

// hasInstallerLabels reports whether an object carries the labels ACM stamps on
// what it installs. installer.namespace is the same selector the RBAC inventory
// uses; installer.name catches objects installed before that label was added.
func hasInstallerLabels(objLabels map[string]string, namespace string) bool {
	if objLabels == nil {
		return false
	}
	if objLabels["installer.namespace"] == namespace {
		return true
	}
	return objLabels["installer.name"] == "multiclusterhub"
}

func firstValidatingWebhookServiceNamespace(hooks []admissionregistrationv1.ValidatingWebhook) string {
	for i := range hooks {
		if hooks[i].ClientConfig.Service != nil {
			return hooks[i].ClientConfig.Service.Namespace
		}
	}
	return ""
}

func firstMutatingWebhookServiceNamespace(hooks []admissionregistrationv1.MutatingWebhook) string {
	for i := range hooks {
		if hooks[i].ClientConfig.Service != nil {
			return hooks[i].ClientConfig.Service.Namespace
		}
	}
	return ""
}
