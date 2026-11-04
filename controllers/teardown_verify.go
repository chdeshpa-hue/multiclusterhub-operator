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

	policy := verify.Policy{
		OperatorNamespace: td.Namespace,
		ACMGroupSuffixes:  acmAPIGroupSuffixes,
		ACMNamespaces:     verifyACMNamespaces,
		KeepCRDs:          verifyKeepCRDs,
		OperatorAPIGroup:  verifyOperatorAPIGroup,
	}
	report := verify.Evaluate(input, policy)

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
		r.setCondition(td, conditionTypeTeardownComplete, metav1.ConditionTrue, "Complete",
			fmt.Sprintf("Teardown verified: no residue outside %d expected survivors.", len(report.Expected)))
		r.setCondition(td, conditionTypeTeardownProgressing, metav1.ConditionFalse, "TeardownComplete",
			"All teardown phases completed and verified.")
		td.Status.BlockingResources = nil
		log.Info("Teardown verified complete", "expectedSurvivors", len(report.Expected))
	} else {
		td.Status.Phase = operatorv1.TeardownPhaseCompleteWithResidue
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
	graph, err := r.buildDependencyGraph(ctx, log)
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
		input.Webhooks = append(input.Webhooks, verify.WebhookRef{
			Kind:             "ValidatingWebhookConfiguration",
			Name:             vwcList.Items[i].Name,
			ServiceNamespace: firstValidatingWebhookServiceNamespace(vwcList.Items[i].Webhooks),
		})
	}

	mwcList := &admissionregistrationv1.MutatingWebhookConfigurationList{}
	if err := r.Client.List(ctx, mwcList); err != nil {
		input.Incomplete = append(input.Incomplete, "mutatingwebhookconfigurations: "+err.Error())
	}
	for i := range mwcList.Items {
		input.Webhooks = append(input.Webhooks, verify.WebhookRef{
			Kind:             "MutatingWebhookConfiguration",
			Name:             mwcList.Items[i].Name,
			ServiceNamespace: firstMutatingWebhookServiceNamespace(mwcList.Items[i].Webhooks),
		})
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
