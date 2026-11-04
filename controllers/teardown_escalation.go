// Copyright Contributors to the Open Cluster Management project

package controllers

import (
	"context"
	"fmt"

	"github.com/go-logr/logr"
	operatorv1 "github.com/stolostron/multiclusterhub-operator/api/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

const (
	// escalationRoleName is the statically shipped ClusterRole that carries the
	// destructive verbs. It is installed by OLM with the bundle, never created
	// by the operator.
	escalationRoleName = "acm-uninstall-escalation"
	// escalationSAName and escalationCRBName are the short-lived identity the
	// Job runs as. They exist only while a Job is being run.
	escalationSAName  = "hubteardown-escalation"
	escalationCRBName = "hubteardown-escalation"

	conditionTypeEscalationReady = "EscalationReady"
)

// teardownConsentGiven reports whether the admin has authorised destructive
// work. dryRun=false alone starts the pipeline, but nothing with cluster-wide
// blast radius is created until one of the approval flags is set.
func teardownConsentGiven(td *operatorv1.HubTeardown) bool {
	if td.Spec.DryRun {
		return false
	}
	return td.Spec.ApprovedDestructiveActions || td.Spec.AcknowledgeCloudResourceRisk
}

func escalationLabels() map[string]string {
	return map[string]string{
		"app": "hubteardown",
		"operator.open-cluster-management.io/teardown": "true",
	}
}

func escalationServiceAccount(namespace string) *corev1.ServiceAccount {
	return &corev1.ServiceAccount{
		TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "ServiceAccount"},
		ObjectMeta: metav1.ObjectMeta{
			Name:      escalationSAName,
			Namespace: namespace,
			Labels:    escalationLabels(),
		},
	}
}

func escalationClusterRoleBinding(namespace string) *rbacv1.ClusterRoleBinding {
	return &rbacv1.ClusterRoleBinding{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "rbac.authorization.k8s.io/v1",
			Kind:       "ClusterRoleBinding",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:   escalationCRBName,
			Labels: escalationLabels(),
		},
		RoleRef: rbacv1.RoleRef{
			APIGroup: "rbac.authorization.k8s.io",
			Kind:     "ClusterRole",
			Name:     escalationRoleName,
		},
		Subjects: []rbacv1.Subject{{
			Kind:      "ServiceAccount",
			Name:      escalationSAName,
			Namespace: namespace,
		}},
	}
}

// ensureEscalationRole refuses to build a binding that cannot exist. Creating a
// ClusterRoleBinding to a missing ClusterRole is rejected by the API server, so
// the absence is reported before anything is created.
func (r *HubTeardownReconciler) ensureEscalationRole(ctx context.Context) error {
	role := &rbacv1.ClusterRole{}
	if err := r.Client.Get(ctx, types.NamespacedName{Name: escalationRoleName}, role); err != nil {
		if errors.IsNotFound(err) {
			return fmt.Errorf("escalation ClusterRole %q is not installed; the bundle must ship it", escalationRoleName)
		}
		return fmt.Errorf("reading escalation ClusterRole %q: %w", escalationRoleName, err)
	}
	return nil
}

// ensureEscalationRBAC creates the ServiceAccount and its ClusterRoleBinding.
// It is idempotent: an existing object is adopted, not duplicated.
func (r *HubTeardownReconciler) ensureEscalationRBAC(ctx context.Context, log logr.Logger, td *operatorv1.HubTeardown) error {
	namespace := teardownNamespace(td)

	sa := escalationServiceAccount(namespace)
	existingSA := &corev1.ServiceAccount{}
	err := r.Client.Get(ctx, types.NamespacedName{Name: sa.Name, Namespace: sa.Namespace}, existingSA)
	switch {
	case err == nil:
		log.Info("Escalation ServiceAccount already exists", "serviceaccount", sa.Name)
	case errors.IsNotFound(err):
		if createErr := r.Client.Create(ctx, sa); createErr != nil {
			return fmt.Errorf("creating escalation ServiceAccount: %w", createErr)
		}
	default:
		return fmt.Errorf("checking for escalation ServiceAccount: %w", err)
	}

	crb := escalationClusterRoleBinding(namespace)
	existingCRB := &rbacv1.ClusterRoleBinding{}
	err = r.Client.Get(ctx, types.NamespacedName{Name: crb.Name}, existingCRB)
	switch {
	case err == nil:
		log.Info("Escalation ClusterRoleBinding already exists", "clusterrolebinding", crb.Name)
	case errors.IsNotFound(err):
		if createErr := r.Client.Create(ctx, crb); createErr != nil {
			return fmt.Errorf("creating escalation ClusterRoleBinding: %w", createErr)
		}
		r.Recorder.Eventf(td, corev1.EventTypeNormal, "EscalationBindingCreated",
			"Bound ServiceAccount %s/%s to ClusterRole %s", namespace, escalationSAName, escalationRoleName)
	default:
		return fmt.Errorf("checking for escalation ClusterRoleBinding: %w", err)
	}

	return nil
}

// ensureEscalation creates the identity and the Job, and only once the admin
// has authorised destructive work. Every failure is reported to the caller so
// it can be surfaced as a condition instead of being logged and dropped.
func (r *HubTeardownReconciler) ensureEscalation(ctx context.Context, log logr.Logger, td *operatorv1.HubTeardown) error {
	if !teardownConsentGiven(td) {
		return nil
	}
	if err := r.ensureEscalationRole(ctx); err != nil {
		return err
	}
	if err := r.ensureEscalationRBAC(ctx, log, td); err != nil {
		return err
	}
	if err := r.ensureTeardownJob(ctx, log, td); err != nil {
		return err
	}
	return nil
}

func (r *HubTeardownReconciler) deleteEscalationRBAC(ctx context.Context, log logr.Logger, td *operatorv1.HubTeardown) error {
	namespace := teardownNamespace(td)

	crb := escalationClusterRoleBinding(namespace)
	if err := r.Client.Delete(ctx, crb); err != nil && !errors.IsNotFound(err) {
		return fmt.Errorf("deleting escalation ClusterRoleBinding: %w", err)
	}

	sa := escalationServiceAccount(namespace)
	if err := r.Client.Delete(ctx, sa); err != nil && !errors.IsNotFound(err) {
		return fmt.Errorf("deleting escalation ServiceAccount: %w", err)
	}

	log.Info("Removed escalation identity", "serviceaccount", sa.Name, "clusterrolebinding", crb.Name)
	return nil
}

// cleanupEscalation removes every object the escalation created. It is used on
// the CR-deletion path, where there is no status left to record.
func (r *HubTeardownReconciler) cleanupEscalation(ctx context.Context, log logr.Logger, td *operatorv1.HubTeardown) error {
	if err := r.deleteEscalationRBAC(ctx, log, td); err != nil {
		return err
	}
	return r.cleanupTeardownJob(ctx, log, td)
}

// finishEscalation runs once the Job reaches a terminal state. The order is the
// one the spec fixes: binding, then ServiceAccount, then the terminal status
// read, then the Job itself. Reading the status last is what makes the record
// authoritative — it is taken after the identity can no longer change anything.
func (r *HubTeardownReconciler) finishEscalation(ctx context.Context, log logr.Logger, td *operatorv1.HubTeardown) error {
	terminal, err := r.getTeardownJobStatus(ctx, td)
	if err != nil {
		return err
	}
	if terminal == nil {
		return nil
	}

	if err := r.deleteEscalationRBAC(ctx, log, td); err != nil {
		return err
	}

	recorded, err := r.getTeardownJobStatus(ctx, td)
	if err != nil {
		return err
	}
	if recorded == nil {
		recorded = terminal
	}

	r.setCondition(td, conditionTypeEscalationReady, metav1.ConditionFalse, string(*recorded),
		fmt.Sprintf("Escalation Job finished with condition %s; escalation identity removed.", *recorded))
	r.Recorder.Eventf(td, corev1.EventTypeNormal, "EscalationFinished",
		"Teardown Job finished with condition %s", *recorded)

	if err := r.cleanupTeardownJob(ctx, log, td); err != nil {
		return err
	}

	log.Info("Escalation finished", "condition", string(*recorded))
	return nil
}
