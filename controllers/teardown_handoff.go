// Copyright Contributors to the Open Cluster Management project

package controllers

import (
	"context"
	"fmt"

	"github.com/go-logr/logr"
	operatorv1 "github.com/stolostron/multiclusterhub-operator/api/v1"
	"github.com/stolostron/multiclusterhub-operator/pkg/policy"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

const (
	// handoffRoleName is the statically shipped ClusterRole that carries the
	// destructive verbs. It is installed by OLM with the bundle, never created
	// by the operator.
	handoffRoleName = "acm-uninstall-handoff"
	// handoffSAName and handoffCRBName are the short-lived identity the
	// Job runs as. They exist only while a Job is being run.
	handoffSAName  = "hubteardown-handoff"
	handoffCRBName = "hubteardown-handoff"

	conditionTypeHandoffReady = "HandoffReady"
)

// teardownApprovalFlagsSet reports whether either approval flag is on. It says
// what kind of work is authorised, not which report it was authorised against.
func teardownApprovalFlagsSet(td *operatorv1.HubTeardown) bool {
	return td.Spec.ApprovedDestructiveActions || td.Spec.AcknowledgeCloudResourceRisk
}

// teardownConsentGiven reports whether the admin has authorised destructive
// work against the report that currently exists. dryRun=false alone starts the
// pipeline, but nothing with cluster-wide blast radius is created until an
// approval flag is set AND the acknowledged digest matches the report digest.
func teardownConsentGiven(td *operatorv1.HubTeardown) bool {
	if td.Spec.DryRun {
		return false
	}
	return policy.ConsentState{
		ApprovedDestructiveActions: td.Spec.ApprovedDestructiveActions,
		CloudRiskAcknowledged:      td.Spec.AcknowledgeCloudResourceRisk,
		AcknowledgedDigest:         td.Spec.AcknowledgedReportDigest,
		ReportDigest:               td.Status.ReportDigest,
	}.Valid()
}

func handoffLabels() map[string]string {
	return map[string]string{
		"app": "hubteardown",
		"operator.open-cluster-management.io/teardown": "true",
	}
}

func handoffServiceAccount(namespace string) *corev1.ServiceAccount {
	return &corev1.ServiceAccount{
		TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "ServiceAccount"},
		ObjectMeta: metav1.ObjectMeta{
			Name:      handoffSAName,
			Namespace: namespace,
			Labels:    handoffLabels(),
		},
	}
}

func handoffClusterRoleBinding(namespace string) *rbacv1.ClusterRoleBinding {
	return &rbacv1.ClusterRoleBinding{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "rbac.authorization.k8s.io/v1",
			Kind:       "ClusterRoleBinding",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:   handoffCRBName,
			Labels: handoffLabels(),
		},
		RoleRef: rbacv1.RoleRef{
			APIGroup: "rbac.authorization.k8s.io",
			Kind:     "ClusterRole",
			Name:     handoffRoleName,
		},
		Subjects: []rbacv1.Subject{{
			Kind:      "ServiceAccount",
			Name:      handoffSAName,
			Namespace: namespace,
		}},
	}
}

// ensureHandoffRole refuses to build a binding that cannot exist. Creating a
// ClusterRoleBinding to a missing ClusterRole is rejected by the API server, so
// the absence is reported before anything is created.
func (r *HubTeardownReconciler) ensureHandoffRole(ctx context.Context) error {
	role := &rbacv1.ClusterRole{}
	if err := r.Client.Get(ctx, types.NamespacedName{Name: handoffRoleName}, role); err != nil {
		if errors.IsNotFound(err) {
			return fmt.Errorf("handoff ClusterRole %q is not installed; the bundle must ship it", handoffRoleName)
		}
		return fmt.Errorf("reading handoff ClusterRole %q: %w", handoffRoleName, err)
	}
	return nil
}

// ensureHandoffRBAC creates the ServiceAccount and its ClusterRoleBinding.
// It is idempotent: an existing object is adopted, not duplicated.
func (r *HubTeardownReconciler) ensureHandoffRBAC(ctx context.Context, log logr.Logger, td *operatorv1.HubTeardown) error {
	namespace := teardownNamespace(td)

	sa := handoffServiceAccount(namespace)
	existingSA := &corev1.ServiceAccount{}
	err := r.Client.Get(ctx, types.NamespacedName{Name: sa.Name, Namespace: sa.Namespace}, existingSA)
	switch {
	case err == nil:
		log.Info("Handoff ServiceAccount already exists", "serviceaccount", sa.Name)
	case errors.IsNotFound(err):
		if createErr := r.Client.Create(ctx, sa); createErr != nil {
			return fmt.Errorf("creating handoff ServiceAccount: %w", createErr)
		}
	default:
		return fmt.Errorf("checking for handoff ServiceAccount: %w", err)
	}

	crb := handoffClusterRoleBinding(namespace)
	existingCRB := &rbacv1.ClusterRoleBinding{}
	err = r.Client.Get(ctx, types.NamespacedName{Name: crb.Name}, existingCRB)
	switch {
	case err == nil:
		log.Info("Handoff ClusterRoleBinding already exists", "clusterrolebinding", crb.Name)
	case errors.IsNotFound(err):
		if createErr := r.Client.Create(ctx, crb); createErr != nil {
			return fmt.Errorf("creating handoff ClusterRoleBinding: %w", createErr)
		}
		r.Recorder.Eventf(td, corev1.EventTypeNormal, "HandoffBindingCreated",
			"Bound ServiceAccount %s/%s to ClusterRole %s", namespace, handoffSAName, handoffRoleName)
	default:
		return fmt.Errorf("checking for handoff ClusterRoleBinding: %w", err)
	}

	return nil
}

// ensureHandoff creates the identity and the Job, and only once the admin
// has authorised destructive work. Every failure is reported to the caller so
// it can be surfaced as a condition instead of being logged and dropped.
func (r *HubTeardownReconciler) ensureHandoff(ctx context.Context, log logr.Logger, td *operatorv1.HubTeardown) error {
	if !teardownConsentGiven(td) {
		return nil
	}
	if err := r.ensureHandoffRole(ctx); err != nil {
		return err
	}
	if err := r.ensureHandoffRBAC(ctx, log, td); err != nil {
		return err
	}
	// Once every prepare phase is recorded complete the Job is finished and
	// finishPrepareJob deletes it. Re-creating it here puts a second writer on
	// the shared progress record while the handoff Job is running (D9b-2).
	prepareDone, _, err := r.prepareProgressComplete(ctx, td)
	if err != nil {
		return err
	}
	if prepareDone {
		return nil
	}
	if err := r.ensurePrepareJob(ctx, log, td); err != nil {
		return err
	}
	return nil
}

func (r *HubTeardownReconciler) deleteHandoffRBAC(ctx context.Context, log logr.Logger, td *operatorv1.HubTeardown) error {
	namespace := teardownNamespace(td)

	crb := handoffClusterRoleBinding(namespace)
	if err := r.Client.Delete(ctx, crb); err != nil && !errors.IsNotFound(err) {
		return fmt.Errorf("deleting handoff ClusterRoleBinding: %w", err)
	}

	sa := handoffServiceAccount(namespace)
	if err := r.Client.Delete(ctx, sa); err != nil && !errors.IsNotFound(err) {
		return fmt.Errorf("deleting handoff ServiceAccount: %w", err)
	}

	log.Info("Removed handoff identity", "serviceaccount", sa.Name, "clusterrolebinding", crb.Name)
	return nil
}

// cleanupHandoff removes every object the handoff created. It is used on
// the CR-deletion path, where there is no status left to record.
func (r *HubTeardownReconciler) cleanupHandoff(ctx context.Context, log logr.Logger, td *operatorv1.HubTeardown) error {
	if err := r.deleteHandoffRBAC(ctx, log, td); err != nil {
		return err
	}
	if err := r.cleanupPrepareJob(ctx, log, td); err != nil {
		return err
	}
	return r.cleanupJobNamed(ctx, log, td, handoffJobName)
}

// ensureHandoffJob creates the handoff Job. It refuses to start while the
// prepare Job is still running: both Jobs write the same progress record and
// there is no lock, so the prepare pod must have exited first (D9b-2).
func (r *HubTeardownReconciler) ensureHandoffJob(ctx context.Context, log logr.Logger, td *operatorv1.HubTeardown) error {
	done, _, err := r.prepareProgressComplete(ctx, td)
	if err != nil {
		return err
	}
	if !done {
		return nil
	}

	exists, terminal, err := r.jobState(ctx, td, prepareJobName)
	if err != nil {
		return err
	}
	if exists && terminal == nil {
		return nil
	}
	if exists && *terminal == batchv1.JobFailed {
		return fmt.Errorf("prepare Job failed; the handoff Job is not started")
	}

	return r.ensureJobNamed(ctx, log, td, handoffJobName, TeardownPlanHandoff)
}

// finishPrepareJob removes the prepare Job once it has reached a terminal
// condition. The identity is deliberately kept: the handoff Job runs under the
// same ServiceAccount, so removing it here would strand the second Job. The
// identity is removed by cleanupHandoff at the terminal phase, on failure, or on
// CR deletion — and in that order: binding, ServiceAccount, then Jobs.
func (r *HubTeardownReconciler) finishPrepareJob(ctx context.Context, log logr.Logger, td *operatorv1.HubTeardown) error {
	exists, terminal, err := r.jobState(ctx, td, prepareJobName)
	if err != nil {
		return err
	}
	if !exists || terminal == nil {
		return nil
	}

	r.Recorder.Eventf(td, corev1.EventTypeNormal, "PrepareJobFinished",
		"Prepare Job finished with condition %s; handoff identity retained.", *terminal)
	if err := r.cleanupPrepareJob(ctx, log, td); err != nil {
		return err
	}

	log.Info("Prepare Job finished", "condition", string(*terminal))
	return nil
}

// finishHandoffJob observes the handoff Job and removes the whole handoff once
// it is terminal. Verify has already written the terminal phase by then, so this
// is the last use of the privileged identity on a successful run.
func (r *HubTeardownReconciler) finishHandoffJob(ctx context.Context, log logr.Logger, td *operatorv1.HubTeardown) error {
	exists, terminal, err := r.jobState(ctx, td, handoffJobName)
	if err != nil {
		return err
	}
	if exists && terminal == nil {
		return nil
	}
	if exists {
		r.setCondition(td, conditionTypeHandoffReady, metav1.ConditionFalse, string(*terminal),
			fmt.Sprintf("Handoff Job finished with condition %s; identity removed.", *terminal))
		r.Recorder.Eventf(td, corev1.EventTypeNormal, "HandoffJobFinished",
			"Handoff Job finished with condition %s", *terminal)
	}
	return r.cleanupHandoff(ctx, log, td)
}
