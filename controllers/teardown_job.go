// Copyright Contributors to the Open Cluster Management project

package controllers

import (
	"context"
	"fmt"
	"os"

	"github.com/go-logr/logr"
	operatorv1 "github.com/stolostron/multiclusterhub-operator/api/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	prepareJobName = "hubteardown-cleanup"
	handoffJobName = "hubteardown-handoff"

	// teardownPlanFlag selects which plan a Job executes. Both Jobs run the same
	// binary and differ by exactly this value, so the Job spec is written once.
	teardownPlanFlag = "--teardown-plan="
	// TeardownPlanPrepare and TeardownPlanHandoff are the two executable plans.
	TeardownPlanPrepare = "prepare"
	TeardownPlanHandoff = "handoff"

	teardownJobFinalizer = "operator.open-cluster-management.io/teardown-job"

	envTeardownName      = "TEARDOWN_NAME"
	envTeardownNamespace = "TEARDOWN_NAMESPACE"
	envPodNamespace      = "POD_NAMESPACE"
)

// teardownNamespace is the namespace every handoff object lives in. The CR
// namespace wins; POD_NAMESPACE covers the case where the controller is asked
// to act on a CR read from the cache before the namespace default is applied.
func teardownNamespace(td *operatorv1.HubTeardown) string {
	if td.Namespace != "" {
		return td.Namespace
	}
	return os.Getenv(envPodNamespace)
}

// teardownJobDeadline bounds the Job by the same wall clock the controller
// uses for stall detection, so a hung Job cannot outlive spec.maxDuration.
func teardownJobDeadline(td *operatorv1.HubTeardown) *int64 {
	maxDur := effectiveMaxDuration(td)
	seconds := int64(maxDur.Seconds())
	if seconds < 60 {
		seconds = 60
	}
	return &seconds
}

// ensurePrepareJob creates or verifies the resilient cleanup Job that survives
// operator pod deletion. The Job runs the same operator binary with a special
// entrypoint flag to continue teardown independently.
func (r *HubTeardownReconciler) ensurePrepareJob(ctx context.Context, log logr.Logger, td *operatorv1.HubTeardown) error {
	return r.ensureJobNamed(ctx, log, td, prepareJobName, TeardownPlanPrepare)
}

// ensureJobNamed creates the Job that runs one phase set. The prepare Job and
// the handoff Job differ only by name, plan value and plan label.
func (r *HubTeardownReconciler) ensureJobNamed(ctx context.Context, log logr.Logger, td *operatorv1.HubTeardown, jobName, plan string) error {
	ns := teardownNamespace(td)
	jobKey := types.NamespacedName{
		Name:      jobName,
		Namespace: ns,
	}

	existing := &batchv1.Job{}
	err := r.Client.Get(ctx, jobKey, existing)
	if err == nil {
		log.Info("Teardown Job already exists", "job", jobKey)
		return nil
	}
	if !errors.IsNotFound(err) {
		return fmt.Errorf("checking for existing teardown Job: %w", err)
	}

	operatorImage := os.Getenv("OPERATOR_IMAGE")
	if operatorImage == "" {
		operatorImage = os.Getenv("RELATED_IMAGE_MULTICLUSTERHUB_OPERATOR")
	}
	if operatorImage == "" {
		log.Info("OPERATOR_IMAGE not set, skipping Job creation (teardown will rely on controller only)")
		return nil
	}

	backoffLimit := int32(2)
	ttlSeconds := int32(3600)
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      jobName,
			Namespace: ns,
			Labels: map[string]string{
				"app": "hubteardown",
				"operator.open-cluster-management.io/teardown":      "true",
				"operator.open-cluster-management.io/teardown-plan": plan,
			},
		},
		Spec: batchv1.JobSpec{
			BackoffLimit:            &backoffLimit,
			ActiveDeadlineSeconds:   teardownJobDeadline(td),
			TTLSecondsAfterFinished: &ttlSeconds,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"app": "hubteardown",
						"operator.open-cluster-management.io/teardown":      "true",
						"operator.open-cluster-management.io/teardown-plan": plan,
					},
				},
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyOnFailure,
					// Both Jobs run as the handoff identity, never as the
					// always-on operator ServiceAccount.
					ServiceAccountName: handoffSAName,
					Containers: []corev1.Container{
						{
							Name:    "teardown",
							Image:   operatorImage,
							Command: []string{"multiclusterhub-operator"},
							Args:    []string{teardownPlanFlag + plan},
							Env: []corev1.EnvVar{
								{Name: envTeardownName, Value: td.Name},
								{Name: envTeardownNamespace, Value: ns},
								{Name: "OPERATOR_VERSION", Value: os.Getenv("OPERATOR_VERSION")},
							},
						},
					},
				},
			},
		},
	}

	if err := r.Client.Create(ctx, job); err != nil {
		return fmt.Errorf("creating teardown Job: %w", err)
	}

	r.Recorder.Event(td, corev1.EventTypeNormal, "TeardownJobCreated",
		"Created resilient teardown Job that will continue cleanup if operator is removed")
	log.Info("Created teardown Job", "job", jobKey, "image", operatorImage)
	return nil
}

// cleanupPrepareJob removes the teardown Job when teardown is complete.
func (r *HubTeardownReconciler) cleanupPrepareJob(ctx context.Context, log logr.Logger, td *operatorv1.HubTeardown) error {
	return r.cleanupJobNamed(ctx, log, td, prepareJobName)
}

func (r *HubTeardownReconciler) cleanupJobNamed(ctx context.Context, log logr.Logger, td *operatorv1.HubTeardown, jobName string) error {
	jobKey := types.NamespacedName{
		Name:      jobName,
		Namespace: teardownNamespace(td),
	}

	existing := &batchv1.Job{}
	if err := r.Client.Get(ctx, jobKey, existing); err != nil {
		if errors.IsNotFound(err) {
			return nil
		}
		return err
	}

	propagation := metav1.DeletePropagationBackground
	if err := r.Client.Delete(ctx, existing, &client.DeleteOptions{
		PropagationPolicy: &propagation,
	}); err != nil && !errors.IsNotFound(err) {
		return fmt.Errorf("cleaning up teardown Job: %w", err)
	}

	log.Info("Cleaned up teardown Job")
	return nil
}

// jobState reports whether a Job exists and, when it has reached a terminal
// condition, which one. A running Job returns exists=true with a nil terminal;
// "not found" returns exists=false. The two are never conflated: the handoff Job
// must not start while the prepare pod is still writing the shared record.
func (r *HubTeardownReconciler) jobState(ctx context.Context, td *operatorv1.HubTeardown, jobName string) (bool, *batchv1.JobConditionType, error) {
	jobKey := types.NamespacedName{
		Name:      jobName,
		Namespace: teardownNamespace(td),
	}

	job := &batchv1.Job{}
	if err := r.Client.Get(ctx, jobKey, job); err != nil {
		if errors.IsNotFound(err) {
			return false, nil, nil
		}
		return false, nil, err
	}

	for i := range job.Status.Conditions {
		cond := job.Status.Conditions[i]
		if cond.Status == corev1.ConditionTrue {
			terminal := cond.Type
			return true, &terminal, nil
		}
	}
	return true, nil, nil
}

// teardownJobFailure reports whether one teardown Job is terminally failed, and
// with which condition reason. A Job whose pod has exhausted backoffLimit never
// writes another line of progress, so callers must not infer "still running"
// from an unchanged progress record.
func (r *HubTeardownReconciler) teardownJobFailure(ctx context.Context, td *operatorv1.HubTeardown, jobName string) (bool, string, error) {
	exists, terminal, err := r.jobState(ctx, td, jobName)
	if err != nil || !exists || terminal == nil {
		return false, "", err
	}
	if *terminal != batchv1.JobFailed {
		return false, "", nil
	}
	reason := string(batchv1.JobFailed)
	jobKey := types.NamespacedName{Name: jobName, Namespace: teardownNamespace(td)}
	job := &batchv1.Job{}
	if err := r.Client.Get(ctx, jobKey, job); err == nil {
		for i := range job.Status.Conditions {
			cond := job.Status.Conditions[i]
			if cond.Type == batchv1.JobFailed && cond.Reason != "" {
				reason = cond.Reason
				break
			}
		}
	}
	return true, reason, nil
}
