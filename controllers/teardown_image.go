// Copyright Contributors to the Open Cluster Management project

package controllers

import (
	"context"
	"fmt"
	"os"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	operatorDeploymentName = "multiclusterhub-operator"
	operatorContainerName  = "multiclusterhub-operator"
	operatorImageEnv       = "OPERATOR_IMAGE"
	operatorImageFallback  = "RELATED_IMAGE_MULTICLUSTERHUB_OPERATOR"
)

// teardownJobImage resolves the image the short-lived teardown Jobs run.
// OPERATOR_IMAGE is the explicit bundle contract. When the bundle does not set
// it, read the image from the operator Deployment so a stock install still
// creates the Job instead of stalling at PrepareJobRunning.
func (r *HubTeardownReconciler) teardownJobImage(ctx context.Context) (string, error) {
	if image := strings.TrimSpace(os.Getenv(operatorImageEnv)); image != "" {
		return image, nil
	}
	if image := strings.TrimSpace(os.Getenv(operatorImageFallback)); image != "" {
		return image, nil
	}

	namespace := strings.TrimSpace(os.Getenv(envPodNamespace))
	if namespace == "" {
		return "", fmt.Errorf("teardown Job image is not configured: set %s or %s, or set %s so the operator Deployment can be read", operatorImageEnv, operatorImageFallback, envPodNamespace)
	}

	reader := r.Client
	if r.UncachedClient != nil {
		reader = r.UncachedClient
	}
	deployment := &appsv1.Deployment{}
	err := reader.Get(ctx, types.NamespacedName{Namespace: namespace, Name: operatorDeploymentName}, deployment)
	if errors.IsNotFound(err) {
		deployment, err = r.operatorDeploymentByLabel(ctx, reader, namespace)
	}
	if err != nil {
		return "", fmt.Errorf("teardown Job image is not configured and the operator Deployment could not be read: %w", err)
	}
	for _, container := range deployment.Spec.Template.Spec.Containers {
		if container.Name == operatorContainerName && strings.TrimSpace(container.Image) != "" {
			return strings.TrimSpace(container.Image), nil
		}
	}
	for _, container := range deployment.Spec.Template.Spec.Containers {
		if image := strings.TrimSpace(container.Image); image != "" {
			return image, nil
		}
	}
	return "", fmt.Errorf("teardown Job image is not configured and Deployment %s/%s has no container image; set %s", namespace, deployment.Name, operatorImageEnv)
}

func (r *HubTeardownReconciler) operatorDeploymentByLabel(ctx context.Context, reader client.Client, namespace string) (*appsv1.Deployment, error) {
	list := &appsv1.DeploymentList{}
	if err := reader.List(ctx, list, client.InNamespace(namespace), client.MatchingLabels{"control-plane": operatorContainerName}); err != nil {
		return nil, fmt.Errorf("listing the operator Deployment in %s: %w", namespace, err)
	}
	switch len(list.Items) {
	case 0:
		return nil, fmt.Errorf("Deployment %s/%s was not found and no Deployment with label control-plane=%s exists", namespace, operatorDeploymentName, operatorContainerName)
	case 1:
		return &list.Items[0], nil
	default:
		return nil, fmt.Errorf("found %d Deployments with label control-plane=%s in %s; cannot choose the operator image", len(list.Items), operatorContainerName, namespace)
	}
}
