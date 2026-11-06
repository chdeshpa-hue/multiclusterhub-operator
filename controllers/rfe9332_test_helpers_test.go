// Copyright Contributors to the Open Cluster Management project

package controllers

import corev1 "k8s.io/api/core/v1"

// testHubRef is the default target used by controller unit tests. Most tests
// do not create a MultiClusterHub; a missing target is a valid "nothing to
// delete" path. Tests that do create one use this same name and namespace.
func testHubRef() corev1.ObjectReference {
	return corev1.ObjectReference{
		APIVersion: "operator.open-cluster-management.io/v1",
		Kind:       "MultiClusterHub",
		Name:       "multiclusterhub",
		Namespace:  "open-cluster-management",
	}
}
