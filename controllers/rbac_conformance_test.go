// Copyright Contributors to the Open Cluster Management project

package controllers

import (
	"os"
	"testing"

	rbacv1 "k8s.io/api/rbac/v1"
	"sigs.k8s.io/yaml"
)

func readClusterRoleFile(t *testing.T, path string) *rbacv1.ClusterRole {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	role := &rbacv1.ClusterRole{}
	if err := yaml.Unmarshal(raw, role); err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	return role
}

func roleAllows(role *rbacv1.ClusterRole, apiGroup, resource, verb string) bool {
	for _, rule := range role.Rules {
		if !contains(rule.APIGroups, apiGroup) || !contains(rule.Resources, resource) || !contains(rule.Verbs, verb) {
			continue
		}
		if len(rule.ResourceNames) == 0 {
			return true
		}
	}
	return false
}

func roleAllowsNamed(role *rbacv1.ClusterRole, apiGroup, resource, name, verb string) bool {
	for _, rule := range role.Rules {
		if !contains(rule.APIGroups, apiGroup) || !contains(rule.Resources, resource) || !contains(rule.Verbs, verb) {
			continue
		}
		if contains(rule.ResourceNames, name) {
			return true
		}
	}
	return false
}

func contains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

// RB-1: the short-lived Job identity carries the destructive verbs. It is a
// separate role, installed statically, and is never created by the operator.
func TestHandoffRoleCarriesDestructiveVerbs(t *testing.T) {
	role := readClusterRoleFile(t, "../config/rbac/handoff_role.yaml")
	if role.Name != "acm-uninstall-handoff" {
		t.Fatalf("role name = %q, want acm-uninstall-handoff", role.Name)
	}
	for _, verb := range []string{"get", "list", "watch", "delete", "deletecollection", "patch", "update"} {
		if !roleAllows(role, "*", "*", verb) {
			t.Errorf("handoff role is missing %q on */*", verb)
		}
	}
	if !roleAllows(role, "", "configmaps", "create") {
		t.Error("handoff role must be able to create its progress/report ConfigMaps")
	}
}

// RB-2: the operator gains a named bind, not the handoff role's permissions.
// The pre-existing broad operator rule is tracked debt; this test guards the
// branch from adding another explicit destructive grant beside it.
func TestOperatorRoleBindsNamedHandoffRoleOnly(t *testing.T) {
	role := readClusterRoleFile(t, "../config/rbac/role.yaml")
	if !roleAllowsNamed(role, "rbac.authorization.k8s.io", "clusterroles", "acm-uninstall-handoff", "bind") {
		t.Error("operator role must bind acm-uninstall-handoff by resourceName")
	}
	for _, rule := range role.Rules {
		if len(rule.Resources) == 1 && rule.Resources[0] == "subscriptions/finalizers" {
			for _, verb := range rule.Verbs {
				if verb == "delete" || verb == "patch" || verb == "update" {
					t.Errorf("operator role must not %s subscriptions/finalizers", verb)
				}
			}
		}
		if len(rule.Resources) == 1 && rule.Resources[0] == "deployments/scale" && contains(rule.Verbs, "patch") {
			t.Error("operator role must not patch deployments/scale for teardown")
		}
	}
}
