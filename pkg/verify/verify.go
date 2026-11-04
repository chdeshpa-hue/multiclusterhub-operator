// Package verify decides whether a teardown left anything behind. It is pure:
// callers collect the observed objects and this package classifies them.
//
// The classification is evidence-based. Expected survivors are an explicit,
// exact list supplied by the caller; nothing is matched by name prefix or
// suffix, which is the same rule the detach path applies to cluster-scoped
// leftovers.
package verify

import (
	"sort"
	"strings"

	operatorv1 "github.com/stolostron/multiclusterhub-operator/api/v1"
)

// ObjectRef is one cluster object, identified by group/kind/namespace/name.
type ObjectRef struct {
	Group     string
	Kind      string
	Namespace string
	Name      string
}

// WebhookRef is an admission webhook and the namespace of its backing Service.
// An empty ServiceNamespace means the webhook has no in-cluster backing.
type WebhookRef struct {
	Kind             string // ValidatingWebhookConfiguration or MutatingWebhookConfiguration
	Name             string
	ServiceNamespace string
}

// Input is everything Verify observed.
type Input struct {
	// Objects are ACM-group custom resources seen by the scan.
	Objects []ObjectRef

	// OperatorNamespaceObjects are core-group objects observed in the
	// preserved namespace. They are not classified as residue — there is no
	// declared allowlist yet — but they are counted so a complete verdict is
	// never silent about objects nobody looked at.
	OperatorNamespaceObjects []ObjectRef

	// CRDs are cluster-scoped definitions, regardless of group.
	CRDs []ObjectRef

	// Webhooks are validating and mutating configurations.
	Webhooks []WebhookRef

	// RBAC are cluster-scoped roles and bindings carrying the installer label.
	RBAC []ObjectRef

	// Namespaces are the names of every namespace that exists.
	Namespaces []string

	// Incomplete lists resource types the scan could not enumerate. A non-empty
	// list invalidates every absence claim, so it can never yield Complete.
	Incomplete []string
}

// Policy is the declared end state.
type Policy struct {
	// OperatorNamespace is the one namespace the teardown preserves (D-G).
	OperatorNamespace string

	// ACMGroupSuffixes identifies ACM/MCE API groups. Supplied by the caller so
	// there is one list in the codebase, not two.
	ACMGroupSuffixes []string

	// ACMNamespaces are namespaces a complete teardown must remove.
	ACMNamespaces []string

	// KeepCRDs are exact CRD names the preserved operator needs.
	KeepCRDs []string

	// KeepObjects are exact group/kind/namespace/name tuples that are expected
	// to survive even though they live in an ACM group.
	KeepObjects []ObjectRef

	// OperatorAPIGroup is the API group served by the preserved operator. Its
	// objects in OperatorNamespace are operator internals, not residue.
	OperatorAPIGroup string
}

// Report is the classified outcome.
type Report struct {
	Complete   bool
	Residue    []operatorv1.ResidueItem
	Expected   []operatorv1.ResidueItem
	Incomplete []string
	// Unclassified are objects in the preserved namespace with no declared
	// expectation. They do not affect Complete; they exist so the terminal
	// message can state the count.
	Unclassified []operatorv1.ResidueItem
	// RefusedReason is set when absence could not be proven.
	RefusedReason string
}

// Evaluate classifies the observation against the declared end state.
func Evaluate(in Input, p Policy) Report {
	report := Report{Incomplete: append([]string(nil), in.Incomplete...)}

	keepObjects := make(map[string]struct{}, len(p.KeepObjects))
	for _, obj := range p.KeepObjects {
		keepObjects[objectKey(obj.Group, obj.Kind, obj.Namespace, obj.Name)] = struct{}{}
	}
	keepCRDs := make(map[string]struct{}, len(p.KeepCRDs))
	for _, name := range p.KeepCRDs {
		keepCRDs[name] = struct{}{}
	}
	acmNamespaces := make(map[string]struct{}, len(p.ACMNamespaces))
	for _, name := range p.ACMNamespaces {
		acmNamespaces[name] = struct{}{}
	}

	for _, ns := range in.Namespaces {
		if p.OperatorNamespace != "" && ns == p.OperatorNamespace {
			report.Expected = append(report.Expected, operatorv1.ResidueItem{
				Kind: "Namespace", Name: ns,
				Reason: "operator namespace preserved by design (D-G)",
			})
			continue
		}
		if _, ok := acmNamespaces[ns]; !ok {
			continue
		}
		report.Residue = append(report.Residue, operatorv1.ResidueItem{
			Kind: "Namespace", Name: ns,
			Reason: "ACM namespace still present after teardown",
		})
	}

	for _, crd := range in.CRDs {
		if !isACMGroup(p.ACMGroupSuffixes, crd.Group) {
			continue
		}
		if _, ok := keepCRDs[crd.Name]; ok {
			report.Expected = append(report.Expected, operatorv1.ResidueItem{
				APIVersion: apiVersion(crd.Group, ""),
				Kind:       "CustomResourceDefinition",
				Name:       crd.Name,
				Reason:     "CRD required by the preserved operator (D-G)",
			})
			continue
		}
		report.Residue = append(report.Residue, operatorv1.ResidueItem{
			APIVersion: apiVersion(crd.Group, ""),
			Kind:       "CustomResourceDefinition",
			Name:       crd.Name,
			Reason:     "ACM CRD still present after teardown",
		})
	}

	for _, obj := range in.Objects {
		if !isACMGroup(p.ACMGroupSuffixes, obj.Group) {
			continue
		}
		if _, ok := keepObjects[objectKey(obj.Group, obj.Kind, obj.Namespace, obj.Name)]; ok {
			report.Expected = append(report.Expected, residueItem(obj, "declared expected survivor"))
			continue
		}
		if p.OperatorAPIGroup != "" && obj.Group == p.OperatorAPIGroup && obj.Namespace == p.OperatorNamespace {
			report.Expected = append(report.Expected, residueItem(obj, "operator-internal object (D-G)"))
			continue
		}
		report.Residue = append(report.Residue, residueItem(obj, "ACM resource still present after teardown"))
	}

	for _, obj := range in.OperatorNamespaceObjects {
		if _, ok := keepObjects[objectKey(obj.Group, obj.Kind, obj.Namespace, obj.Name)]; ok {
			report.Expected = append(report.Expected, residueItem(obj, "declared expected survivor"))
			continue
		}
		report.Unclassified = append(report.Unclassified, residueItem(obj,
			"core-group object in the preserved namespace; no declared expectation"))
	}

	for _, hook := range in.Webhooks {
		if hook.ServiceNamespace == "" {
			report.Residue = append(report.Residue, operatorv1.ResidueItem{
				Kind: hook.Kind, Name: hook.Name,
				Reason: "admission webhook has no backing Service",
			})
			continue
		}
		if _, ok := acmNamespaces[hook.ServiceNamespace]; ok && hook.ServiceNamespace != p.OperatorNamespace {
			report.Residue = append(report.Residue, operatorv1.ResidueItem{
				Kind: hook.Kind, Name: hook.Name, Namespace: hook.ServiceNamespace,
				Reason: "admission webhook backed by a removed ACM namespace",
			})
		}
	}

	for _, role := range in.RBAC {
		report.Residue = append(report.Residue, residueItem(role, "ACM RBAC still present after teardown"))
	}

	sortItems(report.Expected)
	sortItems(report.Residue)
	sortItems(report.Unclassified)

	if len(in.Incomplete) > 0 {
		report.RefusedReason = "inventory incomplete, absence cannot be proven: " + strings.Join(in.Incomplete, "; ")
	}
	report.Complete = len(report.Residue) == 0 && len(in.Incomplete) == 0
	return report
}

func residueItem(obj ObjectRef, reason string) operatorv1.ResidueItem {
	return operatorv1.ResidueItem{
		APIVersion: apiVersion(obj.Group, ""),
		Kind:       obj.Kind,
		Namespace:  obj.Namespace,
		Name:       obj.Name,
		Reason:     reason,
	}
}

func objectKey(group, kind, namespace, name string) string {
	return strings.Join([]string{group, kind, namespace, name}, "/")
}

func apiVersion(group, version string) string {
	switch {
	case group == "":
		return version
	case version == "":
		return group
	default:
		return group + "/" + version
	}
}

func isACMGroup(suffixes []string, group string) bool {
	for _, suffix := range suffixes {
		if group == suffix || strings.HasSuffix(group, suffix) {
			return true
		}
	}
	return false
}

func sortItems(items []operatorv1.ResidueItem) {
	sort.SliceStable(items, func(i, j int) bool {
		a, b := items[i], items[j]
		if a.Kind != b.Kind {
			return a.Kind < b.Kind
		}
		if a.Namespace != b.Namespace {
			return a.Namespace < b.Namespace
		}
		return a.Name < b.Name
	})
}
