package ownership

import (
	"fmt"
	"sort"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"

	"github.com/stolostron/multiclusterhub-operator/pkg/inventory"
)

// maxOwnerDepth bounds ownerReference traversal. Real ACM chains are shallow
// (MCH -> MCE -> ClusterManager -> Deployment); a bound turns a malformed
// cycle into "unresolved" instead of a hang.
const maxOwnerDepth = 16

// Classify returns an ownership verdict for obj. It performs no I/O and reads
// no clock: every input comes from obj, idx and observedAt, which the caller
// supplies from the scan that produced them. Evidence is sorted so repeated
// calls with the same observedAt produce byte-identical results.
func Classify(obj metav1.PartialObjectMetadata, idx *inventory.Index, mch types.NamespacedName, observedAt metav1.Time) OwnershipVerdict {
	evidence := make([]Evidence, 0, 4)

	if hasInstallerLabels(obj.GetLabels(), mch) {
		evidence = append(evidence, Evidence{
			Kind:     EvidenceInstallerLabel,
			Detail:   installerDetail(obj.GetLabels()),
			Source:   "metadata.labels",
			Observed: observedAt,
		})
	} else if detail, ok := ownerChainEvidence(obj, idx, mch); ok {
		evidence = append(evidence, Evidence{
			Kind:     EvidenceOwnerRefChain,
			Detail:   detail,
			Source:   "metadata.ownerReferences",
			Observed: observedAt,
		})
	}

	if ev, ok := mchIdentityEvidence(obj, mch, observedAt); ok {
		evidence = append(evidence, ev)
	}
	if ev, ok := acmGroupCRDEvidence(obj, idx, observedAt); ok {
		evidence = append(evidence, ev)
	}
	if ev, ok := managedByEvidence(obj, observedAt); ok {
		evidence = append(evidence, ev)
	}
	if ev, ok := olmOwnerEvidence(obj, observedAt); ok {
		evidence = append(evidence, ev)
	}
	if ev, ok := definingCRDEvidence(obj, idx, mch, observedAt); ok {
		evidence = append(evidence, ev)
	}
	if ev, ok := apiServiceEvidence(obj, idx, mch, observedAt); ok {
		evidence = append(evidence, ev)
	}
	if nameMatchesACM(obj.GetName()) {
		evidence = append(evidence, Evidence{
			Kind:     EvidenceNameMatch,
			Detail:   fmt.Sprintf("name %q matches an ACM naming pattern", obj.GetName()),
			Source:   "metadata.name",
			Observed: observedAt,
		})
	}

	sort.Slice(evidence, func(i, j int) bool {
		if evidence[i].Kind != evidence[j].Kind {
			return evidence[i].Kind < evidence[j].Kind
		}
		return evidence[i].Detail < evidence[j].Detail
	})

	confidence := ConfidenceFromEvidence(evidence)
	return OwnershipVerdict{
		Owned:      allowsConfidence(confidence),
		Confidence: confidence,
		Evidence:   evidence,
	}
}

// ClassifyCRD returns the ownership verdict for a CustomResourceDefinition.
// A CRD is the defining object for its kind, so the object index cannot answer
// "which CRD defines this CRD" the way it does for an instance. The caller
// supplies whether the group is inside the ACM/MCE ownership boundary; the
// confidence rules stay here so CRD deletion and instance deletion cannot
// drift apart.
func ClassifyCRD(crd metav1.PartialObjectMetadata, acmOwnedGroup bool, observedAt metav1.Time) OwnershipVerdict {
	evidence := make([]Evidence, 0, 3)

	if labels := crd.GetLabels(); labels["installer.name"] != "" && labels["installer.namespace"] != "" {
		evidence = append(evidence, Evidence{
			Kind:     EvidenceInstallerLabel,
			Detail:   installerDetail(labels),
			Source:   "metadata.labels",
			Observed: observedAt,
		})
	}
	if acmOwnedGroup {
		evidence = append(evidence, Evidence{
			Kind:     EvidenceACMGroupCRD,
			Detail:   fmt.Sprintf("CRD %s defines an ACM/MCE API group", crd.GetName()),
			Source:   "CustomResourceDefinition.spec.group",
			Observed: observedAt,
		})
	}
	if nameMatchesACM(crd.GetName()) {
		evidence = append(evidence, Evidence{
			Kind:     EvidenceNameMatch,
			Detail:   fmt.Sprintf("name %q matches an ACM naming pattern", crd.GetName()),
			Source:   "metadata.name",
			Observed: observedAt,
		})
	}

	sort.Slice(evidence, func(i, j int) bool {
		if evidence[i].Kind != evidence[j].Kind {
			return evidence[i].Kind < evidence[j].Kind
		}
		return evidence[i].Detail < evidence[j].Detail
	})

	confidence := ConfidenceFromEvidence(evidence)
	return OwnershipVerdict{
		Owned:      allowsConfidence(confidence),
		Confidence: confidence,
		Evidence:   evidence,
	}
}

// AllowsDestruction reports whether the verdict authorises a destructive
// action. Definitive and Strong qualify; Weak and None never do.
func AllowsDestruction(v OwnershipVerdict) bool {
	return allowsConfidence(v.Confidence)
}

func allowsConfidence(c Confidence) bool {
	return rank(c) >= rank(ConfidenceStrong)
}

// ConfidenceFromEvidence applies the maximum-over-evidence rule. Weak evidence
// never accumulates, and ControllerAbsent is ignored entirely.
func ConfidenceFromEvidence(evidence []Evidence) Confidence {
	best := ConfidenceNone
	for _, ev := range evidence {
		var candidate Confidence
		switch ev.Kind {
		case EvidenceInstallerLabel, EvidenceOwnerRefChain, EvidenceMCHIdentity:
			candidate = ConfidenceDefinitive
		case EvidenceDefiningCRDLabelled, EvidenceAPIServiceBacking,
			EvidenceACMGroupCRD, EvidenceManagedByLabel, EvidenceOLMOwner:
			candidate = ConfidenceStrong
		case EvidenceNameMatch:
			candidate = ConfidenceWeak
		case EvidenceControllerAbsent:
			continue
		default:
			continue
		}
		if rank(candidate) > rank(best) {
			best = candidate
		}
	}
	return best
}

func rank(c Confidence) int {
	switch c {
	case ConfidenceDefinitive:
		return 3
	case ConfidenceStrong:
		return 2
	case ConfidenceWeak:
		return 1
	default:
		return 0
	}
}

func hasInstallerLabels(labels map[string]string, mch types.NamespacedName) bool {
	if mch.Name == "" || mch.Namespace == "" {
		return false
	}
	return labels["installer.name"] == mch.Name && labels["installer.namespace"] == mch.Namespace
}

func installerDetail(labels map[string]string) string {
	return fmt.Sprintf("installer.name=%s installer.namespace=%s",
		labels["installer.name"], labels["installer.namespace"])
}

func ownerChainEvidence(obj metav1.PartialObjectMetadata, idx *inventory.Index, mch types.NamespacedName) (string, bool) {
	seen := map[types.UID]bool{obj.GetUID(): true}
	current := obj
	path := []string{objectLabel(current.Kind, current.GetName())}

	for depth := 0; depth < maxOwnerDepth; depth++ {
		owner, ok := controllerOwner(current.GetOwnerReferences())
		if !ok || owner.UID == "" || seen[owner.UID] {
			return "", false
		}
		seen[owner.UID] = true
		path = append(path, objectLabel(owner.Kind, owner.Name))

		next, found := idx.Get(owner.UID)
		if !found {
			return "", false
		}
		if hasInstallerLabels(next.GetLabels(), mch) {
			return strings.Join(path, " -> "), true
		}
		current = next
	}
	return "", false
}

func controllerOwner(owners []metav1.OwnerReference) (metav1.OwnerReference, bool) {
	if len(owners) == 0 {
		return metav1.OwnerReference{}, false
	}
	for _, owner := range owners {
		if owner.Controller != nil && *owner.Controller {
			return owner, true
		}
	}
	return owners[0], true
}

// mchIdentityEvidence recognises the MultiClusterHub itself. Identity is
// stronger than any label: the hub is the owner, not a tenant.
func mchIdentityEvidence(obj metav1.PartialObjectMetadata, mch types.NamespacedName, observedAt metav1.Time) (Evidence, bool) {
	if mch.Name == "" || mch.Namespace == "" || obj.Kind != "MultiClusterHub" {
		return Evidence{}, false
	}
	if obj.GetNamespace() != mch.Namespace || obj.GetName() != mch.Name {
		return Evidence{}, false
	}
	return Evidence{
		Kind:     EvidenceMCHIdentity,
		Detail:   fmt.Sprintf("%s/%s is the MultiClusterHub", mch.Namespace, mch.Name),
		Source:   "metadata.name",
		Observed: observedAt,
	}, true
}

// acmGroupCRDEvidence uses the defining CRD's declared ACM ownership. The
// index only receives CRDs the scan classified as ACM-owned, so a hit here
// means the object's kind is part of the ACM/MCE product surface.
func acmGroupCRDEvidence(obj metav1.PartialObjectMetadata, idx *inventory.Index, observedAt metav1.Time) (Evidence, bool) {
	group := apiGroup(obj.APIVersion)
	crd, ok := idx.DefiningCRD(group, obj.Kind)
	if !ok || !crd.ACMOwned {
		return Evidence{}, false
	}
	return Evidence{
		Kind:     EvidenceACMGroupCRD,
		Detail:   fmt.Sprintf("%s/%s is defined by ACM API group %s", group, obj.Kind, crd.Group),
		Source:   "CustomResourceDefinition.spec.group",
		Observed: observedAt,
	}, true
}

// managedByEvidence reads the marker the hub applies to the operands it
// manages. It does not depend on installer.* labels, which 5.0 no longer sets
// on the MCH, the MCE or the ACM namespaces.
func managedByEvidence(obj metav1.PartialObjectMetadata, observedAt metav1.Time) (Evidence, bool) {
	labels := obj.GetLabels()
	if labels["multiclusterhubs.operator.open-cluster-management.io/managed-by"] == "true" {
		return Evidence{
			Kind:     EvidenceManagedByLabel,
			Detail:   "multiclusterhubs.operator.open-cluster-management.io/managed-by=true",
			Source:   "metadata.labels",
			Observed: observedAt,
		}, true
	}
	if value := labels["backplaneconfig.name"]; value != "" {
		return Evidence{
			Kind:     EvidenceManagedByLabel,
			Detail:   fmt.Sprintf("backplaneconfig.name=%s", value),
			Source:   "metadata.labels",
			Observed: observedAt,
		}, true
	}
	return Evidence{}, false
}

// olmOwnerEvidence reads the ClusterServiceVersion OLM recorded as the owner.
func olmOwnerEvidence(obj metav1.PartialObjectMetadata, observedAt metav1.Time) (Evidence, bool) {
	labels := obj.GetLabels()
	if labels["olm.managed"] != "true" {
		return Evidence{}, false
	}
	owner := labels["olm.owner"]
	if !isACMSVName(owner) {
		return Evidence{}, false
	}
	return Evidence{
		Kind: EvidenceOLMOwner,
		Detail: fmt.Sprintf("olm.owner=%s olm.owner.namespace=%s",
			owner, labels["olm.owner.namespace"]),
		Source:   "metadata.labels",
		Observed: observedAt,
	}, true
}

// isACMSVName reports whether an OLM owner is an ACM or MCE ClusterServiceVersion.
func isACMSVName(name string) bool {
	return strings.HasPrefix(name, "advanced-cluster-management.") ||
		strings.HasPrefix(name, "multicluster-engine.")
}

func definingCRDEvidence(obj metav1.PartialObjectMetadata, idx *inventory.Index, mch types.NamespacedName, observedAt metav1.Time) (Evidence, bool) {
	group := apiGroup(obj.APIVersion)
	crd, ok := idx.DefiningCRD(group, obj.Kind)
	if !ok || !hasInstallerLabels(crd.Labels, mch) {
		return Evidence{}, false
	}
	return Evidence{
		Kind:     EvidenceDefiningCRDLabelled,
		Detail:   fmt.Sprintf("CRD %s/%s carries installer labels", crd.Group, crd.Kind),
		Source:   "CustomResourceDefinition.metadata.labels",
		Observed: observedAt,
	}, true
}

func apiServiceEvidence(obj metav1.PartialObjectMetadata, idx *inventory.Index, mch types.NamespacedName, observedAt metav1.Time) (Evidence, bool) {
	group := apiGroup(obj.APIVersion)
	namespace, ok := idx.APIServiceBackingNamespace(group)
	if !ok || namespace == "" {
		return Evidence{}, false
	}
	nsObj, found := idx.ByName("", namespace)
	if !found || !hasInstallerLabels(nsObj.GetLabels(), mch) {
		return Evidence{}, false
	}
	return Evidence{
		Kind:     EvidenceAPIServiceBacking,
		Detail:   fmt.Sprintf("API group %s is served from owned namespace %s", group, namespace),
		Source:   "APIService.spec.service.namespace",
		Observed: observedAt,
	}, true
}

func apiGroup(apiVersion string) string {
	gv, err := schema.ParseGroupVersion(apiVersion)
	if err != nil {
		return ""
	}
	return gv.Group
}

func objectLabel(kind, name string) string {
	if kind == "" {
		return name
	}
	return kind + "/" + name
}

// nameMatchesACM is report-only evidence. Any future rule that grants an
// action on NameMatch alone reintroduces the over-match defect this design
// replaces (for example, deleting a customer namespace named "hive-prod").
func nameMatchesACM(name string) bool {
	n := strings.ToLower(name)
	switch {
	case strings.HasPrefix(n, "open-cluster-management"):
		return true
	case strings.HasPrefix(n, "multiclusterhub"):
		return true
	case n == "klusterlet", strings.HasPrefix(n, "klusterlet-"):
		return true
	case strings.Contains(n, "submariner"):
		return true
	default:
		return false
	}
}
