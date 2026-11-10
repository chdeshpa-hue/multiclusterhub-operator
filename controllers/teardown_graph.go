// Copyright Contributors to the Open Cluster Management project

package controllers

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/go-logr/logr"
	operatorv1 "github.com/stolostron/multiclusterhub-operator/api/v1"
	"github.com/stolostron/multiclusterhub-operator/pkg/inventory"
	"github.com/stolostron/multiclusterhub-operator/pkg/ownership"
	apixv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
)

// acmAPIGroupSuffixes are the API group suffixes that belong to the ACM/MCE ecosystem.
var acmAPIGroupSuffixes = []string{
	"open-cluster-management.io",
	"hive.openshift.io",
	"hiveinternal.openshift.io",
	"agent-install.openshift.io",
	"hypershift.openshift.io",
	"multicluster.openshift.io",
	"metal3.io",
}

// acmOwnedGroupSuffixes are the scanned groups whose instances are actually
// owned by ACM/MCE. metal3.io is scanned because ACM integrates with baremetal
// provisioning, but the objects belong to the platform, so it is excluded from
// ownership evidence.
var acmOwnedGroupSuffixes = []string{
	"open-cluster-management.io",
	"hive.openshift.io",
	"hiveinternal.openshift.io",
	"agent-install.openshift.io",
	"hypershift.openshift.io",
	"multicluster.openshift.io",
}

// DependencyGraph holds the scanned cluster state for teardown planning.
type DependencyGraph struct {
	// All discovered CRD instances that have finalizers or are blocking teardown.
	DiscoveredResources []DiscoveredResource

	// Resources that block MCH webhook validation.
	BlockingResources []operatorv1.BlockingResource

	// Resources with Tier 1 (cloud-protecting) finalizers. This is the P3
	// candidate set; selection is by finalizer evidence, never by kind.
	InfrastructureResources []DiscoveredResource

	// Resources whose Tier-1 finalizers may orphan resources outside the hub.
	// KubeVirt is excluded here but remains in InfrastructureResources.
	CloudRiskResources []DiscoveredResource

	// Resources with Tier 2 (non-cloud) finalizers.
	NonCloudFinalizerResources []DiscoveredResource

	// ManagedClusters that are not the local-cluster.
	NonLocalManagedClusters []DiscoveredResource

	// ManagedClusterAddOns across all non-local clusters.
	Addons []DiscoveredResource

	// MCH instance (if found).
	MCH *DiscoveredResource

	// MCE instance (if found).
	MCE *DiscoveredResource

	// ScanTime is when discovery started. Ownership evidence and blocker
	// timestamps are stamped with it so every observation in one report shares
	// a single, reportable time.
	ScanTime metav1.Time

	// Incomplete records every resource type whose instances could not be
	// listed. A non-empty list invalidates absence claims for those types: the
	// report may say what was seen, never that something does not exist.
	Incomplete []ScanIncomplete

	// CRDRefs is the ownership projection of the CRDs seen by the scan: group,
	// kind and installer labels. The index needs it to answer "is the defining
	// CRD part of this installation?" without a second API read.
	CRDRefs []inventory.CRDRef

	// CRDs is the full CRD set seen by the same scan. Execution phases must use
	// this snapshot instead of re-listing CRDs, so the object that is authorized
	// is the object that was inspected.
	CRDs []apixv1.CustomResourceDefinition

	// DiscoveryPath records which discovery API the inventory walk used.
	DiscoveryPath string

	// InventoryComplete is false when any resource type could not be listed.
	InventoryComplete bool

	// ObjectsSeen is the number of objects the walk returned, across all types.
	ObjectsSeen int

	// LocalCluster is the self-managed ManagedCluster when present. It is
	// carried separately because it is deliberately excluded from the detach
	// phase and removed only after MCH is gone; a silent skip is not a
	// disposition.
	LocalCluster *DiscoveredResource

	// LocalClusterDisposition is the explicit, reported treatment of
	// local-cluster. Empty when the hub is not self-managed.
	LocalClusterDisposition string

	// TargetHub is the exact MultiClusterHub reference this graph describes.
	TargetHub types.NamespacedName
}

// ScanIncomplete is one reason the scan did not see everything.
type ScanIncomplete struct {
	Group    string `json:"group"`
	Resource string `json:"resource"`
	Reason   string `json:"reason"`
}

// DiscoveredResource represents a single resource instance found during scanning.
type DiscoveredResource struct {
	Ref               operatorv1.ResourceRef
	Version           string
	UID               types.UID
	Finalizers        []string
	DeletionTimestamp *metav1.Time
	IsDeleting        bool
	Phase             string
	Platform          string
	// Labels and OwnerReferences are the inputs to ownership classification.
	// They are metadata only; no object body is retained.
	Labels          map[string]string
	OwnerReferences []metav1.OwnerReference

	// Ownership is the evidence-based verdict for this object. It is filled by
	// classifyOwnership after the whole scan has completed, because the owner
	// chain needs every object in the index.
	Ownership operatorv1.OwnershipVerdict

	// AvailableStatus and AvailableSince are the ManagedCluster Available
	// condition, read once here because the scan is the only place that holds
	// the object body. The unreachable-spoke rule needs the transition time to
	// distinguish a spoke that is starting from one that is gone.
	AvailableStatus string
	AvailableSince  *metav1.Time

	// Classification, OwningController, ControllerAlive and ControllerEvidence
	// are the classification pass output consumed by pkg/policy. They live on
	// the scan record so the report, status and verification all read the same
	// verdict without re-probing the cluster.
	Classification     operatorv1.Classification
	OwningController   *operatorv1.ControllerRef
	ControllerAlive    *bool
	ControllerEvidence []operatorv1.Evidence
}

// buildDependencyGraph scans the cluster for all ACM/MCE resources and classifies them.
// teardownScanTimeout bounds one discovery pass. Without it a degraded API
// server turns a single scan into a multi-minute reconcile that blocks the
// controller's queue, including the CR delete path.
var teardownScanTimeout = 90 * time.Second

func (r *HubTeardownReconciler) buildDependencyGraph(ctx context.Context, td *operatorv1.HubTeardown, log logr.Logger) (*DependencyGraph, error) {
	ctx, cancel := context.WithTimeout(ctx, teardownScanTimeout)
	defer cancel()

	graph := &DependencyGraph{
		ScanTime:      metav1.Now(),
		DiscoveryPath: inventory.DiscoveryPathLegacy,
	}
	if td != nil {
		graph.TargetHub = types.NamespacedName{
			Namespace: td.Spec.HubRef.Namespace,
			Name:      td.Spec.HubRef.Name,
		}
	}

	crdList := &apixv1.CustomResourceDefinitionList{}
	if err := r.Client.List(ctx, crdList); err != nil {
		return nil, fmt.Errorf("listing CRDs: %w", err)
	}
	graph.CRDs = append([]apixv1.CustomResourceDefinition(nil), crdList.Items...)
	for i := range crdList.Items {
		crd := &crdList.Items[i]
		graph.CRDRefs = append(graph.CRDRefs, inventory.CRDRef{
			Group:    crd.Spec.Group,
			Kind:     crd.Spec.Names.Kind,
			Labels:   crd.Labels,
			ACMOwned: ownsACMGroups(crd.Spec.Group),
		})
	}

	// Layer 1 is the production inventory. The manual CRD-instance scan
	// remains only as a unit-test fallback for a reconciler without a metadata
	// client; a missing walker is reported as the legacy path, never hidden.
	if r.MetadataClient != nil && r.DiscoveryClient != nil {
		if err := r.walkInventory(ctx, log, graph); err != nil {
			log.Error(err, "Layer 1 inventory walk failed; reporting an incomplete inventory")
		}
	} else {
		log.Info("Metadata client not configured; using the legacy full-object scan for this unit-test fallback")
		if err := r.scanCRDInventory(ctx, log, graph, crdList); err != nil {
			return nil, err
		}
	}
	graph.InventoryComplete = len(graph.Incomplete) == 0
	classifyFinalizerResources(graph)

	r.classifyBlockingResources(graph)
	r.classifyManagedClusters(graph)
	r.classifyAddons(graph)
	r.findMCHAndMCE(graph)
	r.classifyOwnership(graph)
	r.classifyBlockerBehaviour(ctx, graph, r.previousBlockerUIDs(ctx, td, log))

	return graph, nil
}

// scanCRDInventory is the unit-test fallback. Production never reaches it
// because MetadataClient is set by main and by both Job entrypoints.
func (r *HubTeardownReconciler) scanCRDInventory(ctx context.Context, log logr.Logger, graph *DependencyGraph, crdList *apixv1.CustomResourceDefinitionList) error {
	for i := range crdList.Items {
		crd := &crdList.Items[i]
		if !isACMAPIGroup(crd.Spec.Group) {
			continue
		}
		if err := r.scanCRDInstances(ctx, log, crd, graph); err != nil {
			log.Error(err, "Failed to scan CRD instances", "crd", crd.Name)
			graph.Incomplete = append(graph.Incomplete, ScanIncomplete{
				Group:    crd.Spec.Group,
				Resource: crd.Spec.Names.Plural,
				Reason:   err.Error(),
			})
		}
	}
	return nil
}

// walkInventory builds the graph from the bounded metadata walk. It lists
// PartialObjectMetadata only; full objects are read individually and only for
// the fields the Blocker model needs and metadata cannot carry (platform and
// the ManagedCluster Available condition).
func (r *HubTeardownReconciler) walkInventory(ctx context.Context, log logr.Logger, graph *DependencyGraph) error {
	walker := &inventory.Walker{
		Discovery: r.DiscoveryClient,
		Lister:    inventory.MetadataLister{Client: r.MetadataClient},
	}
	result, err := walker.Walk(ctx, inventory.WalkOptions{})
	if result == nil {
		return err
	}

	graph.DiscoveryPath = result.DiscoveryPath
	graph.ObjectsSeen = len(result.Objects)
	for _, incomplete := range result.Incomplete {
		graph.Incomplete = append(graph.Incomplete, ScanIncomplete{
			Group:    incomplete.Group,
			Resource: incomplete.Resource,
			Reason:   incomplete.Reason,
		})
	}

	for i := range result.Objects {
		object := &result.Objects[i]
		groupVersion, parseErr := schema.ParseGroupVersion(object.APIVersion)
		if parseErr != nil || !isACMAPIGroup(groupVersion.Group) {
			continue
		}
		// The HubTeardown CR is the controller's own control object. Its
		// finalizer must not appear in the inventory: adding the finalizer
		// when the CR becomes active would otherwise add a blocker and change
		// the report digest, making the acknowledged dry-run digest lapse
		// before the teardown Job can start.
		if isTeardownControlResource(groupVersion.Group, object.Kind) {
			continue
		}
		dr := discoveredResourceFromMetadata(object, groupVersion)
		if needsFullObject(dr.Ref.Kind) {
			if enrichErr := r.enrichDiscoveredResource(ctx, &dr); enrichErr != nil {
				graph.Incomplete = append(graph.Incomplete, ScanIncomplete{
					Group:    dr.Ref.Group,
					Resource: dr.Ref.Kind,
					Reason:   fmt.Sprintf("reading %s/%s: %v", dr.Ref.Namespace, dr.Ref.Name, enrichErr),
				})
			}
		}
		graph.DiscoveredResources = append(graph.DiscoveredResources, dr)
	}
	if err != nil {
		log.Error(err, "Inventory walk returned an error with a partial result")
	}
	return nil
}

func isTeardownControlResource(group, kind string) bool {
	return group == operatorv1.GroupVersion.Group && kind == "HubTeardown"
}

func discoveredResourceFromMetadata(object *metav1.PartialObjectMetadata, groupVersion schema.GroupVersion) DiscoveredResource {
	return DiscoveredResource{
		Ref: operatorv1.ResourceRef{
			Group:     groupVersion.Group,
			Kind:      object.Kind,
			Namespace: object.GetNamespace(),
			Name:      object.GetName(),
		},
		Version:           groupVersion.Version,
		UID:               object.GetUID(),
		Finalizers:        object.GetFinalizers(),
		DeletionTimestamp: object.GetDeletionTimestamp(),
		IsDeleting:        object.GetDeletionTimestamp() != nil,
		Labels:            object.GetLabels(),
		OwnerReferences:   object.GetOwnerReferences(),
	}
}

// needsFullObject names the few kinds whose non-metadata fields feed the
// Blocker model. Everything else stays metadata-only.
func needsFullObject(kind string) bool {
	switch kind {
	case "HostedCluster", "HostedControlPlane", "NodePool", "ClusterDeployment", "ClusterPool", "ManagedCluster":
		return true
	default:
		return false
	}
}

func (r *HubTeardownReconciler) enrichDiscoveredResource(ctx context.Context, dr *DiscoveredResource) error {
	gvk := schema.GroupVersionKind{Group: dr.Ref.Group, Version: dr.Version, Kind: dr.Ref.Kind}
	object := &unstructured.Unstructured{}
	object.SetGroupVersionKind(gvk)
	if err := r.Client.Get(ctx, types.NamespacedName{Namespace: dr.Ref.Namespace, Name: dr.Ref.Name}, object); err != nil {
		return err
	}
	dr.Platform = detectPlatform(dr.Ref.Kind, object)
	if dr.Ref.Kind == "ManagedCluster" {
		dr.AvailableStatus, dr.AvailableSince = availableCondition(object)
	}
	return nil
}

// isACMAPIGroup checks if an API group belongs to the ACM/MCE ecosystem.
func isACMAPIGroup(group string) bool {
	for _, suffix := range acmAPIGroupSuffixes {
		if strings.HasSuffix(group, suffix) || group == suffix {
			return true
		}
	}
	return false
}

// ownsACMGroups reports whether instances of a group are ACM/MCE-owned.
func ownsACMGroups(group string) bool {
	for _, suffix := range acmOwnedGroupSuffixes {
		if strings.HasSuffix(group, suffix) || group == suffix {
			return true
		}
	}
	return false
}

// scanCRDInstances lists all instances of a CRD and adds them to the graph.
func (r *HubTeardownReconciler) scanCRDInstances(ctx context.Context, log logr.Logger, crd *apixv1.CustomResourceDefinition, graph *DependencyGraph) error {
	version := preferredVersion(crd)
	if version == "" {
		return nil
	}

	gvk := schema.GroupVersionKind{
		Group:   crd.Spec.Group,
		Version: version,
		Kind:    crd.Spec.Names.Kind + "List",
	}

	list := &unstructured.UnstructuredList{}
	list.SetGroupVersionKind(gvk)

	if err := r.Client.List(ctx, list); err != nil {
		return fmt.Errorf("listing %s: %w", gvk.String(), err)
	}

	for i := range list.Items {
		item := &list.Items[i]
		dr := newDiscoveredResource(item, crd, version)
		graph.DiscoveredResources = append(graph.DiscoveredResources, dr)
	}

	return nil
}

// classifyFinalizerResources derives every finalizer-based planning view from
// the same inventory snapshot. P3 selects infrastructure candidates by Tier-1
// finalizer evidence, not by a product-kind list; a resource name or kind can
// never make an object destructive on its own.
func classifyFinalizerResources(graph *DependencyGraph) {
	graph.InfrastructureResources = nil
	graph.CloudRiskResources = nil
	graph.NonCloudFinalizerResources = nil

	for i := range graph.DiscoveredResources {
		dr := graph.DiscoveredResources[i]
		infrastructure := false
		cloudRisk := false
		nonCloud := false
		for _, finalizer := range dr.Finalizers {
			if IsCloudProtectingFinalizer(finalizer) {
				infrastructure = true
				if requiresCloudAcknowledgement(finalizer, dr.Platform) {
					cloudRisk = true
				}
				continue
			}
			if IsAllowlistedFinalizer(finalizer) {
				nonCloud = true
			}
		}
		if infrastructure {
			graph.InfrastructureResources = append(graph.InfrastructureResources, dr)
		}
		if cloudRisk {
			graph.CloudRiskResources = append(graph.CloudRiskResources, dr)
		}
		if nonCloud {
			graph.NonCloudFinalizerResources = append(graph.NonCloudFinalizerResources, dr)
		}
	}

	sortDiscoveredResources(graph.InfrastructureResources)
	sortDiscoveredResources(graph.CloudRiskResources)
	sortDiscoveredResources(graph.NonCloudFinalizerResources)
}

func sortDiscoveredResources(resources []DiscoveredResource) {
	sort.Slice(resources, func(i, j int) bool {
		a, b := resources[i], resources[j]
		if a.Ref.Group != b.Ref.Group {
			return a.Ref.Group < b.Ref.Group
		}
		if a.Ref.Kind != b.Ref.Kind {
			return a.Ref.Kind < b.Ref.Kind
		}
		if a.Ref.Namespace != b.Ref.Namespace {
			return a.Ref.Namespace < b.Ref.Namespace
		}
		if a.Ref.Name != b.Ref.Name {
			return a.Ref.Name < b.Ref.Name
		}
		return a.UID < b.UID
	})
}

// newDiscoveredResource is the one place a scanned object becomes a graph
// entry. Ownership inputs (labels, owner references) are captured here so the
// later classification pass never has to re-read the object.
func newDiscoveredResource(item *unstructured.Unstructured, crd *apixv1.CustomResourceDefinition, version string) DiscoveredResource {
	dr := DiscoveredResource{
		Ref: operatorv1.ResourceRef{
			Group:     crd.Spec.Group,
			Kind:      crd.Spec.Names.Kind,
			Namespace: item.GetNamespace(),
			Name:      item.GetName(),
		},
		Version:           version,
		UID:               item.GetUID(),
		Finalizers:        item.GetFinalizers(),
		DeletionTimestamp: item.GetDeletionTimestamp(),
		IsDeleting:        item.GetDeletionTimestamp() != nil,
		Labels:            item.GetLabels(),
		OwnerReferences:   item.GetOwnerReferences(),
	}

	// Extract phase/status if available
	if phase, ok, _ := unstructured.NestedString(item.Object, "status", "phase"); ok {
		dr.Phase = phase
	}

	// Extract platform for Hypershift HostedClusters and Hive ClusterDeployments
	dr.Platform = detectPlatform(crd.Spec.Names.Kind, item)
	if crd.Spec.Group == "cluster.open-cluster-management.io" && crd.Spec.Names.Kind == "ManagedCluster" {
		dr.AvailableStatus, dr.AvailableSince = availableCondition(item)
	}
	return dr
}

// availableCondition extracts the ManagedCluster Available condition. A
// missing condition is returned as empty, not Unknown: absence of evidence is
// not evidence that the spoke is unreachable.
func availableCondition(item *unstructured.Unstructured) (string, *metav1.Time) {
	conditions, ok, _ := unstructured.NestedSlice(item.Object, "status", "conditions")
	if !ok {
		return "", nil
	}
	for _, raw := range conditions {
		condition, ok := raw.(map[string]interface{})
		if !ok || condition["type"] != "Available" {
			continue
		}
		status, _ := condition["status"].(string)
		var since *metav1.Time
		if value, ok := condition["lastTransitionTime"].(string); ok {
			if parsed, err := time.Parse(time.RFC3339, value); err == nil {
				observed := metav1.NewTime(parsed)
				since = &observed
			}
		}
		return status, since
	}
	return "", nil
}

// classifyOwnership answers "does this installation own this object?" from
// evidence. It runs after the scan because the owner chain needs every object
// in the index, and after findMCHAndMCE because installer labels are matched
// against the MCH identity.
func (r *HubTeardownReconciler) classifyOwnership(graph *DependencyGraph) {
	objects := make([]metav1.PartialObjectMetadata, 0, len(graph.DiscoveredResources))
	for i := range graph.DiscoveredResources {
		objects = append(objects, partialMetadata(&graph.DiscoveredResources[i]))
	}
	idx := inventory.NewIndex(objects, graph.CRDRefs, nil)

	mch := types.NamespacedName{}
	if graph.MCH != nil {
		mch = types.NamespacedName{
			Name:      graph.MCH.Ref.Name,
			Namespace: graph.MCH.Ref.Namespace,
		}
	}

	for i := range graph.DiscoveredResources {
		dr := &graph.DiscoveredResources[i]
		dr.Ownership = ownership.Classify(partialMetadata(dr), idx, mch, graph.ScanTime)
	}
}

// partialMetadata is the ownership view of a discovered object: identity,
// labels and owner references, nothing else.
func partialMetadata(dr *DiscoveredResource) metav1.PartialObjectMetadata {
	return metav1.PartialObjectMetadata{
		TypeMeta: metav1.TypeMeta{
			APIVersion: apiVersionFor(dr.Ref, dr.Version),
			Kind:       dr.Ref.Kind,
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:            dr.Ref.Name,
			Namespace:       dr.Ref.Namespace,
			UID:             dr.UID,
			Labels:          dr.Labels,
			OwnerReferences: dr.OwnerReferences,
		},
	}
}

// preferredVersion returns the served+storage version for a CRD, or the first served version.
func preferredVersion(crd *apixv1.CustomResourceDefinition) string {
	for _, v := range crd.Spec.Versions {
		if v.Served && v.Storage {
			return v.Name
		}
	}
	for _, v := range crd.Spec.Versions {
		if v.Served {
			return v.Name
		}
	}
	return ""
}

// detectPlatform extracts the cloud platform from a HostedCluster or ClusterDeployment.
func detectPlatform(kind string, item *unstructured.Unstructured) string {
	switch kind {
	case "HostedCluster":
		if p, ok, _ := unstructured.NestedString(item.Object, "spec", "platform", "type"); ok {
			return strings.ToUpper(p)
		}
	case "ClusterDeployment", "ClusterPool":
		spec, ok, _ := unstructured.NestedMap(item.Object, "spec", "platform")
		if ok {
			for platform := range spec {
				return strings.ToUpper(platform)
			}
		}
	case "HostedControlPlane", "NodePool":
		if p, ok, _ := unstructured.NestedString(item.Object, "spec", "platform", "type"); ok {
			return strings.ToUpper(p)
		}
	}
	return ""
}

// classifyBlockingResources identifies resources that block MCH webhook ValidateDelete.
func (r *HubTeardownReconciler) classifyBlockingResources(graph *DependencyGraph) {
	blockingKinds := map[string]string{
		"MultiClusterObservability": "observability.open-cluster-management.io",
		"DiscoveryConfig":           "discovery.open-cluster-management.io",
		"AgentServiceConfig":        "agent-install.openshift.io",
	}

	for _, dr := range graph.DiscoveredResources {
		expectedGroup, isBlocking := blockingKinds[dr.Ref.Kind]
		if isBlocking && dr.Ref.Group == expectedGroup {
			graph.BlockingResources = append(graph.BlockingResources, operatorv1.BlockingResource{
				Group:     dr.Ref.Group,
				Kind:      dr.Ref.Kind,
				Name:      dr.Ref.Name,
				Namespace: dr.Ref.Namespace,
				Reason:    fmt.Sprintf("%s is in MCH webhook blockDeletionResources list", dr.Ref.Kind),
			})
		}
	}
}

// classifyManagedClusters finds non-local ManagedClusters that block MCH deletion.
func (r *HubTeardownReconciler) classifyManagedClusters(graph *DependencyGraph) {
	for i := range graph.DiscoveredResources {
		dr := &graph.DiscoveredResources[i]
		if dr.Ref.Kind == "ManagedCluster" && dr.Ref.Group == "cluster.open-cluster-management.io" {
			if dr.Ref.Name == "local-cluster" {
				// local-cluster cannot be detached before MCH is deleted: the
				// MCH webhook requires it. It is not skipped silently; the
				// treatment is carried in the report and status.
				graph.LocalCluster = dr
				graph.LocalClusterDisposition = "Retained until MultiClusterHub deletion, then detached as the final cleanup step"
				continue
			}
			graph.NonLocalManagedClusters = append(graph.NonLocalManagedClusters, *dr)
			graph.BlockingResources = append(graph.BlockingResources, operatorv1.BlockingResource{
				Group:  dr.Ref.Group,
				Kind:   dr.Ref.Kind,
				Name:   dr.Ref.Name,
				Reason: "Non-local ManagedCluster blocks MCH webhook (only local-cluster is exempt)",
			})
		}
	}

	// Discovery order is not guaranteed. An order-only difference would make
	// the status look changed on every resync and rewrite the CR for nothing.
	sortBlockingResources(graph)
}

// sortBlockingResources puts the webhook-blocking list in a stable order.
func sortBlockingResources(graph *DependencyGraph) {
	sort.Slice(graph.BlockingResources, func(i, j int) bool {
		a, b := graph.BlockingResources[i], graph.BlockingResources[j]
		if a.Group != b.Group {
			return a.Group < b.Group
		}
		if a.Kind != b.Kind {
			return a.Kind < b.Kind
		}
		if a.Namespace != b.Namespace {
			return a.Namespace < b.Namespace
		}
		return a.Name < b.Name
	})
}

// classifyAddons collects ManagedClusterAddOns for non-local clusters.
func (r *HubTeardownReconciler) classifyAddons(graph *DependencyGraph) {
	for _, dr := range graph.DiscoveredResources {
		if dr.Ref.Kind == "ManagedClusterAddOn" && dr.Ref.Group == "addon.open-cluster-management.io" {
			if dr.Ref.Namespace == "local-cluster" {
				continue
			}
			graph.Addons = append(graph.Addons, dr)
		}
	}
}

// findMCHAndMCE locates the target MultiClusterHub and its MultiClusterEngine.
// The MCH is matched by spec.hubRef namespace/name, never by being the only
// MCH the walk happened to see.
func (r *HubTeardownReconciler) findMCHAndMCE(graph *DependencyGraph) {
	// The caller's teardown reference is attached to the graph by
	// buildDependencyGraph through targetHubRef.
	for i, dr := range graph.DiscoveredResources {
		if dr.Ref.Kind == "MultiClusterHub" && dr.Ref.Group == "operator.open-cluster-management.io" &&
			dr.Ref.Namespace == graph.TargetHub.Namespace && dr.Ref.Name == graph.TargetHub.Name {
			graph.MCH = &graph.DiscoveredResources[i]
		}
		if dr.Ref.Kind == "MultiClusterEngine" && dr.Ref.Group == "multicluster.openshift.io" &&
			dr.Labels["installer.name"] == graph.TargetHub.Name &&
			dr.Labels["installer.namespace"] == graph.TargetHub.Namespace {
			graph.MCE = &graph.DiscoveredResources[i]
		}
	}
}

// buildDryRunReport generates a structured preview of the teardown plan from the dependency graph.
func (r *HubTeardownReconciler) buildDryRunReport(graph *DependencyGraph) *operatorv1.DryRunReport {
	report := &operatorv1.DryRunReport{
		ScanTime:                metav1.Now(),
		TotalBlockingResources:  len(graph.BlockingResources),
		TotalCloudRiskResources: len(graph.CloudRiskResources),
	}

	var phases []operatorv1.PlannedPhase

	// Phase: RemoveBlockingCRs
	blockingDetails := make([]string, 0)
	blockingCount := 0
	for _, br := range graph.BlockingResources {
		if br.Kind != "ManagedCluster" {
			detail := fmt.Sprintf("%s/%s", br.Kind, br.Name)
			if br.Namespace != "" {
				detail = fmt.Sprintf("%s/%s/%s", br.Kind, br.Namespace, br.Name)
			}
			finalizerInfo := r.finalizerInfoForResource(graph, br.Kind, br.Group, br.Name, br.Namespace)
			detail += finalizerInfo
			blockingDetails = append(blockingDetails, detail)
			blockingCount++
		}
	}
	phases = append(phases, operatorv1.PlannedPhase{
		Phase:             operatorv1.TeardownPhaseRemoveBlockingCRs,
		Action:            "Delete webhook-blocking CRs: MultiClusterObservability, DiscoveryConfig, AgentServiceConfig",
		ResourcesAffected: blockingCount,
		Details:           blockingDetails,
	})

	// Phase: DisableAddons
	addonDetails := make([]string, 0)
	for _, addon := range graph.Addons {
		detail := fmt.Sprintf("ManagedClusterAddOn/%s/%s", addon.Ref.Namespace, addon.Ref.Name)
		if len(addon.Finalizers) > 0 {
			detail += fmt.Sprintf(" (finalizers: %s -- Tier 2)", strings.Join(addon.Finalizers, ", "))
		}
		addonDetails = append(addonDetails, detail)
	}
	phases = append(phases, operatorv1.PlannedPhase{
		Phase:             operatorv1.TeardownPhaseDisableAddons,
		Action:            "Delete ManagedClusterAddOns for non-local ManagedClusters",
		ResourcesAffected: len(graph.Addons),
		Details:           addonDetails,
	})

	// Phase: DeleteInfrastructureCRs
	infraDetails := make([]string, 0)
	infraCount := 0
	infraCloudRiskCount := 0
	for _, cr := range graph.InfrastructureResources {
		detail := fmt.Sprintf("%s/%s/%s", cr.Ref.Kind, cr.Ref.Namespace, cr.Ref.Name)
		if cr.Platform != "" {
			detail += fmt.Sprintf(" (platform: %s)", cr.Platform)
		}
		infraDetails = append(infraDetails, detail)
		infraCount++
		for _, finalizer := range cr.Finalizers {
			if requiresCloudAcknowledgement(finalizer, cr.Platform) {
				infraCloudRiskCount++
				break
			}
		}
	}
	phases = append(phases, operatorv1.PlannedPhase{
		Phase:              operatorv1.TeardownPhaseDeleteInfrastructureCRs,
		Action:             "Delete discovered resources carrying Tier-1 cloud-protecting finalizers while their controllers are alive",
		ResourcesAffected:  infraCount,
		CloudRiskResources: infraCloudRiskCount,
		Details:            infraDetails,
	})

	// Phase: DetachManagedClusters
	mcDetails := make([]string, 0)
	for _, mc := range graph.NonLocalManagedClusters {
		detail := fmt.Sprintf("ManagedCluster/%s", mc.Ref.Name)
		if len(mc.Finalizers) > 0 {
			detail += fmt.Sprintf(" (%d finalizers -- Tier 2)", len(mc.Finalizers))
		}
		mcDetails = append(mcDetails, detail)
	}
	phases = append(phases, operatorv1.PlannedPhase{
		Phase:             operatorv1.TeardownPhaseDetachManagedClusters,
		Action:            "Delete ManagedClusters other than local-cluster",
		ResourcesAffected: len(graph.NonLocalManagedClusters),
		Details:           mcDetails,
	})

	// Phase: DeleteMCH
	mchDetail := "MultiClusterHub not found"
	if graph.MCH != nil {
		mchDetail = fmt.Sprintf("MultiClusterHub/%s/%s", graph.MCH.Ref.Namespace, graph.MCH.Ref.Name)
	}
	phases = append(phases, operatorv1.PlannedPhase{
		Phase:             operatorv1.TeardownPhaseDeleteMCH,
		Action:            "Delete MultiClusterHub CR (webhook should pass after prior phases)",
		ResourcesAffected: 1,
		Details:           []string{mchDetail},
	})

	// Phase: MonitorMCEChain
	mceDetails := []string{}
	if graph.MCE != nil {
		mceDetails = append(mceDetails, fmt.Sprintf("MultiClusterEngine/%s (finalizers: %s)",
			graph.MCE.Ref.Name, strings.Join(graph.MCE.Finalizers, ", ")))
	}
	phases = append(phases, operatorv1.PlannedPhase{
		Phase:             operatorv1.TeardownPhaseMonitorMCEChain,
		Action:            "Track MCE -> ClusterManager -> Hypershift operator removal chain",
		ResourcesAffected: len(mceDetails),
		Details:           mceDetails,
	})

	// Phase: DeleteACMCRDs
	crdCount := 0
	for _, dr := range graph.DiscoveredResources {
		if dr.Ref.Kind != "HubTeardown" {
			crdCount++
		}
	}
	phases = append(phases, operatorv1.PlannedPhase{
		Phase:             operatorv1.TeardownPhaseDeleteACMCRDs,
		Action:            "Remove all ACM/MCE CRDs (HubTeardown CRD is preserved)",
		ResourcesAffected: crdCount,
	})

	// Phase: Verify
	orphanDetails := make([]string, 0)
	cloudRiskCount := 0
	for _, cr := range graph.CloudRiskResources {
		detail := fmt.Sprintf("%s/%s/%s POTENTIAL ORPHAN", cr.Ref.Kind, cr.Ref.Namespace, cr.Ref.Name)
		hasCloudFinalizer := false
		for _, f := range cr.Finalizers {
			if IsCloudProtectingFinalizer(f) {
				detail += fmt.Sprintf(" -- finalizer %s (Tier 1 CLOUD RISK", f)
				if cr.Platform != "" {
					detail += fmt.Sprintf(": %s", cr.Platform)
				}
				detail += ")"
				hasCloudFinalizer = true
			}
		}
		if hasCloudFinalizer {
			cloudRiskCount++
		}
		orphanDetails = append(orphanDetails, detail)
	}
	phases = append(phases, operatorv1.PlannedPhase{
		Phase:              operatorv1.TeardownPhaseVerifying,
		Action:             "Verify the postcondition and report residue; the operator, its namespace, the CR and the report are expected survivors",
		ResourcesAffected:  len(graph.CloudRiskResources) + len(graph.NonCloudFinalizerResources),
		CloudRiskResources: cloudRiskCount,
		Details:            orphanDetails,
	})

	// Phase: Complete
	phases = append(phases, operatorv1.PlannedPhase{
		Phase:             operatorv1.TeardownPhaseComplete,
		Action:            "Verify writes the terminal phase (Complete or CompleteWithResidue) and the residue report",
		ResourcesAffected: 1,
	})

	report.PlannedPhases = phases

	totalResources := 0
	for _, p := range phases {
		totalResources += p.ResourcesAffected
	}
	report.Summary = fmt.Sprintf(
		"Found %d blocking resources across the RHACM ecosystem. %d resources have cloud-protecting finalizers (Tier 1). %d resources have non-cloud finalizers (Tier 2). Estimated teardown phases: %d.",
		report.TotalBlockingResources, report.TotalCloudRiskResources,
		len(graph.NonCloudFinalizerResources), len(phases))

	return report
}

// finalizerInfoForResource returns a formatted string of finalizer info for a resource in the graph.
func (r *HubTeardownReconciler) finalizerInfoForResource(graph *DependencyGraph, kind, group, name, namespace string) string {
	for _, dr := range graph.DiscoveredResources {
		if dr.Ref.Kind == kind && dr.Ref.Group == group && dr.Ref.Name == name && dr.Ref.Namespace == namespace {
			if len(dr.Finalizers) > 0 {
				tier := "Tier 2"
				for _, f := range dr.Finalizers {
					if IsCloudProtectingFinalizer(f) {
						tier = "Tier 1 CLOUD RISK"
						break
					}
				}
				return fmt.Sprintf(" (finalizers: %s -- %s)", strings.Join(dr.Finalizers, ", "), tier)
			}
			return " (no finalizers, clean delete)"
		}
	}
	return ""
}
