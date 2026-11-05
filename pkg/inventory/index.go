// Package inventory holds the read-only object index that the teardown
// inspector builds from a hub. It performs no I/O; the walk populates it and
// callers query it by identity, never by name alone.
package inventory

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// Object is the metadata-only view the walk produces for every resource.
// Using PartialObjectMetadata keeps the walk bounded; full objects are read
// only where a field outside metadata is required (see CRDRef).
type Object = metav1.PartialObjectMetadata

// CRDRef is the minimal CRD projection needed to answer "which CRD defines
// this group/kind, and does it carry this MCH's installer labels?".
// CRDs are a bounded set and must be read as full objects: spec.group and
// spec.names.kind are not available from PartialObjectMetadata.
type CRDRef struct {
	Group  string
	Kind   string
	Labels map[string]string
	// ACMOwned records whether this CRD's group is part of the ACM/MCE
	// product surface, as opposed to merely being scanned by the teardown.
	// The two are not the same: metal3.io is scanned because ACM installs
	// baremetal integrations, but its instances are not ACM-owned objects and
	// must never be authorised for deletion on group membership alone.
	ACMOwned bool
}

// Index is an in-memory snapshot of object identity and ownership inputs.
// It is safe for concurrent reads after construction and is never mutated.
type Index struct {
	byUID  map[types.UID]Object
	byName map[string]Object
	crds   map[string]CRDRef
	apiSvc map[string]string
}

// NewIndex builds an index from objects, CRD projections and the map of
// aggregated API group to backing Service namespace.
func NewIndex(objects []Object, crds []CRDRef, apiServiceBacking map[string]string) *Index {
	idx := &Index{
		byUID:  make(map[types.UID]Object, len(objects)),
		byName: make(map[string]Object, len(objects)),
		crds:   make(map[string]CRDRef, len(crds)),
		apiSvc: make(map[string]string, len(apiServiceBacking)),
	}
	for _, obj := range objects {
		idx.byUID[obj.GetUID()] = obj
		idx.byName[objectKey(obj.GetNamespace(), obj.GetName())] = obj
	}
	for _, crd := range crds {
		idx.crds[crdKey(crd.Group, crd.Kind)] = crd
	}
	for group, namespace := range apiServiceBacking {
		idx.apiSvc[group] = namespace
	}
	return idx
}

func objectKey(namespace, name string) string {
	if namespace == "" {
		return name
	}
	return namespace + "/" + name
}

func crdKey(group, kind string) string {
	return group + "/" + kind
}

// Get resolves an object by UID. The bool is false when the walk did not see
// the object, which callers must treat as unresolved rather than unowned.
func (i *Index) Get(uid types.UID) (Object, bool) {
	if i == nil || uid == "" {
		return Object{}, false
	}
	obj, ok := i.byUID[uid]
	return obj, ok
}

// ByName resolves an object by namespace and name. It exists for
// cluster-scoped lookups such as Namespace, which have no ownerReference chain.
func (i *Index) ByName(namespace, name string) (Object, bool) {
	if i == nil || name == "" {
		return Object{}, false
	}
	obj, ok := i.byName[objectKey(namespace, name)]
	return obj, ok
}

// DefiningCRD returns the CRD projection that defines group/kind.
func (i *Index) DefiningCRD(group, kind string) (CRDRef, bool) {
	if i == nil {
		return CRDRef{}, false
	}
	crd, ok := i.crds[crdKey(group, kind)]
	return crd, ok
}

// APIServiceBackingNamespace returns the namespace of the Service backing an
// aggregated API group.
func (i *Index) APIServiceBackingNamespace(group string) (string, bool) {
	if i == nil {
		return "", false
	}
	namespace, ok := i.apiSvc[group]
	return namespace, ok
}

// Len reports how many objects were indexed. It is used by evidence harnesses.
func (i *Index) Len() int {
	if i == nil {
		return 0
	}
	return len(i.byUID)
}
