// Copyright Contributors to the Open Cluster Management project

package controllers

import (
	"context"
	"fmt"
	"strings"

	"github.com/go-logr/logr"
	operatorv1 "github.com/stolostron/multiclusterhub-operator/api/v1"
	"github.com/stolostron/multiclusterhub-operator/pkg/inventory"
	"github.com/stolostron/multiclusterhub-operator/pkg/ownership"
	"github.com/stolostron/multiclusterhub-operator/pkg/policy"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
)

// destructiveAction is the kind of mutation being authorized. One function
// answers all of them so CR deletion, CRD deletion, MCH/MCE cleanup, webhook
// cleanup and finalizer patching cannot drift into separate truth tables.
type destructiveAction string

const (
	actionDeleteObject    destructiveAction = "delete-object"
	actionRemoveFinalizer destructiveAction = "remove-finalizer"
	actionDeleteCRD       destructiveAction = "delete-crd"
)

// authzRequest is everything the authorizer is allowed to consider. It carries
// metadata, never a name-prefix or kind guess as ownership evidence.
type authzRequest struct {
	Action         destructiveAction
	Object         metav1.PartialObjectMetadata
	Finalizers     []string
	Platform       string
	Classification operatorv1.Classification

	// CRD-only inputs.
	ACMOwnedGroup bool

	// Registration-only input: the namespace of the Service backing a webhook
	// or APIService. It is a structural join, not a name match.
	ServiceNamespace string
}

// authzDecision is the authorizer's answer plus the evidence that produced it.
// A caller may log the evidence, but must not re-derive the answer.
type authzDecision struct {
	Allowed         bool
	Reason          string
	Verdict         operatorv1.OwnershipVerdict
	ControllerAlive *bool
	CloudRisk       *operatorv1.CloudRisk
	Policy          policy.Decision
}

// authzContext is one inventory snapshot shared by every action in a phase.
// Building it per phase, rather than per object, keeps the walk bounded and
// ensures every decision in that phase is made against the same observation.
type authzContext struct {
	td        *operatorv1.HubTeardown
	graph     *DependencyGraph
	index     *inventory.Index
	consent   policy.ConsentState
	scanTime  metav1.Time
	workloads map[string]*bool
}

func (r *HubTeardownReconciler) newAuthzContext(ctx context.Context, log logr.Logger, td *operatorv1.HubTeardown) (*authzContext, error) {
	graph, err := r.buildDependencyGraph(ctx, td, log)
	if err != nil {
		return nil, err
	}
	return r.authzFromGraph(td, graph), nil
}

func (r *HubTeardownReconciler) authzFromGraph(td *operatorv1.HubTeardown, graph *DependencyGraph) *authzContext {
	objects := make([]metav1.PartialObjectMetadata, 0, len(graph.DiscoveredResources))
	for i := range graph.DiscoveredResources {
		objects = append(objects, partialMetadata(&graph.DiscoveredResources[i]))
	}
	return &authzContext{
		td:        td,
		graph:     graph,
		index:     inventory.NewIndex(objects, graph.CRDRefs, nil),
		consent:   reportConsentState(td),
		scanTime:  graph.ScanTime,
		workloads: map[string]*bool{},
	}
}

// authorizationFor returns the current reconcile's authorization snapshot,
// building it once on first use. The cache is cleared by Reconcile and by each
// Job loop iteration.
func (r *HubTeardownReconciler) authorizationFor(ctx context.Context, log logr.Logger, td *operatorv1.HubTeardown) (*authzContext, error) {
	if r.authz != nil {
		return r.authz, nil
	}
	authz, err := r.newAuthzContext(ctx, log, td)
	if err != nil {
		return nil, err
	}
	r.authz = authz
	return authz, nil
}

// authorize answers one destructive question from evidence. Ownership is the
// first gate for every action; finalizer removal additionally requires a dead
// controller; cloud risk additionally requires digest-bound consent.
func (r *HubTeardownReconciler) authorize(ctx context.Context, authz *authzContext, req authzRequest) (authzDecision, error) {
	observed := authz.scanTime
	if observed.IsZero() {
		observed = metav1.Now()
	}

	decision := authzDecision{}
	if req.Action == actionDeleteCRD {
		decision.Verdict = ownership.ClassifyCRD(req.Object, req.ACMOwnedGroup, observed)
	} else {
		mch := types.NamespacedName{Namespace: authz.td.Spec.HubRef.Namespace, Name: authz.td.Spec.HubRef.Name}
		decision.Verdict = ownership.Classify(req.Object, authz.index, mch, observed)
		if !ownership.AllowsDestruction(decision.Verdict) && authz.namespaceOwned(req.ServiceNamespace) {
			// An APIService or webhook whose backing Service lives in an owned
			// namespace is Strong evidence, the same rule the classifier uses
			// for APIServiceBacking.
			decision.Verdict.Owned = true
			decision.Verdict.Confidence = operatorv1.ConfidenceStrong
			decision.Verdict.Evidence = append(decision.Verdict.Evidence, operatorv1.Evidence{
				Kind:     operatorv1.EvidenceAPIServiceBacking,
				Detail:   fmt.Sprintf("backing service namespace %q is owned by this installation", req.ServiceNamespace),
				Source:   "spec.service.namespace",
				Observed: observed,
			})
		}
	}

	if !ownership.AllowsDestruction(decision.Verdict) {
		confidence := decision.Verdict.Confidence
		if confidence == "" {
			confidence = operatorv1.ConfidenceNone
		}
		decision.Reason = fmt.Sprintf("ownership confidence is %s; report only", confidence)
		return decision, nil
	}

	switch req.Action {
	case actionDeleteCRD:
		decision.Allowed = true
		decision.Reason = "CRD ownership is evidence-backed"

	case actionRemoveFinalizer:
		dr := &DiscoveredResource{
			Ref: operatorv1.ResourceRef{
				Group:     groupOf(req.Object.APIVersion),
				Kind:      req.Object.Kind,
				Namespace: req.Object.Namespace,
				Name:      req.Object.Name,
			},
			Version:           versionOf(req.Object.APIVersion),
			UID:               req.Object.UID,
			Finalizers:        req.Finalizers,
			Labels:            req.Object.Labels,
			OwnerReferences:   req.Object.OwnerReferences,
			Platform:          req.Platform,
			Classification:    req.Classification,
			Ownership:         decision.Verdict,
			DeletionTimestamp: req.Object.DeletionTimestamp,
		}
		alive, controller, controllerEvidence := r.resolveFinalizerController(ctx, observed, dr, authz.workloads)
		decision.ControllerAlive = alive
		decision.CloudRisk = cloudRiskForFinalizers(req.Finalizers, req.Platform)

		blocker := operatorv1.Blocker{
			APIVersion:         req.Object.APIVersion,
			Kind:               req.Object.Kind,
			Name:               req.Object.Name,
			Namespace:          req.Object.Namespace,
			UID:                req.Object.UID,
			Finalizers:         req.Finalizers,
			Ownership:          decision.Verdict,
			ControllerAlive:    alive,
			OwningController:   controller,
			ControllerEvidence: controllerEvidence,
			CloudRisk:          decision.CloudRisk,
			Classification:     req.Classification,
		}
		decision.Policy = policy.Evaluate(blocker, authz.consent)
		switch decision.Policy.Action {
		case operatorv1.ActionResolveFinalizer, operatorv1.ActionSweepDanglingRegistration:
			decision.Allowed = true
			decision.Reason = decision.Policy.Reason
		case operatorv1.ActionResolveFinalizerWithConsent:
			decision.Reason = decision.Policy.ConsentReason
		default:
			decision.Reason = decision.Policy.Reason
		}

	case actionDeleteObject:
		decision.CloudRisk = cloudRiskForFinalizers(req.Finalizers, req.Platform)
		if decision.CloudRisk != nil && decision.CloudRisk.MayOrphan && !authz.consent.CloudRiskValid() {
			decision.Reason = "acting may orphan cloud resources and no valid digest-bound consent exists"
			return decision, nil
		}
		decision.Allowed = true
		decision.Reason = "object ownership is evidence-backed"
	}

	return decision, nil
}

// namespaceOwned answers whether a Service namespace is part of this
// installation. The operator namespace and the declared ACM namespace set are
// direct evidence; any object in the namespace with an owned verdict is the
// structural fallback.
func (a *authzContext) namespaceOwned(namespace string) bool {
	if namespace == "" {
		return false
	}
	if namespace == a.td.Namespace {
		return true
	}
	for _, known := range verifyACMNamespaces {
		if namespace == known {
			return true
		}
	}
	for i := range a.graph.DiscoveredResources {
		dr := &a.graph.DiscoveredResources[i]
		if dr.Ref.Namespace == namespace && dr.Ownership.Owned {
			return true
		}
	}
	return false
}

// cloudRiskForFinalizers is the only consumer of the Tier-1 table in an
// authorization path. The tier says what may happen outside the cluster; it no
// longer says whether an action is allowed. An unrecognised platform is
// fail-closed: MayOrphan is true, so consent is required.
func cloudRiskForFinalizers(finalizers []string, platform string) *operatorv1.CloudRisk {
	for _, finalizer := range finalizers {
		if !IsCloudProtectingFinalizer(finalizer) {
			continue
		}
		normalised := strings.ToUpper(strings.TrimSpace(platform))
		if normalised == "" {
			normalised = "Unknown"
		}
		mayOrphan := true
		if normalised == "KUBEVIRT" {
			mayOrphan = false
		}
		return &operatorv1.CloudRisk{
			Platform:      normalised,
			PlatformKnown: isKnownPlatform(normalised),
			ResourceType:  cloudResourceType(finalizer),
			MayOrphan:     mayOrphan,
		}
	}
	return nil
}

func cloudResourceType(finalizer string) string {
	switch {
	case strings.HasPrefix(finalizer, "hypershift"):
		return "hosted-control-plane"
	case strings.HasPrefix(finalizer, "hive"):
		return "cluster-infrastructure"
	case strings.Contains(finalizer, "agent-install"):
		return "bare-metal-infrastructure"
	default:
		return "cloud"
	}
}

func groupOf(apiVersion string) string {
	gv, err := schema.ParseGroupVersion(apiVersion)
	if err != nil {
		return ""
	}
	return gv.Group
}

func versionOf(apiVersion string) string {
	gv, err := schema.ParseGroupVersion(apiVersion)
	if err != nil {
		return ""
	}
	return gv.Version
}

// partialFromUnstructured is the metadata-only view used for authorization.
// It deep-copies ObjectMeta so a caller's patch cannot change the evidence the
// authorizer saw.
func partialFromUnstructured(object *unstructured.Unstructured) metav1.PartialObjectMetadata {
	return metav1.PartialObjectMetadata{
		TypeMeta: metav1.TypeMeta{
			APIVersion: object.GetAPIVersion(),
			Kind:       object.GetKind(),
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:              object.GetName(),
			Namespace:         object.GetNamespace(),
			UID:               object.GetUID(),
			ResourceVersion:   object.GetResourceVersion(),
			Labels:            object.GetLabels(),
			Annotations:       object.GetAnnotations(),
			OwnerReferences:   object.GetOwnerReferences(),
			Finalizers:        object.GetFinalizers(),
			DeletionTimestamp: object.GetDeletionTimestamp(),
		},
	}
}
