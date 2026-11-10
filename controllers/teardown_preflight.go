// Copyright Contributors to the Open Cluster Management project

package controllers

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/go-logr/logr"
	operatorv1 "github.com/stolostron/multiclusterhub-operator/api/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	apiregistrationv1 "k8s.io/kube-aggregator/pkg/apis/apiregistration/v1"
)

const (
	// conditionTypePreflightBlocked is the start gate. True means no teardown
	// Job may be created from the current cluster snapshot.
	conditionTypePreflightBlocked = "PreflightBlocked"

	preflightReasonStaleAPIService = "StaleAPIService"
	preflightReasonError           = "PreflightError"
	preflightReasonPassed          = "PreflightPassed"

	// preflightMessageLimit bounds the object enumeration in one condition
	// message so a large residue set cannot crowd out the recovery guidance.
	preflightMessageLimit = 5
)

// staleAPIService is one APIService whose backing Service is absent and whose
// deletion the shared authorizer does not allow. It is evidence for the block,
// never a deletion candidate.
type staleAPIService struct {
	Name             string
	ServiceNamespace string
	ServiceName      string
	Reason           string
}

// teardownJobStarted reports whether a teardown Job already exists. The
// preflight is a start gate, not a watcher: once a Job exists the destructive
// work has begun and the Job-side retain-and-report policy owns the decision.
func (r *HubTeardownReconciler) teardownJobStarted(ctx context.Context, td *operatorv1.HubTeardown) (bool, error) {
	for _, name := range []string{prepareJobName, handoffJobName} {
		found, _, err := r.jobState(ctx, td, name)
		if err != nil {
			return false, err
		}
		if found {
			return true, nil
		}
	}
	return false, nil
}

// preflightStaleAPIServices lists APIServices whose spec.service points at a
// Service that does not exist and whose ownership does not authorize deletion.
// It performs reads only. The ownership question is answered by the same
// r.authorize call the Job sweep uses, so the start gate cannot disagree with
// the sweep's retain decision on the same snapshot.
func (r *HubTeardownReconciler) preflightStaleAPIServices(
	ctx context.Context,
	log logr.Logger,
	td *operatorv1.HubTeardown,
) ([]staleAPIService, error) {
	authz, err := r.authorizationFor(ctx, log, td)
	if err != nil {
		return nil, fmt.Errorf("building the ownership snapshot: %w", err)
	}

	list := &apiregistrationv1.APIServiceList{}
	if err := r.Client.List(ctx, list); err != nil {
		if isNoMatchError(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("listing APIServices: %w", err)
	}

	stale := make([]staleAPIService, 0)
	for i := range list.Items {
		apiService := &list.Items[i]
		service := apiService.Spec.Service
		if service == nil || service.Name == "" || service.Namespace == "" {
			// Local aggregated APIs have no Service, and an incomplete
			// reference cannot prove a backing Service absent. The Job sweep
			// treats both as non-decisive; the gate does the same.
			continue
		}

		backing := &corev1.Service{}
		err := r.Client.Get(ctx, types.NamespacedName{Namespace: service.Namespace, Name: service.Name}, backing)
		switch {
		case err == nil:
			continue
		case !apierrors.IsNotFound(err):
			return nil, fmt.Errorf("reading backing Service %s/%s for APIService %q: %w",
				service.Namespace, service.Name, apiService.Name, err)
		}

		decision, authzErr := r.authorize(ctx, authz, authzRequest{
			Action:           actionDeleteObject,
			Object:           partialMetadataFor(apiService, "apiregistration.k8s.io/v1", "APIService"),
			Classification:   operatorv1.ClassDanglingRegistration,
			ServiceNamespace: service.Namespace,
		})
		if authzErr != nil {
			return nil, fmt.Errorf("authorizing APIService %q: %w", apiService.Name, authzErr)
		}
		if decision.Allowed {
			// Evidence-backed dangling registrations belong to the existing
			// Job sweep. The gate must not duplicate that deletion.
			continue
		}

		stale = append(stale, staleAPIService{
			Name:             apiService.Name,
			ServiceNamespace: service.Namespace,
			ServiceName:      service.Name,
			Reason:           decision.Reason,
		})
	}

	sort.Slice(stale, func(i, j int) bool { return stale[i].Name < stale[j].Name })
	if len(stale) > 0 {
		log.Info("Preflight found stale APIServices that ownership forbids deleting",
			"count", len(stale), "first", stale[0].Name)
	}
	return stale, nil
}

// staleAPIServiceBlockMessage explains the block in terms of the exact objects
// and the recovery action. It never suggests that the operator will delete an
// unowned registration.
func staleAPIServiceBlockMessage(stale []staleAPIService) string {
	const guidance = " The operator will not delete an unowned registration. Restore each backing Service or delete the APIService; teardown resumes on the next reconcile."

	if len(stale) == 1 {
		s := stale[0]
		return fmt.Sprintf("Active teardown is blocked before a teardown Job starts: APIService %q points at absent backing Service %s/%s and ownership does not authorize its deletion (%s).%s",
			s.Name, s.ServiceNamespace, s.ServiceName, s.Reason, guidance)
	}

	var b strings.Builder
	fmt.Fprintf(&b, "Active teardown is blocked before a teardown Job starts: %d APIServices point at absent backing Services and ownership does not authorize their deletion:", len(stale))
	for i, s := range stale {
		if i == preflightMessageLimit {
			fmt.Fprintf(&b, "; and %d more", len(stale)-preflightMessageLimit)
			break
		}
		if i > 0 {
			b.WriteString(";")
		}
		fmt.Fprintf(&b, " %q -> %s/%s (%s)", s.Name, s.ServiceNamespace, s.ServiceName, s.Reason)
	}
	b.WriteString(".")
	b.WriteString(guidance)
	return b.String()
}

// blockOnPreflight records the start-gate decision. It writes status only; no
// object, Job or identity is created or deleted here.
func (r *HubTeardownReconciler) blockOnPreflight(
	ctx context.Context,
	log logr.Logger,
	td *operatorv1.HubTeardown,
	reason, message string,
) {
	previous := meta.FindStatusCondition(td.Status.Conditions, conditionTypePreflightBlocked)
	r.setCondition(td, conditionTypePreflightBlocked, metav1.ConditionTrue, reason, message)
	r.setCondition(td, conditionTypeTeardownProgressing, metav1.ConditionTrue, reason, message)
	if previous == nil || previous.Status != metav1.ConditionTrue || previous.Reason != reason {
		r.Recorder.Eventf(td, corev1.EventTypeWarning, reason, "%s", message)
	}
	if err := r.Client.Status().Update(ctx, td); err != nil {
		log.Error(err, "Failed to persist the preflight block")
	}
}

// clearPreflightBlocked records recovery. It only writes when the condition is
// present and not already in the passed state, so a healthy reconcile does not
// manufacture status churn.
func (r *HubTeardownReconciler) clearPreflightBlocked(
	ctx context.Context,
	log logr.Logger,
	td *operatorv1.HubTeardown,
) {
	previous := meta.FindStatusCondition(td.Status.Conditions, conditionTypePreflightBlocked)
	if previous == nil {
		return
	}
	if previous.Status == metav1.ConditionFalse && previous.Reason == preflightReasonPassed {
		return
	}
	r.setCondition(td, conditionTypePreflightBlocked, metav1.ConditionFalse, preflightReasonPassed,
		"No unowned dangling APIService blocks the teardown Job.")
	if err := r.Client.Status().Update(ctx, td); err != nil {
		log.Error(err, "Failed to persist the preflight pass")
	}
}
