// Copyright Contributors to the Open Cluster Management project

package v1

import (
	"context"
	"fmt"
	"strings"
	"time"

	admissionregistration "k8s.io/api/admissionregistration/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

const (
	// hubTeardownWebhookPath is registered on the manager's webhook server.
	hubTeardownWebhookPath = "/validate-operator-open-cluster-management-io-v1-hubteardown"

	// hubTeardownWebhookName is the name of the webhook entry.
	hubTeardownWebhookName = "hubteardown.validating-webhook.open-cluster-management.io"

	// hubTeardownWebhookConfigName is the ValidatingWebhookConfiguration that
	// carries it. It is deliberately separate from the MultiClusterHub
	// configuration: that object is owned by the MCH CRD and is garbage
	// collected when the CRD is deleted during teardown, which would silently
	// disable this validator exactly when a stalled teardown is being retried.
	hubTeardownWebhookConfigName = "hubteardown-operator-validating-webhook"

	// HubTeardownCRDName owns the configuration above.
	HubTeardownCRDName = "hubteardowns.operator.open-cluster-management.io"

	// maxAllowedTeardownDuration bounds spec.maxDuration. The controller
	// default is 2h; an uninstall that has not finished in a day is a support
	// case, not an automation, and an unbounded value re-creates the
	// "never terminates" hole that the terminal bound exists to close.
	maxAllowedTeardownDuration = 24 * time.Hour
)

var htdlog = log.Log.WithName("hubteardown-resource")

// hubTeardownWebhookClient is an uncached client. The singleton check must not
// read from an informer cache: a cache that has not yet observed the first
// teardown would happily admit a second one.
var hubTeardownWebhookClient client.Client

// SetupWebhookWithManager registers the HubTeardown admission webhook.
func (r *HubTeardown) SetupWebhookWithManager(mgr ctrl.Manager) error {
	uncached, err := client.New(mgr.GetConfig(), client.Options{Scheme: mgr.GetScheme()})
	if err != nil {
		return err
	}
	hubTeardownWebhookClient = uncached

	return builder.WebhookManagedBy(mgr, r).
		WithDefaulter(r).
		WithValidator(r).
		Complete()
}

var _ admission.Defaulter[*HubTeardown] = &HubTeardown{}
var _ admission.Validator[*HubTeardown] = &HubTeardown{}

// Default supplies the non-boolean defaults that make a programmatically
// created CR behave like one admitted through the CRD schema. dryRun is left
// to the CRD because a bool default cannot distinguish omitted from false.
func (r *HubTeardown) Default(ctx context.Context, obj *HubTeardown) error {
	htdlog.Info("default", "name", obj.Name, "namespace", obj.Namespace)
	if obj.Spec.MCE == "" {
		obj.Spec.MCE = MCEDispositionDefault
	}
	if obj.Spec.Escalation == nil {
		obj.Spec.Escalation = &EscalationSpec{}
	}
	if obj.Spec.Escalation.StallTimeout == nil {
		obj.Spec.Escalation.StallTimeout = &metav1.Duration{Duration: 15 * time.Minute}
	}
	if obj.Spec.Escalation.MaxDuration == nil {
		obj.Spec.Escalation.MaxDuration = &metav1.Duration{Duration: 2 * time.Hour}
	}
	if obj.Spec.Escalation.Mode == "" {
		obj.Spec.Escalation.Mode = EscalationModeManual
	}
	return nil
}

//+kubebuilder:webhook:name=hubteardown-operator-validating-webhook,path=/validate-operator-open-cluster-management-io-v1-hubteardown,mutating=false,failurePolicy=fail,sideEffects=None,groups=operator.open-cluster-management.io,resources=hubteardowns,verbs=create;update,versions=v1,name=hubteardown.validating-webhook.open-cluster-management.io,admissionReviewVersions={v1,v1beta1}

// ValidateCreate rejects a second HubTeardown anywhere in the cluster and
// rejects durations that would disable the controller's terminal bound.
func (r *HubTeardown) ValidateCreate(ctx context.Context, obj *HubTeardown) (admission.Warnings, error) {
	htdlog.Info("validate create", "Name", obj.Name, "Namespace", obj.Namespace)

	if err := validateHubTeardownTarget(obj); err != nil {
		return nil, err
	}
	if err := validateHubTeardownDurations(obj); err != nil {
		return nil, err
	}
	if err := ensureSingleHubTeardown(ctx, obj); err != nil {
		return nil, err
	}
	return nil, nil
}

// ValidateUpdate applies the same duration rules and re-checks the singleton,
// excluding the object being updated by UID.
func (r *HubTeardown) ValidateUpdate(ctx context.Context, oldObj, newObj *HubTeardown) (admission.Warnings, error) {
	htdlog.Info("validate update", "Name", newObj.Name, "Namespace", newObj.Namespace)

	if err := validateHubTeardownTarget(newObj); err != nil {
		return nil, err
	}
	if err := validateHubTeardownDurations(newObj); err != nil {
		return nil, err
	}
	if err := ensureSingleHubTeardown(ctx, newObj); err != nil {
		return nil, err
	}
	return nil, nil
}

// validateHubTeardownTarget requires an explicit, well-formed MultiClusterHub
// reference. Inferring the target from the only MCH present was the C2 failure:
// a teardown created in the wrong namespace could act against the hub anyway.
func validateHubTeardownTarget(obj *HubTeardown) error {
	ref := obj.Spec.HubRef
	if strings.TrimSpace(ref.Name) == "" {
		return fmt.Errorf("spec.hubRef.name is required")
	}
	if strings.TrimSpace(ref.Namespace) == "" {
		return fmt.Errorf("spec.hubRef.namespace is required")
	}
	if ref.Kind != "MultiClusterHub" {
		return fmt.Errorf("spec.hubRef.kind must be MultiClusterHub")
	}
	if ref.APIVersion != "" && ref.APIVersion != GroupVersion.String() {
		return fmt.Errorf("spec.hubRef.apiVersion must be %s", GroupVersion.String())
	}
	if errs := validation.IsDNS1123Subdomain(ref.Name); len(errs) > 0 {
		return fmt.Errorf("spec.hubRef.name is invalid: %s", strings.Join(errs, "; "))
	}
	if errs := validation.IsDNS1123Label(ref.Namespace); len(errs) > 0 {
		return fmt.Errorf("spec.hubRef.namespace is invalid: %s", strings.Join(errs, "; "))
	}
	return nil
}

// ValidateDelete permits deletion. Deleting the teardown is the operator's
// escape hatch when a teardown has wedged, so it must never be blocked here.
func (r *HubTeardown) ValidateDelete(ctx context.Context, obj *HubTeardown) (admission.Warnings, error) {
	htdlog.Info("validate delete", "Name", obj.Name, "Namespace", obj.Namespace)
	return nil, nil
}

// ensureSingleHubTeardown enforces one HubTeardown in the cluster. The defect
// it closes is cross-namespace: a CR created in "default" is accepted by the
// namespaced CRD and used to reconcile alongside the intended one.
func ensureSingleHubTeardown(ctx context.Context, obj *HubTeardown) error {
	if hubTeardownWebhookClient == nil {
		// Fail closed: without a client the singleton rule cannot be proven,
		// and admitting a second teardown is the failure this webhook prevents.
		return fmt.Errorf("cannot verify that no other HubTeardown exists: webhook client is not initialised")
	}

	list := &HubTeardownList{}
	if err := hubTeardownWebhookClient.List(ctx, list); err != nil {
		if apimeta.IsNoMatchError(err) {
			// The HubTeardown CRD is not installed, so no other teardown can
			// exist. This is the only error that is safe to treat as empty.
			return nil
		}
		return fmt.Errorf("unable to list HubTeardowns: %w", err)
	}

	for i := range list.Items {
		existing := &list.Items[i]
		if obj.UID != "" && existing.UID == obj.UID {
			continue
		}
		return fmt.Errorf(
			"a HubTeardown already exists in %s/%s; only one HubTeardown may exist in the cluster",
			existing.Namespace, existing.Name)
	}
	return nil
}

// validateHubTeardownDurations rejects values that would silently disable the
// controller's terminal bound or make the operator intervene on every
// finalizer before its controller has had a chance to run.
func validateHubTeardownDurations(obj *HubTeardown) error {
	maxDuration := obj.Spec.MaxDuration
	forceTimeout := obj.Spec.ForceFinalizerTimeout

	if maxDuration != nil {
		if maxDuration.Duration <= 0 {
			return fmt.Errorf("spec.maxDuration must be greater than zero")
		}
		if maxDuration.Duration > maxAllowedTeardownDuration {
			return fmt.Errorf("spec.maxDuration must not exceed %s", maxAllowedTeardownDuration)
		}
	}

	if forceTimeout != nil && forceTimeout.Duration <= 0 {
		return fmt.Errorf("spec.forceFinalizerTimeout must be greater than zero")
	}

	if maxDuration != nil && forceTimeout != nil && forceTimeout.Duration >= maxDuration.Duration {
		return fmt.Errorf(
			"spec.forceFinalizerTimeout (%s) must be shorter than spec.maxDuration (%s)",
			forceTimeout.Duration, maxDuration.Duration)
	}

	if obj.Spec.Escalation != nil {
		escalation := obj.Spec.Escalation
		if escalation.StallTimeout != nil && escalation.StallTimeout.Duration <= 0 {
			return fmt.Errorf("spec.escalation.stallTimeout must be greater than zero")
		}
		if escalation.MaxDuration != nil {
			if escalation.MaxDuration.Duration <= 0 {
				return fmt.Errorf("spec.escalation.maxDuration must be greater than zero")
			}
			if escalation.MaxDuration.Duration > maxAllowedTeardownDuration {
				return fmt.Errorf("spec.escalation.maxDuration must not exceed %s", maxAllowedTeardownDuration)
			}
		}
		if escalation.StallTimeout != nil && escalation.MaxDuration != nil &&
			escalation.StallTimeout.Duration >= escalation.MaxDuration.Duration {
			return fmt.Errorf("spec.escalation.stallTimeout (%s) must be shorter than spec.escalation.maxDuration (%s)",
				escalation.StallTimeout.Duration, escalation.MaxDuration.Duration)
		}
		switch escalation.Mode {
		case "", EscalationModeManual, EscalationModeAutomatic:
		default:
			return fmt.Errorf("spec.escalation.mode must be Manual or Automatic")
		}
	}
	return nil
}

// HubTeardownValidatingWebhook returns the ValidatingWebhookConfiguration for
// the HubTeardown validator, mirroring ValidatingWebhook for the MultiClusterHub.
//
// The rule names the "hubteardowns" resource and not "hubteardowns/status":
// the controller writes status on every reconcile, and an admission call that
// lists every HubTeardown cluster-wide must not run on that path.
func HubTeardownValidatingWebhook(namespace string) *admissionregistration.ValidatingWebhookConfiguration {
	fail := admissionregistration.Fail
	none := admissionregistration.SideEffectClassNone
	path := hubTeardownWebhookPath
	return &admissionregistration.ValidatingWebhookConfiguration{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "admissionregistration.k8s.io/v1",
			Kind:       "ValidatingWebhookConfiguration",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:        hubTeardownWebhookConfigName,
			Annotations: map[string]string{"service.beta.openshift.io/inject-cabundle": "true"},
		},
		Webhooks: []admissionregistration.ValidatingWebhook{
			{
				AdmissionReviewVersions: []string{
					"v1",
					"v1beta1",
				},
				Name: hubTeardownWebhookName,
				ClientConfig: admissionregistration.WebhookClientConfig{
					Service: &admissionregistration.ServiceReference{
						Name:      "multiclusterhub-operator-webhook",
						Namespace: namespace,
						Path:      &path,
					},
				},
				FailurePolicy: &fail,
				Rules: []admissionregistration.RuleWithOperations{
					{
						Rule: admissionregistration.Rule{
							APIGroups:   []string{GroupVersion.Group},
							APIVersions: []string{GroupVersion.Version},
							Resources:   []string{"hubteardowns"},
						},
						Operations: []admissionregistration.OperationType{
							admissionregistration.Create,
							admissionregistration.Update,
						},
					},
				},
				SideEffects: &none,
			},
		},
	}
}
