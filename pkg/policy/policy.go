package policy

import (
	"fmt"

	"github.com/stolostron/multiclusterhub-operator/pkg/ownership"
)

// Evaluate applies rules P-1..P-8 to one Blocker. Order matters: ownership
// confidence gates everything, classification that is not a teardown problem
// (external reconciler, recreate loop) is named before liveness, and liveness
// is fail-closed.
func Evaluate(b Blocker, consent ConsentState) Decision {
	if b.Ownership.Confidence == ownership.ConfidenceWeak || b.Ownership.Confidence == ownership.ConfidenceNone {
		return Decision{
			Action: ActionReportOnly,
			Reason: fmt.Sprintf("ownership confidence is %s; report only", b.Ownership.Confidence),
		}
	}

	switch b.Classification {
	case ClassExternalReconciler:
		return Decision{
			Action: ActionStopExternalReconciler,
			Reason: "an external reconciler is recreating ACM objects; stop it before retrying",
		}
	case ClassRecreateLoop:
		reason := "object is recreated by an operand controller"
		if b.RelatedFix != "" {
			reason += "; related fix " + b.RelatedFix
		}
		return Decision{Action: ActionFixOperand, Reason: reason}
	}

	if b.ControllerAlive == nil {
		return Decision{
			Action: ActionWaitForController,
			Reason: "controller liveness could not be determined; treating it as alive (fail-closed)",
		}
	}
	if *b.ControllerAlive {
		return Decision{
			Action: ActionWaitForController,
			Reason: "owning controller is alive and may still process the finalizer",
		}
	}

	if len(b.Finalizers) == 0 {
		return Decision{
			Action: ActionReportOnly,
			Reason: "no finalizers to resolve; deletion is outside policy scope",
		}
	}

	if b.CloudRisk != nil && b.CloudRisk.MayOrphan && !consent.CloudRiskValid() {
		return Decision{
			Action:          ActionResolveFinalizerWithConsent,
			RequiresConsent: true,
			ConsentReason:   consentReason(b),
			Reason:          "acting may orphan resources outside the cluster",
		}
	}

	if b.Classification == ClassDanglingRegistration {
		return Decision{
			Action: ActionSweepDanglingRegistration,
			Reason: "dangling registration with definitive ownership and no live controller",
		}
	}

	return Decision{
		Action: ActionResolveFinalizer,
		Reason: "controller is not alive and acting is safe",
	}
}

func consentReason(b Blocker) string {
	if b.CloudRisk == nil {
		return ""
	}
	platform := b.CloudRisk.Platform
	if platform == "" {
		platform = "unknown"
	}
	resourceType := b.CloudRisk.ResourceType
	if resourceType == "" {
		resourceType = "cloud"
	}
	return fmt.Sprintf(
		"Removing finalizers from %s %s/%s may orphan %s resources on %s. Manual cleanup in the provider console may be required.",
		b.Kind, b.Namespace, b.Name, resourceType, platform,
	)
}
