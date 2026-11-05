// Copyright Contributors to the Open Cluster Management project

package controllers

import (
	"context"
	stderrors "errors"
	"fmt"
	"time"

	"github.com/go-logr/logr"
	operatorv1 "github.com/stolostron/multiclusterhub-operator/api/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	ctrllog "sigs.k8s.io/controller-runtime/pkg/log"
)

const (
	teardownResyncDryRun = 60 * time.Second
	teardownResyncActive = 15 * time.Second

	conditionTypeDryRunComplete      = "DryRunComplete"
	conditionTypeCloudResourcesRisk  = "CloudResourcesAtRisk"
	conditionTypeTeardownProgressing = "Progressing"

	// conditionTypeContractReady reports the cross-repo teardown-active signal
	// (DF-1..DF-5). It is written by the operator from the prepare Job's
	// progress record, never by the Job itself.
	conditionTypeContractReady      = "ContractReady"
	conditionTypeTeardownComplete   = "Complete"
	conditionTypeTeardownStalled    = "Stalled"
	conditionTypeDryRunReportStale  = "DryRunReportStale"
	conditionTypeWaitingForApproval = "WaitingForApproval"
	conditionTypeInspectIncomplete  = "InspectIncomplete"
	conditionTypeReportWriteFailed  = "ReportWriteFailed"

	defaultMaxDuration = 2 * time.Hour

	annotationWarningsEmitted = "operator.open-cluster-management.io/warnings-emitted-generation"
)

// HubTeardownReconciler reconciles a HubTeardown object.
type HubTeardownReconciler struct {
	Client          client.Client
	UncachedClient  client.Client // reserved for strong-consistency reads (bypasses informer cache)
	Scheme          *runtime.Scheme
	Log             logr.Logger
	Recorder        record.EventRecorder
	DiscoveryClient discovery.DiscoveryInterface
}

//+kubebuilder:rbac:groups=operator.open-cluster-management.io,resources=hubteardowns,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=operator.open-cluster-management.io,resources=hubteardowns/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=operator.open-cluster-management.io,resources=hubteardowns/finalizers,verbs=update
//+kubebuilder:rbac:groups="",resources=events,verbs=create;patch
//+kubebuilder:rbac:groups=apiextensions.k8s.io,resources=customresourcedefinitions,verbs=get;list;watch;delete
//+kubebuilder:rbac:groups=operators.coreos.com,resources=subscriptions;subscriptions/finalizers;clusterserviceversions,verbs=get;list;watch;update;patch;delete
//+kubebuilder:rbac:groups=hypershift.openshift.io,resources=hostedclusters;nodepools,verbs=get;list;watch;delete;patch
//+kubebuilder:rbac:groups=hive.openshift.io,resources=clusterdeployments;clusterpools,verbs=get;list;watch;delete;patch
//+kubebuilder:rbac:groups=agent-install.openshift.io,resources=infraenvs,verbs=get;list;watch;delete;patch
//+kubebuilder:rbac:groups=apps,resources=deployments;deployments/scale,verbs=get;list;patch
//+kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=admissionregistration.k8s.io,resources=validatingwebhookconfigurations;mutatingwebhookconfigurations,verbs=get;list;delete
//+kubebuilder:rbac:groups="",resources=endpoints,verbs=get;list
//+kubebuilder:rbac:groups="",resources=namespaces,verbs=get;list;delete
//+kubebuilder:rbac:groups="",resources=services,verbs=get;list
//+kubebuilder:rbac:groups="",resources=pods,verbs=list
//+kubebuilder:rbac:groups=rbac.authorization.k8s.io,resources=clusterroles;clusterrolebindings,verbs=list;delete
//+kubebuilder:rbac:groups=addon.open-cluster-management.io,resources=clustermanagementaddons,verbs=get;list;watch;patch
//+kubebuilder:rbac:groups=cluster.open-cluster-management.io,resources=managedclustersets,verbs=get;list;watch;patch

func (r *HubTeardownReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := ctrllog.FromContext(ctx).WithName("hubteardown")

	teardown := &operatorv1.HubTeardown{}
	if err := r.Client.Get(ctx, req.NamespacedName, teardown); err != nil {
		if errors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	// Singleton guard: only one HubTeardown CR is allowed per namespace.
	// If multiple exist, refuse to reconcile any except the oldest.
	allTeardowns := &operatorv1.HubTeardownList{}
	if err := r.Client.List(ctx, allTeardowns, client.InNamespace(req.Namespace)); err != nil {
		return ctrl.Result{}, err
	}
	if len(allTeardowns.Items) > 1 {
		var oldest *operatorv1.HubTeardown
		for i := range allTeardowns.Items {
			td := &allTeardowns.Items[i]
			if td.GetDeletionTimestamp() != nil {
				continue
			}
			if oldest == nil || td.CreationTimestamp.Before(&oldest.CreationTimestamp) {
				oldest = td
			}
		}
		if oldest != nil && teardown.Name != oldest.Name {
			log.Info("Rejecting duplicate HubTeardown — only the oldest CR is active",
				"active", oldest.Name, "rejected", teardown.Name)
			r.setCondition(teardown, conditionTypeTeardownProgressing, metav1.ConditionFalse, "DuplicateRejected",
				fmt.Sprintf("Another HubTeardown %q already exists and is active. Delete this CR or the other one.", oldest.Name))
			if statusErr := r.Client.Status().Update(ctx, teardown); statusErr != nil {
				log.Error(statusErr, "Failed to update rejected teardown status")
			}
			return ctrl.Result{}, nil
		}
	}

	// Handle CR deletion: remove the finalizer so the CR can be garbage-collected.
	if teardown.GetDeletionTimestamp() != nil {
		if controllerutil.ContainsFinalizer(teardown, teardownJobFinalizer) {
			// Deleting the CR is the only explicit cancel. Clear the contract
			// first so a surviving MCH does not stay in teardown mode. Best
			// effort: the finalizer must not be held by a failed patch.
			if err := r.clearTeardownActiveAnnotation(ctx, log); err != nil {
				log.Error(err, "Failed to clear teardown-active contract during CR deletion")
			}
			if err := r.cleanupHandoff(ctx, log, teardown); err != nil {
				log.Error(err, "Failed to cleanup teardown handoff during CR deletion")
			}
			controllerutil.RemoveFinalizer(teardown, teardownJobFinalizer)
			if err := r.Client.Update(ctx, teardown); err != nil {
				return ctrl.Result{}, err
			}
		}
		return ctrl.Result{}, nil
	}

	// Ensure the finalizer is present to protect the CR while teardown is active.
	if !controllerutil.ContainsFinalizer(teardown, teardownJobFinalizer) {
		controllerutil.AddFinalizer(teardown, teardownJobFinalizer)
		if err := r.Client.Update(ctx, teardown); err != nil {
			return ctrl.Result{}, err
		}
	}

	if teardown.Spec.DryRun {
		return r.reconcileDryRun(ctx, log, teardown)
	}

	return r.reconcileActive(ctx, log, teardown)
}

// reconcileDryRun performs a read-only scan and populates the dry-run report.
func (r *HubTeardownReconciler) reconcileDryRun(ctx context.Context, log logr.Logger, td *operatorv1.HubTeardown) (ctrl.Result, error) {
	log.Info("Dry-run mode: scanning cluster state")
	originalStatus := td.Status.DeepCopy()

	// Pausing an active teardown stops the deadline from accruing. The shift is
	// applied on resume, so a long pause cannot fail a later run.
	if td.Status.ActiveSince != nil && td.Status.PausedAt == nil {
		now := metav1.Now()
		td.Status.PausedAt = &now
	}

	graph, err := r.buildDependencyGraph(ctx, td, log)
	if err != nil {
		r.setCondition(td, conditionTypeDryRunComplete, metav1.ConditionFalse, "ScanFailed",
			fmt.Sprintf("Dependency graph scan failed: %v", err))
		if statusErr := r.Client.Status().Update(ctx, td); statusErr != nil {
			log.Error(statusErr, "Failed to update status after scan error")
		}
		return ctrl.Result{RequeueAfter: teardownResyncDryRun}, nil
	}

	td.Status.Phase = operatorv1.TeardownPhaseInspecting
	report := r.buildDryRunReport(graph)
	td.Status.DryRunReport = report
	td.Status.BlockingResources = graph.BlockingResources
	td.Status.CloudResourceWarnings = r.buildCloudWarnings(ctx, log, graph)

	if err := r.applyInspection(ctx, log, td, graph); err != nil {
		log.Error(err, "Inspection report write failed; refusing to advance to ReportReady")
		if statusErr := r.Client.Status().Update(ctx, td); statusErr != nil {
			log.Error(statusErr, "Failed to update status after report write error")
		}
		return ctrl.Result{RequeueAfter: teardownResyncDryRun}, nil
	}
	td.Status.Phase = operatorv1.TeardownPhaseReportReady

	blockingTotal := int(report.TotalBlockingResources)
	blockingDetail := ""
	if td.Status.Summary != nil && int(td.Status.Summary.Total) != blockingTotal {
		blockingTotal = int(td.Status.Summary.Total)
		blockingDetail = fmt.Sprintf(" (%d webhook-blocking)", report.TotalBlockingResources)
	}
	r.setCondition(td, conditionTypeDryRunComplete, metav1.ConditionTrue, "ScanComplete",
		fmt.Sprintf("Dry-run analysis complete. Found %d blocking resources%s, %d cloud-risk resources. Set spec.dryRun=false to begin teardown.",
			blockingTotal, blockingDetail, report.TotalCloudRiskResources))

	if len(td.Status.CloudResourceWarnings) > 0 {
		r.setCondition(td, conditionTypeCloudResourcesRisk, metav1.ConditionTrue, "UnacknowledgedWarnings",
			fmt.Sprintf("%d resources have cloud-protecting finalizers. Review status.cloudResourceWarnings and set spec.acknowledgeCloudResourceRisk=true after verifying cloud state.",
				len(td.Status.CloudResourceWarnings)))
	} else {
		r.setCondition(td, conditionTypeCloudResourcesRisk, metav1.ConditionFalse, "NoCloudRisk",
			"No resources with cloud-protecting finalizers found.")
	}

	// Only persist a status that actually changed. The controller watches its
	// own CR, so an unconditional write re-triggers this reconcile and turns a
	// read-only dry run into a continuous scan loop.
	if statusEquivalent(originalStatus, &td.Status) {
		return ctrl.Result{RequeueAfter: teardownResyncDryRun}, nil
	}
	if err := r.Client.Status().Update(ctx, td); err != nil {
		log.Error(err, "Failed to update dry-run status")
		return ctrl.Result{}, err
	}

	// Emit cloud warning events only on the first scan (when the condition is freshly set).
	// Subsequent resyncs skip event emission to avoid flooding the event stream;
	// the warnings remain in status.cloudResourceWarnings for inspection.
	if !r.hasEmittedWarnings(td) {
		r.emitCloudWarningEvents(td)
		r.markWarningsEmitted(ctx, td)
	}

	return ctrl.Result{RequeueAfter: teardownResyncDryRun}, nil
}

// reconcileActive drives the actual teardown phases.
func (r *HubTeardownReconciler) reconcileActive(ctx context.Context, log logr.Logger, td *operatorv1.HubTeardown) (ctrl.Result, error) {
	log.Info("Active teardown mode", "phase", td.Status.Phase)

	// Failed is terminal. Editing the CR does not resume a failed run; a new
	// HubTeardown is the retry mechanism.
	if td.Status.Phase == operatorv1.TeardownPhaseFailed {
		return ctrl.Result{}, nil
	}

	// Track when active teardown started for overall stall detection.
	// On the first active reconcile, always establish the inspection report:
	// consent is bound to its digest, so a CR created directly in active mode
	// has nothing to consent to until the scan has run. When a dry-run report
	// already exists, the re-scan also records whether the cluster drifted.
	if td.Status.ActiveSince == nil {
		graph, err := r.buildDependencyGraph(ctx, td, log)
		if err != nil {
			log.Error(err, "Failed to scan on activation; proceeding without a fresh report")
		} else {
			freshReport := r.buildDryRunReport(graph)
			if td.Status.DryRunReport != nil {
				oldBlocking := td.Status.DryRunReport.TotalBlockingResources
				oldCloudRisk := td.Status.DryRunReport.TotalCloudRiskResources
				if freshReport.TotalBlockingResources != oldBlocking || freshReport.TotalCloudRiskResources != oldCloudRisk {
					r.setCondition(td, conditionTypeDryRunReportStale, metav1.ConditionTrue, "ClusterStateChanged",
						fmt.Sprintf("Cluster state changed since dry-run scan: blocking resources %d→%d, cloud-risk resources %d→%d. Dry-run report updated.",
							oldBlocking, freshReport.TotalBlockingResources, oldCloudRisk, freshReport.TotalCloudRiskResources))
					r.Recorder.Eventf(td, corev1.EventTypeWarning, "DryRunReportStale",
						"Cluster state changed since dry-run: blocking %d→%d, cloud-risk %d→%d",
						oldBlocking, freshReport.TotalBlockingResources, oldCloudRisk, freshReport.TotalCloudRiskResources)
				} else {
					r.setCondition(td, conditionTypeDryRunReportStale, metav1.ConditionFalse, "ReportCurrent",
						"Cluster state matches dry-run report.")
				}
			}
			td.Status.DryRunReport = freshReport
			td.Status.CloudResourceWarnings = r.buildCloudWarnings(ctx, log, graph)
			if inspectErr := r.applyInspection(ctx, log, td, graph); inspectErr != nil {
				// The report is the consent anchor. Record the failure, but do
				// not stop the clock: the next reconcile retries the scan.
				log.Error(inspectErr, "Failed to write inspection report on activation")
			}
		}

		now := metav1.Now()
		td.Status.ActiveSince = &now
		if err := r.Client.Status().Update(ctx, td); err != nil {
			log.Error(err, "Failed to persist ActiveSince timestamp")
			return ctrl.Result{}, err
		}
	}

	// Check for overall teardown stall. Skip if the final phase is already complete.
	if td.Status.ActiveSince == nil {
		return ctrl.Result{RequeueAfter: teardownResyncActive}, nil
	}
	if r.isPhaseComplete(td, operatorv1.TeardownPhaseVerifying) {
		if r.isConditionTrue(td, conditionTypeTeardownStalled) {
			r.setCondition(td, conditionTypeTeardownStalled, metav1.ConditionFalse, "TeardownComplete",
				"Teardown completed successfully; stall condition cleared.")
			if err := r.Client.Status().Update(ctx, td); err != nil {
				log.Error(err, "Failed to clear Stalled condition after completion")
			}
		}
		return ctrl.Result{}, nil
	}
	// The deadline does not accrue while the CR is paused. Shift the start
	// forward by the paused interval before evaluating it.
	if td.Status.PausedAt != nil && td.Status.ActiveSince != nil {
		shifted := td.Status.ActiveSince.Add(time.Since(td.Status.PausedAt.Time))
		td.Status.ActiveSince = &metav1.Time{Time: shifted}
		td.Status.PausedAt = nil
		if err := r.Client.Status().Update(ctx, td); err != nil {
			log.Error(err, "Failed to resume the teardown clock")
		}
	}

	maxDur := defaultMaxDuration
	if td.Spec.MaxDuration != nil {
		maxDur = td.Spec.MaxDuration.Duration
	}
	elapsed := time.Since(td.Status.ActiveSince.Time)
	if elapsed > maxDur {
		return r.failOnTimeout(ctx, log, td, elapsed, maxDur)
	}
	if r.isConditionTrue(td, conditionTypeTeardownStalled) {
		r.setCondition(td, conditionTypeTeardownStalled, metav1.ConditionFalse, "NotStalled",
			"Teardown is running within spec.maxDuration.")
		if err := r.Client.Status().Update(ctx, td); err != nil {
			log.Error(err, "Failed to clear Stalled condition")
		}
		r.Recorder.Event(td, corev1.EventTypeNormal, "StallCleared", "Teardown no longer stalled")
	}

	handoffErr := r.ensureHandoff(ctx, log, td)
	if handoffErr != nil {
		// Visible, not fatal: teardown can still finish in-process, but the
		// missing handoff identity must never be silent (J-6).
		r.setCondition(td, conditionTypeHandoffReady, metav1.ConditionFalse, "HandoffUnavailable", handoffErr.Error())
		log.Error(handoffErr, "Failed to ensure teardown handoff (controller will continue without resilient Job)")
	} else if teardownConsentGiven(td) {
		r.setCondition(td, conditionTypeHandoffReady, metav1.ConditionTrue, "HandoffReady",
			fmt.Sprintf("Handoff Job runs as ServiceAccount %s bound to ClusterRole %s.", handoffSAName, handoffRoleName))
	}

	// The destructive phases run in the Job. The operator holds at the boundary
	// rather than duplicating them, and holds there entirely until the admin has
	// authorised destructive work.
	if !teardownConsentGiven(td) {
		reason := "WaitingForApproval"
		message := "Destructive prepare phases require spec.approvedDestructiveActions or spec.acknowledgeCloudResourceRisk."
		if teardownApprovalFlagsSet(td) {
			// Approval flags without a matching digest is a lapsed consent:
			// the report changed, or no report was ever written. The empty
			// case gets its own message because there is nothing to read yet.
			reason = "ConsentLapsed"
			if td.Status.ReportDigest == "" {
				message = "Approval flags are set but no inspection report exists yet " +
					"(status.reportDigest is empty), so there is nothing to consent to. The operator " +
					"writes the report when the cluster scan succeeds; then set " +
					"spec.acknowledgedReportDigest to status.reportDigest."
			} else {
				message = fmt.Sprintf("Approval flags are set but consent is bound to the inspection report: "+
					"spec.acknowledgedReportDigest=%q, status.reportDigest=%q. Read the current report and set "+
					"the matching digest to continue.", td.Spec.AcknowledgedReportDigest, td.Status.ReportDigest)
			}
			if prev := meta.FindStatusCondition(td.Status.Conditions, conditionTypeTeardownProgressing); prev == nil || prev.Reason != reason {
				r.Recorder.Event(td, corev1.EventTypeWarning, "ConsentLapsed", message)
			}
		}
		r.setCondition(td, conditionTypeTeardownProgressing, metav1.ConditionTrue, reason, message)
		if err := r.Client.Status().Update(ctx, td); err != nil {
			log.Error(err, "Failed to persist approval wait")
		}
		return ctrl.Result{RequeueAfter: teardownResyncActive}, nil
	}

	// A Job that cannot be created is not a running Job. The destructive work
	// lives in the Job, so an unavailable handoff must be reported as the
	// reason teardown is not progressing rather than hidden behind
	// PrepareJobRunning.
	if handoffErr != nil && teardownConsentGiven(td) {
		r.setCondition(td, conditionTypeTeardownProgressing, metav1.ConditionTrue, "HandoffUnavailable",
			"The destructive Job cannot start: "+handoffErr.Error())
		if statusErr := r.Client.Status().Update(ctx, td); statusErr != nil {
			log.Error(statusErr, "Failed to persist handoff failure")
		}
		return ctrl.Result{RequeueAfter: teardownResyncActive}, nil
	}

	prepareDone, progress, err := r.prepareProgressComplete(ctx, td)
	if err != nil {
		log.Error(err, "Failed to read prepare progress")
	}
	if !prepareDone {
		// A failed Job never writes the progress record its phases would have
		// completed, so reading progress alone reports a dead Job as running
		// forever. Surface the terminal state and stop claiming progress.
		if failed, reason, jobErr := r.teardownJobFailure(ctx, td, prepareJobName); jobErr != nil {
			log.Error(jobErr, "Failed to read prepare Job state")
		} else if failed {
			r.setCondition(td, conditionTypeTeardownProgressing, metav1.ConditionTrue, "PrepareJobFailed",
				fmt.Sprintf("The prepare Job reached %s; delete Job %s/%s to retry, or inspect its logs for the cause.",
					reason, teardownNamespace(td), prepareJobName))
			if statusErr := r.Client.Status().Update(ctx, td); statusErr != nil {
				log.Error(statusErr, "Failed to persist prepare failure")
			}
			return ctrl.Result{RequeueAfter: teardownResyncActive}, nil
		}
		r.setCondition(td, conditionTypeTeardownProgressing, metav1.ConditionTrue, "PrepareJobRunning",
			"The prepare Job is executing the destructive phases; the operator does not duplicate them.")
		if statusErr := r.Client.Status().Update(ctx, td); statusErr != nil {
			log.Error(statusErr, "Failed to persist prepare wait")
		}
		return ctrl.Result{RequeueAfter: teardownResyncActive}, nil
	}
	r.mirrorPrepareProgress(td, progress)

	// The prepare Job is done: remove it, then start the handoff Job. The
	// identity stays in place, and the handoff Job is only created once the
	// prepare pod has exited, so the shared progress record never has two
	// writers (D9b-2).
	if err := r.finishPrepareJob(ctx, log, td); err != nil {
		log.Error(err, "Failed to finish the prepare Job")
	}
	if err := r.ensureHandoffJob(ctx, log, td); err != nil {
		r.setCondition(td, conditionTypeHandoffReady, metav1.ConditionFalse, "HandoffUnavailable", err.Error())
		log.Error(err, "Failed to create the handoff Job")
	}
	if handoffProgress, err := r.loadJobProgress(ctx, td); err != nil {
		log.Error(err, "Failed to read handoff progress")
	} else {
		r.mirrorHandoffProgress(td, handoffProgress)
	}

	// Clear WaitingForApproval if the user has now granted the required approvals.
	if td.Spec.ApprovedDestructiveActions || td.Spec.AcknowledgeCloudResourceRisk {
		r.clearWaitingForApproval(td)
	}

	r.setCondition(td, conditionTypeTeardownProgressing, metav1.ConditionTrue, "TeardownActive",
		fmt.Sprintf("Teardown is executing phase: %s", td.Status.Phase))

	// Each phase persists its own completion status immediately, so there is
	// no batched status update here. Errors from phases or status writes
	// trigger a requeue.
	result, err := r.executePhases(ctx, log, td)
	if err != nil {
		// A handoff Job that ended without recording completion is a terminal
		// failure, not a wait: the deadline is not the only way out (D9b-3).
		if stderrors.Is(err, errHandoffJobFailed) {
			return r.failOnHandoffJob(ctx, log, td, err)
		}
		if statusErr := r.Client.Status().Update(ctx, td); statusErr != nil {
			log.Error(statusErr, "Failed to persist phase failure status")
		}
		return ctrl.Result{RequeueAfter: teardownResyncActive}, nil
	}

	// Verify writes the terminal phase. Once it is complete the handoff Job has
	// nothing left to do, so the identity and both Jobs go away.
	if r.isPhaseComplete(td, operatorv1.TeardownPhaseVerifying) {
		if finishErr := r.finishHandoffJob(ctx, log, td); finishErr != nil {
			log.Error(finishErr, "Failed to finish teardown handoff")
		} else if statusErr := r.Client.Status().Update(ctx, td); statusErr != nil {
			log.Error(statusErr, "Failed to persist handoff condition")
		}
	}
	return result, nil
}

// teardownPhaseStep is one entry in the ordered teardown plan.
type teardownPhaseStep struct {
	phase   operatorv1.TeardownPhase
	execute func(ctx context.Context, log logr.Logger, td *operatorv1.HubTeardown) (bool, error)
}

// teardownPhasePlan is the ordered plan. GateOLMSubscription, CleanOrphans and
// RemoveOLMOperator are absent by design: the operator is preserved (D-G) and
// Verify owns the terminal write.
//
// The two Job-owned phases are present as waiters: they keep the phase names and
// ordering in status, but the operator never executes them (D9b-1). The handoff
// Job does, under an identity the admin consented to and a deadline the
// controller cannot give itself.
func (r *HubTeardownReconciler) teardownPhasePlan() []teardownPhaseStep {
	return []teardownPhaseStep{
		{operatorv1.TeardownPhaseDeleteMCH, r.phaseDeleteMCH},
		{operatorv1.TeardownPhaseMonitorMCEChain, r.awaitJobPhase(operatorv1.TeardownPhaseMonitorMCEChain)},
		{operatorv1.TeardownPhaseDeleteACMCRDs, r.awaitJobPhase(operatorv1.TeardownPhaseDeleteACMCRDs)},
		{operatorv1.TeardownPhaseVerifying, r.phaseVerify},
	}
}

// executePhases runs through teardown phases in order, resuming from the last completed phase.
func (r *HubTeardownReconciler) executePhases(ctx context.Context, log logr.Logger, td *operatorv1.HubTeardown) (ctrl.Result, error) {
	phases := r.teardownPhasePlan()

	for _, p := range phases {
		if r.isPhaseComplete(td, p.phase) {
			continue
		}

		// Check if admin paused back to dry-run mid-execution
		if td.Spec.DryRun {
			log.Info("Admin re-enabled dry-run, pausing teardown")
			return ctrl.Result{}, nil
		}

		td.Status.Phase = p.phase
		r.setPhaseStatus(td, p.phase, operatorv1.PhaseStateInProgress, "Executing phase")

		done, err := p.execute(ctx, log, td)
		if err != nil {
			r.setPhaseStatus(td, p.phase, operatorv1.PhaseStateFailed, err.Error())
			r.Recorder.Eventf(td, corev1.EventTypeWarning, "PhaseFailed",
				"Phase %s failed: %v", p.phase, err)
			return ctrl.Result{}, err
		}

		if !done {
			r.setPhaseStatus(td, p.phase, operatorv1.PhaseStateInProgress, "Waiting for phase to complete")
			if err := r.Client.Status().Update(ctx, td); err != nil {
				log.Error(err, "Failed to persist mid-phase status", "phase", p.phase)
			}
			return ctrl.Result{RequeueAfter: teardownResyncActive}, nil
		}

		now := metav1.Now()
		r.setPhaseStatusWithTime(td, p.phase, operatorv1.PhaseStateComplete, "Phase completed", &now)
		r.Recorder.Eventf(td, corev1.EventTypeNormal, "PhaseComplete",
			"Phase %s completed successfully", p.phase)

		// Persist phase completion immediately so a controller crash between
		// phases doesn't lose progress and cause re-execution of completed work.
		if err := r.Client.Status().Update(ctx, td); err != nil {
			log.Error(err, "Failed to persist phase completion", "phase", p.phase)
			return ctrl.Result{}, err
		}
	}

	// Verify wrote the terminal phase (Complete or CompleteWithResidue) and the
	// residue report. Nothing left to do: the operator is preserved (D-G).
	return ctrl.Result{}, nil
}

// isPhaseComplete checks if a phase has already been completed.
func (r *HubTeardownReconciler) isPhaseComplete(td *operatorv1.HubTeardown, phase operatorv1.TeardownPhase) bool {
	for _, ps := range td.Status.Phases {
		if ps.Phase == phase && (ps.State == operatorv1.PhaseStateComplete || ps.State == operatorv1.PhaseStateSkipped) {
			return true
		}
	}
	return false
}

// setPhaseStatus updates or appends a phase status entry.
func (r *HubTeardownReconciler) setPhaseStatus(td *operatorv1.HubTeardown, phase operatorv1.TeardownPhase, state operatorv1.TeardownPhaseState, msg string) {
	now := metav1.Now()
	for i, ps := range td.Status.Phases {
		if ps.Phase == phase {
			td.Status.Phases[i].State = state
			td.Status.Phases[i].Message = msg
			if state == operatorv1.PhaseStateInProgress && ps.StartTime == nil {
				td.Status.Phases[i].StartTime = &now
			}
			return
		}
	}
	entry := operatorv1.TeardownPhaseStatus{
		Phase:   phase,
		State:   state,
		Message: msg,
	}
	if state == operatorv1.PhaseStateInProgress {
		entry.StartTime = &now
	}
	td.Status.Phases = append(td.Status.Phases, entry)
}

// setPhaseStatusWithTime updates a phase status with an explicit completion time.
func (r *HubTeardownReconciler) setPhaseStatusWithTime(td *operatorv1.HubTeardown, phase operatorv1.TeardownPhase, state operatorv1.TeardownPhaseState, msg string, completionTime *metav1.Time) {
	r.setPhaseStatus(td, phase, state, msg)
	for i, ps := range td.Status.Phases {
		if ps.Phase == phase {
			td.Status.Phases[i].CompletionTime = completionTime
			return
		}
	}
}

// setCondition sets a metav1.Condition on the HubTeardown status.
func (r *HubTeardownReconciler) setCondition(td *operatorv1.HubTeardown, condType string, status metav1.ConditionStatus, reason, message string) {
	meta.SetStatusCondition(&td.Status.Conditions, metav1.Condition{
		Type:               condType,
		Status:             status,
		ObservedGeneration: td.Generation,
		Reason:             reason,
		Message:            truncateMessage(message, 4096),
	})
}

// isConditionTrue checks whether a condition is currently set to True.
func (r *HubTeardownReconciler) isConditionTrue(td *operatorv1.HubTeardown, condType string) bool {
	for _, c := range td.Status.Conditions {
		if c.Type == condType {
			return c.Status == metav1.ConditionTrue
		}
	}
	return false
}

func truncateMessage(msg string, max int) string {
	if len(msg) > max {
		return msg[:max-15] + "... (truncated)"
	}
	return msg
}

// hasEmittedWarnings checks whether cloud warning events have already been emitted
// for the current spec generation, preventing duplicate event flooding on resyncs.
func (r *HubTeardownReconciler) hasEmittedWarnings(td *operatorv1.HubTeardown) bool {
	if td.Annotations == nil {
		return false
	}
	return td.Annotations[annotationWarningsEmitted] == fmt.Sprintf("%d", td.Generation)
}

// markWarningsEmitted stamps the CR annotation so subsequent resyncs skip event emission.
func (r *HubTeardownReconciler) markWarningsEmitted(ctx context.Context, td *operatorv1.HubTeardown) {
	if td.Annotations == nil {
		td.Annotations = make(map[string]string)
	}
	td.Annotations[annotationWarningsEmitted] = fmt.Sprintf("%d", td.Generation)
	if err := r.Client.Update(ctx, td); err != nil {
		ctrllog.FromContext(ctx).Error(err, "Failed to mark warnings as emitted")
	}
}

func (r *HubTeardownReconciler) SetupWithManager(mgr ctrl.Manager) error {
	r.Recorder = mgr.GetEventRecorderFor("hubteardown-controller")

	restConfig := mgr.GetConfig()
	dc, err := discovery.NewDiscoveryClientForConfig(restConfig)
	if err != nil {
		return fmt.Errorf("failed to create discovery client: %w", err)
	}
	r.DiscoveryClient = dc

	return ctrl.NewControllerManagedBy(mgr).
		For(&operatorv1.HubTeardown{}).
		Named("hubteardown").
		Complete(r)
}
