// Copyright Contributors to the Open Cluster Management project

package controllers

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/go-logr/logr"
	operatorv1 "github.com/stolostron/multiclusterhub-operator/api/v1"
	"github.com/stolostron/multiclusterhub-operator/pkg/inventory"
	"github.com/stolostron/multiclusterhub-operator/pkg/policy"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	// reportLabelUninstall marks the object for collection by must-gather.
	reportLabelUninstall = "open-cluster-management.io/uninstall-report"
	// reportLabelTeardown scopes the retention sweep to one HubTeardown.
	reportLabelTeardown = "open-cluster-management.io/teardown"

	reportKeyJSON = "report.json"
	reportKeyText = "report.txt"
	reportKeyMeta = "meta.json"

	// reportMaxBytes is the per-object payload budget. etcd refuses objects
	// above 1.5MB; 900KB leaves room for object metadata and the text
	// rendering without approaching that wall.
	reportMaxBytes = 900 * 1024

	// reportRetention is the number of reports kept per HubTeardown.
	reportRetention = 5

	// reportStatusBlockerCap mirrors the CRD MaxItems on status.blockers.
	reportStatusBlockerCap = 32

	// reportMaxNameLength keeps the generated name a valid DNS subdomain.
	reportMaxNameLength = 240

	// reportOwnershipClassified records that every discovered object carries an
	// evidence-based ownership verdict (increment 17). The verdict itself is on
	// each blocker; this string only says the classification ran.
	reportOwnershipClassified = "classified"
)

// inspectReport is the complete typed record written to the ConfigMap.
// Blockers are never dropped; only the inventory section may be omitted under
// size pressure.
type inspectReport struct {
	Teardown  string                    `json:"teardown"`
	ScanTime  metav1.Time               `json:"scanTime"`
	Inventory inventorySection          `json:"inventory"`
	Summary   operatorv1.BlockerSummary `json:"summary"`
	Blockers  []operatorv1.Blocker      `json:"blockers"`
}

// inventorySection states what the scan covered. It exists so a reader can
// tell "nothing found" from "not looked at".
type inventorySection struct {
	ObjectsSeen       int              `json:"objectsSeen"`
	DiscoveryPath     string           `json:"discoveryPath"`
	InventoryComplete bool             `json:"inventoryComplete"`
	Incomplete        []ScanIncomplete `json:"incomplete,omitempty"`
	MCHPresent        bool             `json:"mchPresent"`
	Ownership         string           `json:"ownershipClassification"`
}

// reportMeta is the index a reader (and the controller) uses before parsing
// the full report. Digest changes only when the observed content changes.
type reportMeta struct {
	Teardown          string      `json:"teardown"`
	ScanTime          metav1.Time `json:"scanTime"`
	Digest            string      `json:"digest"`
	KubeVersion       string      `json:"kubeVersion,omitempty"`
	DiscoveryPath     string      `json:"discoveryPath"`
	InventoryComplete bool        `json:"inventoryComplete"`
	InventoryOmitted  bool        `json:"inventoryOmitted"`
	Ownership         string      `json:"ownershipClassification"`
	Parts             []string    `json:"parts,omitempty"`
}

// blockersFromGraph converts the scan into the single record type consumed by
// the report, status and (later) verification. UID is the identity; two
// objects with the same name are two blockers.
func blockersFromGraph(graph *DependencyGraph, scanTime metav1.Time, consent policy.ConsentState) []operatorv1.Blocker {
	if graph == nil {
		return nil
	}
	seen := make(map[types.UID]struct{}, len(graph.DiscoveredResources))
	blockers := make([]operatorv1.Blocker, 0, len(graph.DiscoveredResources))
	for i := range graph.DiscoveredResources {
		dr := &graph.DiscoveredResources[i]
		if len(dr.Finalizers) == 0 {
			continue
		}
		if dr.UID != "" {
			if _, ok := seen[dr.UID]; ok {
				continue
			}
			seen[dr.UID] = struct{}{}
		}

		cloud := false
		allowlisted := false
		for _, finalizer := range dr.Finalizers {
			if IsCloudProtectingFinalizer(finalizer) {
				cloud = true
				continue
			}
			if IsAllowlistedFinalizer(finalizer) {
				allowlisted = true
			}
		}
		if !cloud && !allowlisted {
			continue
		}

		blocker := operatorv1.Blocker{
			APIVersion:      apiVersionFor(dr.Ref, dr.Version),
			Kind:            dr.Ref.Kind,
			Name:            dr.Ref.Name,
			Namespace:       dr.Ref.Namespace,
			UID:             dr.UID,
			Finalizers:      append([]string(nil), dr.Finalizers...),
			BlockedSince:    scanTime,
			ObservedInPhase: operatorv1.TeardownPhaseInspecting,
			Ownership:       dr.Ownership,
			Classification:  operatorv1.ClassUnknown,
		}
		if dr.DeletionTimestamp != nil {
			blocker.DeletionTimestamp = dr.DeletionTimestamp.DeepCopy()
			blocker.BlockedSince = *dr.DeletionTimestamp
		}
		if cloud {
			blocker.Classification = operatorv1.ClassCloudProtecting
			blocker.CloudRisk = &operatorv1.CloudRisk{
				Platform:     dr.Platform,
				ResourceType: dr.Ref.Kind,
				MayOrphan:    true,
			}
			// RequiresConsent is a property of acting on a cloud-protecting
			// finalizer, not of the action chosen right now: waiting for the
			// controller and resolving the finalizer both leave the requirement
			// in place.
			blocker.RequiresConsent = true
			blocker.ConsentReason = "removing this finalizer may orphan cloud infrastructure outside the cluster"
		}

		blocker.SuggestedAction = policy.Evaluate(blocker, consent).Action
		blockers = append(blockers, blocker)
	}

	sort.Slice(blockers, func(i, j int) bool {
		a, b := blockers[i], blockers[j]
		if a.Namespace != b.Namespace {
			return a.Namespace < b.Namespace
		}
		if a.Kind != b.Kind {
			return a.Kind < b.Kind
		}
		if a.Name != b.Name {
			return a.Name < b.Name
		}
		return string(a.UID) < string(b.UID)
	})
	return blockers
}

func apiVersionFor(ref operatorv1.ResourceRef, version string) string {
	if version == "" {
		return ref.Group
	}
	if ref.Group == "" {
		return version
	}
	return ref.Group + "/" + version
}

// summarizeBlockers produces the never-truncated aggregate.
func summarizeBlockers(blockers []operatorv1.Blocker) operatorv1.BlockerSummary {
	summary := operatorv1.BlockerSummary{
		Total:            int32(len(blockers)),
		ByClassification: make(map[string]int32, 4),
	}
	for i := range blockers {
		if blockers[i].RequiresConsent {
			summary.RequiringConsent++
		}
		summary.ByClassification[string(blockers[i].Classification)]++
	}
	return summary
}

// capBlockers returns the status view and the number of omitted entries.
func capBlockers(blockers []operatorv1.Blocker, limit int) ([]operatorv1.Blocker, int32) {
	if limit < 0 {
		limit = 0
	}
	if len(blockers) <= limit {
		return append([]operatorv1.Blocker(nil), blockers...), 0
	}
	return append([]operatorv1.Blocker(nil), blockers[:limit]...), int32(len(blockers) - limit)
}

// reportConsentState is the consent the report is rendered against. A dry run
// previews what would be required, so it is rendered with no grant; an active
// run shows the grant it actually holds.
func reportConsentState(td *operatorv1.HubTeardown) policy.ConsentState {
	if td.Spec.DryRun {
		return policy.ConsentState{}
	}
	return policy.ConsentState{
		ApprovedDestructiveActions: td.Spec.ApprovedDestructiveActions,
		CloudRiskAcknowledged:      td.Spec.AcknowledgeCloudResourceRisk,
		AcknowledgedDigest:         td.Spec.AcknowledgedReportDigest,
		ReportDigest:               td.Status.ReportDigest,
	}
}

// buildInspectReport renders the graph into the report and its meta.
func buildInspectReport(td *operatorv1.HubTeardown, graph *DependencyGraph, scanTime metav1.Time, kubeVersion string) (inspectReport, reportMeta) {
	blockers := blockersFromGraph(graph, scanTime, reportConsentState(td))
	complete := len(graph.Incomplete) == 0

	report := inspectReport{
		Teardown: td.Name,
		ScanTime: scanTime,
		Inventory: inventorySection{
			ObjectsSeen:       len(graph.DiscoveredResources),
			DiscoveryPath:     inventory.DiscoveryPathLegacy,
			InventoryComplete: complete,
			Incomplete:        append([]ScanIncomplete(nil), graph.Incomplete...),
			MCHPresent:        graph.MCH != nil,
			Ownership:         reportOwnershipClassified,
		},
		Summary:  summarizeBlockers(blockers),
		Blockers: blockers,
	}
	meta := reportMeta{
		Teardown:          td.Name,
		ScanTime:          scanTime,
		Digest:            reportDigest(report),
		KubeVersion:       kubeVersion,
		DiscoveryPath:     report.Inventory.DiscoveryPath,
		InventoryComplete: complete,
		Ownership:         reportOwnershipClassified,
	}
	return report, meta
}

// reportDigest is computed over observed content only. scanTime is excluded
// deliberately: a 60s dry-run resync over unchanged state must not create a
// new report, because the report name is also the consent key.
func reportDigest(report inspectReport) string {
	// BlockedSince is an observation timestamp derived from scanTime for
	// objects that are not deleting. Including it would make every resync
	// produce a new report and silently lapse consent.
	canonicalBlockers := make([]operatorv1.Blocker, len(report.Blockers))
	for i := range report.Blockers {
		canonicalBlockers[i] = report.Blockers[i]
		canonicalBlockers[i].BlockedSince = metav1.Time{}
		// Ownership evidence carries the scan time it was observed at, for the
		// same reason as BlockedSince: a resync must not change the digest and
		// silently lapse consent. Copy the slice first so canonicalising the
		// digest cannot mutate the report that is about to be written to status.
		if evidence := report.Blockers[i].Ownership.Evidence; len(evidence) > 0 {
			canonical := make([]operatorv1.Evidence, len(evidence))
			copy(canonical, evidence)
			for j := range canonical {
				canonical[j].Observed = metav1.Time{}
			}
			canonicalBlockers[i].Ownership.Evidence = canonical
		}
	}
	canonical := struct {
		Summary    operatorv1.BlockerSummary `json:"summary"`
		Blockers   []operatorv1.Blocker      `json:"blockers"`
		Incomplete []ScanIncomplete          `json:"incomplete"`
		Ownership  string                    `json:"ownership"`
	}{
		Summary:    report.Summary,
		Blockers:   canonicalBlockers,
		Incomplete: report.Inventory.Incomplete,
		Ownership:  report.Inventory.Ownership,
	}
	body, err := json.Marshal(canonical)
	if err != nil {
		return "unmarshalable"
	}
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:])
}

// renderInspectReportText is what a support engineer reads.
func renderInspectReportText(report inspectReport, meta reportMeta) string {
	var b strings.Builder
	fmt.Fprintf(&b, "HubTeardown report: %s\n", report.Teardown)
	fmt.Fprintf(&b, "Scan time: %s\n", report.ScanTime.UTC().Format("2006-01-02T15:04:05Z"))
	fmt.Fprintf(&b, "Digest: %s\n", meta.Digest)
	fmt.Fprintf(&b, "Inventory: %d objects seen via %s, complete=%t\n",
		report.Inventory.ObjectsSeen, report.Inventory.DiscoveryPath, report.Inventory.InventoryComplete)
	if !report.Inventory.InventoryComplete {
		b.WriteString("Incomplete coverage (absence claims are invalid for these types):\n")
		for _, inc := range report.Inventory.Incomplete {
			fmt.Fprintf(&b, "  - %s/%s: %s\n", inc.Group, inc.Resource, inc.Reason)
		}
	}
	fmt.Fprintf(&b, "Blockers: %d total, %d require consent\n\n",
		report.Summary.Total, report.Summary.RequiringConsent)
	for i := range report.Blockers {
		blocker := &report.Blockers[i]
		fmt.Fprintf(&b, "- %s %s", blocker.Kind, blocker.Name)
		if blocker.Namespace != "" {
			fmt.Fprintf(&b, " (namespace %s)", blocker.Namespace)
		}
		fmt.Fprintf(&b, "\n  uid: %s\n", blocker.UID)
		fmt.Fprintf(&b, "  apiVersion: %s\n", blocker.APIVersion)
		fmt.Fprintf(&b, "  finalizers: %s\n", strings.Join(blocker.Finalizers, ", "))
		fmt.Fprintf(&b, "  classification: %s\n", blocker.Classification)
		fmt.Fprintf(&b, "  ownership: owned=%t confidence=%s\n", blocker.Ownership.Owned, blocker.Ownership.Confidence)
		fmt.Fprintf(&b, "  suggestedAction: %s\n", blocker.SuggestedAction)
		if blocker.CloudRisk != nil {
			fmt.Fprintf(&b, "  cloudRisk: platform=%s mayOrphan=%t\n", blocker.CloudRisk.Platform, blocker.CloudRisk.MayOrphan)
		}
	}
	return b.String()
}

// applyInspection projects the scan into status and writes the report. It is
// the only writer of summary, blockers, lastInspection and reportRef.
func (r *HubTeardownReconciler) applyInspection(ctx context.Context, log logr.Logger, td *operatorv1.HubTeardown, graph *DependencyGraph) error {
	// One scan, one time: the report and the evidence it contains must agree on
	// when the cluster was observed.
	now := graph.ScanTime
	if now.IsZero() {
		now = metav1.Now()
	}

	// Capture the pre-teardown object set before anything can delete one. The
	// record is written once and never replaced, so re-inspection cannot
	// re-baseline onto a half-destroyed namespace.
	snapshotConfigMap, snapshotReused, snapshotErr := r.ensureOperatorNamespaceSnapshot(ctx, td)
	if snapshotErr != nil {
		log.Error(snapshotErr, "Could not record the pre-teardown object snapshot")
	} else {
		log.Info("Pre-teardown object snapshot recorded", "configMap", snapshotConfigMap.Name, "reused", snapshotReused)
	}

	kubeVersion := ""
	if r.DiscoveryClient != nil {
		if info, err := r.DiscoveryClient.ServerVersion(); err == nil && info != nil {
			kubeVersion = info.GitVersion
		}
	}

	report, meta := buildInspectReport(td, graph, now, kubeVersion)
	ref, reused, err := r.writeInspectReport(ctx, log, td, report, meta)
	if err != nil {
		r.setCondition(td, conditionTypeReportWriteFailed, metav1.ConditionTrue, "ReportWriteFailed",
			fmt.Sprintf("The inspection report could not be written, so no reportRef is recorded: %v", err))
		td.Status.Phase = operatorv1.TeardownPhaseInspecting
		return err
	}

	r.setCondition(td, conditionTypeReportWriteFailed, metav1.ConditionFalse, "ReportWritten",
		fmt.Sprintf("Inspection report is available at ConfigMap %s.", ref.Name))

	summary := report.Summary
	td.Status.Summary = &summary
	capped, truncated := capBlockers(report.Blockers, reportStatusBlockerCap)
	td.Status.Blockers = capped
	td.Status.BlockersTruncated = truncated
	td.Status.ReportRef = ref
	// Consent is compared against this digest, never against the report name:
	// a name can be reused after retention, a digest cannot.
	td.Status.ReportDigest = meta.Digest
	inspected := now
	td.Status.LastInspection = &inspected

	if report.Inventory.InventoryComplete {
		r.setCondition(td, conditionTypeInspectIncomplete, metav1.ConditionFalse, "InventoryComplete",
			"Every discovered resource type was listed.")
	} else {
		r.setCondition(td, conditionTypeInspectIncomplete, metav1.ConditionTrue, "InventoryIncomplete",
			fmt.Sprintf("%d resource types could not be listed; the report may not claim absence for them.", len(report.Inventory.Incomplete)))
	}
	if snapshotErr != nil {
		// Verification can still run, but it will not be able to say when an
		// operator-namespace object appeared. Say so now rather than at the end.
		r.setCondition(td, conditionTypeInspectIncomplete, metav1.ConditionTrue, "SnapshotFailed",
			fmt.Sprintf("The pre-teardown object snapshot could not be recorded: %v. Verification will report operator-namespace objects as unclassified without a baseline.", snapshotErr))
	}

	log.Info("Inspection report written", "configMap", ref.Name, "reused", reused,
		"blockers", report.Summary.Total, "inventoryComplete", report.Inventory.InventoryComplete)
	return nil
}

// writeInspectReport writes the report unless the observed content is
// unchanged. The boolean reports whether the previous report was reused.
func (r *HubTeardownReconciler) writeInspectReport(ctx context.Context, log logr.Logger, td *operatorv1.HubTeardown, report inspectReport, meta reportMeta) (*corev1.LocalObjectReference, bool, error) {
	if td.Status.ReportRef != nil {
		existing := &corev1.ConfigMap{}
		err := r.Client.Get(ctx, types.NamespacedName{Name: td.Status.ReportRef.Name, Namespace: td.Namespace}, existing)
		switch {
		case err == nil:
			if digestFromMeta(existing) == meta.Digest {
				return &corev1.LocalObjectReference{Name: existing.Name}, true, nil
			}
		case apierrors.IsNotFound(err):
		default:
			return nil, false, fmt.Errorf("reading existing report %q: %w", td.Status.ReportRef.Name, err)
		}
	}

	base := reportObjectName(td.Name, report.ScanTime)
	name, existing, err := r.pickReportName(ctx, td, base, meta.Digest)
	if err != nil {
		return nil, false, err
	}
	if existing != nil {
		return &corev1.LocalObjectReference{Name: existing.Name}, true, nil
	}

	objects, err := r.buildReportObjects(td, name, report, meta)
	if err != nil {
		return nil, false, err
	}
	for _, object := range objects {
		if err := r.Client.Create(ctx, object); err != nil {
			return nil, false, fmt.Errorf("creating report %q: %w", object.Name, err)
		}
	}

	r.pruneInspectReports(ctx, log, td)
	return &corev1.LocalObjectReference{Name: name}, false, nil
}

// pickReportName returns a free object name. A collision with different
// content gets a numeric suffix rather than an overwrite: the report name is
// the consent key, so content must never change under a name an administrator
// already approved.
func (r *HubTeardownReconciler) pickReportName(ctx context.Context, td *operatorv1.HubTeardown, base, digest string) (string, *corev1.ConfigMap, error) {
	for attempt := 0; attempt < 5; attempt++ {
		name := base
		if attempt > 0 {
			name = fmt.Sprintf("%s-%d", base, attempt+1)
		}
		existing := &corev1.ConfigMap{}
		err := r.Client.Get(ctx, types.NamespacedName{Name: name, Namespace: td.Namespace}, existing)
		switch {
		case apierrors.IsNotFound(err):
			return name, nil, nil
		case err != nil:
			return "", nil, fmt.Errorf("checking report name %q: %w", name, err)
		}
		if digestFromMeta(existing) == digest {
			return name, existing, nil
		}
	}
	return "", nil, fmt.Errorf("no free report name for %q after 5 attempts", base)
}

// buildReportObjects renders one ConfigMap, or a main object plus parts when
// the blockers alone exceed the payload budget.
func (r *HubTeardownReconciler) buildReportObjects(td *operatorv1.HubTeardown, base string, report inspectReport, meta reportMeta) ([]*corev1.ConfigMap, error) {
	body, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("rendering report: %w", err)
	}

	if len(body) <= reportMaxBytes {
		return []*corev1.ConfigMap{r.reportConfigMap(td, base, report, meta, body)}, nil
	}

	// Size pressure: the inventory section is the first thing dropped. There
	// is no raw inventory in this increment, so the omission is recorded in
	// meta and the incomplete list is left out of the JSON.
	meta.InventoryOmitted = true
	trimmed := report
	trimmed.Inventory.Incomplete = nil
	body, err = json.MarshalIndent(trimmed, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("rendering report: %w", err)
	}
	if len(body) <= reportMaxBytes {
		return []*corev1.ConfigMap{r.reportConfigMap(td, base, trimmed, meta, body)}, nil
	}

	chunks := chunkBlockers(report.Blockers, reportMaxBytes-(len(body)-mustMarshalLen(report.Blockers)))
	objects := make([]*corev1.ConfigMap, 0, len(chunks)+1)
	for i, chunk := range chunks {
		part := trimmed
		part.Blockers = chunk
		partBody, marshalErr := json.MarshalIndent(part, "", "  ")
		if marshalErr != nil {
			return nil, fmt.Errorf("rendering report part %d: %w", i+1, marshalErr)
		}
		partName := fmt.Sprintf("%s-part%d", base, i+1)
		meta.Parts = append(meta.Parts, partName)
		objects = append(objects, r.reportConfigMap(td, partName, part, meta, partBody))
	}

	index := trimmed
	index.Blockers = nil
	indexBody, err := json.MarshalIndent(index, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("rendering report index: %w", err)
	}
	objects = append(objects, r.reportConfigMap(td, base, index, meta, indexBody))
	return objects, nil
}

func mustMarshalLen(blockers []operatorv1.Blocker) int {
	body, err := json.Marshal(blockers)
	if err != nil {
		return 0
	}
	return len(body)
}

// chunkBlockers splits the list by rendered size so each part stays inside
// the budget. Order is preserved and nothing is dropped.
func chunkBlockers(blockers []operatorv1.Blocker, budget int) [][]operatorv1.Blocker {
	if budget <= 0 {
		budget = reportMaxBytes / 2
	}
	var chunks [][]operatorv1.Blocker
	var current []operatorv1.Blocker
	currentSize := 0
	for i := range blockers {
		size := mustMarshalLen([]operatorv1.Blocker{blockers[i]}) + 1
		if currentSize+size > budget && len(current) > 0 {
			chunks = append(chunks, current)
			current = nil
			currentSize = 0
		}
		current = append(current, blockers[i])
		currentSize += size
	}
	if len(current) > 0 {
		chunks = append(chunks, current)
	}
	if len(chunks) == 0 {
		chunks = append(chunks, nil)
	}
	return chunks
}

func (r *HubTeardownReconciler) reportConfigMap(td *operatorv1.HubTeardown, name string, report inspectReport, meta reportMeta, body []byte) *corev1.ConfigMap {
	metaBody, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		metaBody = []byte("{}")
	}
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: td.Namespace,
			Labels: map[string]string{
				reportLabelUninstall: "true",
				reportLabelTeardown:  td.Name,
			},
		},
		Data: map[string]string{
			reportKeyJSON: string(body),
			reportKeyText: renderInspectReportText(report, meta),
			reportKeyMeta: string(metaBody),
		},
	}
}

func digestFromMeta(configMap *corev1.ConfigMap) string {
	if configMap == nil || configMap.Data == nil {
		return ""
	}
	raw := configMap.Data[reportKeyMeta]
	if raw == "" {
		return ""
	}
	var meta reportMeta
	if err := json.Unmarshal([]byte(raw), &meta); err != nil {
		return ""
	}
	return meta.Digest
}

// reportObjectName is deterministic for a scan second, so a resync in the
// same second is idempotent. Overlong teardown names are truncated with a
// hash so the result stays a valid DNS subdomain.
func reportObjectName(teardown string, scanTime metav1.Time) string {
	const prefix = "hubteardown-report-"
	base := prefix + teardown + "-" + scanTime.UTC().Format("20060102t150405z")
	if len(base) <= reportMaxNameLength {
		return base
	}
	sum := sha256.Sum256([]byte(base))
	suffix := "-" + hex.EncodeToString(sum[:4])
	keep := reportMaxNameLength - len(prefix) - len(suffix) - len("-") - len("20060102t150405z")
	if keep < 1 {
		keep = 1
	}
	return prefix + teardown[:minInt(keep, len(teardown))] + suffix + "-" + scanTime.UTC().Format("20060102t150405z")
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// pruneInspectReports keeps the newest reportRetention reports for this
// teardown. The label selector is mandatory: a namespace-wide sweep would
// delete another teardown's evidence.
func (r *HubTeardownReconciler) pruneInspectReports(ctx context.Context, log logr.Logger, td *operatorv1.HubTeardown) {
	list := &corev1.ConfigMapList{}
	if err := r.Client.List(ctx, list, client.InNamespace(td.Namespace), client.MatchingLabels{reportLabelTeardown: td.Name}); err != nil {
		log.Error(err, "Failed to list inspection reports for retention")
		return
	}
	if len(list.Items) <= reportRetention {
		return
	}
	sort.SliceStable(list.Items, func(i, j int) bool {
		ti, tj := list.Items[i].CreationTimestamp, list.Items[j].CreationTimestamp
		if ti.Equal(&tj) {
			return list.Items[i].Name < list.Items[j].Name
		}
		return ti.Before(&tj)
	})
	for i := 0; i < len(list.Items)-reportRetention; i++ {
		item := list.Items[i]
		if err := r.Client.Delete(ctx, &item); err != nil && !apierrors.IsNotFound(err) {
			log.Error(err, "Failed to delete expired inspection report", "configMap", item.Name)
		}
	}
}
