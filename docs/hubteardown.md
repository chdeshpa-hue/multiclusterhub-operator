# HubTeardown: Orchestrated RHACM Uninstallation

HubTeardown is a phased, observable, evidence-gated alternative to
`oc delete multiclusterhub`. It inspects the hub first, reports every blocker
and cloud risk, and only then runs teardown in a short-lived Job with its own
ServiceAccount. The always-on operator performs the dry-run inspection, deletes
the target MCH, and verifies the postcondition; it does not execute the
destructive prepare or handoff phases.

## When to use it

| Scenario | Recommended method |
|---|---|
| Simple hub with no managed clusters | `oc delete multiclusterhub` |
| Managed clusters, hosted clusters, or Hive clusters present | HubTeardown |
| Need a preview of blockers and cloud risk before deleting anything | HubTeardown |
| Need explicit, report-bound consent for destructive work | HubTeardown |
| History of stuck finalizers during uninstall | HubTeardown |

Creating a HubTeardown with the default spec only inspects. A plain
`oc delete multiclusterhub` with no HubTeardown keeps its existing behaviour.

## Prerequisites

- The MultiClusterHub operator is installed and running.
- The `hubteardowns.operator.open-cluster-management.io` CRD is present.
- The report ConfigMap and the `HubTeardown` CR live in the operator namespace,
  which is preserved by design (the operator, CSV, Subscription and
  OperatorGroup survive a teardown).
- The user can create and patch `HubTeardown` resources.

## Workflow

### 1. Inspect

```bash
cat <<EOF | oc apply -f -
apiVersion: operator.open-cluster-management.io/v1
kind: HubTeardown
metadata:
  name: teardown
  namespace: open-cluster-management
spec:
  hubRef:
    apiVersion: operator.open-cluster-management.io/v1
    kind: MultiClusterHub
    name: multiclusterhub
    namespace: open-cluster-management
  dryRun: true
EOF
```

`spec.hubRef` is required. The controller targets exactly that MCH and never
infers a target from whichever MCH happens to exist. Only one HubTeardown may
exist in the cluster; the validating webhook rejects a second CR, regardless of
namespace or `hubRef`.

### 2. Read the report

```bash
oc get hubteardown teardown -n open-cluster-management
oc get hubteardown teardown -n open-cluster-management -o yaml
oc get configmap -n open-cluster-management \
  -l open-cluster-management.io/uninstall-report=true
```

The report ConfigMap carries `report.json` (complete typed inventory, blocker
and cloud-risk records), `report.txt` (human-readable), and `meta.json`
(generation, timestamps, discovery mode, exclusions). Status carries only the
first 32 blockers; the ConfigMap is the complete record. `status.summary` and
`status.cloudRisk` are never truncated.

### 3. Consent to the exact report

Consent is bound to the report digest, not its name:

```bash
DIGEST=$(oc get hubteardown teardown -n open-cluster-management \
  -o jsonpath='{.status.reportDigest}')
oc patch hubteardown teardown -n open-cluster-management --type=merge \
  -p "{\"spec\":{\"dryRun\":false,\"acknowledgedReportDigest\":\"${DIGEST}\"}}"
```

If the inspection regenerates the report, the digest changes and consent
lapses. An empty `status.reportDigest` means there is nothing to consent to yet.

If an action may orphan resources outside the cluster, read the cloud-risk
section and then acknowledge it explicitly:

```bash
oc patch hubteardown teardown -n open-cluster-management --type=merge \
  -p '{"spec":{"acknowledgeCloudResourceRisk":true}}'
```

This is the only consent flag. It is required for every cloud-risk action in
every escalation mode, including `Automatic`.

### 4. Monitor

```bash
oc get hubteardown teardown -n open-cluster-management -w
oc get hubteardown teardown -n open-cluster-management \
  -o jsonpath='{range .status.phases[*]}{.phase}: {.state} - {.message}{"\n"}{end}'
oc get events -n open-cluster-management \
  --field-selector reason!=LeaderElection --sort-by='.lastTimestamp' | grep -i teardown
```

### 5. Pause

Setting `dryRun: true` pauses execution and re-scans. Completed phases are not
re-run when teardown resumes.

### 6. Verify the postcondition

Verify runs after the MCH is gone and writes `Complete` only when the declared
survivors are the only objects left. Anything else produces
`CompleteWithResidue` with a `status.residue` record. `Failed` is terminal and
carries the residue observed at the deadline.

## Spec reference

| Field | Type | Default | Description |
|---|---|---|---|
| `hubRef` | `ObjectReference` | required | Exact MultiClusterHub to tear down. |
| `dryRun` | `bool` | `true` | Inspect only. Set to `false` to execute. |
| `mce` | `Default` \| `Preserve` | `Default` | `Default` removes an MCE created by this MCH and preserves a pre-existing one. `Preserve` never touches MCE. Removing a pre-existing MCE is not offered. |
| `acknowledgeCloudResourceRisk` | `bool` | `false` | Accepts that an authorized action may orphan cloud resources. Required whenever `cloudRisk.mayOrphan` is true. |
| `acknowledgedReportDigest` | `string` | `""` | Digest of the report the administrator read. Must equal `status.reportDigest`. |
| `escalation.stallTimeout` | `duration` | `15m` | No-progress bound, measured over the blocker model. |
| `escalation.maxDuration` | `duration` | `2h` | Terminal bound; expiry writes `Failed` with the residue. |
| `escalation.mode` | `Manual` \| `Automatic` | `Manual` | `Automatic` may escalate a stalled non-cloud action without further consent; cloud-risk actions always require consent. |
| `forceFinalizerTimeout` | `duration` | deprecated | Fallback for `escalation.stallTimeout`. |
| `maxDuration` | `duration` | deprecated | Fallback for `escalation.maxDuration`. |

## Status reference

| Field | Meaning |
|---|---|
| `phase` | Current high-level phase. |
| `observedGeneration` | Generation reflected by the status. |
| `summary` | Never-truncated blocker and consent counts. |
| `blockers` / `blockersTruncated` | First 32 blockers, ordered by consent, confidence, age and name; the rest are counted. |
| `cloudRisk` | Aggregated by platform and resource type; never truncated. |
| `reportRef` / `reportDigest` | Current report and the digest consent must match. |
| `residue` | Postcondition record written by Verify or a terminal failure. |
| `phases` | Per-phase state and message. |
| `conditions` | Inspection, consent, stall, handoff and verification state. |

## Phases and jobs

The operator and the two Jobs interleave in this order:

| Order | Executor | Phase | Work |
|---:|---|---|---|
| 1 | prepare Job (`hubteardown-cleanup`, `--teardown-plan=prepare`) | `DisableAddons` | Clears addon placements, scales down addon controllers, and deletes `ManagedClusterAddOn` resources for non-local clusters. |
| 2 | prepare Job | `DeleteInfrastructureCRs` | Deletes HyperShift, Hive, assisted-installer and siteconfig infrastructure CRs while their controllers are alive. |
| 3 | prepare Job | `DetachManagedClusters` | Deletes non-local `ManagedCluster` resources. `local-cluster` is retained until MCH deletion and reported as an explicit disposition, not silently skipped. |
| 4 | handoff Job (`hubteardown-handoff`, `--teardown-plan=handoff`) | `RemoveBlockingCRs` | Deletes MCH-webhook blockers and sweeps dangling `ValidatingWebhookConfiguration`, `MutatingWebhookConfiguration` and `APIService` registrations whose backing Service is gone. |
| 5 | operator | `DeleteMCH` | Deletes exactly `spec.hubRef`; it never patches the MCH or a blocker finalizer. |
| 6 | handoff Job | `MonitorMCEChain` | Resolves finalizers blocking `finalizeHub`, then waits for the MCH and MCE chain to settle. `spec.mce` decides whether a pre-existing MCE is retained. |
| 7 | handoff Job | `DeleteACMCRDs` | Deletes only evidence-backed ACM-owned CRDs, disabling conversion webhooks first. `metal3.io` CRDs and instances are report-only. |
| 8 | operator | `Verifying` | Runs the postcondition and writes the terminal state. |

The operator waits for each handoff-Job phase to record completion before moving
to the next operator-owned step. `MonitorMCEChain` begins after
`RemoveBlockingCRs` completes and may start resolving finalizers before the
operator deletes the MCH; it does not report completion until the MCH and MCE
chain have settled.

Both Jobs run as ServiceAccount `hubteardown-handoff`, bound to the static
`acm-uninstall-handoff` ClusterRole shipped in the bundle. They use
`backoffLimit: 2` and a one-hour TTL after completion. The operator creates the
identity and the Jobs; the destructive phase verbs are carried by the Job's
static role. This feature does not add those destructive grants to the
operator's role. The operator's pre-existing broad permissions remain separate,
tracked conformance debt.

## Authorization model

Every destructive action goes through one authorizer:

- Ownership must be `Definitive` or `Strong`, backed by evidence such as the
  installer labels, an owner-reference chain, a labelled defining CRD, or a
  backing Service in an owned namespace. A name or API-group match is
  report-only.
- Removing a finalizer additionally requires `controllerAlive == false`.
  `nil` means unknown and is treated as alive.
- Cloud-risk actions additionally require valid digest-bound consent.
- Unknown or unowned dangling registrations are retained and reported.

There is no force-strip path and no Tier-based authorization. The Tier-1 table
survives only as the cloud-risk classification input.

## Troubleshooting

| Symptom | Check |
|---|---|
| Phase does not advance | `status.phases[*].message`, events, and the report's blocker rows. |
| Stuck in `ConsentRequired` | Compare `spec.acknowledgedReportDigest` with `status.reportDigest`; re-read a regenerated report. |
| Cloud action refused | `status.cloudRisk`, then set `acknowledgeCloudResourceRisk` after review. |
| MCH deleting but not gone | The handoff Job resolves evidence-backed operand finalizers before waiting; `controllerAlive: true` or unknown means wait. Check operator logs. |
| Prepare Job failed | Inspect `oc logs job/hubteardown-cleanup -n open-cluster-management`; delete the Job to retry. |
| Handoff Job failed | Inspect `oc logs job/hubteardown-handoff -n open-cluster-management`; the teardown ends `Failed` rather than waiting out the deadline. |

## Key files

| File | Purpose |
|---|---|
| `api/v1/hubteardown_types.go` | Spec, status, phases and escalation types |
| `controllers/hubteardown_controller.go` | Operator plan, inspection, stall and handoff lifecycle |
| `controllers/teardown_authorization.go` | Single authorizer for every destructive action |
| `controllers/teardown_execute.go` | Phase implementations |
| `controllers/teardown_finalizers.go` | Finalizer classification and policy-bound removal |
| `controllers/teardown_graph.go` | Layer-1 inventory walk and dependency graph |
| `controllers/teardown_job.go` | Prepare and handoff Job lifecycle |
| `controllers/teardown_verify.go` | Postcondition and terminal state |
| `config/rbac/handoff_role.yaml` | Static destructive identity used only by the Jobs |
