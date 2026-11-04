// Copyright Contributors to the Open Cluster Management project

package controllers

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"

	operatorv1 "github.com/stolostron/multiclusterhub-operator/api/v1"
	"github.com/stolostron/multiclusterhub-operator/pkg/verify"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	snapshotConfigMapPrefix = "hubteardown-snapshot-"
	snapshotKeyObjects      = "snapshot.json"
	snapshotLabelSnapshot   = "operator.open-cluster-management.io/teardown-snapshot"
)

// operatorNamespaceSnapshot is the pre-teardown record of the objects Verify
// later has to classify. It stores identity and Secret type metadata only: no
// Secret data and no object bodies ever enter this record.
type operatorNamespaceSnapshot struct {
	Teardown   string             `json:"teardown"`
	Namespace  string             `json:"namespace"`
	ObservedAt metav1.Time        `json:"observedAt"`
	Objects    []verify.ObjectRef `json:"objects"`
}

// snapshotConfigMapName is deterministic per teardown. The snapshot is never
// labelled as a report: pruneInspectReports selects on the report label and
// would otherwise delete the baseline the verification depends on.
func snapshotConfigMapName(td *operatorv1.HubTeardown) string {
	base := snapshotConfigMapPrefix + td.Name
	if len(base) <= reportMaxNameLength {
		return base
	}
	sum := sha256.Sum256([]byte(base))
	suffix := "-" + hex.EncodeToString(sum[:4])
	keep := reportMaxNameLength - len(snapshotConfigMapPrefix) - len(suffix)
	if keep < 1 {
		keep = 1
	}
	return snapshotConfigMapPrefix + td.Name[:minInt(keep, len(td.Name))] + suffix
}

// listOperatorNamespaceObjects is the single definition of "the core objects
// the operator namespace is judged on". Capture and verification both call it,
// so the snapshot and the observation cannot drift apart.
func (r *HubTeardownReconciler) listOperatorNamespaceObjects(ctx context.Context, td *operatorv1.HubTeardown) ([]verify.ObjectRef, []string) {
	var objects []verify.ObjectRef
	var incomplete []string

	cmList := &corev1.ConfigMapList{}
	if err := r.Client.List(ctx, cmList, client.InNamespace(td.Namespace)); err != nil {
		incomplete = append(incomplete, "configmaps: "+err.Error())
	} else {
		for i := range cmList.Items {
			objects = append(objects, verify.ObjectRef{
				Kind:      "ConfigMap",
				Namespace: cmList.Items[i].Namespace,
				Name:      cmList.Items[i].Name,
			})
		}
	}

	secretList := &corev1.SecretList{}
	if err := r.Client.List(ctx, secretList, client.InNamespace(td.Namespace)); err != nil {
		incomplete = append(incomplete, "secrets: "+err.Error())
	} else {
		for i := range secretList.Items {
			secret := &secretList.Items[i]
			ref := verify.ObjectRef{
				Kind:      "Secret",
				Namespace: secret.Namespace,
				Name:      secret.Name,
			}
			// Only a token Secret needs the extra evidence; recording the type
			// of every Secret would widen the snapshot without a consumer.
			if secret.Type == corev1.SecretTypeServiceAccountToken {
				ref.SecretType = string(secret.Type)
				ref.ServiceAccount = secret.Annotations[corev1.ServiceAccountNameKey]
			}
			objects = append(objects, ref)
		}
	}

	return objects, incomplete
}

// ensureOperatorNamespaceSnapshot records the pre-teardown object set once. A
// second call returns the first capture unchanged: a resumed run must never
// re-baseline onto a half-destroyed namespace and then call every deleted
// object "never existed".
func (r *HubTeardownReconciler) ensureOperatorNamespaceSnapshot(ctx context.Context, td *operatorv1.HubTeardown) (*corev1.ConfigMap, bool, error) {
	key := types.NamespacedName{Name: snapshotConfigMapName(td), Namespace: teardownNamespace(td)}

	existing := &corev1.ConfigMap{}
	err := r.Client.Get(ctx, key, existing)
	switch {
	case err == nil:
		return existing, true, nil
	case !apierrors.IsNotFound(err):
		return nil, false, fmt.Errorf("reading teardown snapshot: %w", err)
	}

	objects, incomplete := r.listOperatorNamespaceObjects(ctx, td)
	if len(incomplete) > 0 {
		// A partial baseline would make absent objects look "reported as
		// present before teardown" when they were never looked at. Record
		// nothing and let Verify say it has no snapshot.
		return nil, false, fmt.Errorf("capturing teardown snapshot: %s", strings.Join(incomplete, "; "))
	}

	body, err := json.Marshal(operatorNamespaceSnapshot{
		Teardown:   td.Name,
		Namespace:  td.Namespace,
		ObservedAt: metav1.Now(),
		Objects:    objects,
	})
	if err != nil {
		return nil, false, fmt.Errorf("encoding teardown snapshot: %w", err)
	}

	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      key.Name,
			Namespace: key.Namespace,
			Labels: map[string]string{
				"app":                 "hubteardown",
				snapshotLabelSnapshot: "true",
			},
		},
		Data: map[string]string{snapshotKeyObjects: string(body)},
	}
	if err := r.Client.Create(ctx, cm); err != nil {
		if apierrors.IsAlreadyExists(err) {
			// Another reconcile won the race; its capture is the pre-teardown
			// one, so read it rather than replace it.
			current := &corev1.ConfigMap{}
			if getErr := r.Client.Get(ctx, key, current); getErr == nil {
				return current, true, nil
			}
		}
		return nil, false, fmt.Errorf("creating teardown snapshot: %w", err)
	}
	return cm, false, nil
}

// loadOperatorNamespaceSnapshot reads the capture. An absent or unreadable
// record is reported as absent, never as an empty snapshot.
func (r *HubTeardownReconciler) loadOperatorNamespaceSnapshot(ctx context.Context, td *operatorv1.HubTeardown) (operatorNamespaceSnapshot, bool, error) {
	cm := &corev1.ConfigMap{}
	key := types.NamespacedName{Name: snapshotConfigMapName(td), Namespace: teardownNamespace(td)}
	if err := r.Client.Get(ctx, key, cm); err != nil {
		if apierrors.IsNotFound(err) {
			return operatorNamespaceSnapshot{}, false, nil
		}
		return operatorNamespaceSnapshot{}, false, fmt.Errorf("reading teardown snapshot: %w", err)
	}

	raw := cm.Data[snapshotKeyObjects]
	if raw == "" {
		return operatorNamespaceSnapshot{}, false, nil
	}

	snapshot := operatorNamespaceSnapshot{}
	if err := json.Unmarshal([]byte(raw), &snapshot); err != nil {
		return operatorNamespaceSnapshot{}, false, nil
	}
	return snapshot, true, nil
}
