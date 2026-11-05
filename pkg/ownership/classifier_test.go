package ownership

import (
	"reflect"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/stolostron/multiclusterhub-operator/pkg/inventory"
)

var testMCH = types.NamespacedName{Name: "multiclusterhub", Namespace: "open-cluster-management"}

// testObservedAt is the scan time every evidence entry must carry.
var testObservedAt = metav1.NewTime(time.Date(2026, 11, 5, 3, 20, 0, 0, time.UTC))

func installerLabels() map[string]string {
	return map[string]string{
		"installer.name":      testMCH.Name,
		"installer.namespace": testMCH.Namespace,
	}
}

func metaObject(name, uid, apiVersion, kind string, labels map[string]string, owners ...metav1.OwnerReference) metav1.PartialObjectMetadata {
	obj := metav1.PartialObjectMetadata{}
	obj.SetName(name)
	obj.SetUID(types.UID(uid))
	obj.SetLabels(labels)
	obj.SetOwnerReferences(owners)
	obj.APIVersion = apiVersion
	obj.Kind = kind
	return obj
}

// nsObject is metaObject with a namespace, for namespaced objects such as the
// MultiClusterHub.
func nsObject(namespace, name, uid, apiVersion, kind string, labels map[string]string) metav1.PartialObjectMetadata {
	obj := metaObject(name, uid, apiVersion, kind, labels)
	obj.SetNamespace(namespace)
	return obj
}

func classify(t *testing.T, obj metav1.PartialObjectMetadata, objects []metav1.PartialObjectMetadata, crds []inventory.CRDRef, apiSvc map[string]string) OwnershipVerdict {
	t.Helper()
	all := append([]metav1.PartialObjectMetadata{obj}, objects...)
	return Classify(obj, inventory.NewIndex(all, crds, apiSvc), testMCH, testObservedAt)
}

func TestClassify(t *testing.T) {
	tests := []struct {
		name       string
		obj        metav1.PartialObjectMetadata
		objects    []metav1.PartialObjectMetadata
		crds       []inventory.CRDRef
		apiSvc     map[string]string
		wantConf   Confidence
		wantOwned  bool
		wantKind   EvidenceKind
		wantNoKind EvidenceKind
	}{
		{
			name:      "OC-1 rendered template carries installer label",
			obj:       metaObject("console", "uid-1", "apps/v1", "Deployment", installerLabels()),
			wantConf:  ConfidenceDefinitive,
			wantOwned: true,
			wantKind:  EvidenceInstallerLabel,
		},
		{
			name:       "OC-2 installer label for a different MCH is not ownership",
			obj:        metaObject("console", "uid-2", "apps/v1", "Deployment", map[string]string{"installer.name": "other", "installer.namespace": "other-ns"}),
			wantConf:   ConfidenceNone,
			wantOwned:  false,
			wantNoKind: EvidenceInstallerLabel,
		},
		{
			name: "OC-3 operand child resolves to a labelled owner",
			obj: metaObject("console-pod", "uid-3", "v1", "Pod", nil, metav1.OwnerReference{
				UID: "uid-owner", Kind: "ReplicaSet", Name: "console-rs",
			}),
			objects: []metav1.PartialObjectMetadata{
				metaObject("console-rs", "uid-owner", "apps/v1", "ReplicaSet", installerLabels()),
			},
			wantConf:  ConfidenceDefinitive,
			wantOwned: true,
			wantKind:  EvidenceOwnerRefChain,
		},
		{
			name: "OC-4 owner chain terminating at an unowned root",
			obj: metaObject("orphan", "uid-4", "v1", "ConfigMap", nil, metav1.OwnerReference{
				UID: "uid-root", Kind: "ConfigMap", Name: "root",
			}),
			objects: []metav1.PartialObjectMetadata{
				metaObject("root", "uid-root", "v1", "ConfigMap", nil),
			},
			wantConf:   ConfidenceNone,
			wantOwned:  false,
			wantNoKind: EvidenceOwnerRefChain,
		},
		{
			name: "OC-5 instance of a labelled ACM-defined type",
			obj:  metaObject("sample", "uid-5", "apps.example.com/v1", "Widget", nil),
			crds: []inventory.CRDRef{
				{Group: "apps.example.com", Kind: "Widget", Labels: installerLabels()},
			},
			wantConf:  ConfidenceStrong,
			wantOwned: true,
			wantKind:  EvidenceDefiningCRDLabelled,
		},
		{
			name: "OC-6 aggregated API backed by an owned namespace",
			obj:  metaObject("metric", "uid-6", "metrics.example.com/v1", "Metric", nil),
			objects: []metav1.PartialObjectMetadata{
				metaObject(testMCH.Namespace, "uid-ns", "v1", "Namespace", installerLabels()),
			},
			apiSvc:    map[string]string{"metrics.example.com": testMCH.Namespace},
			wantConf:  ConfidenceStrong,
			wantOwned: true,
			wantKind:  EvidenceAPIServiceBacking,
		},
		{
			name:       "OC-7 name match only is weak",
			obj:        metaObject("open-cluster-management-hub", "uid-7", "v1", "Service", nil),
			wantConf:   ConfidenceWeak,
			wantOwned:  false,
			wantKind:   EvidenceNameMatch,
			wantNoKind: EvidenceInstallerLabel,
		},
		{
			name:       "OC-12 customer namespace named hive-prod is not owned",
			obj:        metaObject("hive-prod", "uid-12", "v1", "Namespace", nil),
			wantConf:   ConfidenceNone,
			wantOwned:  false,
			wantNoKind: EvidenceInstallerLabel,
		},
		{
			name:      "OC-14 the MultiClusterHub is owned by identity",
			obj:       nsObject(testMCH.Namespace, testMCH.Name, "uid-14", "operator.open-cluster-management.io/v1", "MultiClusterHub", nil),
			wantConf:  ConfidenceDefinitive,
			wantOwned: true,
			wantKind:  EvidenceMCHIdentity,
		},
		{
			name:      "OC-15 instance of an ACM-owned CRD",
			obj:       metaObject("prod", "uid-15", "cluster.open-cluster-management.io/v1", "ManagedCluster", nil),
			crds:      []inventory.CRDRef{{Group: "cluster.open-cluster-management.io", Kind: "ManagedCluster", ACMOwned: true}},
			wantConf:  ConfidenceStrong,
			wantOwned: true,
			wantKind:  EvidenceACMGroupCRD,
		},
		{
			name:       "OC-16 scanned but not ACM-owned CRD is not ownership",
			obj:        metaObject("prod", "uid-16", "metal3.io/v1alpha1", "BareMetalHost", nil),
			crds:       []inventory.CRDRef{{Group: "metal3.io", Kind: "BareMetalHost", ACMOwned: false}},
			wantConf:   ConfidenceNone,
			wantOwned:  false,
			wantNoKind: EvidenceACMGroupCRD,
		},
		{
			name:      "OC-17 managed-by marker",
			obj:       metaObject("multiclusterengine", "uid-17", "multicluster.openshift.io/v1", "MultiClusterEngine", map[string]string{"multiclusterhubs.operator.open-cluster-management.io/managed-by": "true"}),
			wantConf:  ConfidenceStrong,
			wantOwned: true,
			wantKind:  EvidenceManagedByLabel,
		},
		{
			name:      "OC-18 backplaneconfig marker",
			obj:       metaObject("cluster-manager", "uid-18", "operator.open-cluster-management.io/v1", "ClusterManager", map[string]string{"backplaneconfig.name": "multiclusterengine"}),
			wantConf:  ConfidenceStrong,
			wantOwned: true,
			wantKind:  EvidenceManagedByLabel,
		},
		{
			name:      "OC-19 OLM owner is an ACM CSV",
			obj:       metaObject("multiclusterhub-operator", "uid-19", "apps/v1", "Deployment", map[string]string{"olm.managed": "true", "olm.owner": "advanced-cluster-management.v5.0.0", "olm.owner.namespace": "open-cluster-management"}),
			wantConf:  ConfidenceStrong,
			wantOwned: true,
			wantKind:  EvidenceOLMOwner,
		},
		{
			name:       "OC-20 OLM owner from another product is not ownership",
			obj:        metaObject("other-operator", "uid-20", "apps/v1", "Deployment", map[string]string{"olm.managed": "true", "olm.owner": "openshift-gitops-operator.v1.22.2", "olm.owner.namespace": "openshift-gitops-operator"}),
			wantConf:   ConfidenceNone,
			wantOwned:  false,
			wantNoKind: EvidenceOLMOwner,
		},
		{
			name:       "OC-13 unrelated webhook named archive-policy-validator",
			obj:        metaObject("archive-policy-validator", "uid-13", "admissionregistration.k8s.io/v1", "ValidatingWebhookConfiguration", nil),
			wantConf:   ConfidenceNone,
			wantOwned:  false,
			wantNoKind: EvidenceNameMatch,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := classify(t, tt.obj, tt.objects, tt.crds, tt.apiSvc)
			if got.Confidence != tt.wantConf {
				t.Fatalf("confidence = %s, want %s (evidence %+v)", got.Confidence, tt.wantConf, got.Evidence)
			}
			if got.Owned != tt.wantOwned {
				t.Fatalf("owned = %v, want %v", got.Owned, tt.wantOwned)
			}
			if tt.wantKind != "" && !hasEvidence(got.Evidence, tt.wantKind) {
				t.Fatalf("missing evidence kind %s in %+v", tt.wantKind, got.Evidence)
			}
			if tt.wantNoKind != "" && hasEvidence(got.Evidence, tt.wantNoKind) {
				t.Fatalf("unexpected evidence kind %s in %+v", tt.wantNoKind, got.Evidence)
			}
			for _, ev := range got.Evidence {
				if !ev.Observed.Equal(&testObservedAt) {
					t.Fatalf("evidence %s observed = %v, want %v", ev.Kind, ev.Observed, testObservedAt)
				}
			}
		})
	}
}

func TestConfidenceFromEvidence(t *testing.T) {
	tests := []struct {
		name     string
		evidence []Evidence
		want     Confidence
	}{
		{
			name:     "OC-8 two weak observations do not make a strong one",
			evidence: []Evidence{{Kind: EvidenceNameMatch}, {Kind: EvidenceNameMatch}},
			want:     ConfidenceWeak,
		},
		{
			name:     "OC-9 controller absent alone is not ownership",
			evidence: []Evidence{{Kind: EvidenceControllerAbsent}},
			want:     ConfidenceNone,
		},
		{
			name:     "OC-10 controller absent plus installer label stays definitive",
			evidence: []Evidence{{Kind: EvidenceControllerAbsent}, {Kind: EvidenceInstallerLabel}},
			want:     ConfidenceDefinitive,
		},
		{
			name:     "OC-11 no evidence",
			evidence: nil,
			want:     ConfidenceNone,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ConfidenceFromEvidence(tt.evidence); got != tt.want {
				t.Fatalf("confidence = %s, want %s", got, tt.want)
			}
		})
	}
}

func TestClassifyDeterministic(t *testing.T) {
	obj := metaObject("open-cluster-management-hub", "uid-14", "v1", "Service", nil)
	idx := inventory.NewIndex([]metav1.PartialObjectMetadata{obj}, nil, nil)

	first := Classify(obj, idx, testMCH, testObservedAt)
	for i := 0; i < 100; i++ {
		got := Classify(obj, idx, testMCH, testObservedAt)
		if !reflect.DeepEqual(first, got) {
			t.Fatalf("iteration %d produced different verdict: %+v vs %+v", i, first, got)
		}
	}
}

func hasEvidence(evidence []Evidence, kind EvidenceKind) bool {
	for _, ev := range evidence {
		if ev.Kind == kind {
			return true
		}
	}
	return false
}
