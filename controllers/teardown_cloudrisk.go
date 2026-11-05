// Copyright Contributors to the Open Cluster Management project

package controllers

import "strings"

// knownCloudPlatforms are the platforms on which removing a Tier-1 finalizer
// can leave resources behind outside the cluster. Membership is a positive
// statement: a platform is cloud-provisioned only when it is named here.
var knownCloudPlatforms = map[string]struct{}{
	"AWS":       {},
	"AZURE":     {},
	"GCP":       {},
	"OPENSTACK": {},
	"IBMCLOUD":  {},
	"POWERVC":   {},
}

// knownNonCloudPlatforms are platforms the classifier recognises but which do
// not provision cloud infrastructure outside the cluster. KubeVirt guests are
// VMs inside the hub; removing their finalizers cannot orphan a cloud account.
var knownNonCloudPlatforms = map[string]struct{}{
	"KUBEVIRT": {},
}

// isKnownCloudPlatform reports whether acting on this platform can orphan
// cloud resources. Empty and unfamiliar strings are false: the caller records
// them as unknown rather than guessing.
func isKnownCloudPlatform(platform string) bool {
	_, ok := knownCloudPlatforms[strings.ToUpper(strings.TrimSpace(platform))]
	return ok
}

// isKnownPlatform reports whether the platform is one the classifier can
// reason about, cloud or not.
func isKnownPlatform(platform string) bool {
	normalised := strings.ToUpper(strings.TrimSpace(platform))
	if _, ok := knownCloudPlatforms[normalised]; ok {
		return true
	}
	_, ok := knownNonCloudPlatforms[normalised]
	return ok
}

// requiresCloudAcknowledgement is the single predicate shared by the report,
// policy inputs and execution gates. It is true only when the finalizer is
// Tier 1 AND the object's platform is a known cloud platform.
func requiresCloudAcknowledgement(finalizer, platform string) bool {
	return IsCloudProtectingFinalizer(finalizer) && isKnownCloudPlatform(platform)
}
