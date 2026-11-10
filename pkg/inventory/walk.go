// Package inventory holds the read-only object index that the teardown
// inspector builds from a hub.
package inventory

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/metadata"
)

const (
	// DefaultPageLimit is the page size for every metadata list. Measured:
	// larger pages make the API server build bigger responses without
	// reducing the number of round trips enough to pay for it.
	DefaultPageLimit int64 = 500

	// DefaultConcurrency bounds concurrent type-walks. Measured: the walk is
	// further 2-5x faster than sequential up to 8, and gains flatten after.
	DefaultConcurrency = 8

	// DefaultListTimeout bounds one page of one resource.
	DefaultListTimeout = 10 * time.Second

	// DefaultDiscoveryTimeout bounds the discovery phase.
	DefaultDiscoveryTimeout = 10 * time.Second

	// DefaultTotalTimeout bounds the whole walk. An inventory that cannot
	// finish in ten minutes is not going to become useful by waiting.
	DefaultTotalTimeout = 10 * time.Minute
)

// Discovery paths recorded in WalkResult.DiscoveryPath.
const (
	DiscoveryPathAggregated = "Aggregated"
	DiscoveryPathLegacy     = "Legacy"
)

// Exclusion is one resource deliberately left out of the walk. The reason is
// rendered verbatim in the report: an inventory that silently omits a resource
// is worse than one that never claimed to be complete.
type Exclusion struct {
	Group    string
	Resource string
	Reason   string
}

// DefaultExclusions returns the resources left out unless the caller opts in.
//
// events carry no ownership signal for teardown and can be the largest
// collection on a hub. projectrequests is a virtual request endpoint: it
// advertises list, but a metadata list returns a Status object instead of
// object metadata.
func DefaultExclusions() []Exclusion {
	return []Exclusion{
		{
			Group:    "",
			Resource: "events",
			Reason:   "events carry no ownership signal for teardown and can be the largest collection on a hub",
		},
		{
			Group:    "events.k8s.io",
			Resource: "events",
			Reason:   "events carry no ownership signal for teardown and can be the largest collection on a hub",
		},
		{
			Group:    "project.openshift.io",
			Resource: "projectrequests",
			Reason:   "virtual create-only request resource; a metadata list returns a Status object, not object metadata",
		},
	}
}

// Incomplete records one reason the walk did not see everything. A dangling
// group, a per-type timeout and a global-deadline cutoff all land here,
// because they all invalidate the same thing: claims about absence.
type Incomplete struct {
	Group    string
	Resource string
	Reason   string
}

// WalkOptions controls the walk. The zero value is usable: every field has a
// measured default.
type WalkOptions struct {
	// PageLimit is the metadata list page size.
	PageLimit int64

	// Concurrency bounds concurrent type-walks.
	Concurrency int

	// DiscoveryTimeout bounds the discovery phase.
	DiscoveryTimeout time.Duration

	// ListTimeout bounds a single page of a single resource.
	ListTimeout time.Duration

	// TotalTimeout bounds the whole walk.
	TotalTimeout time.Duration

	// Exclusions replaces the default exclusion list. Nil means
	// DefaultExclusions, unless IncludeEvents is set.
	Exclusions []Exclusion

	// IncludeEvents keeps events in the walk. It exists so the default can be
	// overridden deliberately rather than by editing code.
	IncludeEvents bool
}

func (o WalkOptions) withDefaults() WalkOptions {
	if o.PageLimit <= 0 {
		o.PageLimit = DefaultPageLimit
	}
	if o.Concurrency <= 0 {
		o.Concurrency = DefaultConcurrency
	}
	if o.DiscoveryTimeout <= 0 {
		o.DiscoveryTimeout = DefaultDiscoveryTimeout
	}
	if o.ListTimeout <= 0 {
		o.ListTimeout = DefaultListTimeout
	}
	if o.TotalTimeout <= 0 {
		o.TotalTimeout = DefaultTotalTimeout
	}
	if o.Exclusions == nil {
		if o.IncludeEvents {
			o.Exclusions = []Exclusion{}
		} else {
			o.Exclusions = DefaultExclusions()
		}
	}
	return o
}

// WalkResult is the outcome of one walk. It is never nil, even on error: a
// partial inventory plus a stated reason is more useful than nothing.
type WalkResult struct {
	// Objects are the metadata-only objects the walk saw, deduplicated by UID.
	Objects []Object

	// Incomplete lists every reason the inventory may be missing something.
	Incomplete []Incomplete

	// Excluded is the exclusion list that was applied.
	Excluded []Exclusion

	// DiscoveryPath is DiscoveryPathAggregated or DiscoveryPathLegacy.
	DiscoveryPath string

	// Pages is the number of metadata list pages fetched.
	Pages int

	// InventoryComplete is false when any Incomplete entry exists. Verify must
	// not write Complete against an incomplete inventory: absence is exactly
	// what a truncated walk cannot prove.
	InventoryComplete bool
}

// PageLister fetches one page of PartialObjectMetadata. The full-object
// clients are deliberately absent from this interface so the walk cannot
// accidentally list real objects.
type PageLister interface {
	ListPage(ctx context.Context, gvr schema.GroupVersionResource, opts metav1.ListOptions) (*metav1.PartialObjectMetadataList, error)
}

// MetadataLister adapts a metadata.Interface to PageLister.
type MetadataLister struct {
	Client metadata.Interface
}

// ListPage implements PageLister.
func (m MetadataLister) ListPage(ctx context.Context, gvr schema.GroupVersionResource, opts metav1.ListOptions) (*metav1.PartialObjectMetadataList, error) {
	return m.Client.Resource(gvr).List(ctx, opts)
}

// Walker performs the read-only inventory walk.
type Walker struct {
	Discovery discovery.DiscoveryInterface
	Lister    PageLister
}

// Walk builds the inventory. It returns a non-nil result on every path,
// including errors: callers must be able to report what was seen even when the
// walk could not finish.
func (w *Walker) Walk(ctx context.Context, opts WalkOptions) (*WalkResult, error) {
	opts = opts.withDefaults()

	result := &WalkResult{Excluded: opts.Exclusions}

	walkCtx, cancel := context.WithTimeout(ctx, opts.TotalTimeout)
	defer cancel()

	discovered := w.discover(opts.DiscoveryTimeout)
	result.DiscoveryPath = discovered.path
	if discovered.timedOut {
		result.Incomplete = append(result.Incomplete, Incomplete{
			Reason: fmt.Sprintf("discovery exceeded timeout of %s", opts.DiscoveryTimeout),
		})
		result.InventoryComplete = false
		return result, nil
	}
	if discovered.err != nil {
		var groupFailed *discovery.ErrGroupDiscoveryFailed
		if !errors.As(discovered.err, &groupFailed) {
			result.Incomplete = append(result.Incomplete, Incomplete{Reason: fmt.Sprintf("discovery failed: %v", discovered.err)})
			result.InventoryComplete = false
			return result, fmt.Errorf("discovery: %w", discovered.err)
		}
		// A failed group is data, not a reason to abort: the rest of the
		// cluster is still worth inspecting.
		for gv, groupErr := range groupFailed.Groups {
			result.Incomplete = append(result.Incomplete, Incomplete{
				Group:  gv.Group,
				Reason: fmt.Sprintf("group discovery failed: %v", groupErr),
			})
		}
	}
	resources := discovered.resources

	gvrs := w.listableResources(resources, opts.Exclusions)
	result.Pages = w.walkResources(walkCtx, gvrs, opts, result)

	if walkCtx.Err() != nil {
		result.Incomplete = append(result.Incomplete, Incomplete{
			Reason: fmt.Sprintf("walk exceeded total timeout of %s", opts.TotalTimeout),
		})
	}

	result.Objects = dedupeByUID(result.Objects)
	result.InventoryComplete = len(result.Incomplete) == 0

	return result, nil
}

type discoveryOutcome struct {
	path      string
	resources []*metav1.APIResourceList
	err       error
	timedOut  bool
}

// discover bounds the discovery phase.
//
// client-go's discovery interface has no context-taking method, so the call
// cannot be cancelled. On timeout the walk stops waiting and reports the
// inventory as incomplete; the goroutine finishes when the HTTP client times
// out. The channel is buffered so that goroutine never blocks on send.
func (w *Walker) discover(timeout time.Duration) discoveryOutcome {
	done := make(chan discoveryOutcome, 1)

	go func() {
		outcome := discoveryOutcome{path: w.discoveryPath()}
		outcome.resources, outcome.err = w.Discovery.ServerPreferredResources()
		done <- outcome
	}()

	select {
	case outcome := <-done:
		return outcome
	case <-time.After(timeout):
		return discoveryOutcome{timedOut: true}
	}
}

func (w *Walker) discoveryPath() string {
	groups, err := w.Discovery.ServerGroups()
	if err != nil {
		return DiscoveryPathLegacy
	}
	for _, group := range groups.Groups {
		if group.Name != "apidiscovery.k8s.io" {
			continue
		}
		for _, version := range group.Versions {
			if version.Version == "v2" {
				return DiscoveryPathAggregated
			}
		}
	}
	return DiscoveryPathLegacy
}

type listTarget struct {
	gvr  schema.GroupVersionResource
	kind string
}

func (w *Walker) listableResources(resources []*metav1.APIResourceList, exclusions []Exclusion) []listTarget {
	var targets []listTarget
	for _, resourceList := range resources {
		if resourceList == nil {
			continue
		}
		gv, err := schema.ParseGroupVersion(resourceList.GroupVersion)
		if err != nil {
			continue
		}
		for _, resource := range resourceList.APIResources {
			// Subresources cannot be listed and are not separate objects.
			if strings.Contains(resource.Name, "/") {
				continue
			}
			if !hasVerb(resource.Verbs, "list") {
				continue
			}
			if isExcluded(exclusions, gv.Group, resource.Name) {
				continue
			}
			targets = append(targets, listTarget{
				gvr:  gv.WithResource(resource.Name),
				kind: resource.Kind,
			})
		}
	}
	sort.Slice(targets, func(i, j int) bool {
		if targets[i].gvr.Group != targets[j].gvr.Group {
			return targets[i].gvr.Group < targets[j].gvr.Group
		}
		if targets[i].gvr.Version != targets[j].gvr.Version {
			return targets[i].gvr.Version < targets[j].gvr.Version
		}
		return targets[i].gvr.Resource < targets[j].gvr.Resource
	})
	return targets
}

// walkResources lists every GVR with a bounded worker pool and returns the
// number of pages fetched.
func (w *Walker) walkResources(ctx context.Context, targets []listTarget, opts WalkOptions, result *WalkResult) int {
	var (
		mu    sync.Mutex
		pages int
	)

	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(opts.Concurrency)

	for _, target := range targets {
		target := target
		group.Go(func() error {
			objects, fetched, err := w.listAll(groupCtx, target, opts)
			mu.Lock()
			defer mu.Unlock()
			pages += fetched
			if err != nil {
				result.Incomplete = append(result.Incomplete, Incomplete{
					Group:    target.gvr.Group,
					Resource: target.gvr.Resource,
					Reason:   err.Error(),
				})
				return nil
			}
			result.Objects = append(result.Objects, objects...)
			return nil
		})
	}
	// Tasks never return an error, so Wait only surfaces context cancellation.
	_ = group.Wait()

	return pages
}

func (w *Walker) listAll(ctx context.Context, target listTarget, opts WalkOptions) ([]Object, int, error) {
	var objects []Object
	pages := 0

	listOptions := metav1.ListOptions{Limit: opts.PageLimit}
	for {
		pageCtx, cancel := context.WithTimeout(ctx, opts.ListTimeout)
		page, err := w.Lister.ListPage(pageCtx, target.gvr, listOptions)
		cancel()
		if err != nil {
			return objects, pages, err
		}
		pages++
		// A metadata list returns meta.k8s.io/v1 / PartialObjectMetadata in
		// each item's TypeMeta. Stamp the discovered GVK back onto the item so
		// callers can classify it by its real group and kind.
		for i := range page.Items {
			page.Items[i].SetGroupVersionKind(target.gvr.GroupVersion().WithKind(target.kind))
		}
		objects = append(objects, page.Items...)

		if page.GetContinue() == "" {
			return objects, pages, nil
		}
		listOptions.Continue = page.GetContinue()
	}
}

func dedupeByUID(objects []Object) []Object {
	seen := make(map[string]bool, len(objects))
	deduped := make([]Object, 0, len(objects))
	for _, object := range objects {
		key := string(object.GetUID())
		if key == "" {
			key = object.GetNamespace() + "/" + object.GetName()
		}
		if seen[key] {
			continue
		}
		seen[key] = true
		deduped = append(deduped, object)
	}
	return deduped
}

func isExcluded(exclusions []Exclusion, group, resource string) bool {
	for _, exclusion := range exclusions {
		if exclusion.Group == group && exclusion.Resource == resource {
			return true
		}
	}
	return false
}

func hasVerb(verbs metav1.Verbs, want string) bool {
	for _, verb := range verbs {
		if verb == want {
			return true
		}
	}
	return false
}
