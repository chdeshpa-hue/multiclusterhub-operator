// Copyright Contributors to the Open Cluster Management project

package inventory

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/discovery"
	fakediscovery "k8s.io/client-go/discovery/fake"
	clienttesting "k8s.io/client-go/testing"
)

// stubDiscovery lets a test control the discovery result, including the
// group-discovery failure that a real client reports as ErrGroupDiscoveryFailed.
type stubDiscovery struct {
	*fakediscovery.FakeDiscovery
	resources []*metav1.APIResourceList
	groups    []metav1.APIGroup
	err       error
	block     time.Duration
}

func (s *stubDiscovery) ServerPreferredResources() ([]*metav1.APIResourceList, error) {
	time.Sleep(s.block)
	return s.resources, s.err
}

func (s *stubDiscovery) ServerGroups() (*metav1.APIGroupList, error) {
	time.Sleep(s.block)
	return &metav1.APIGroupList{Groups: s.groups}, nil
}

func newStubDiscovery(resources []*metav1.APIResourceList, groups ...string) *stubDiscovery {
	stub := &stubDiscovery{
		FakeDiscovery: &fakediscovery.FakeDiscovery{Fake: &clienttesting.Fake{}},
		resources:     resources,
	}
	for _, group := range groups {
		stub.groups = append(stub.groups, metav1.APIGroup{
			Name:     group,
			Versions: []metav1.GroupVersionForDiscovery{{GroupVersion: group + "/v2", Version: "v2"}},
		})
	}
	return stub
}

func listable(groupVersion string, names ...string) *metav1.APIResourceList {
	resourceList := &metav1.APIResourceList{GroupVersion: groupVersion}
	for _, name := range names {
		resourceList.APIResources = append(resourceList.APIResources, metav1.APIResource{
			Name:  name,
			Verbs: metav1.Verbs{"get", "list", "watch"},
		})
	}
	return resourceList
}

func listableWithKind(groupVersion, kind, name string) *metav1.APIResourceList {
	return &metav1.APIResourceList{
		GroupVersion: groupVersion,
		APIResources: []metav1.APIResource{
			{Name: name, Kind: kind, Verbs: metav1.Verbs{"get", "list", "watch"}},
		},
	}
}

type listerCall struct {
	gvr   schema.GroupVersionResource
	limit int64
}

// fakeLister serves scripted pages and records how it was called.
type fakeLister struct {
	mu       sync.Mutex
	pages    map[schema.GroupVersionResource][]*metav1.PartialObjectMetadataList
	errs     map[schema.GroupVersionResource]error
	delay    time.Duration
	calls    []listerCall
	inFlight int
	peak     int
}

func newFakeLister() *fakeLister {
	return &fakeLister{
		pages: map[schema.GroupVersionResource][]*metav1.PartialObjectMetadataList{},
		errs:  map[schema.GroupVersionResource]error{},
	}
}

func (f *fakeLister) setPages(gvr schema.GroupVersionResource, pages ...*metav1.PartialObjectMetadataList) {
	f.pages[gvr] = pages
}

func (f *fakeLister) ListPage(ctx context.Context, gvr schema.GroupVersionResource, opts metav1.ListOptions) (*metav1.PartialObjectMetadataList, error) {
	f.mu.Lock()
	f.calls = append(f.calls, listerCall{gvr: gvr, limit: opts.Limit})
	f.inFlight++
	if f.inFlight > f.peak {
		f.peak = f.inFlight
	}
	delay := f.delay
	f.mu.Unlock()

	defer func() {
		f.mu.Lock()
		f.inFlight--
		f.mu.Unlock()
	}()

	if delay > 0 {
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	if err, ok := f.errs[gvr]; ok {
		return nil, err
	}
	queued := f.pages[gvr]
	if len(queued) == 0 {
		return &metav1.PartialObjectMetadataList{}, nil
	}
	page := queued[0]
	f.pages[gvr] = queued[1:]
	return page, nil
}

func (f *fakeLister) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

func (f *fakeLister) calledFor(gvr schema.GroupVersionResource) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, call := range f.calls {
		if call.gvr == gvr {
			return true
		}
	}
	return false
}

func object(uid, namespace, name string, gvk schema.GroupVersionKind) Object {
	obj := Object{}
	obj.SetUID(types.UID(uid))
	obj.SetNamespace(namespace)
	obj.SetName(name)
	obj.SetGroupVersionKind(gvk)
	return obj
}

var (
	gvrWidgets = schema.GroupVersionResource{Group: "acme.io", Version: "v1", Resource: "widgets"}
	gvrGadgets = schema.GroupVersionResource{Group: "acme.io", Version: "v1", Resource: "gadgets"}
)

// I-1: aggregated discovery is recorded when the server advertises it.
func TestWalkRecordsAggregatedDiscovery(t *testing.T) {
	walker := &Walker{
		Discovery: newStubDiscovery([]*metav1.APIResourceList{listable("acme.io/v1", "widgets")}, "apidiscovery.k8s.io"),
		Lister:    newFakeLister(),
	}

	result, err := walker.Walk(context.Background(), WalkOptions{})
	if err != nil {
		t.Fatalf("walk: %v", err)
	}
	if result.DiscoveryPath != DiscoveryPathAggregated {
		t.Fatalf("discovery path = %q, want %q", result.DiscoveryPath, DiscoveryPathAggregated)
	}
}

// I-2: legacy discovery is recorded when the aggregated group is absent.
func TestWalkRecordsLegacyDiscovery(t *testing.T) {
	walker := &Walker{
		Discovery: newStubDiscovery([]*metav1.APIResourceList{listable("acme.io/v1", "widgets")}, "apps"),
		Lister:    newFakeLister(),
	}

	result, err := walker.Walk(context.Background(), WalkOptions{})
	if err != nil {
		t.Fatalf("walk: %v", err)
	}
	if result.DiscoveryPath != DiscoveryPathLegacy {
		t.Fatalf("discovery path = %q, want %q", result.DiscoveryPath, DiscoveryPathLegacy)
	}
}

// I-3: the only data source is PageLister. The type system forbids full
// objects; this asserts the walk actually went through the injected lister and
// did not take some other path.
func TestWalkListsOnlyThroughPageLister(t *testing.T) {
	lister := newFakeLister()
	walker := &Walker{
		Discovery: newStubDiscovery([]*metav1.APIResourceList{listable("acme.io/v1", "widgets", "gadgets")}),
		Lister:    lister,
	}

	if _, err := walker.Walk(context.Background(), WalkOptions{}); err != nil {
		t.Fatalf("walk: %v", err)
	}
	if got := lister.callCount(); got != 2 {
		t.Fatalf("expected exactly 2 lister calls (one per GVR), got %d", got)
	}
}

// A metadata list leaves TypeMeta at meta.k8s.io/v1 / PartialObjectMetadata.
// The walk must replace it with the discovered GVK, otherwise the graph's
// ACM-group filter drops every object and the teardown sees an empty cluster.
func TestWalkStampsDiscoveredGroupVersionKind(t *testing.T) {
	lister := newFakeLister()
	lister.setPages(gvrWidgets, &metav1.PartialObjectMetadataList{Items: []Object{
		object("1", "a", "one", schema.GroupVersionKind{Group: "meta.k8s.io", Version: "v1", Kind: "PartialObjectMetadata"}),
	}})

	walker := &Walker{
		Discovery: newStubDiscovery([]*metav1.APIResourceList{listableWithKind("acme.io/v1", "Widget", "widgets")}),
		Lister:    lister,
	}

	result, err := walker.Walk(context.Background(), WalkOptions{})
	if err != nil {
		t.Fatalf("walk: %v", err)
	}
	if len(result.Objects) != 1 {
		t.Fatalf("expected 1 object, got %d", len(result.Objects))
	}
	want := schema.GroupVersionKind{Group: "acme.io", Version: "v1", Kind: "Widget"}
	if got := result.Objects[0].GroupVersionKind(); got != want {
		t.Fatalf("object GVK = %v, want %v", got, want)
	}
}

// I-4: pagination uses the default page size and follows every continue token.
func TestWalkPaginates(t *testing.T) {
	lister := newFakeLister()
	first := &metav1.PartialObjectMetadataList{Items: []Object{object("1", "a", "one", schema.GroupVersionKind{})}}
	first.SetContinue("token-2")
	second := &metav1.PartialObjectMetadataList{Items: []Object{object("2", "a", "two", schema.GroupVersionKind{})}}
	second.SetContinue("token-3")
	third := &metav1.PartialObjectMetadataList{Items: []Object{object("3", "a", "three", schema.GroupVersionKind{})}}
	lister.setPages(gvrWidgets, first, second, third)

	walker := &Walker{
		Discovery: newStubDiscovery([]*metav1.APIResourceList{listable("acme.io/v1", "widgets")}),
		Lister:    lister,
	}

	result, err := walker.Walk(context.Background(), WalkOptions{})
	if err != nil {
		t.Fatalf("walk: %v", err)
	}
	if len(result.Objects) != 3 {
		t.Fatalf("expected 3 objects across pages, got %d", len(result.Objects))
	}
	if result.Pages != 3 {
		t.Fatalf("expected 3 pages, got %d", result.Pages)
	}
	for _, call := range lister.calls {
		if call.limit != DefaultPageLimit {
			t.Fatalf("page limit = %d, want %d", call.limit, DefaultPageLimit)
		}
	}
	if !result.InventoryComplete {
		t.Fatalf("complete walk reported incomplete: %+v", result.Incomplete)
	}
}

// I-5: concurrency never exceeds the configured bound.
func TestWalkBoundsConcurrency(t *testing.T) {
	const typesCount = 40
	names := make([]string, 0, typesCount)
	for i := 0; i < typesCount; i++ {
		names = append(names, fmt.Sprintf("type%02d", i))
	}

	lister := newFakeLister()
	lister.delay = 2 * time.Millisecond

	walker := &Walker{
		Discovery: newStubDiscovery([]*metav1.APIResourceList{listable("acme.io/v1", names...)}),
		Lister:    lister,
	}

	if _, err := walker.Walk(context.Background(), WalkOptions{}); err != nil {
		t.Fatalf("walk: %v", err)
	}
	if lister.peak > DefaultConcurrency {
		t.Fatalf("peak concurrency = %d, want <= %d", lister.peak, DefaultConcurrency)
	}
	if lister.peak < 2 {
		t.Fatalf("peak concurrency = %d, the walk did not run in parallel", lister.peak)
	}
}

// I-6: a failed group is recorded and the rest of the walk continues.
func TestWalkRecordsGroupDiscoveryFailure(t *testing.T) {
	stub := newStubDiscovery([]*metav1.APIResourceList{listable("acme.io/v1", "widgets")})
	stub.err = &discovery.ErrGroupDiscoveryFailed{
		Groups: map[schema.GroupVersion]error{
			{Group: "broken.io", Version: "v1"}: errors.New("the server is currently unable to handle the request"),
		},
	}

	walker := &Walker{Discovery: stub, Lister: newFakeLister()}

	result, err := walker.Walk(context.Background(), WalkOptions{})
	if err != nil {
		t.Fatalf("walk must not abort on a failed group: %v", err)
	}
	if len(result.Objects) != 0 {
		t.Fatalf("expected no objects from a stub with no pages, got %d", len(result.Objects))
	}
	if result.InventoryComplete {
		t.Fatalf("a failed group must make the inventory incomplete")
	}
	if len(result.Incomplete) != 1 || result.Incomplete[0].Group != "broken.io" {
		t.Fatalf("failed group not recorded: %+v", result.Incomplete)
	}
}

// I-7: a per-type timeout is recorded as incomplete and does not abort others.
func TestWalkRecordsPerTypeTimeout(t *testing.T) {
	lister := newFakeLister()
	lister.delay = 200 * time.Millisecond
	lister.setPages(gvrGadgets, &metav1.PartialObjectMetadataList{Items: []Object{object("9", "a", "nine", schema.GroupVersionKind{})}})

	walker := &Walker{
		Discovery: newStubDiscovery([]*metav1.APIResourceList{listable("acme.io/v1", "widgets", "gadgets")}),
		Lister:    lister,
	}

	// Both types will exceed the per-type timeout, which is the point: the
	// walk records each one and still returns a result.
	result, err := walker.Walk(context.Background(), WalkOptions{ListTimeout: 20 * time.Millisecond})
	if err != nil {
		t.Fatalf("walk: %v", err)
	}
	if result.InventoryComplete {
		t.Fatalf("timeouts must make the inventory incomplete")
	}
	if len(result.Incomplete) != 2 {
		t.Fatalf("expected 2 timeout entries, got %+v", result.Incomplete)
	}
}

// I-8: the walk performs exactly one list call per resource and no follow-up
// reads. ownerRef resolution is the index's job and must not hit the API.
func TestWalkIssuesNoFollowUpReads(t *testing.T) {
	lister := newFakeLister()
	lister.setPages(gvrWidgets, &metav1.PartialObjectMetadataList{Items: []Object{
		object("leaf", "a", "leaf", schema.GroupVersionKind{}),
	}})

	walker := &Walker{
		Discovery: newStubDiscovery([]*metav1.APIResourceList{listable("acme.io/v1", "widgets")}),
		Lister:    lister,
	}

	result, err := walker.Walk(context.Background(), WalkOptions{})
	if err != nil {
		t.Fatalf("walk: %v", err)
	}
	idx := NewIndex(result.Objects, nil, nil)
	if _, ok := idx.Get("leaf"); !ok {
		t.Fatalf("walked object not resolvable by UID")
	}
	if lister.callCount() != 1 {
		t.Fatalf("follow-up reads happened: %d calls for 1 resource", lister.callCount())
	}
}

// I-9: events are excluded and the exclusion is reported.
func TestWalkExcludesEventsAndSaysSo(t *testing.T) {
	lister := newFakeLister()
	walker := &Walker{
		Discovery: newStubDiscovery([]*metav1.APIResourceList{
			listable("acme.io/v1", "widgets"),
			listable("v1", "events", "pods"),
			listable("events.k8s.io/v1", "events"),
		}),
		Lister: lister,
	}

	result, err := walker.Walk(context.Background(), WalkOptions{})
	if err != nil {
		t.Fatalf("walk: %v", err)
	}
	if lister.calledFor(schema.GroupVersionResource{Group: "", Version: "v1", Resource: "events"}) {
		t.Fatalf("core events must not be listed")
	}
	if lister.calledFor(schema.GroupVersionResource{Group: "events.k8s.io", Version: "v1", Resource: "events"}) {
		t.Fatalf("events.k8s.io events must not be listed")
	}
	if len(result.Excluded) != len(DefaultExclusions()) {
		t.Fatalf("exclusions must be reported, got %+v", result.Excluded)
	}
	for _, exclusion := range result.Excluded {
		if exclusion.Reason == "" {
			t.Fatalf("exclusion %+v has no reason to render", exclusion)
		}
	}
}

// I-9b: opting in keeps events.
func TestWalkIncludeEventsOptIn(t *testing.T) {
	lister := newFakeLister()
	walker := &Walker{
		Discovery: newStubDiscovery([]*metav1.APIResourceList{
			listable("acme.io/v1", "widgets"),
			listable("v1", "events"),
		}),
		Lister: lister,
	}

	result, err := walker.Walk(context.Background(), WalkOptions{IncludeEvents: true})
	if err != nil {
		t.Fatalf("walk: %v", err)
	}
	if !lister.calledFor(schema.GroupVersionResource{Group: "", Version: "v1", Resource: "events"}) {
		t.Fatalf("events should be listed when explicitly included")
	}
	if len(result.Excluded) != 0 {
		t.Fatalf("no exclusions expected, got %+v", result.Excluded)
	}
}

// I-10: the global deadline truncates the walk instead of hanging.
func TestWalkHonoursTotalTimeout(t *testing.T) {
	lister := newFakeLister()
	lister.delay = 500 * time.Millisecond

	walker := &Walker{
		Discovery: newStubDiscovery([]*metav1.APIResourceList{listable("acme.io/v1", "widgets", "gadgets")}),
		Lister:    lister,
	}

	start := time.Now()
	result, err := walker.Walk(context.Background(), WalkOptions{TotalTimeout: 30 * time.Millisecond})
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("walk must return a partial result, not an error: %v", err)
	}
	if result == nil {
		t.Fatalf("result must never be nil")
	}
	if result.InventoryComplete {
		t.Fatalf("a truncated walk must not report a complete inventory")
	}
	if elapsed > 2*time.Second {
		t.Fatalf("walk did not honour the total timeout: %s", elapsed)
	}
	found := false
	for _, incomplete := range result.Incomplete {
		if strings.Contains(incomplete.Reason, "total timeout") {
			found = true
		}
	}
	if !found {
		t.Fatalf("truncation reason missing: %+v", result.Incomplete)
	}
}

// I-11: an empty cluster is a complete, empty inventory.
func TestWalkEmptyCluster(t *testing.T) {
	walker := &Walker{
		Discovery: newStubDiscovery(nil),
		Lister:    newFakeLister(),
	}

	result, err := walker.Walk(context.Background(), WalkOptions{})
	if err != nil {
		t.Fatalf("walk: %v", err)
	}
	if !result.InventoryComplete {
		t.Fatalf("empty cluster must be complete: %+v", result.Incomplete)
	}
	if len(result.Objects) != 0 {
		t.Fatalf("expected no objects, got %d", len(result.Objects))
	}
}

// I-12: the same object returned by two resources is counted once.
func TestWalkDeduplicatesByUID(t *testing.T) {
	lister := newFakeLister()
	shared := object("same-uid", "a", "shared", schema.GroupVersionKind{})
	lister.setPages(gvrWidgets, &metav1.PartialObjectMetadataList{Items: []Object{shared}})
	lister.setPages(gvrGadgets, &metav1.PartialObjectMetadataList{Items: []Object{shared}})

	walker := &Walker{
		Discovery: newStubDiscovery([]*metav1.APIResourceList{listable("acme.io/v1", "widgets", "gadgets")}),
		Lister:    lister,
	}

	result, err := walker.Walk(context.Background(), WalkOptions{})
	if err != nil {
		t.Fatalf("walk: %v", err)
	}
	if len(result.Objects) != 1 {
		t.Fatalf("expected 1 object after dedup, got %d", len(result.Objects))
	}
}

// I-14: the zero value carries the measured defaults.
func TestWalkOptionsDefaults(t *testing.T) {
	opts := WalkOptions{}.withDefaults()

	if opts.PageLimit != DefaultPageLimit {
		t.Fatalf("PageLimit = %d", opts.PageLimit)
	}
	if opts.Concurrency != DefaultConcurrency {
		t.Fatalf("Concurrency = %d", opts.Concurrency)
	}
	if opts.ListTimeout != DefaultListTimeout {
		t.Fatalf("ListTimeout = %s", opts.ListTimeout)
	}
	if opts.TotalTimeout != DefaultTotalTimeout {
		t.Fatalf("TotalTimeout = %s", opts.TotalTimeout)
	}
	if len(opts.Exclusions) != len(DefaultExclusions()) {
		t.Fatalf("default exclusions not applied: %+v", opts.Exclusions)
	}
}

func TestDefaultExclusionsExcludeProjectRequests(t *testing.T) {
	for _, exclusion := range DefaultExclusions() {
		if exclusion.Group == "project.openshift.io" && exclusion.Resource == "projectrequests" {
			if exclusion.Reason == "" {
				t.Fatal("projectrequests exclusion must state why it is omitted")
			}
			return
		}
	}
	t.Fatal("projectrequests must be excluded from the metadata walk")
}

// I-16: a discovery endpoint that never answers is bounded, not waited on.
// The 5.0 hub's APIService failures are exactly this shape: a Service with no
// endpoints, reachable at the TCP level and silent above it.
func TestWalkHonoursDiscoveryTimeout(t *testing.T) {
	stub := newStubDiscovery([]*metav1.APIResourceList{listable("acme.io/v1", "widgets")})
	stub.block = 500 * time.Millisecond

	walker := &Walker{Discovery: stub, Lister: newFakeLister()}

	start := time.Now()
	result, err := walker.Walk(context.Background(), WalkOptions{DiscoveryTimeout: 20 * time.Millisecond})
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("a discovery timeout must return a partial result, not an error: %v", err)
	}
	if result == nil {
		t.Fatalf("result must never be nil")
	}
	if elapsed > 2*time.Second {
		t.Fatalf("discovery timeout not honoured: %s", elapsed)
	}
	if result.InventoryComplete {
		t.Fatalf("a timed-out discovery must not produce a complete inventory")
	}
	found := false
	for _, incomplete := range result.Incomplete {
		if strings.Contains(incomplete.Reason, "discovery exceeded timeout") {
			found = true
		}
	}
	if !found {
		t.Fatalf("discovery timeout reason missing: %+v", result.Incomplete)
	}
}
