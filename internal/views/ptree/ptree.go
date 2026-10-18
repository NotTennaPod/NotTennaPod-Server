// Package ptree is a persistent, canonical treap: an ordered map whose
// updates copy only the path they change, so every earlier tree stays valid
// and unchanged, and whose shape is a function of its keys alone (each key's
// priority is a hash of it, ties broken by the key), so two trees with the
// same entries have the same shape however they were built.
//
// Every node carries the aggregate of its subtree: the entry count and two
// sums and a maximum that each tree's Measure gives per entry. They answer
// order statistics (Rank, Select), prefix sums (PrefixAgg) and searches by
// maximum (MaxBelowWithMax) in O(log n).
package ptree

import (
	"fmt"
	"math"
	"sync/atomic"
)

// Agg is a subtree's aggregate: its entry count, the sums of its entries'
// A and B, and the maximum of their Max (math.MinInt64 when empty).
type Agg struct {
	N    int
	A, B int64
	Max  int64
}

// Empty is the aggregate of no entries.
var Empty = Agg{Max: math.MinInt64}

// Plus combines two aggregates.
func (a Agg) Plus(b Agg) Agg {
	return Agg{N: a.N + b.N, A: a.A + b.A, B: a.B + b.B, Max: max(a.Max, b.Max)}
}

// Minus removes b's sums from a (counts and sums only: a maximum cannot be
// taken back).
func (a Agg) Minus(b Agg) Agg {
	return Agg{N: a.N - b.N, A: a.A - b.A, B: a.B - b.B, Max: a.Max}
}

// Spec is what a tree's entries are ordered and measured by.
type Spec[K, V any] struct {
	// Compare orders keys.
	Compare func(a, b K) int
	// Priority is a key's heap priority: a hash of it (SplitMix64 of FNV-1a
	// of the key's identifying bytes, Hash).
	Priority func(k K) uint64
	// Measure is one entry's A, B and Max (N is counted by the tree); nil
	// measures nothing.
	Measure func(k K, v V) (a, b, max int64)
}

// Tree is a persistent treap. The zero Tree of a spec is empty; a Tree's
// methods never change it.
type Tree[K, V any] struct {
	spec *Spec[K, V]
	root *node[K, V]
}

type node[K, V any] struct {
	key         K
	val         V
	prio        uint64
	left, right *node[K, V]
	agg         Agg
}

// New returns an empty tree of the spec.
func New[K, V any](spec *Spec[K, V]) Tree[K, V] { return Tree[K, V]{spec: spec} }

// Work counters, for tests that bound the work of an operation: nodes
// visited and nodes allocated, since the process started.
var visitedNodes, allocatedNodes atomic.Int64

// Counts returns the nodes visited and allocated so far.
func Counts() (visited, allocated int64) { return visitedNodes.Load(), allocatedNodes.Load() }

// Hash is SplitMix64 of FNV-1a of the bytes: a key's priority.
func Hash(parts ...string) uint64 {
	h := uint64(14695981039346656037)
	for i, p := range parts {
		if i > 0 {
			h ^= 0xff
			h *= 1099511628211
		}
		for j := 0; j < len(p); j++ {
			h ^= uint64(p[j])
			h *= 1099511628211
		}
	}
	return SplitMix64(h)
}

// SplitMix64 is the SplitMix64 finalizer.
func SplitMix64(x uint64) uint64 {
	x += 0x9e3779b97f4a7c15
	x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9
	x = (x ^ (x >> 27)) * 0x94d049bb133111eb
	return x ^ (x >> 31)
}

// Len is the number of entries.
func (t Tree[K, V]) Len() int {
	if t.root == nil {
		return 0
	}
	return t.root.agg.N
}

// Agg is the aggregate of every entry.
func (t Tree[K, V]) Agg() Agg {
	if t.root == nil {
		return Empty
	}
	return t.root.agg
}

// op counts one operation's work.
type op struct{ visited, allocated int64 }

func (o *op) done() {
	if o.visited != 0 {
		visitedNodes.Add(o.visited)
	}
	if o.allocated != 0 {
		allocatedNodes.Add(o.allocated)
	}
}

func (t Tree[K, V]) measure(n *node[K, V]) Agg {
	a := Agg{N: 1, Max: math.MinInt64}
	if t.spec.Measure != nil {
		a.A, a.B, a.Max = t.spec.Measure(n.key, n.val)
	}
	return a
}

func (t Tree[K, V]) update(n *node[K, V]) {
	a := t.measure(n)
	if n.left != nil {
		a = n.left.agg.Plus(a)
	}
	if n.right != nil {
		a = a.Plus(n.right.agg)
	}
	n.agg = a
}

func (t Tree[K, V]) copyOf(n *node[K, V], o *op) *node[K, V] {
	o.allocated++
	c := *n
	return &c
}

// higher orders nodes by heap priority, ties by key (the smaller first).
func (t Tree[K, V]) higher(a, b *node[K, V]) bool {
	if a.prio != b.prio {
		return a.prio > b.prio
	}
	return t.spec.Compare(a.key, b.key) < 0
}

// Get returns the value of key k.
func (t Tree[K, V]) Get(k K) (V, bool) {
	var o op
	defer o.done()
	for n := t.root; n != nil; {
		o.visited++
		switch c := t.spec.Compare(k, n.key); {
		case c < 0:
			n = n.left
		case c > 0:
			n = n.right
		default:
			return n.val, true
		}
	}
	var zero V
	return zero, false
}

// Set returns the tree with k set to v.
func (t Tree[K, V]) Set(k K, v V) Tree[K, V] {
	var o op
	defer o.done()
	n := &node[K, V]{key: k, val: v, prio: t.spec.Priority(k)}
	o.allocated++
	return Tree[K, V]{spec: t.spec, root: t.insert(t.root, n, &o)}
}

func (t Tree[K, V]) insert(root, n *node[K, V], o *op) *node[K, V] {
	if root == nil {
		t.update(n)
		return n
	}
	o.visited++
	if t.higher(n, root) {
		n.left, n.right = t.split(root, n.key, o)
		t.update(n)
		return n
	}
	c := t.spec.Compare(n.key, root.key)
	cp := t.copyOf(root, o)
	switch {
	case c == 0:
		cp.val = n.val
	case c < 0:
		cp.left = t.insert(root.left, n, o)
	default:
		cp.right = t.insert(root.right, n, o)
	}
	t.update(cp)
	return cp
}

// split divides a subtree into its keys below k and above k (an entry with
// key k is left out).
func (t Tree[K, V]) split(n *node[K, V], k K, o *op) (l, r *node[K, V]) {
	if n == nil {
		return nil, nil
	}
	o.visited++
	c := t.spec.Compare(k, n.key)
	if c == 0 {
		return n.left, n.right
	}
	cp := t.copyOf(n, o)
	if c < 0 {
		l, cp.left = t.split(n.left, k, o)
		t.update(cp)
		return l, cp
	}
	cp.right, r = t.split(n.right, k, o)
	t.update(cp)
	return cp, r
}

// merge joins two subtrees whose keys are all ordered a before b.
func (t Tree[K, V]) merge(a, b *node[K, V], o *op) *node[K, V] {
	switch {
	case a == nil:
		return b
	case b == nil:
		return a
	}
	o.visited += 2
	if t.higher(a, b) {
		cp := t.copyOf(a, o)
		cp.right = t.merge(a.right, b, o)
		t.update(cp)
		return cp
	}
	cp := t.copyOf(b, o)
	cp.left = t.merge(a, b.left, o)
	t.update(cp)
	return cp
}

// Delete returns the tree without key k (the same tree when k is absent).
func (t Tree[K, V]) Delete(k K) Tree[K, V] {
	var o op
	defer o.done()
	root, ok := t.del(t.root, k, &o)
	if !ok {
		return t
	}
	return Tree[K, V]{spec: t.spec, root: root}
}

func (t Tree[K, V]) del(n *node[K, V], k K, o *op) (*node[K, V], bool) {
	if n == nil {
		return nil, false
	}
	o.visited++
	c := t.spec.Compare(k, n.key)
	if c == 0 {
		return t.merge(n.left, n.right, o), true
	}
	var child *node[K, V]
	var ok bool
	if c < 0 {
		child, ok = t.del(n.left, k, o)
	} else {
		child, ok = t.del(n.right, k, o)
	}
	if !ok {
		return n, false
	}
	cp := t.copyOf(n, o)
	if c < 0 {
		cp.left = child
	} else {
		cp.right = child
	}
	t.update(cp)
	return cp, true
}

// Rank is the number of keys below k.
func (t Tree[K, V]) Rank(k K) int { return t.PrefixAgg(k).N }

// PrefixAgg is the aggregate of the entries with keys below k.
func (t Tree[K, V]) PrefixAgg(k K) Agg {
	var o op
	defer o.done()
	acc := Empty
	for n := t.root; n != nil; {
		o.visited++
		if t.spec.Compare(n.key, k) < 0 {
			if n.left != nil {
				acc = acc.Plus(n.left.agg)
			}
			acc = acc.Plus(t.measure(n))
			n = n.right
		} else {
			n = n.left
		}
	}
	return acc
}

// Select returns the entry at index i (0-based, in key order).
func (t Tree[K, V]) Select(i int) (K, V, bool) {
	var o op
	defer o.done()
	for n := t.root; n != nil; {
		o.visited++
		left := 0
		if n.left != nil {
			left = n.left.agg.N
		}
		switch {
		case i < left:
			n = n.left
		case i == left:
			return n.key, n.val, true
		default:
			i -= left + 1
			n = n.right
		}
	}
	var k K
	var v V
	return k, v, false
}

// MaxBelowWithMax returns the largest key below k among the entries whose
// Max exceeds c, in O(log n).
func (t Tree[K, V]) MaxBelowWithMax(k K, c int64) (K, V, bool) {
	var o op
	defer o.done()
	if n := t.maxBelow(t.root, k, c, &o); n != nil {
		return n.key, n.val, true
	}
	var key K
	var v V
	return key, v, false
}

func (t Tree[K, V]) maxBelow(n *node[K, V], k K, c int64, o *op) *node[K, V] {
	if n == nil || n.agg.Max <= c {
		return nil
	}
	o.visited++
	if t.spec.Compare(n.key, k) >= 0 {
		return t.maxBelow(n.left, k, c, o)
	}
	if r := t.maxBelow(n.right, k, c, o); r != nil {
		return r
	}
	if t.measure(n).Max > c {
		return n
	}
	return t.maxBelow(n.left, k, c, o)
}

// FromSorted builds the tree of entries given in strictly increasing key
// order, in O(n); it is the tree inserting them in any order gives.
func FromSorted[K, V any](spec *Spec[K, V], keys []K, vals []V) (Tree[K, V], error) {
	t := Tree[K, V]{spec: spec}
	if len(keys) != len(vals) {
		return t, fmt.Errorf("ptree: %d keys and %d values", len(keys), len(vals))
	}
	var o op
	defer o.done()
	// The right spine of the tree built so far, root first.
	var spine []*node[K, V]
	for i := range keys {
		if i > 0 && spec.Compare(keys[i-1], keys[i]) >= 0 {
			return t, fmt.Errorf("ptree: keys %d and %d are not in strictly increasing order", i-1, i)
		}
		n := &node[K, V]{key: keys[i], val: vals[i], prio: spec.Priority(keys[i])}
		o.allocated++
		var last *node[K, V]
		for len(spine) > 0 && t.higher(n, spine[len(spine)-1]) {
			last = spine[len(spine)-1]
			spine = spine[:len(spine)-1]
		}
		n.left = last
		if len(spine) > 0 {
			spine[len(spine)-1].right = n
		}
		spine = append(spine, n)
	}
	if len(spine) > 0 {
		t.root = spine[0]
		t.fill(t.root)
	}
	return t, nil
}

func (t Tree[K, V]) fill(n *node[K, V]) {
	if n == nil {
		return
	}
	t.fill(n.left)
	t.fill(n.right)
	t.update(n)
}

// Iter walks entries in key order (or reversed). It holds the path it
// stands on: a tree it walks stays valid whatever later trees do.
type Iter[K, V any] struct {
	t     Tree[K, V]
	stack []*node[K, V]
	rev   bool
}

// Seek starts at the first key at or after k.
func (t Tree[K, V]) Seek(k K) *Iter[K, V] {
	it := &Iter[K, V]{t: t}
	var visited int64
	for n := t.root; n != nil; {
		visited++
		if t.spec.Compare(n.key, k) >= 0 {
			it.stack = append(it.stack, n)
			n = n.left
		} else {
			n = n.right
		}
	}
	visitedNodes.Add(visited)
	return it
}

// SeekRev starts at the last key at or before k, walking down.
func (t Tree[K, V]) SeekRev(k K) *Iter[K, V] {
	it := &Iter[K, V]{t: t, rev: true}
	var visited int64
	for n := t.root; n != nil; {
		visited++
		if t.spec.Compare(n.key, k) <= 0 {
			it.stack = append(it.stack, n)
			n = n.right
		} else {
			n = n.left
		}
	}
	visitedNodes.Add(visited)
	return it
}

// SeekAt starts at the entry at index i (0-based, in key order): the
// entries from i on, in O(log n).
func (t Tree[K, V]) SeekAt(i int) *Iter[K, V] {
	it := &Iter[K, V]{t: t}
	var visited int64
	for n := t.root; n != nil; {
		visited++
		left := 0
		if n.left != nil {
			left = n.left.agg.N
		}
		switch {
		case i < left:
			it.stack = append(it.stack, n)
			n = n.left
		case i == left:
			it.stack = append(it.stack, n)
			n = nil
		default:
			i -= left + 1
			n = n.right
		}
	}
	visitedNodes.Add(visited)
	return it
}

// SeekAtRev starts at the entry at index i, walking down: the entries i,
// i − 1, … 0, in O(log n).
func (t Tree[K, V]) SeekAtRev(i int) *Iter[K, V] {
	it := &Iter[K, V]{t: t, rev: true}
	var visited int64
	for n := t.root; n != nil; {
		visited++
		left := 0
		if n.left != nil {
			left = n.left.agg.N
		}
		switch {
		case i < left:
			n = n.left
		case i == left:
			it.stack = append(it.stack, n)
			n = nil
		default:
			it.stack = append(it.stack, n)
			i -= left + 1
			n = n.right
		}
	}
	visitedNodes.Add(visited)
	return it
}

// First starts at the first key.
func (t Tree[K, V]) First() *Iter[K, V] {
	it := &Iter[K, V]{t: t}
	it.pushLeft(t.root)
	return it
}

// Last starts at the last key, walking down.
func (t Tree[K, V]) Last() *Iter[K, V] {
	it := &Iter[K, V]{t: t, rev: true}
	it.pushRight(t.root)
	return it
}

func (it *Iter[K, V]) pushLeft(n *node[K, V]) {
	var visited int64
	for ; n != nil; n = n.left {
		visited++
		it.stack = append(it.stack, n)
	}
	visitedNodes.Add(visited)
}

func (it *Iter[K, V]) pushRight(n *node[K, V]) {
	var visited int64
	for ; n != nil; n = n.right {
		visited++
		it.stack = append(it.stack, n)
	}
	visitedNodes.Add(visited)
}

// Next returns the next entry, false at the end.
func (it *Iter[K, V]) Next() (K, V, bool) {
	if len(it.stack) == 0 {
		var k K
		var v V
		return k, v, false
	}
	n := it.stack[len(it.stack)-1]
	it.stack = it.stack[:len(it.stack)-1]
	if it.rev {
		it.pushRight(n.left)
	} else {
		it.pushLeft(n.right)
	}
	return n.key, n.val, true
}

// All walks every entry in key order.
func (t Tree[K, V]) All() func(yield func(K, V) bool) {
	return func(yield func(K, V) bool) {
		it := t.First()
		for {
			k, v, ok := it.Next()
			if !ok || !yield(k, v) {
				return
			}
		}
	}
}

// Depth is the height of the tree (0 when empty).
func (t Tree[K, V]) Depth() int { return depth(t.root) }

func depth[K, V any](n *node[K, V]) int {
	if n == nil {
		return 0
	}
	return 1 + max(depth(n.left), depth(n.right))
}

// Diff reports the first difference between two trees of one spec, in
// entries, values (by eq), aggregates or shape; "" when they are equal.
func Diff[K, V any](a, b Tree[K, V], eq func(x, y V) bool, show func(K) string) string {
	if a.root == b.root {
		return ""
	}
	cmp := a.spec.Compare
	if cmp == nil {
		cmp = b.spec.Compare
	}
	return diff(a.root, b.root, cmp, eq, show, 0)
}

func diff[K, V any](a, b *node[K, V], cmp func(x, y K) int, eq func(x, y V) bool, show func(K) string, depth int) string {
	switch {
	case a == b:
		return ""
	case a == nil:
		return fmt.Sprintf("at depth %d: missing, the other has %s", depth, show(b.key))
	case b == nil:
		return fmt.Sprintf("at depth %d: has %s, the other none", depth, show(a.key))
	}
	if cmp(a.key, b.key) != 0 {
		return fmt.Sprintf("at depth %d: key %s, the other %s", depth, show(a.key), show(b.key))
	}
	if !eq(a.val, b.val) {
		return fmt.Sprintf("at depth %d: the values of %s differ", depth, show(a.key))
	}
	if a.agg != b.agg || a.prio != b.prio {
		return fmt.Sprintf("at depth %d: the aggregates of %s differ: %+v, %+v", depth, show(a.key), a.agg, b.agg)
	}
	if d := diff(a.left, b.left, cmp, eq, show, depth+1); d != "" {
		return d
	}
	return diff(a.right, b.right, cmp, eq, show, depth+1)
}

// Shared counts the nodes two trees share (the same allocation): tests of
// path copying.
func Shared[K, V any](a, b Tree[K, V]) int {
	seen := map[*node[K, V]]bool{}
	var walk func(n *node[K, V])
	walk = func(n *node[K, V]) {
		if n != nil {
			seen[n] = true
			walk(n.left)
			walk(n.right)
		}
	}
	walk(a.root)
	n := 0
	var count func(x *node[K, V])
	count = func(x *node[K, V]) {
		if x != nil {
			if seen[x] {
				n++
			}
			count(x.left)
			count(x.right)
		}
	}
	count(b.root)
	return n
}

// SameRoot reports whether two trees are the same tree (one allocation):
// an update that did not touch a tree leaves it the same.
func (t Tree[K, V]) SameRoot(o Tree[K, V]) bool { return t.root == o.root }
