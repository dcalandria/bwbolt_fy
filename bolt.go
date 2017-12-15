// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package bwbolt contains the implementation of the BadWolf driver using
// BoltDB.

package bwbolt

import (
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/binary"
	"fmt"
	"hash"
	"io"
	"math"
	"sync"
	"time"

	"github.com/boltdb/bolt"
	"github.com/google/badwolf/storage"
	"github.com/google/badwolf/triple"
	"github.com/google/badwolf/triple/literal"
	"github.com/google/badwolf/triple/node"
	"github.com/google/badwolf/triple/predicate"
)

// driver implements BadWolf storage.Store for a fully compliant driver.
type driver struct {
	path string
	db   *bolt.DB
	lb   literal.Builder
}

// graphBucket contians the name of the bucket containing the graphs.
const graphBucket = "GRAPHS"

var sha1Pool = sync.Pool{
	New: func() interface{} {
		return sha1.New()
	},
}

// New create a new BadWolf driver using BoltDB as a storage driver.
func New(path string, lb literal.Builder, timeOut time.Duration, readOnly bool) (storage.Store, *bolt.DB, error) {
	// Bolt open options.
	opts := &bolt.Options{
		Timeout:  timeOut,
		ReadOnly: readOnly,
	}
	// Open the DB.
	db, err := bolt.Open(path, 0600, opts)
	if err != nil {
		return nil, nil, fmt.Errorf("bwbolt driver initialization failure for file %q %v", path, err)
	}
	// Create the graph bucket if it does not exist.
	db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(graphBucket))
		if err != nil {
			return fmt.Errorf("driver initialization failed to create bucket %s for path %s with error %v", graphBucket, path, err)
		}
		return nil
	})
	// Return the initilized driver.
	return &driver{
		path: path,
		db:   db,
		lb:   lb,
	}, db, nil
}

// Name returns the ID of the backend being used.
func (d *driver) Name(ctx context.Context) string {
	return fmt.Sprintf("bwbolt/%s", d.path)
}

// Version returns the version of the driver implementation.
func (d *driver) Version(ctx context.Context) string {
	return "HEAD"
}

// NewGraph creates a new graph. Creating an already existing graph
// should return an error.
func (d *driver) NewGraph(ctx context.Context, id string) (storage.Graph, error) {
	err := d.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(graphBucket))
		if b == nil {
			return fmt.Errorf("inconsistent driver failed to retrieve top bucket %q", graphBucket)
		}
		_, err := b.CreateBucket([]byte(id))
		if err != nil {
			return fmt.Errorf("failed to create new graph %q with error %v", id, err)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &graph{
		id: id,
		db: d.db,
		lb: d.lb,
	}, nil
}

// Graph returns an existing graph if available. Getting a non existing
// graph should return an error.
func (d *driver) Graph(ctx context.Context, id string) (storage.Graph, error) {
	err := d.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(graphBucket))
		if b == nil {
			return fmt.Errorf("inconsistent driver failed to retrieve top bucket %q", graphBucket)
		}
		g := b.Bucket([]byte(id))
		if g == nil {
			return fmt.Errorf("graph %q does not exist", id)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &graph{
		id: id,
		db: d.db,
		lb: d.lb,
	}, nil
}

// DeleteGraph deletes an existing graph. Deleting a non existing graph
// should return an error.
func (d *driver) DeleteGraph(ctx context.Context, id string) error {
	return d.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(graphBucket))
		if b == nil {
			return fmt.Errorf("inconsistent driver failed to retrieve top bucket %q", graphBucket)
		}
		err := b.DeleteBucket([]byte(id))
		if err != nil {
			return err
		}
		return nil
	})
}

// GraphNames returns the current available graph names in the store.
func (d *driver) GraphNames(ctx context.Context, names chan<- string) error {
	defer close(names)
	return d.db.View(func(tx *bolt.Tx) error {
		// Assume bucket exists and has keys
		b := tx.Bucket([]byte(graphBucket))
		if b == nil {
			return fmt.Errorf("inconsistent driver failed to retrieve top bucket %q", graphBucket)
		}
		c := b.Cursor()
		for k, _ := c.First(); k != nil; k, _ = c.Next() {
			names <- string(k)
		}
		return nil
	})
}

// graph implements BadWolf storage.Graph for a fully compliant driver.
type graph struct {
	id string
	db *bolt.DB
	lb literal.Builder
}

// ID returns the id for this graph.
func (g *graph) ID(ctx context.Context) string {
	return g.id
}

// indexUpdate contains the update to perform to a given index.
type indexUpdate struct {
	idx   string
	key   []byte
	value []byte
}

const (
	idxTriples = "TRIPLES"
	idxSPO     = "SPO"
	idxSOP     = "SOP"
	idxPOS     = "POS"
	idxPSO     = "PSO"
	idxOPS     = "OPS"
	idxOSP     = "OSP"
	idxCountSP = "cSP"
	idxCountOP = "cOP"
)

func (g *graph) setCount(idx string, tx *bolt.Tx, key string, count int64) error {
	b, err := tx.CreateBucketIfNotExists([]byte(idx))
	if err != nil {
		return err
	}
	v := make([]byte, 9)
	binary.PutVarint(v, count)
	return b.Put([]byte(key), v)
}

func (g *graph) getCount(idx string, tx *bolt.Tx, s *node.Node, p *predicate.Predicate, o *triple.Object) (int64, string, error) {
	var key []byte
	if idx == idxCountSP {
		key = joinKeys(nodeKey(s), []byte(p.ID()))
	} else {
		key = joinKeys(objectKey(o), []byte(p.ID()))
	}
	b := tx.Bucket([]byte(idx))
	if b != nil {
		v := b.Get(key)
		if v != nil {
			i64, _ := binary.Varint(v)
			return i64, string(key), nil
		}
	}
	return 0, string(key), nil
}

func nodeKey(n *node.Node) []byte {
	return append([]byte(n.String()), 0)
}

func predicateKey(p *predicate.Predicate) []byte {
	idLen := len(p.ID())
	if p.Type() == predicate.Immutable {
		b := make([]byte, idLen+1)
		copy(b, string(p.ID()))
		b[idLen] = 0
		return b
	} else {
		b := make([]byte, idLen+9)
		copy(b, string(p.ID()))
		b[idLen] = 0
		t, _ := p.TimeAnchor()
		binary.BigEndian.PutUint64(b[idLen+1:], uint64(t.UnixNano()))
		return b
	}
}

func literalKey(l *literal.Literal) []byte {
	var b []byte
	switch l.Type() {
	case literal.Bool:
		b = make([]byte, 2)
		b[0] = byte(literal.Blob)
		if l.Interface().(bool) {
			b[1] = 1
		} else {
			b[0] = 0
		}
	case literal.Int64:
		b = make([]byte, 9)
		b[0] = byte(literal.Int64)
		binary.BigEndian.PutUint64(b[1:], uint64(l.Interface().(int64)))
	case literal.Float64:
		b = make([]byte, 9)
		b[0] = byte(literal.Float64)
		binary.BigEndian.PutUint64(b[1:], math.Float64bits(l.Interface().(float64)))
	case literal.Text:
		v, _ := l.Text()
		b = make([]byte, 2+len(v))
		b[0] = byte(literal.Text)
		copy(b[1:], v)
		b[len(v)-1] = 0
	case literal.Blob:
		v, _ := l.Blob()
		b = make([]byte, 1+4+len(v))
		b[0] = byte(literal.Blob)
		binary.BigEndian.PutUint32(b[1:], uint32(len(v)))
		copy(b[5:], v)
	default:
		return []byte("UNKNOWN")
	}
	return b
}

func objectKey(o *triple.Object) []byte {
	if n, err := o.Node(); err == nil {
		return nodeKey(n)
	}
	if p, err := o.Predicate(); err == nil {
		return predicateKey(p)
	}
	if l, err := o.Literal(); err == nil {
		return literalKey(l)
	}
	return []byte("@@INVALID_OBJECT@@@")
}

func (g *graph) tripleKey(t *triple.Triple) []byte {
	k := joinKeys(nodeKey(t.Subject()), predicateKey(t.Predicate()), objectKey(t.Object()))
	h := sha1sum(k)
	return h
}

func joinKeys(keys ...[]byte) []byte {
	l := 0
	for _, k := range keys {
		l += len(k)
	}

	b := make([]byte, 0, l)
	for _, k := range keys {
		b = append(b, k...)
	}

	return b
}

func sha1sum(src []byte) []byte {
	h := sha1Pool.Get().(hash.Hash)
	h.Reset()
	h.Write(src)
	sum := h.Sum(nil)
	sha1Pool.Put(h)
	return sum[:]
}

// Given a triple, returns the updates to perform to the indices.
func (g *graph) tripleToIndexUpdate(t *triple.Triple) []*indexUpdate {
	var updates []*indexUpdate
	s, p, o := nodeKey(t.Subject()), predicateKey(t.Predicate()), objectKey(t.Object())

	k := joinKeys(s, p, o)
	h := sha1sum(k)
	tt := []byte(t.String())

	updates = append(updates,
		&indexUpdate{
			idx:   idxTriples,
			key:   h,
			value: tt,
		},
		&indexUpdate{
			idx:   idxSPO,
			key:   joinKeys(s, p, o),
			value: h,
		},
		&indexUpdate{
			idx:   idxSOP,
			key:   joinKeys(s, o, p),
			value: h,
		},
		&indexUpdate{
			idx:   idxPOS,
			key:   joinKeys(p, o, s),
			value: h,
		},
		&indexUpdate{
			idx:   idxPSO,
			key:   joinKeys(p, s, o),
			value: h,
		},
		&indexUpdate{
			idx:   idxOPS,
			key:   joinKeys(o, p, s),
			value: h,
		},
		&indexUpdate{
			idx:   idxOSP,
			key:   joinKeys(o, s, p),
			value: h,
		})
	return updates
}

// AddTriples adds the triples to the storage. Adding a triple that already
// exists should not fail. Efficiently resolving all the operations below
// require proper indexing. This driver provides the follow indices:
//
//   * SPO: Textual representation of the triple to allow range queries.
//   * SOP: Conbination to allow efficient query of S + P queries.
//   * POS: Conbination to allow efficient query of P + O queries.
//   * PSO: Conbination to allow efficient query of P + S queries.
//   * OPS: Conbination to allow efficient query of O + P queries.
//   * OSP: Conbination to allow efficient query of O + S queries.
//
// The GUID index containst the fully serialized triple. The other indices
// only contains as a value the GUID of the triple.
func (g *graph) AddTriples(ctx context.Context, ts []*triple.Triple) error {
	return g.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(graphBucket))
		if b == nil {
			return fmt.Errorf("inconsistent driver failed to retrieve top bucket %q", graphBucket)
		}
		gb := b.Bucket([]byte(g.id))
		if gb == nil {
			return fmt.Errorf("graph %q does not exist", g.id)
		}

		rts := make([]*triple.Triple, 0, len(ts))
		for _, t := range ts {
			if exist, err := g.Exist(ctx, t); err != nil {
				return err
			} else if exist {
				continue
			}
			rts = append(rts, t)
			for _, iu := range g.tripleToIndexUpdate(t) {
				b, err := gb.CreateBucketIfNotExists([]byte(iu.idx))
				if err != nil {
					return fmt.Errorf("failed to create bucket %s for graph %s with error %v", iu.idx, g.id, err)
				}
				err = b.Put(iu.key, iu.value)
				if err != nil {
					return err
				}
			}
		}
		return g.updateAllCounts(tx, rts, 1)
	})
}

func (g *graph) updateAllCounts(tx *bolt.Tx, ts []*triple.Triple, delta int64) error {
	if err := g.updateCounts(idxCountSP, tx, ts, delta); err != nil {
		return err
	}
	if err := g.updateCounts(idxCountOP, tx, ts, delta); err != nil {
		return err
	}
	return nil
}

func (g *graph) updateCounts(idx string, tx *bolt.Tx, ts []*triple.Triple, delta int64) error {
	cs := make(map[string]int64)
	keys := make([]string, len(ts))
	for i, t := range ts {
		n, key, err := g.getCount(idx, tx, t.Subject(), t.Predicate(), t.Object())
		if err != nil {
			return err
		}
		cs[key] = n
		keys[i] = key
	}

	for i, _ := range ts {
		cs[keys[i]] = cs[keys[i]] + delta
		if cs[keys[i]] < 0 {
			cs[keys[i]] = 0
		}
	}

	for k, c := range cs {
		err := g.setCount(idx, tx, k, c)
		if err != nil {
			return err
		}
	}

	return nil
}

// RemoveTriples removes the trilpes from the storage. Removing triples that
// are not present on the store should not fail.
func (g *graph) RemoveTriples(ctx context.Context, ts []*triple.Triple) error {
	return g.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(graphBucket))
		if b == nil {
			return fmt.Errorf("inconsistent driver failed to retrieve top bucket %q", graphBucket)
		}
		gb := b.Bucket([]byte(g.id))
		if gb == nil {
			return fmt.Errorf("graph %q does not exist", g.id)
		}

		rts := make([]*triple.Triple, 0, len(ts))
		for _, t := range ts {
			if exist, err := g.Exist(ctx, t); err != nil {
				return err
			} else if !exist {
				continue
			}
			rts = append(rts, t)
			for _, iu := range g.tripleToIndexUpdate(t) {
				b, err := gb.CreateBucketIfNotExists([]byte(iu.idx))
				if err != nil {
					return fmt.Errorf("failed to create bucket %s for graph %s with error %v", iu.idx, g.id, err)
				}
				b.Delete(iu.key)
				if err != nil {
					if b.Get(iu.key) != nil {
						return err
					}
				}
			}
		}
		return g.updateAllCounts(tx, rts, -1)
	})
}

// shouldAccept returns is the triple should be accepted
func shouldAccept(t *triple.Triple, lo *storage.LookupOptions) bool {
	p := t.Predicate()
	if p.Type() == predicate.Temporal {
		if lo.LowerAnchor != nil {
			ta, err := t.Predicate().TimeAnchor()
			if err != nil {
				panic(fmt.Errorf("should have never failed to retrieve time anchor from triple %s with error %v", t.String(), err))
			}
			if lo.LowerAnchor.After(*ta) {
				return false
			}
		}
		if lo.UpperAnchor != nil {
			ta, err := t.Predicate().TimeAnchor()
			if err != nil {
				panic(fmt.Errorf("should have never failed to retrieve time anchor from triple %s with error %v", t.String(), err))
			}
			if lo.UpperAnchor.Before(*ta) {
				return false
			}
		}
	}
	return true
}

// Exist checks if the provided triple exists on the store.
func (g *graph) Exist(ctx context.Context, t *triple.Triple) (bool, error) {
	res := false

	err := g.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(graphBucket))
		if b == nil {
			return fmt.Errorf("inconsistent driver failed to retrieve top bucket %q", graphBucket)
		}
		gb := b.Bucket([]byte(g.id))
		if gb == nil {
			return fmt.Errorf("graph %q does not exist", g.id)
		}
		triples := gb.Bucket([]byte(idxTriples))
		if triples != nil && triples.Get(g.tripleKey(t)) != nil {
			res = true
		}
		return nil
	})

	return res, err
}

type cursor struct {
	prefix  []byte
	tx      *bolt.Tx
	c       *bolt.Cursor
	tb      *bolt.Bucket
	reverse bool
	lb      literal.Builder
	lo      *storage.LookupOptions
	eof     bool
	first   bool
}

func (c *cursor) Close() {
	if !c.eof {
		c.eof = true
		c.tx.Rollback()
	}
}

func (c *cursor) next() ([]byte, []byte) {
	if !c.reverse {
		return c.c.Next()
	} else {
		return c.c.Prev()
	}
}

func (c *cursor) valid(k []byte) bool {
	if k == nil || len(k) == 0 {
		return false
	}
	if !c.reverse {
		return bytes.Compare(k[:len(c.prefix)], c.prefix) <= 0
	} else {
		return bytes.Compare(k[:len(c.prefix)], c.prefix) >= 0
	}
}

func (c *cursor) seekLast(prefix []byte) ([]byte, []byte) {
	p := make([]byte, len(c.prefix))
	copy(p, c.prefix)
	ok := false
	for i := len(p) - 1; i >= 0; i-- {
		if p[i] != 255 {
			p[i] = p[i] + 1
			ok = true
			break
		} else {
			p[i] = 0
		}
	}
	if !ok {
		p = append([]byte{1}, p...)
	}

	k, v := c.c.Seek(p)
	if k == nil {
		k, v = c.c.Prev()
	}

	for {
		if k == nil {
			break
		}
		var l int
		if len(k) < len(c.prefix) {
			l = len(k)
		} else {
			l = len(c.prefix)
		}
		if bytes.Compare(k[:l], c.prefix[:l]) <= 0 {
			break
		}
		k, v = c.c.Prev()
	}
	return k, v
}

func (c *cursor) Next() (*triple.Triple, error) {
	if c.eof {
		return nil, io.EOF
	}

	var k, v []byte

	if c.first {
		if len(c.prefix) == 0 {
			if !c.reverse {
				k, v = c.c.First()
			} else {
				k, v = c.c.Last()
			}
		} else {
			if !c.reverse {
				k, v = c.c.Seek(c.prefix)
			} else {
				k, v = c.seekLast(c.prefix)
			}
		}
		c.first = false
	} else {
		k, v = c.next()
	}

	for ; k != nil && bytes.HasPrefix(k, c.prefix); k, v = c.next() {
		if v == nil || len(v) == 0 {
			continue
		}

		v := c.tb.Get(v)
		if v == nil || len(v) == 0 {
			continue
		}

		t, err := triple.Parse(string(v), c.lb)
		if err != nil {
			return nil, fmt.Errorf("corrupt index failing to parse %q with error %v", string(v), err)
		}

		if shouldAccept(t, c.lo) {
			return t, nil
		}
	}
	c.Close()
	return nil, io.EOF
}

func (g *graph) newCursor(idx string, prefix []byte, lb literal.Builder, lo *storage.LookupOptions) (storage.Cursor, error) {
	tx, err := g.db.Begin(false)
	if err != nil {
		return nil, err
	}
	b := tx.Bucket([]byte(graphBucket))
	if b == nil {
		return nil, fmt.Errorf("inconsistent driver failed to retrieve top bucket %q", graphBucket)
	}
	gb := b.Bucket([]byte(g.id))
	if gb == nil {
		return nil, fmt.Errorf("graph %q does not exist", g.id)
	}
	ib := gb.Bucket([]byte(idx))
	if ib == nil {
		return nil, fmt.Errorf("failed to load bucket %s for graph %s", idx, g.id)
	}
	c := ib.Cursor()

	tb := gb.Bucket([]byte(idxTriples))
	if tb == nil {
		return nil, fmt.Errorf("failed to load bucket TRIPLES for graph %s", g.id)
	}

	return &cursor{
		prefix:  prefix,
		tx:      tx,
		c:       c,
		tb:      tb,
		lo:      lo,
		lb:      lb,
		first:   true,
		reverse: lo.LowerAnchor == nil && lo.UpperAnchor != nil,
	}, nil
}

func (g *graph) CursorForTriple(ctx context.Context, s *node.Node, p *predicate.Predicate, o *triple.Object, lo *storage.LookupOptions) (storage.Cursor, error) {
	sk, pk, ok := nodeKey(s), predicateKey(p), objectKey(o)
	return g.newCursor(idxSPO, joinKeys(sk, pk, ok), g.lb, lo)
}

func (g *graph) CursorForSubjectAndPredicate(ctx context.Context, s *node.Node, p *predicate.Predicate, lo *storage.LookupOptions) (storage.Cursor, error) {
	sk, pk := nodeKey(s), predicateKey(p)
	return g.newCursor(idxSPO, joinKeys(sk, pk), g.lb, lo)
}

func (g *graph) CursorForSubjectAndObject(ctx context.Context, s *node.Node, o *triple.Object, lo *storage.LookupOptions) (storage.Cursor, error) {
	sk, ok := nodeKey(s), objectKey(o)
	return g.newCursor(idxSOP, joinKeys(sk, ok), g.lb, lo)
}

func (g *graph) CursorForObjectAndPredicate(ctx context.Context, o *triple.Object, p *predicate.Predicate, lo *storage.LookupOptions) (storage.Cursor, error) {
	ok, pk := objectKey(o), predicateKey(p)
	return g.newCursor(idxOPS, joinKeys(ok, pk), g.lb, lo)
}

func (g *graph) CursorForSubject(ctx context.Context, s *node.Node, lo *storage.LookupOptions) (storage.Cursor, error) {
	sk := nodeKey(s)
	var index string = idxSPO
	if lo != nil && lo.Preferred == storage.Object {
		index = idxSOP
	}
	return g.newCursor(index, sk, g.lb, lo)
}

func (g *graph) CursorForPredicate(ctx context.Context, p *predicate.Predicate, lo *storage.LookupOptions) (storage.Cursor, error) {
	pk := predicateKey(p)
	var index string = idxPSO
	if lo != nil && lo.Preferred == storage.Object {
		index = idxPOS
	}
	return g.newCursor(index, pk, g.lb, lo)
}

func (g *graph) CursorForObject(ctx context.Context, o *triple.Object, lo *storage.LookupOptions) (storage.Cursor, error) {
	pk := objectKey(o)
	var index string = idxOPS
	if lo != nil && lo.Preferred == storage.Subject {
		index = idxOSP
	}
	return g.newCursor(index, pk, g.lb, lo)
}

func (g *graph) Cursor(ctx context.Context, lo *storage.LookupOptions, prefix string) (storage.Cursor, error) {
	b := []byte(prefix)
	if lo != nil {
		if lo.Preferred == storage.Object {
			return g.newCursor(idxOPS, b, g.lb, lo)
		} else if lo.Preferred == storage.Predicate {
			return g.newCursor(idxPOS, b, g.lb, lo)
		}
	}
	return g.newCursor(idxSPO, b, g.lb, lo)
}

func (g *graph) CountForSubjectAndPredicate(ctx context.Context, s *node.Node, p *predicate.Predicate) (int64, error) {
	var c int64
	err := g.db.View(func(tx *bolt.Tx) error {
		n, _, err := g.getCount(idxCountSP, tx, s, p, nil)
		if err != nil {
			return err
		}
		c = n
		return nil
	})
	return c, err
}

func (g *graph) CountForObjectAndPredicate(ctx context.Context, o *triple.Object, p *predicate.Predicate) (int64, error) {
	var c int64
	err := g.db.View(func(tx *bolt.Tx) error {
		n, _, err := g.getCount(idxCountOP, tx, nil, p, o)
		if err != nil {
			return err
		}
		c = n
		return nil
	})
	return c, err
}
