// Copyright 2026 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"fmt"
	"math/rand/v2"
	"slices"
	"testing"
	"time"
)

func TestAckLimitsClassify(t *testing.T) {
	entries := func(e ...MaxAckPendingPerSubjectTokens) []MaxAckPendingPerSubjectTokens { return e }
	for _, test := range []struct {
		filter    string
		wildcards []int
		subject   string
		inScope   bool
		key       string
	}{
		// The table from the ADR.
		{_EMPTY_, nil, "operations.X.deposit", true, "operations.X.deposit"},
		{">", nil, "operations.X.deposit", true, "operations.X.deposit"},
		{"operations.*.>", []int{1}, "operations.X.deposit", true, "X"},
		{"operations.*.*", []int{1}, "operations.X.deposit", true, "X"},
		{"operations.*.*", []int{1, 2}, "operations.X.deposit", true, "X.deposit"},
		{"operations.*.*", []int{2, 1}, "operations.X.deposit", true, "deposit.X"},
		{"operations.*.*", []int{2}, "operations.X.deposit", true, "deposit"},
		{"operations.*.*", nil, "operations.X.deposit", true, "operations.X.deposit"},
		{"orders.*.*.>", []int{2}, "operations.X.deposit", false, _EMPTY_},
		{"orders.*.*.>", []int{2}, "orders.eu.X.new", true, "X"},
		{"$KV.bucket.>", nil, "operations.X.deposit", false, _EMPTY_},
		{"$KV.bucket.>", nil, "$KV.bucket.a.b.c", true, "$KV.bucket.a.b.c"},
		{"$KV.bucket.*.>", []int{1}, "$KV.bucket.a.b.c", true, "a"},
		{"operations.*.*", []int{1}, "operations.X.deposit.extra", false, _EMPTY_},
		{"operations.*.*", []int{1}, "operations.X", false, _EMPTY_},
	} {
		t.Run(fmt.Sprintf("%s %v %s", test.filter, test.wildcards, test.subject), func(t *testing.T) {
			cfg := &ConsumerConfig{MaxAckPendingPerSubjectTokens: entries(
				MaxAckPendingPerSubjectTokens{Filter: test.filter, Wildcards: test.wildcards, MaxAckPending: 1})}
			al := newAckLimits(cfg, 100)
			e, key := al.classify(test.subject)
			require_Equal(t, e != nil, test.inScope)
			require_Equal(t, key, test.key)
		})
	}

	// Several entries, first match wins and they do not overlap.
	cfg := &ConsumerConfig{MaxAckPendingPerSubjectTokens: entries(
		MaxAckPendingPerSubjectTokens{Filter: "operations.*.>", Wildcards: []int{1}, MaxAckPending: 1},
		MaxAckPendingPerSubjectTokens{Filter: "billing.*.*", Wildcards: []int{2}, MaxAckPending: 5},
	)}
	al := newAckLimits(cfg, 100)
	e, key := al.classify("billing.X.invoice")
	require_True(t, e == al.entries[1])
	require_Equal(t, key, "invoice")
	e, key = al.classify("operations.Y.withdrawal")
	require_True(t, e == al.entries[0])
	require_Equal(t, key, "Y")
	e, _ = al.classify("other.Y")
	require_True(t, e == nil)

	// No entries means nothing is in scope.
	al = newAckLimits(&ConsumerConfig{}, 100)
	e, _ = al.classify("operations.Y.withdrawal")
	require_True(t, e == nil)
	require_True(t, al.draining())

	// The lookahead bound follows max ack pending.
	require_Equal(t, newAckLimits(cfg, 0).limit, JsDefaultMaxAckPending)
	require_Equal(t, newAckLimits(cfg, -1).limit, JsDefaultMaxAckPending)
	require_Equal(t, newAckLimits(cfg, 7).limit, 7)
}

// checkAckLimitsInvariants verifies the internal consistency of the runtime against the
// test's own view of what is pending (with its consumer sequence) and deferred.
func checkAckLimitsInvariants(t *testing.T, al *ackLimits, pending map[uint64]uint64, deferred map[uint64]struct{}, keyOf map[uint64]string) {
	t.Helper()
	// Every pending and deferred sequence is in pseqs, and nothing else.
	require_Equal(t, al.pseqs.Size(), len(pending)+len(deferred))
	for seq := range pending {
		require_True(t, al.pseqs.Exists(seq))
		require_False(t, al.dset.Exists(seq))
	}
	require_Equal(t, al.pdseqs.Size(), len(pending))
	for _, dseq := range pending {
		require_True(t, al.pdseqs.Exists(dseq))
	}
	// Every deferred sequence is in dset and nothing else.
	require_Equal(t, al.dset.Size(), len(deferred))
	require_Equal(t, al.ndeferred(), len(deferred))
	for seq := range deferred {
		require_True(t, al.dset.Exists(seq))
		require_True(t, al.pseqs.Exists(seq))
		require_True(t, al.isDeferred(seq))
	}
	// Ready is a subset of deferred.
	al.ready.Range(func(seq uint64) bool {
		_, ok := deferred[seq]
		require_True(t, ok)
		return true
	})
	// Per key accounting.
	var nready int
	for _, e := range al.entries {
		for k, ks := range e.keys {
			// Keys are dropped when empty.
			require_True(t, ks.pending > 0 || len(ks.deferred) > 0)
			require_True(t, ks.pending <= e.maxp)
			require_True(t, slices.IsSorted(ks.deferred))
			// Count pending for that key from our own view.
			var np int
			var maxPending uint64
			for seq := range pending {
				if keyOf[seq] == e.filter+"/"+k {
					np++
					maxPending = max(maxPending, seq)
				}
			}
			require_Equal(t, ks.pending, np)
			for _, seq := range ks.deferred {
				_, ok := deferred[seq]
				require_True(t, ok)
				require_Equal(t, keyOf[seq], e.filter+"/"+k)
				// Deferred messages of a key are always newer than its pending ones.
				require_True(t, seq > maxPending)
				ref := al.seqs[seq]
				require_True(t, ref.e == e && ref.ks == ks)
			}
			// Exactly the head of a key with room is ready.
			var inReady int
			for _, seq := range ks.deferred {
				if al.ready.Exists(seq) {
					inReady++
					require_Equal(t, seq, ks.deferred[0])
				}
			}
			if len(ks.deferred) > 0 && ks.pending < e.maxp {
				require_Equal(t, inReady, 1)
			} else {
				require_Equal(t, inReady, 0)
			}
			nready += inReady
		}
	}
	// Untracked deferred sequences are all ready.
	for seq := range deferred {
		if keyOf[seq] == _EMPTY_ {
			require_True(t, al.ready.Exists(seq))
			nready++
		}
	}
	require_Equal(t, al.ready.Size(), nready)
}

func TestAckLimitsRandomized(t *testing.T) {
	cfg := &ConsumerConfig{MaxAckPendingPerSubjectTokens: []MaxAckPendingPerSubjectTokens{
		{Filter: "operations.*.>", Wildcards: []int{1}, MaxAckPending: 1},
		{Filter: "billing.*.*", Wildcards: []int{2}, MaxAckPending: 3},
	}}
	seed := uint64(time.Now().UnixNano())
	t.Logf("seed %d", seed)
	rng := rand.New(rand.NewPCG(seed, seed))

	subjects := []string{
		"operations.X.deposit", "operations.X.withdrawal", "operations.Y.deposit", "operations.Z.deposit",
		"billing.A.invoice", "billing.B.invoice", "billing.C.refund", "other.A", "other.B",
	}
	al := newAckLimits(cfg, 1000)
	o := &consumer{}

	pending := make(map[uint64]uint64)    // seq -> dseq
	deferred := make(map[uint64]struct{}) // deferred seqs
	keyOf := make(map[uint64]string)      // seq -> "filter/key", empty for untracked
	var seq, dseq uint64                  // next stream and consumer sequences
	o.sseq, o.dseq = 1, 1

	keyFor := func(subj string) string {
		if e, k := al.classify(subj); e != nil {
			return e.filter + "/" + k
		}
		return _EMPTY_
	}
	deliver := func(s uint64) {
		dseq++
		pending[s] = dseq
		al.tracked(dseq)
		o.dseq = dseq + 1
	}
	randomKey := func(m map[uint64]uint64) (uint64, bool) {
		if len(m) == 0 {
			return 0, false
		}
		keys := make([]uint64, 0, len(m))
		for k := range m {
			keys = append(keys, k)
		}
		return keys[rng.IntN(len(keys))], true
	}
	randomDeferred := func() (uint64, bool) {
		if len(deferred) == 0 {
			return 0, false
		}
		keys := make([]uint64, 0, len(deferred))
		for k := range deferred {
			keys = append(keys, k)
		}
		return keys[rng.IntN(len(keys))], true
	}

	for i := 0; i < 20_000; i++ {
		switch rng.IntN(10) {
		case 0, 1, 2, 3:
			// Scan the next message.
			seq++
			o.sseq = seq + 1
			subj := subjects[rng.IntN(len(subjects))]
			keyOf[seq] = keyFor(subj)
			if al.admit(seq, subj) {
				if rng.IntN(10) == 0 {
					// No waiting request, undo.
					al.undeliver(seq)
					delete(keyOf, seq)
					seq--
					o.sseq = seq + 1
				} else {
					deliver(seq)
				}
			} else {
				deferred[seq] = struct{}{}
			}
		case 4, 5:
			// Deliver a ready deferred message.
			if s, ok := al.nextReady(); ok {
				_, isDeferred := deferred[s]
				require_True(t, isDeferred)
				al.takeReady(s)
				if rng.IntN(10) == 0 {
					al.undeliverDeferred(s)
				} else {
					delete(deferred, s)
					deliver(s)
				}
			}
		case 6, 7, 8:
			// Ack a pending message.
			if s, ok := randomKey(pending); ok {
				al.release(s, pending[s])
				delete(pending, s)
				delete(keyOf, s)
			}
		case 9:
			// A deferred message is removed from the stream.
			if s, ok := randomDeferred(); ok {
				al.dropDeferred(s)
				delete(deferred, s)
				delete(keyOf, s)
			}
		}
		checkAckLimitsInvariants(t, al, pending, deferred, keyOf)

		// Floors are one below the lowest outstanding sequences.
		adflr, asflr := al.floors(o)
		minSeq, minDseq := o.sseq-1, o.dseq-1
		for s, d := range pending {
			minSeq = min(minSeq, s-1)
			minDseq = min(minDseq, d-1)
		}
		for s := range deferred {
			minSeq = min(minSeq, s-1)
		}
		require_Equal(t, asflr, minSeq)
		require_Equal(t, adflr, minDseq)
	}

	// Drain everything and make sure we end up empty.
	for len(pending) > 0 || len(deferred) > 0 {
		if s, ok := al.nextReady(); ok {
			al.takeReady(s)
			delete(deferred, s)
			deliver(s)
		}
		if s, ok := randomKey(pending); ok {
			al.release(s, pending[s])
			delete(pending, s)
			delete(keyOf, s)
		}
		checkAckLimitsInvariants(t, al, pending, deferred, keyOf)
	}
	require_True(t, al.empty())
	for _, e := range al.entries {
		require_Equal(t, len(e.keys), 0)
	}
	require_Equal(t, len(al.seqs), 0)
}

func TestAckLimitsRebuildAndResolve(t *testing.T) {
	ms, err := newMemStore(&StreamConfig{Storage: MemoryStorage, Subjects: []string{"operations.>", "other.>"}})
	require_NoError(t, err)
	defer ms.Stop()
	mset := &stream{store: ms}
	store := func(subj string) uint64 {
		seq, _, err := ms.StoreMsg(subj, nil, nil, 0)
		require_NoError(t, err)
		return seq
	}
	// 1: X, 2: Y, 3: X, 4: X, 5: other, 6: Y, 7: X, 8: Z
	for _, subj := range []string{
		"operations.X.a", "operations.Y.a", "operations.X.b", "operations.X.c",
		"other.a", "operations.Y.b", "operations.X.d", "operations.Z.a",
	} {
		store(subj)
	}
	cfg := &ConsumerConfig{MaxAckPendingPerSubjectTokens: []MaxAckPendingPerSubjectTokens{
		{Filter: "operations.*.>", Wildcards: []int{1}, MaxAckPending: 1},
	}}

	// 1 (X) and 2 (Y) pending, 3 and 4 (X) deferred, 5 (other) pending, 6 (Y) deferred,
	// 7 (X) deferred, 8 (Z) deferred, 9 and 10 not in the stream yet.
	o := &consumer{mset: mset, cfg: *cfg, maxp: 100, sseq: 11, dseq: 4}
	o.pending = map[uint64]*Pending{1: {1, 0}, 2: {2, 0}, 5: {3, 0}, 10: {4, 0}}
	deferred := map[uint64]struct{}{3: {}, 4: {}, 6: {}, 7: {}, 8: {}, 9: {}}
	al := newAckLimits(cfg, 100)
	gone := al.rebuild(o, deferred)
	require_Equal(t, len(gone), 0)
	require_Equal(t, al.ndeferred(), 6)
	require_True(t, slices.Equal(al.unresolved, []uint64{9, 10}))
	// X and Y are full, Z has room.
	seq, ok := al.nextReady()
	require_True(t, ok)
	require_Equal(t, seq, 8)
	require_Equal(t, al.ready.Size(), 1)
	// Floors.
	adflr, asflr := al.floors(o)
	require_Equal(t, asflr, 0)
	require_Equal(t, adflr, 0)

	// Ack 1 (X), 3 becomes ready.
	require_True(t, al.release(1, 1))
	delete(o.pending, 1)
	seq, _ = al.nextReady()
	require_Equal(t, seq, 3)
	adflr, asflr = al.floors(o)
	require_Equal(t, asflr, 1)
	require_Equal(t, adflr, 1)

	// Nothing to resolve yet.
	require_Equal(t, len(al.resolve(o)), 0)
	require_True(t, slices.Equal(al.unresolved, []uint64{9, 10}))

	// The stream catches up: 9 is a Y message (deferred behind 6), 10 an X message (pending).
	store("operations.Y.c")
	store("operations.X.e")
	require_Equal(t, len(al.resolve(o)), 0)
	require_Equal(t, len(al.unresolved), 0)
	e, _ := al.classify("operations.Y.c")
	ks := e.keys["Y"]
	require_True(t, slices.Equal(ks.deferred, []uint64{6, 9}))
	require_Equal(t, ks.pending, 1)
	ks = e.keys["X"]
	require_Equal(t, ks.pending, 1)
	require_True(t, slices.Equal(ks.deferred, []uint64{3, 4, 7}))
	// X is full again, so 3 is no longer ready.
	require_False(t, al.ready.Exists(3))

	// A deferred message that is gone is reported by rebuild.
	_, err = ms.RemoveMsg(4)
	require_NoError(t, err)
	al = newAckLimits(cfg, 100)
	gone = al.rebuild(o, deferred)
	require_True(t, slices.Equal(gone, []uint64{4}))
	require_False(t, al.isDeferred(4))

	// Untracked deferred messages, after the entries were removed, are all ready in order.
	o.cfg.MaxAckPendingPerSubjectTokens = nil
	al = newAckLimits(&o.cfg, 100)
	gone = al.rebuild(o, deferred)
	require_True(t, slices.Equal(gone, []uint64{4}))
	require_True(t, al.draining())
	// 3, 6, 7, 8 and 9, which the stream has by now.
	require_Equal(t, al.ready.Size(), 5)
	seq, _ = al.nextReady()
	require_Equal(t, seq, 3)
	al.takeReady(3)
	seq, _ = al.nextReady()
	require_Equal(t, seq, 6)
}
