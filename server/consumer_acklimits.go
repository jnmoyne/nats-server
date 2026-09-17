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
	"slices"
	"strings"

	"github.com/nats-io/nats-server/v2/server/avl"
)

// This file holds the consumer leader runtime for max ack pending per subject tokens (ADR-42).
//
// Each configured entry bounds the number of unacknowledged messages per key, the key being
// made of selected wildcard tokens of the message subject. When the next message in stream
// order belongs to a key at its limit it is deferred: its sequence is recorded, the delivery
// cursor moves past it and the scan continues with later messages. Deferred messages are
// delivered later, lowest sequence first per key, as soon as their key has room.
//
// Deferred messages are therefore delivered out of stream sequence order, with a consumer
// sequence higher than messages delivered before them. The ack floors can then no longer be
// derived by walking forward from an acked sequence, they are the minimum of the pending and
// deferred sets minus one. The stores keep the same index, see ackIdx.

// ackLimitEntry is a compiled MaxAckPendingPerSubjectTokens entry.
type ackLimitEntry struct {
	filter string
	tokens []string // Tokenized filter.
	stis   []int    // Token positions of the selected '*' tokens, nil means the whole subject is the key.
	maxp   int      // Max ack pending per key.
	keys   map[string]*ackLimitKey
}

// ackLimitKey is the accounting for one key of an entry.
// Only keys with pending or deferred messages are kept.
type ackLimitKey struct {
	pending  int      // Delivered and not yet acked.
	deferred []uint64 // Ascending stream sequences waiting for room.
}

// ackKeyRef points a tracked stream sequence to its entry and key.
// A nil entry marks a deferred sequence not tracked by any entry: it matched none after a
// config change, or the entries were removed and the consumer drains its deferred messages.
type ackKeyRef struct {
	e  *ackLimitEntry
	k  string
	ks *ackLimitKey
}

// ackLimits is the consumer leader runtime state for max ack pending per subject tokens.
// It is nil on followers and when the feature is off and nothing is left over from it.
// All operations require the consumer lock to be held.
type ackLimits struct {
	// Compiled entries, nil while draining after the entries were removed from the config.
	entries []*ackLimitEntry
	// Every pending or deferred stream sequence tracked by an entry, and untracked deferred ones.
	seqs map[uint64]ackKeyRef
	// Deferred sequences that can be delivered now: the heads of keys with room and untracked ones.
	ready *avl.SequenceSet
	// All deferred stream sequences.
	dset *avl.SequenceSet
	// Pending and deferred stream sequences, for the stream ack floor.
	pseqs *avl.SequenceSet
	// Pending consumer sequences, for the consumer ack floor.
	pdseqs *avl.SequenceSet
	// Ascending sequences that were above the stream's last sequence when we rebuilt, they
	// can not be classified until our stream replica has them. See resolve.
	unresolved []uint64
	// Bound on the number of deferred messages, we stall like max ack pending when reached.
	limit int
}

func newAckLimits(cfg *ConsumerConfig, maxp int) *ackLimits {
	al := &ackLimits{
		seqs:   make(map[uint64]ackKeyRef),
		ready:  &avl.SequenceSet{},
		dset:   &avl.SequenceSet{},
		pseqs:  &avl.SequenceSet{},
		pdseqs: &avl.SequenceSet{},
	}
	al.setEntries(cfg)
	al.setLimit(maxp)
	return al
}

// setLimit sets the lookahead bound from the consumer level max ack pending.
func (al *ackLimits) setLimit(maxp int) {
	if maxp > 0 {
		al.limit = maxp
	} else {
		al.limit = JsDefaultMaxAckPending
	}
}

// setEntries compiles the entries from the config. Accounting must be rebuilt afterwards.
func (al *ackLimits) setEntries(cfg *ConsumerConfig) {
	al.entries = nil
	for _, mape := range cfg.MaxAckPendingPerSubjectTokens {
		filter := mape.Filter
		if filter == _EMPTY_ {
			filter = fwcs
		}
		tokens := tokenizeSubjectIntoSlice(nil, filter)
		var stis []int
		if len(mape.Wildcards) > 0 {
			// Map the 1-based ordinals among the '*' tokens to token positions.
			var positions []int
			for i, t := range tokens {
				if t == pwcs {
					positions = append(positions, i)
				}
			}
			for _, w := range mape.Wildcards {
				if w >= 1 && w <= len(positions) {
					stis = append(stis, positions[w-1])
				}
			}
		}
		al.entries = append(al.entries, &ackLimitEntry{
			filter: filter,
			tokens: tokens,
			stis:   stis,
			maxp:   mape.MaxAckPending,
			keys:   make(map[string]*ackLimitKey),
		})
	}
}

// draining returns whether the entries were removed while deferred messages remained.
func (al *ackLimits) draining() bool {
	return len(al.entries) == 0
}

// classify returns the entry the subject is in scope of and the key of the subject for it,
// or nil when the subject matches no entry.
func (al *ackLimits) classify(subj string) (*ackLimitEntry, string) {
	if len(al.entries) == 0 {
		return nil, _EMPTY_
	}
	var tsa [32]string
	tts := tokenizeSubjectIntoSlice(tsa[:0], subj)
	for _, e := range al.entries {
		if !isSubsetMatchTokenized(tts, e.tokens) {
			continue
		}
		switch len(e.stis) {
		case 0:
			return e, subj
		case 1:
			return e, tts[e.stis[0]]
		}
		n := len(e.stis) - 1
		for _, ti := range e.stis {
			n += len(tts[ti])
		}
		var sb strings.Builder
		sb.Grow(n)
		for i, ti := range e.stis {
			if i > 0 {
				sb.WriteByte(btsep)
			}
			sb.WriteString(tts[ti])
		}
		return e, sb.String()
	}
	return nil, _EMPTY_
}

// key returns the accounting for a key, creating it if needed.
func (e *ackLimitEntry) key(k string) *ackLimitKey {
	ks := e.keys[k]
	if ks == nil {
		ks = &ackLimitKey{}
		e.keys[k] = ks
	}
	return ks
}

// keyDone drops the accounting of a key once nothing is pending or deferred for it.
func (al *ackLimits) keyDone(ref ackKeyRef) {
	if ref.e != nil && ref.ks.pending <= 0 && len(ref.ks.deferred) == 0 {
		delete(ref.e.keys, ref.k)
	}
}

// promote makes the head of a key ready when the key has room.
// Returns true when a sequence became ready.
func (al *ackLimits) promote(ref ackKeyRef) bool {
	ks := ref.ks
	if len(ks.deferred) == 0 || ks.pending >= ref.e.maxp {
		return false
	}
	head := ks.deferred[0]
	if al.ready.Exists(head) {
		return false
	}
	al.ready.Insert(head)
	return true
}

// admit is called by the forward scan for the next message in stream order.
// It returns false when the message must be deferred because its key is at its limit,
// in which case it is recorded as deferred. Otherwise the message is accounted as
// pending for its key and must be delivered, or undone with undeliver.
func (al *ackLimits) admit(seq uint64, subj string) bool {
	al.pseqs.Insert(seq)
	e, key := al.classify(subj)
	if e == nil {
		// Only the consumer level max ack pending applies.
		return true
	}
	ks := e.key(key)
	al.seqs[seq] = ackKeyRef{e, key, ks}
	// Deliver in order per key, so queue behind any deferred message of the key.
	if len(ks.deferred) > 0 || ks.pending >= e.maxp {
		ks.deferred = append(ks.deferred, seq)
		al.dset.Insert(seq)
		return false
	}
	ks.pending++
	return true
}

// undeliver undoes an admit when the message could not be delivered after all.
func (al *ackLimits) undeliver(seq uint64) {
	al.pseqs.Delete(seq)
	ref, ok := al.seqs[seq]
	if !ok {
		return
	}
	delete(al.seqs, seq)
	ref.ks.pending--
	al.keyDone(ref)
}

// nextReady returns the lowest deferred sequence that can be delivered now.
func (al *ackLimits) nextReady() (uint64, bool) {
	if al.ready.IsEmpty() {
		return 0, false
	}
	seq, _ := al.ready.MinMax()
	return seq, true
}

// takeReady marks a ready deferred sequence as being delivered now.
func (al *ackLimits) takeReady(seq uint64) {
	al.ready.Delete(seq)
	al.dset.Delete(seq)
	ref, ok := al.seqs[seq]
	if !ok {
		return
	}
	if ref.e == nil {
		// Untracked, it is a plain pending message from now on.
		delete(al.seqs, seq)
		return
	}
	ks := ref.ks
	if i := slices.Index(ks.deferred, seq); i >= 0 {
		ks.deferred = slices.Delete(ks.deferred, i, i+1)
	}
	ks.pending++
	al.promote(ref)
}

// undeliverDeferred undoes a takeReady when the message could not be delivered after all.
func (al *ackLimits) undeliverDeferred(seq uint64) {
	al.dset.Insert(seq)
	ref, ok := al.seqs[seq]
	if !ok {
		// Untracked, put it back as such.
		al.seqs[seq] = ackKeyRef{}
		al.ready.Insert(seq)
		return
	}
	ks := ref.ks
	ks.pending--
	// It goes back to the head, so the head we may have promoted is no longer ready.
	if len(ks.deferred) > 0 {
		al.ready.Delete(ks.deferred[0])
	}
	ks.deferred = slices.Insert(ks.deferred, 0, seq)
	al.ready.Insert(seq)
}

// tracked records the consumer sequence of a first delivery.
func (al *ackLimits) tracked(dseq uint64) {
	if dseq > 0 {
		al.pdseqs.Insert(dseq)
	}
}

// release is called when a pending message leaves the pending set: ack, term or max deliver.
// Returns true when a deferred message became ready as a result.
func (al *ackLimits) release(seq, dseq uint64) bool {
	al.pseqs.Delete(seq)
	if dseq > 0 {
		al.pdseqs.Delete(dseq)
	}
	ref, ok := al.seqs[seq]
	if !ok {
		return false
	}
	delete(al.seqs, seq)
	if ref.ks.pending > 0 {
		ref.ks.pending--
	}
	promoted := al.promote(ref)
	al.keyDone(ref)
	return promoted
}

// dropDeferred removes a deferred message that is gone from the stream.
// Returns true when another deferred message became ready as a result.
func (al *ackLimits) dropDeferred(seq uint64) bool {
	if !al.dset.Delete(seq) {
		return false
	}
	al.pseqs.Delete(seq)
	al.ready.Delete(seq)
	al.removeUnresolved(seq)
	ref, ok := al.seqs[seq]
	delete(al.seqs, seq)
	if !ok || ref.e == nil {
		return false
	}
	ks := ref.ks
	if i := slices.Index(ks.deferred, seq); i >= 0 {
		ks.deferred = slices.Delete(ks.deferred, i, i+1)
	}
	promoted := al.promote(ref)
	al.keyDone(ref)
	return promoted
}

func (al *ackLimits) removeUnresolved(seq uint64) {
	if i, ok := slices.BinarySearch(al.unresolved, seq); ok {
		al.unresolved = slices.Delete(al.unresolved, i, i+1)
	}
}

// isDeferred returns whether the sequence is deferred.
func (al *ackLimits) isDeferred(seq uint64) bool {
	return al.dset.Exists(seq)
}

// ndeferred returns the number of deferred messages.
func (al *ackLimits) ndeferred() int {
	return al.dset.Size()
}

// empty returns whether nothing is pending or deferred.
func (al *ackLimits) empty() bool {
	return al.pseqs.IsEmpty()
}

// floors returns the exact ack floors, one below the lowest pending or deferred stream
// sequence and one below the lowest pending consumer sequence, or the current cursors
// minus one when nothing is outstanding.
func (al *ackLimits) floors(o *consumer) (adflr, asflr uint64) {
	if o.dseq > 0 {
		adflr = o.dseq - 1
	}
	if o.sseq > 0 {
		asflr = o.sseq - 1
	}
	if !al.pseqs.IsEmpty() {
		min, _ := al.pseqs.MinMax()
		asflr = min - 1
	}
	if !al.pdseqs.IsEmpty() {
		min, _ := al.pdseqs.MinMax()
		adflr = min - 1
	}
	return adflr, asflr
}

// deferredSet returns the deferred sequences as a set for the consumer state.
func (al *ackLimits) deferredSet() map[uint64]struct{} {
	if al.dset.IsEmpty() {
		return nil
	}
	deferred := make(map[uint64]struct{}, al.dset.Size())
	al.dset.Range(func(seq uint64) bool {
		deferred[seq] = struct{}{}
		return true
	})
	return deferred
}

// deferredBelow returns the deferred sequences below seq, ascending.
func (al *ackLimits) deferredBelow(seq uint64) []uint64 {
	var seqs []uint64
	al.dset.Range(func(dseq uint64) bool {
		if dseq >= seq {
			return false
		}
		seqs = append(seqs, dseq)
		return true
	})
	return seqs
}

// deferredSeqs returns all deferred sequences, ascending.
func (al *ackLimits) deferredSeqs() []uint64 {
	seqs := make([]uint64, 0, al.dset.Size())
	al.dset.Range(func(seq uint64) bool {
		seqs = append(seqs, seq)
		return true
	})
	return seqs
}

// addDeferred accounts a restored deferred sequence. Sequences must be added in ascending order.
func (al *ackLimits) addDeferred(seq uint64, subj string) {
	e, key := al.classify(subj)
	if e == nil {
		// Untracked, deliver it as soon as possible.
		al.seqs[seq] = ackKeyRef{}
		al.ready.Insert(seq)
		return
	}
	ks := e.key(key)
	ks.deferred = append(ks.deferred, seq)
	al.seqs[seq] = ackKeyRef{e, key, ks}
	al.promote(ackKeyRef{e, key, ks})
}

// addPending accounts a restored pending sequence.
func (al *ackLimits) addPending(seq uint64, subj string) {
	e, key := al.classify(subj)
	if e == nil {
		return
	}
	ks := e.key(key)
	ks.pending++
	al.seqs[seq] = ackKeyRef{e, key, ks}
	// The key may have lost its room.
	if ks.pending >= e.maxp && len(ks.deferred) > 0 {
		al.ready.Delete(ks.deferred[0])
	}
}

// clear drops all accounting but keeps the entries and the limit.
func (al *ackLimits) clear() {
	for _, e := range al.entries {
		clear(e.keys)
	}
	clear(al.seqs)
	al.ready.Empty()
	al.dset.Empty()
	al.pseqs.Empty()
	al.pdseqs.Empty()
	al.unresolved = nil
}

// rebuild recreates the accounting from the consumer's pending set and the given deferred
// set, deriving each message's key from its subject. Sequences above the stream's last
// sequence can not be classified yet, our stream replica may lag, they are kept and
// resolved lazily. Deferred sequences that are gone from the stream are returned so the
// caller can drop them from the state.
// Consumer lock should be held.
func (al *ackLimits) rebuild(o *consumer, deferred map[uint64]struct{}) (gone []uint64) {
	al.clear()
	if o.mset == nil || o.mset.store == nil {
		return nil
	}
	store := o.mset.store
	var ss StreamState
	store.FastState(&ss)

	// Deferred first and in order, so per key lists stay ascending.
	dseqs := make([]uint64, 0, len(deferred))
	for seq := range deferred {
		dseqs = append(dseqs, seq)
	}
	slices.Sort(dseqs)
	for _, seq := range dseqs {
		if seq < ss.FirstSeq {
			gone = append(gone, seq)
			continue
		}
		al.dset.Insert(seq)
		al.pseqs.Insert(seq)
		if seq > ss.LastSeq {
			al.unresolved = append(al.unresolved, seq)
			continue
		}
		subj, err := store.SubjectForSeq(seq)
		if err != nil {
			al.dset.Delete(seq)
			al.pseqs.Delete(seq)
			gone = append(gone, seq)
			continue
		}
		al.addDeferred(seq, subj)
	}

	pseqs := make([]uint64, 0, len(o.pending))
	for seq := range o.pending {
		pseqs = append(pseqs, seq)
	}
	slices.Sort(pseqs)
	for _, seq := range pseqs {
		p := o.pending[seq]
		al.pseqs.Insert(seq)
		if p != nil {
			al.tracked(p.Sequence)
		}
		if seq > ss.LastSeq {
			al.unresolved = append(al.unresolved, seq)
			continue
		}
		// A pending message that is gone is cleaned up by checkPending, it stays untracked here.
		if subj, err := store.SubjectForSeq(seq); err == nil {
			al.addPending(seq, subj)
		}
	}
	slices.Sort(al.unresolved)
	return gone
}

// resolve classifies the unresolved sequences our stream replica now has. It runs before
// every scan, so per key order is preserved: the scan starts above every unresolved sequence
// and can not pass the stream's last sequence before they are resolved.
// Returns deferred sequences that turned out to be gone from the stream.
// Consumer lock should be held.
func (al *ackLimits) resolve(o *consumer) (gone []uint64) {
	if len(al.unresolved) == 0 || o.mset == nil || o.mset.store == nil {
		return nil
	}
	store := o.mset.store
	var ss StreamState
	store.FastState(&ss)
	var i int
	for i < len(al.unresolved) {
		seq := al.unresolved[i]
		if seq > ss.LastSeq {
			break
		}
		i++
		subj, err := store.SubjectForSeq(seq)
		if al.dset.Exists(seq) {
			if err != nil {
				gone = append(gone, seq)
				continue
			}
			al.addDeferred(seq, subj)
		} else if err == nil {
			al.addPending(seq, subj)
		}
	}
	al.unresolved = al.unresolved[i:]
	return gone
}

// Consumer side helpers.

// setupAckLimitsLocked creates, rebuilds or drops the max ack pending per subject tokens
// runtime from the config, the pending set and the given deferred set. The runtime is kept,
// draining, as long as deferred messages or out of order pending messages remain after the
// entries were removed. Leader only. Lock should be held.
func (o *consumer) setupAckLimitsLocked(deferred map[uint64]struct{}) {
	if len(o.cfg.MaxAckPendingPerSubjectTokens) == 0 && len(deferred) == 0 && !pendingOutOfOrder(o.pending) {
		o.ackLimits = nil
		return
	}
	al := newAckLimits(&o.cfg, o.maxp)
	gone := al.rebuild(o, deferred)
	o.ackLimits = al
	for _, seq := range gone {
		// Pass 0 as the delivered sequence to only remove the deferred state.
		o.updateAcks(0, seq, _EMPTY_)
	}
	o.adflr, o.asflr = al.floors(o)
}

// maybeDropAckLimitsLocked drops the runtime once the entries were removed from the config
// and nothing is outstanding anymore. Lock should be held.
func (o *consumer) maybeDropAckLimitsLocked() {
	if o.ackLimits != nil && o.ackLimits.draining() && o.ackLimits.empty() && len(o.pending) == 0 {
		o.ackLimits = nil
	}
}

// releasePendingLocked accounts a message that left the pending set and signals when a
// deferred message became ready as a result. Lock should be held.
func (o *consumer) releasePendingLocked(seq, dseq uint64) {
	if o.ackLimits != nil && o.ackLimits.release(seq, dseq) {
		o.signalNewMessages()
	}
}

// dropDeferredLocked drops a deferred message that is gone from the stream.
// Lock should be held.
func (o *consumer) dropDeferredLocked(seq uint64) {
	if o.ackLimits == nil {
		return
	}
	if o.ackLimits.dropDeferred(seq) {
		o.signalNewMessages()
	}
	// Pass 0 as the delivered sequence to only remove the deferred state.
	o.updateAcks(0, seq, _EMPTY_)
}

// dropDeferredBelowLocked drops the deferred messages below seq, they are gone from the stream.
// Returns true when any was dropped. Lock should be held.
func (o *consumer) dropDeferredBelowLocked(seq uint64) bool {
	if o.ackLimits == nil {
		return false
	}
	seqs := o.ackLimits.deferredBelow(seq)
	for _, dseq := range seqs {
		o.dropDeferredLocked(dseq)
	}
	return len(seqs) > 0
}

// numDeferred returns the number of deferred messages. Lock should be held.
func (o *consumer) numDeferred() int {
	if o.ackLimits == nil {
		return 0
	}
	return o.ackLimits.ndeferred()
}

// deferredForStoreLocked returns the deferred set to write to the store: ours when we track
// it, otherwise what the store already has so that a state write never loses it.
// Lock should be held.
func (o *consumer) deferredForStoreLocked() map[uint64]struct{} {
	if o.ackLimits != nil {
		return o.ackLimits.deferredSet()
	}
	if o.store != nil {
		if state, err := o.store.BorrowState(); err == nil && state != nil {
			return state.Deferred
		}
	}
	return nil
}

// updateDeferred records sequences the scan passed over because their key was at its limit.
// Lock should be held.
func (o *consumer) updateDeferred(seqs []uint64) {
	// Clustered mode and R>1 is handled by replication, see the cluster entries.
	if o.node == nil && o.store != nil {
		o.store.UpdateDeferred(seqs)
	}
}
