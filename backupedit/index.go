// Package backupedit reads a JetStream v2 stream backup, applies message-level
// filters, and writes a new backup. This file is the on-disk index used only by
// the per-subject path (last-N, kv-compact); stateless filters open no index.
// See filter.go for the algorithm.
package backupedit

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"go.etcd.io/bbolt"
)

// Bucket names.
var (
	bucketKeep      = []byte("keep")
	bucketLatest    = []byte("latest")
	bucketRing      = []byte("ring")
	bucketRingCount = []byte("ring_count")
	bucketMeta      = []byte("meta")
)

func allBuckets() [][]byte {
	return [][]byte{bucketKeep, bucketLatest, bucketRing, bucketRingCount, bucketMeta}
}

const (
	metaStatusKey      = "status"
	metaStatusPartial  = "partial"
	metaStatusComplete = "complete"

	defaultBatchSize = 10000
)

var (
	bboltOpenTimeout   = 1 * time.Second
	bboltReopenTimeout = 30 * time.Second
)

// ErrIndexIncomplete is returned by openExistingIndex when the index file
// was not promoted to "complete" status before its producer exited.
var ErrIndexIncomplete = errors.New("backupedit: index is incomplete (producer did not call MarkComplete)")

// ErrReadOnly is returned from write operations when the index was opened
// read-only.
var ErrReadOnly = errors.New("backupedit: index opened read-only")

// indexSlot is the per-candidate payload in the latest bucket. IsTombstone is
// used only by kv-compact, to drop subjects whose newest message is a delete.
type indexSlot struct {
	Seq         uint64
	IsTombstone bool
}

// indexOptions controls index lifecycle.
type indexOptions struct {
	Logger   *slog.Logger // nil → slog.Default()
	Dir      string       // empty → os.TempDir(); ignored by openExistingIndex
	Keep     bool         // when true, Close does NOT remove the file; ignored by openExistingIndex (always keeps)
	ReadOnly bool         // openExistingIndex: take the shared lock; writes return ErrReadOnly
}

// pendingOp is a single queued mutation. pushRing eviction is handled
// during flush so the delete-and-insert lives in one tx.
type pendingOp struct {
	bucket []byte
	key    []byte
	value  []byte

	// pushRing-specific fields. When ringCap > 0 the flusher runs the
	// compound delete-oldest-if-cap + put + count-update sequence.
	ringCap int
	subj    []byte
}

// index is the bbolt-backed decision store for the per-subject path.
type index struct {
	db       *bbolt.DB
	path     string
	log      *slog.Logger
	keep     bool
	readOnly bool

	pending   []pendingOp
	batchSize int
}

// openIndex creates a fresh bbolt DB.
func openIndex(opts indexOptions) (*index, error) {
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	dir := opts.Dir
	if dir == "" {
		dir = os.TempDir()
	}

	var rnd [8]byte
	if _, err := rand.Read(rnd[:]); err != nil {
		return nil, fmt.Errorf("backupedit: entropy unavailable: %w", err)
	}
	path := filepath.Join(dir, fmt.Sprintf("backupedit-idx-%s-%d.bolt", hex.EncodeToString(rnd[:]), os.Getpid()))

	db, err := bbolt.Open(path, 0o600, &bbolt.Options{Timeout: bboltOpenTimeout})
	if err != nil {
		os.Remove(path)
		return nil, fmt.Errorf("backupedit: open index: %w", err)
	}

	if uerr := db.Update(func(tx *bbolt.Tx) error {
		for _, b := range allBuckets() {
			if _, e := tx.CreateBucketIfNotExists(b); e != nil {
				return e
			}
		}
		return tx.Bucket(bucketMeta).Put([]byte(metaStatusKey), []byte(metaStatusPartial))
	}); uerr != nil {
		db.Close()
		os.Remove(path)
		return nil, fmt.Errorf("backupedit: initialise index: %w", uerr)
	}

	x := &index{
		db:        db,
		path:      path,
		log:       opts.Logger.With("component", "backupedit.index", "path", path),
		keep:      opts.Keep,
		readOnly:  false,
		batchSize: defaultBatchSize,
	}
	x.log.Info("index opened", "pid", os.Getpid(), "keep", opts.Keep)
	return x, nil
}

// openExistingIndex reopens an index produced with Keep=true and a successful
// MarkComplete.
func openExistingIndex(path string, opts indexOptions) (*index, error) {
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	// bbolt.Open creates a fresh file on ENOENT; we must check first so
	// callers see a clean fs.ErrNotExist.
	if _, err := os.Stat(path); err != nil {
		return nil, fmt.Errorf("backupedit: open existing index %s: %w", path, err)
	}
	db, err := bbolt.Open(path, 0o600, &bbolt.Options{Timeout: bboltReopenTimeout, ReadOnly: opts.ReadOnly})
	if err != nil {
		return nil, fmt.Errorf("backupedit: open existing index %s: %w", path, err)
	}

	verifyErr := db.View(func(tx *bbolt.Tx) error {
		for _, b := range allBuckets() {
			if tx.Bucket(b) == nil {
				return fmt.Errorf("backupedit: index missing bucket %q", b)
			}
		}
		if string(tx.Bucket(bucketMeta).Get([]byte(metaStatusKey))) != metaStatusComplete {
			return ErrIndexIncomplete
		}
		return nil
	})
	if verifyErr != nil {
		db.Close()
		return nil, verifyErr
	}

	x := &index{
		db:        db,
		path:      path,
		log:       opts.Logger.With("component", "backupedit.index", "path", path),
		keep:      true, // never delete a reopened file
		readOnly:  opts.ReadOnly,
		batchSize: defaultBatchSize,
	}
	x.log.Info("index reopened", "pid", os.Getpid(), "readonly", opts.ReadOnly)
	return x, nil
}

// Path returns the absolute path of the underlying bbolt file.
func (x *index) Path() string { return x.path }

// MarkComplete promotes meta/status to "complete". Call only after walk 1 has
// finished successfully.
func (x *index) MarkComplete() error {
	if x.readOnly {
		return ErrReadOnly
	}
	if err := x.flush(); err != nil {
		return err
	}
	err := x.db.Update(func(tx *bbolt.Tx) error {
		return tx.Bucket(bucketMeta).Put([]byte(metaStatusKey), []byte(metaStatusComplete))
	})
	if err != nil {
		return fmt.Errorf("backupedit: mark complete: %w", err)
	}
	x.log.Info("index marked complete")
	return nil
}

// Close flushes pending writes, closes the DB, and removes the file unless
// Keep is set. Returns errors.Join of every error so callers see them all.
func (x *index) Close() error {
	if x.db == nil {
		return nil
	}
	flushErr := x.flush()
	closeErr := x.db.Close()
	x.db = nil
	var removeErr error
	if x.keep {
		x.log.Info("index closed (kept)", "close_err", errOrNil(closeErr), "flush_err", errOrNil(flushErr))
	} else {
		removeErr = os.Remove(x.path)
		if removeErr != nil && errors.Is(removeErr, fs.ErrNotExist) {
			removeErr = nil
		}
		x.log.Info("index closed", "close_err", errOrNil(closeErr), "remove_err", errOrNil(removeErr), "flush_err", errOrNil(flushErr))
	}
	joined := errors.Join(flushErr, closeErr, removeErr)
	if joined != nil {
		x.log.Error("index close failed", "err", joined)
	}
	return joined
}

// setLatest upserts a slot into the latest bucket (ringCap == 1 path).
func (x *index) setLatest(subj string, s indexSlot) error {
	if x.readOnly {
		return ErrReadOnly
	}
	x.pending = append(x.pending, pendingOp{
		bucket: bucketLatest,
		key:    []byte(subj),
		value:  encodeLatestSlot(s),
	})
	return x.maybeFlush()
}

// pushRing appends a surviving seq to the per-subject ring, evicting the oldest
// when the ring is full. The ring stores no value — the seq is the key suffix
// and last-N never consults tombstones. The compound delete+put runs atomically
// inside flush().
func (x *index) pushRing(subj string, ringCap int, s indexSlot) error {
	if x.readOnly {
		return ErrReadOnly
	}
	if ringCap < 1 {
		return fmt.Errorf("backupedit: pushRing: ringCap must be >= 1, got %d", ringCap)
	}
	x.pending = append(x.pending, pendingOp{
		bucket:  bucketRing,
		subj:    []byte(subj),
		key:     ringKey(subj, s.Seq),
		value:   []byte{},
		ringCap: ringCap,
	})
	return x.maybeFlush()
}

// finalizePerSubject resolves the per-subject buckets (latest/ring) into the
// keep bucket, all inside one db.Update so we never nest a write tx inside a
// read tx. If dropTombstoneLatest is true (kv-compact), subjects whose latest
// slot is a tombstone are dropped entirely. Returns the kept count — the only
// aggregate it can know; bytes/timestamps are accumulated by the writing walk.
func (x *index) finalizePerSubject(dropTombstoneLatest bool) (uint64, error) {
	if x.readOnly {
		return 0, ErrReadOnly
	}
	if err := x.flush(); err != nil {
		return 0, err
	}

	var kept uint64
	err := x.db.Update(func(tx *bbolt.Tx) error {
		keep := tx.Bucket(bucketKeep)
		latest := tx.Bucket(bucketLatest)
		ring := tx.Bucket(bucketRing)
		rcb := tx.Bucket(bucketRingCount)

		putKeep := func(seq uint64) error {
			var k [8]byte
			binary.BigEndian.PutUint64(k[:], seq)
			if err := keep.Put(k[:], []byte{}); err != nil {
				return err
			}
			kept++
			return nil
		}

		// Latest bucket: one slot per subject.
		if err := latest.ForEach(func(k, v []byte) error {
			s, err := decodeLatestSlot(v)
			if err != nil {
				return err
			}
			if dropTombstoneLatest && s.IsTombstone {
				return nil
			}
			return putKeep(s.Seq)
		}); err != nil {
			return err
		}

		// Ring bucket: enumerate distinct subjects via ring_count, then
		// prefix-scan ring for each. The surviving seq is the key suffix;
		// last-N never drops on tombstones, so no value decode is needed.
		c := rcb.Cursor()
		for k, _ := c.First(); k != nil; k, _ = c.Next() {
			subjBytes := append([]byte{}, k...)
			prefix := append(append([]byte{}, subjBytes...), 0x00)
			rc := ring.Cursor()
			for rk, _ := rc.Seek(prefix); rk != nil && bytes.HasPrefix(rk, prefix); rk, _ = rc.Next() {
				if err := putKeep(binary.BigEndian.Uint64(rk[len(prefix):])); err != nil {
					return err
				}
			}
		}
		return nil
	})
	if err != nil {
		return 0, fmt.Errorf("backupedit: finalize per-subject: %w", err)
	}
	return kept, nil
}

// forEachSubject is a read-only inspection helper for tests/debugging. It calls
// fn inside db.View; fn MUST NOT call back into the index for writes. Use
// finalizePerSubject for the production end-of-walk-1 pass.
func (x *index) forEachSubject(fn func(subj string, slots []indexSlot)) error {
	if err := x.flush(); err != nil {
		return err
	}
	return x.db.View(func(tx *bbolt.Tx) error {
		latest := tx.Bucket(bucketLatest)
		ring := tx.Bucket(bucketRing)

		if err := latest.ForEach(func(k, v []byte) error {
			s, err := decodeLatestSlot(v)
			if err != nil {
				return err
			}
			fn(string(k), []indexSlot{s})
			return nil
		}); err != nil {
			return err
		}

		rcb := tx.Bucket(bucketRingCount)
		c := rcb.Cursor()
		for k, _ := c.First(); k != nil; k, _ = c.Next() {
			subj := append([]byte{}, k...)
			prefix := append(append([]byte{}, subj...), 0x00)
			var slots []indexSlot
			rc := ring.Cursor()
			for rk, _ := rc.Seek(prefix); rk != nil && bytes.HasPrefix(rk, prefix); rk, _ = rc.Next() {
				slots = append(slots, indexSlot{Seq: binary.BigEndian.Uint64(rk[len(prefix):])})
			}
			if len(slots) > 0 {
				fn(string(subj), slots)
			}
		}
		return nil
	})
}

// isKept reports whether the source seq survived. Safe for concurrent callers —
// bbolt allows arbitrary parallel read txns.
func (x *index) isKept(seq uint64) (bool, error) {
	if err := x.flush(); err != nil {
		return false, err
	}
	var key [8]byte
	binary.BigEndian.PutUint64(key[:], seq)
	var found bool
	err := x.db.View(func(tx *bbolt.Tx) error {
		// keep never holds nested buckets, so non-nil Get means present
		// (even for the empty value).
		found = tx.Bucket(bucketKeep).Get(key[:]) != nil
		return nil
	})
	return found, err
}

// flush drains pending ops into one bbolt transaction.
func (x *index) flush() error {
	if len(x.pending) == 0 {
		return nil
	}
	if x.readOnly {
		return ErrReadOnly
	}
	ops := x.pending
	x.pending = nil

	err := x.db.Update(func(tx *bbolt.Tx) error {
		// Per-subject ring counts tracked across this flush so multiple
		// pushRing ops for the same subject within one tx stay consistent.
		ringCounts := map[string]uint32{}
		readRingCount := func(rcb *bbolt.Bucket, subj []byte) uint32 {
			if n, ok := ringCounts[string(subj)]; ok {
				return n
			}
			if v := rcb.Get(subj); v != nil {
				return binary.BigEndian.Uint32(v)
			}
			return 0
		}
		writeRingCount := func(rcb *bbolt.Bucket, subj []byte, n uint32) error {
			ringCounts[string(subj)] = n
			var buf [4]byte
			binary.BigEndian.PutUint32(buf[:], n)
			return rcb.Put(subj, buf[:])
		}

		for _, op := range ops {
			if op.ringCap > 0 {
				rcb := tx.Bucket(bucketRingCount)
				ring := tx.Bucket(bucketRing)
				n := readRingCount(rcb, op.subj)
				if int(n) >= op.ringCap {
					// Evict oldest entry for this subject. Terminate on the
					// prefix boundary, not cursor exhaustion, so we never
					// touch the next subject's keys.
					prefix := append(append([]byte{}, op.subj...), 0x00)
					c := ring.Cursor()
					if k, _ := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix) {
						if err := c.Delete(); err != nil {
							return err
						}
					}
					n--
				}
				if err := ring.Put(op.key, op.value); err != nil {
					return err
				}
				n++
				if err := writeRingCount(rcb, op.subj, n); err != nil {
					return err
				}
				continue
			}
			b := tx.Bucket(op.bucket)
			if b == nil {
				return fmt.Errorf("missing bucket %q", op.bucket)
			}
			if err := b.Put(op.key, op.value); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		x.log.Error("bbolt update failed", "err", err)
		return fmt.Errorf("backupedit: flush %d ops: %w", len(ops), err)
	}
	return nil
}

func (x *index) maybeFlush() error {
	if len(x.pending) >= x.batchSize {
		return x.flush()
	}
	return nil
}

// ---- Slot encoding ----

// Latest slot, 9 bytes fixed: seq(8 BE) || isTombstone(1).
func encodeLatestSlot(s indexSlot) []byte {
	var b [9]byte
	binary.BigEndian.PutUint64(b[0:8], s.Seq)
	if s.IsTombstone {
		b[8] = 1
	}
	return b[:]
}

func decodeLatestSlot(b []byte) (indexSlot, error) {
	if len(b) != 9 {
		return indexSlot{}, fmt.Errorf("backupedit: latest slot wrong length %d", len(b))
	}
	return indexSlot{
		Seq:         binary.BigEndian.Uint64(b[0:8]),
		IsTombstone: b[8] != 0,
	}, nil
}

// ringKey is `subject || 0x00 || seq(8 BE)`. The 0x00 separator is safe because
// NATS subject tokens are 7-bit ASCII with no null byte; big-endian seq makes
// lexicographic key order match numeric seq order for oldest-first prefix scans.
func ringKey(subj string, seq uint64) []byte {
	k := make([]byte, 0, len(subj)+1+8)
	k = append(k, subj...)
	k = append(k, 0x00)
	var sb [8]byte
	binary.BigEndian.PutUint64(sb[:], seq)
	return append(k, sb[:]...)
}

func errOrNil(err error) any {
	if err == nil {
		return nil
	}
	return err
}
