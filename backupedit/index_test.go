package backupedit

import (
	"context"
	"errors"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

func newTestIndex(t *testing.T, keep bool) (*index, string) {
	t.Helper()
	dir := t.TempDir()
	x, err := openIndex(indexOptions{Dir: dir, Keep: keep, Logger: silentLogger()})
	if err != nil {
		t.Fatalf("openIndex: %v", err)
	}
	return x, dir
}

func silentLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io_discard{}, &slog.HandlerOptions{Level: slog.LevelError + 10}))
}

type io_discard struct{}

func (io_discard) Write(p []byte) (int, error) { return len(p), nil }

// seedKeep records each seq as the sole survivor of its own subject (via the
// latest bucket) and finalizes, so isKept reflects them — the production way
// the keep bucket gets populated.
func seedKeep(t *testing.T, x *index, seqs ...uint64) {
	t.Helper()
	for i, s := range seqs {
		must(t, x.setLatest("subj"+itoa(i), indexSlot{Seq: s}))
	}
	if _, err := x.finalizePerSubject(false); err != nil {
		t.Fatalf("finalizePerSubject: %v", err)
	}
}

func TestIndex_OpenCreatesFileInDir(t *testing.T) {
	x, dir := newTestIndex(t, false)
	if _, err := os.Stat(x.Path()); err != nil {
		t.Fatalf("expected file at %s, stat err: %v", x.Path(), err)
	}
	if !strings.HasPrefix(filepath.Base(x.Path()), "backupedit-idx-") {
		t.Fatalf("file name should match pattern, got %s", filepath.Base(x.Path()))
	}
	if filepath.Dir(x.Path()) != dir {
		t.Fatalf("file in wrong dir: got %s want %s", filepath.Dir(x.Path()), dir)
	}
	if err := x.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestIndex_CloseRemovesFile(t *testing.T) {
	x, _ := newTestIndex(t, false)
	p := x.Path()
	if err := x.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := os.Stat(p); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("file should be gone, stat err: %v", err)
	}
	// Second Close is a no-op.
	if err := x.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

func TestIndex_KeepMode_PreservesFileAndReopens(t *testing.T) {
	x, _ := newTestIndex(t, true)
	p := x.Path()
	seedKeep(t, x, 42)
	if err := x.MarkComplete(); err != nil {
		t.Fatalf("MarkComplete: %v", err)
	}
	if err := x.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := os.Stat(p); err != nil {
		t.Fatalf("file should still exist: %v", err)
	}

	y, err := openExistingIndex(p, indexOptions{ReadOnly: true, Logger: silentLogger()})
	if err != nil {
		t.Fatalf("openExistingIndex: %v", err)
	}
	ok, err := y.isKept(42)
	if err != nil || !ok {
		t.Fatalf("isKept(42) after reopen: %v %v want true", ok, err)
	}
	if err := y.Close(); err != nil {
		t.Fatalf("y.Close: %v", err)
	}
	// File still there because Keep is implicit on reopen.
	if _, err := os.Stat(p); err != nil {
		t.Fatalf("file should still exist after reopen+close: %v", err)
	}
	os.Remove(p)
}

func TestIndex_OpenExistingIndex_RejectsPartial(t *testing.T) {
	x, _ := newTestIndex(t, true)
	p := x.Path()
	must(t, x.setLatest("foo", indexSlot{Seq: 1}))
	// Close WITHOUT MarkComplete.
	if err := x.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	defer os.Remove(p)
	_, err := openExistingIndex(p, indexOptions{Logger: silentLogger()})
	if !errors.Is(err, ErrIndexIncomplete) {
		t.Fatalf("expected ErrIndexIncomplete, got %v", err)
	}
}

func TestIndex_OpenExistingIndex_ReadOnlyRejectsWrites(t *testing.T) {
	x, _ := newTestIndex(t, true)
	p := x.Path()
	if err := x.MarkComplete(); err != nil {
		t.Fatalf("MarkComplete: %v", err)
	}
	if err := x.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	defer os.Remove(p)

	y, err := openExistingIndex(p, indexOptions{ReadOnly: true, Logger: silentLogger()})
	if err != nil {
		t.Fatalf("openExistingIndex ro: %v", err)
	}
	defer y.Close()
	if err := y.setLatest("foo", indexSlot{Seq: 1}); !errors.Is(err, ErrReadOnly) {
		t.Fatalf("expected ErrReadOnly on setLatest, got %v", err)
	}
	if err := y.pushRing("foo", 2, indexSlot{Seq: 1}); !errors.Is(err, ErrReadOnly) {
		t.Fatalf("expected ErrReadOnly on pushRing, got %v", err)
	}
}

func TestIndex_OpenExistingIndex_MissingFile(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "nope.bolt")
	_, err := openExistingIndex(missing, indexOptions{Logger: silentLogger()})
	if err == nil {
		t.Fatalf("expected error for missing file")
	}
	if !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("expected fs.ErrNotExist, got %v", err)
	}
}

func TestIndex_Finalize_IsKept_RoundTrip(t *testing.T) {
	x, _ := newTestIndex(t, false)
	defer x.Close()
	seedKeep(t, x, 1, 3, 7, 1<<40)
	for _, s := range []uint64{1, 3, 7, 1 << 40} {
		ok, err := x.isKept(s)
		if err != nil || !ok {
			t.Fatalf("isKept(%d): %v %v want true", s, ok, err)
		}
	}
	for _, s := range []uint64{0, 2, 999} {
		ok, err := x.isKept(s)
		if err != nil || ok {
			t.Fatalf("isKept(%d): %v %v want false", s, ok, err)
		}
	}
}

func TestIndex_Finalize_DropsTombstoneLatest(t *testing.T) {
	x, _ := newTestIndex(t, false)
	defer x.Close()
	must(t, x.setLatest("a", indexSlot{Seq: 1, IsTombstone: false}))
	must(t, x.setLatest("b", indexSlot{Seq: 2, IsTombstone: true}))
	kept, err := x.finalizePerSubject(true) // kv-compact: drop tombstone-latest
	if err != nil {
		t.Fatalf("finalizePerSubject: %v", err)
	}
	if kept != 1 {
		t.Fatalf("kept = %d want 1", kept)
	}
	if ok, _ := x.isKept(1); !ok {
		t.Fatalf("seq 1 (non-tombstone) should be kept")
	}
	if ok, _ := x.isKept(2); ok {
		t.Fatalf("seq 2 (tombstone latest) should be dropped")
	}
}

func TestIndex_Finalize_RingKept(t *testing.T) {
	x, _ := newTestIndex(t, false)
	defer x.Close()
	for _, s := range []uint64{1, 2, 3} {
		must(t, x.pushRing("a", 2, indexSlot{Seq: s}))
	}
	kept, err := x.finalizePerSubject(false)
	if err != nil {
		t.Fatalf("finalizePerSubject: %v", err)
	}
	if kept != 2 {
		t.Fatalf("kept = %d want 2 (ringCap 2 evicts oldest)", kept)
	}
	if ok, _ := x.isKept(1); ok {
		t.Fatalf("seq 1 should have been evicted")
	}
	for _, s := range []uint64{2, 3} {
		if ok, _ := x.isKept(s); !ok {
			t.Fatalf("seq %d should be kept", s)
		}
	}
}

func TestIndex_IsKept_Concurrent(t *testing.T) {
	x, _ := newTestIndex(t, false)
	defer x.Close()
	var seqs []uint64
	for s := uint64(0); s < 200; s += 2 {
		seqs = append(seqs, s)
	}
	seedKeep(t, x, seqs...)
	var wg sync.WaitGroup
	var miss atomic.Uint64
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for _, s := range seqs {
				ok, err := x.isKept(s)
				if err != nil || !ok {
					miss.Add(1)
				}
			}
		}()
	}
	wg.Wait()
	if miss.Load() != 0 {
		t.Fatalf("%d unexpected misses across goroutines", miss.Load())
	}
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}

func TestIndex_SetLatest_Upsert(t *testing.T) {
	x, _ := newTestIndex(t, false)
	defer x.Close()
	must(t, x.setLatest("foo", indexSlot{Seq: 5}))
	must(t, x.setLatest("foo", indexSlot{Seq: 9, IsTombstone: true}))

	var got []indexSlot
	if err := x.forEachSubject(func(subj string, slots []indexSlot) {
		if subj == "foo" {
			got = append(got, slots...)
		}
	}); err != nil {
		t.Fatalf("forEachSubject: %v", err)
	}
	if len(got) != 1 || got[0].Seq != 9 || !got[0].IsTombstone {
		t.Fatalf("expected single slot with seq=9 tombstone=true, got %+v", got)
	}
}

func TestIndex_PushRing_EvictsOldest(t *testing.T) {
	x, _ := newTestIndex(t, false)
	defer x.Close()
	for s := uint64(1); s <= 5; s++ {
		must(t, x.pushRing("foo", 3, indexSlot{Seq: s}))
	}
	var seqs []uint64
	if err := x.forEachSubject(func(subj string, slots []indexSlot) {
		for _, sl := range slots {
			seqs = append(seqs, sl.Seq)
		}
	}); err != nil {
		t.Fatalf("forEachSubject: %v", err)
	}
	if want := []uint64{3, 4, 5}; !equalSeq(seqs, want) {
		t.Fatalf("ring seqs after eviction: %v want %v", seqs, want)
	}
}

func TestIndex_PushRing_MultipleSubjects(t *testing.T) {
	x, _ := newTestIndex(t, false)
	defer x.Close()
	pushes := []struct {
		subj string
		seq  uint64
	}{
		{"a", 1}, {"b", 2}, {"a", 3}, {"b", 4}, {"a", 5}, {"b", 6},
	}
	for _, p := range pushes {
		must(t, x.pushRing(p.subj, 2, indexSlot{Seq: p.seq}))
	}
	got := map[string][]uint64{}
	if err := x.forEachSubject(func(subj string, slots []indexSlot) {
		for _, sl := range slots {
			got[subj] = append(got[subj], sl.Seq)
		}
	}); err != nil {
		t.Fatalf("forEachSubject: %v", err)
	}
	if !equalSeq(got["a"], []uint64{3, 5}) {
		t.Fatalf("subject a: %v want [3 5]", got["a"])
	}
	if !equalSeq(got["b"], []uint64{4, 6}) {
		t.Fatalf("subject b: %v want [4 6]", got["b"])
	}
}

func TestIndex_PushRing_SameSubjectAcrossBatch(t *testing.T) {
	x, _ := newTestIndex(t, false)
	defer x.Close()
	x.batchSize = 2 // force multiple flushes within one ring
	for s := uint64(1); s <= 4; s++ {
		must(t, x.pushRing("foo", 2, indexSlot{Seq: s}))
	}
	if err := x.flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	var seqs []uint64
	if err := x.forEachSubject(func(subj string, slots []indexSlot) {
		for _, sl := range slots {
			seqs = append(seqs, sl.Seq)
		}
	}); err != nil {
		t.Fatalf("forEachSubject: %v", err)
	}
	if !equalSeq(seqs, []uint64{3, 4}) {
		t.Fatalf("seqs across batch: %v want [3 4]", seqs)
	}
}

func TestIndex_SlotEncoding_RoundTrip(t *testing.T) {
	for _, s := range []indexSlot{
		{Seq: 0, IsTombstone: false},
		{Seq: 1, IsTombstone: true},
		{Seq: 1 << 40, IsTombstone: false},
	} {
		b := encodeLatestSlot(s)
		if len(b) != 9 {
			t.Fatalf("latest encoded length: %d want 9", len(b))
		}
		got, err := decodeLatestSlot(b)
		if err != nil || got != s {
			t.Fatalf("latest round-trip: got %+v err %v want %+v", got, err, s)
		}
	}
	if _, err := decodeLatestSlot([]byte{1, 2, 3}); err == nil {
		t.Fatalf("expected error decoding wrong-length slot")
	}
}

func TestIndex_BatchFlush_AcrossBoundary(t *testing.T) {
	x, _ := newTestIndex(t, false)
	defer x.Close()
	x.batchSize = 100
	const n = 251
	for i := 0; i < n; i++ {
		must(t, x.setLatest("s"+itoa(i), indexSlot{Seq: uint64(i + 1)}))
	}
	// Without explicit flushes the wrapper flushed at 100 and 200; the rest
	// flush on read.
	count := 0
	if err := x.forEachSubject(func(string, []indexSlot) { count++ }); err != nil {
		t.Fatalf("forEachSubject: %v", err)
	}
	if count != n {
		t.Fatalf("forEachSubject saw %d subjects want %d", count, n)
	}
	if _, err := x.finalizePerSubject(false); err != nil {
		t.Fatalf("finalizePerSubject: %v", err)
	}
	for i := 0; i < n; i++ {
		if ok, err := x.isKept(uint64(i + 1)); err != nil || !ok {
			t.Fatalf("isKept(%d): %v %v", i+1, ok, err)
		}
	}
}

func TestIndex_ForEachSubject_LatestPath(t *testing.T) {
	x, _ := newTestIndex(t, false)
	defer x.Close()
	must(t, x.setLatest("a", indexSlot{Seq: 1, IsTombstone: false}))
	must(t, x.setLatest("b", indexSlot{Seq: 2, IsTombstone: true}))

	got := map[string]indexSlot{}
	if err := x.forEachSubject(func(subj string, slots []indexSlot) {
		if len(slots) != 1 {
			t.Errorf("subject %q expected 1 slot got %d", subj, len(slots))
			return
		}
		got[subj] = slots[0]
	}); err != nil {
		t.Fatalf("forEachSubject: %v", err)
	}
	if got["a"].Seq != 1 || got["a"].IsTombstone {
		t.Fatalf("a slot wrong: %+v", got["a"])
	}
	if got["b"].Seq != 2 || !got["b"].IsTombstone {
		t.Fatalf("b slot wrong: %+v", got["b"])
	}
}

func TestIndex_ForEachSubject_RingPath(t *testing.T) {
	x, _ := newTestIndex(t, false)
	defer x.Close()
	for _, s := range []uint64{1, 2, 3, 4} {
		must(t, x.pushRing("a", 4, indexSlot{Seq: s}))
	}
	var seqs []uint64
	if err := x.forEachSubject(func(subj string, slots []indexSlot) {
		for _, sl := range slots {
			seqs = append(seqs, sl.Seq)
		}
	}); err != nil {
		t.Fatalf("forEachSubject: %v", err)
	}
	if !equalSeq(seqs, []uint64{1, 2, 3, 4}) {
		t.Fatalf("ring slots: %v want [1 2 3 4]", seqs)
	}
}

func TestIndex_RingKey_HasPrefixTermination(t *testing.T) {
	// Confirm the prefix scan terminates on a prefix boundary when one
	// subject's lexical successor begins with the same byte stream.
	x, _ := newTestIndex(t, false)
	defer x.Close()
	must(t, x.pushRing("a", 2, indexSlot{Seq: 1}))
	must(t, x.pushRing("a.b", 2, indexSlot{Seq: 2}))
	got := map[string][]uint64{}
	if err := x.forEachSubject(func(subj string, slots []indexSlot) {
		for _, sl := range slots {
			got[subj] = append(got[subj], sl.Seq)
		}
	}); err != nil {
		t.Fatalf("forEachSubject: %v", err)
	}
	if !equalSeq(got["a"], []uint64{1}) {
		t.Fatalf("subject \"a\" slots: %v want [1]", got["a"])
	}
	if !equalSeq(got["a.b"], []uint64{2}) {
		t.Fatalf("subject \"a.b\" slots: %v want [2]", got["a.b"])
	}
}

func TestIndex_LogsPhaseLines(t *testing.T) {
	captured := &captureHandler{}
	lg := slog.New(captured)
	x, err := openIndex(indexOptions{Dir: t.TempDir(), Keep: true, Logger: lg})
	if err != nil {
		t.Fatalf("openIndex: %v", err)
	}
	must(t, x.MarkComplete())
	if err := x.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	os.Remove(x.Path())

	for _, w := range []string{"index opened", "index marked complete", "index closed"} {
		if !captured.has(w) {
			t.Fatalf("expected log line %q in:\n%s", w, captured.dump())
		}
	}
}

type captureHandler struct {
	mu    sync.Mutex
	lines []string
}

func (h *captureHandler) Enabled(_ context.Context, _ slog.Level) bool { return true }
func (h *captureHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	var attrs []string
	r.Attrs(func(a slog.Attr) bool {
		attrs = append(attrs, a.Key+"="+a.Value.String())
		return true
	})
	h.lines = append(h.lines, r.Message+" "+strings.Join(attrs, " "))
	return nil
}
func (h *captureHandler) WithAttrs(attrs []slog.Attr) slog.Handler { return h }
func (h *captureHandler) WithGroup(name string) slog.Handler       { return h }
func (h *captureHandler) has(needle string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, l := range h.lines {
		if strings.Contains(l, needle) {
			return true
		}
	}
	return false
}
func (h *captureHandler) dump() string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return strings.Join(h.lines, "\n")
}

// Helpers ----

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func equalSeq(a, b []uint64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
