package backupedit

import (
	"archive/tar"
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/jsm.go/api"
)

// srcMsg is a message to seed into a test source backup.
type srcMsg struct {
	seq     uint64
	subject string
	payload string
	ts      time.Time
	kvOp    string // when set, message carries a "KV-Operation: <kvOp>" header (e.g. DEL/PURGE)
}

// writeTestBackup writes a minimal v2 backup (backup.json + stream.tar.s2) into
// dir using the same on-disk shape the server produces: state.json first, then
// msgs/<seq> entries whose body is "<hlen> <slen> <subject>\r\n<payload>".
func writeTestBackup(t *testing.T, dir string, msgs []srcMsg) {
	t.Helper()

	st := api.StreamState{Msgs: uint64(len(msgs)), Consumers: 0}
	if len(msgs) > 0 {
		st.FirstSeq = msgs[0].seq
		st.LastSeq = msgs[len(msgs)-1].seq
	}

	tw, err := createTar(filepath.Join(dir, "stream.tar.s2"))
	if err != nil {
		t.Fatalf("createTar: %v", err)
	}
	if err := writeStateJSON(tw, st); err != nil {
		t.Fatalf("writeStateJSON: %v", err)
	}
	for _, m := range msgs {
		hblock := ""
		if m.kvOp != "" {
			hblock = "NATS/1.0\r\nKV-Operation: " + m.kvOp + "\r\n\r\n"
		}
		body := fmt.Sprintf("%d %d %s\r\n%s%s", len(hblock), len(m.subject), m.subject, hblock, m.payload)
		hdr := &tar.Header{
			Name:    "msgs/" + strconv.FormatUint(m.seq, 10),
			Mode:    0o600,
			Size:    int64(len(body)),
			ModTime: m.ts,
			Format:  tar.FormatPAX,
		}
		if err := tw.WriteHeader(hdr); err != nil {
			t.Fatalf("write msg header: %v", err)
		}
		if _, err := tw.Write([]byte(body)); err != nil {
			t.Fatalf("write msg body: %v", err)
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatalf("close source tar: %v", err)
	}

	if err := writeBackupJSON(filepath.Join(dir, "backup.json"), &backupMeta{State: st}); err != nil {
		t.Fatalf("write backup.json: %v", err)
	}
}

// readOutputMsgs decodes a backup's stream.tar.s2 and returns the parsed
// state.json plus the surviving (renumbered) message subjects keyed by new seq.
func readOutputMsgs(t *testing.T, dir string) (api.StreamState, map[uint64]string) {
	t.Helper()
	tr, err := openTar(filepath.Join(dir, "stream.tar.s2"))
	if err != nil {
		t.Fatalf("openTar output: %v", err)
	}
	defer tr.Close()

	var st api.StreamState
	msgs := map[uint64]string{}
	first := true
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("read output tar: %v", err) // a CRC failure surfaces here
		}
		buf, err := io.ReadAll(tr)
		if err != nil {
			t.Fatalf("read entry %s: %v", hdr.Name, err)
		}
		if first {
			if hdr.Name != "state.json" {
				t.Fatalf("first entry = %q, want state.json", hdr.Name)
			}
			if err := decodeState(buf, &st); err != nil {
				t.Fatalf("decode state.json: %v\nraw: %q", err, buf)
			}
			first = false
			continue
		}
		if !strings.HasPrefix(hdr.Name, "msgs/") {
			continue
		}
		seq, err := strconv.ParseUint(strings.TrimPrefix(hdr.Name, "msgs/"), 10, 64)
		if err != nil {
			t.Fatalf("bad output msg name %q", hdr.Name)
		}
		subj, _, err := parsePreamble(bufio.NewReader(bytes.NewReader(buf)))
		if err != nil {
			t.Fatalf("parse output preamble %s: %v", hdr.Name, err)
		}
		msgs[seq] = subj
	}
	return st, msgs
}

func decodeState(b []byte, st *api.StreamState) error {
	// state.json must remain valid JSON despite the space padding after the
	// messages value (insignificant whitespace before the next token).
	return json.Unmarshal(b, st)
}

func TestStreamStateless_SinglePass_SubjectFilter(t *testing.T) {
	ctx := context.Background()
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	src := t.TempDir()
	dst := filepath.Join(t.TempDir(), "out")

	writeTestBackup(t, src, []srcMsg{
		{seq: 1, subject: "keep.a", payload: "p1", ts: base},
		{seq: 2, subject: "drop.b", payload: "p2", ts: base.Add(time.Second)},
		{seq: 3, subject: "keep.c", payload: "p3", ts: base.Add(2 * time.Second)},
		{seq: 4, subject: "drop.d", payload: "p4", ts: base.Add(3 * time.Second)},
		{seq: 5, subject: "keep.e", payload: "p5", ts: base.Add(4 * time.Second)},
	})

	rep, err := Filter(ctx, src, dst, WithSubject("keep.>"))
	if err != nil {
		t.Fatalf("Filter: %v", err)
	}
	if rep.Kept != 3 || rep.Dropped != 2 {
		t.Fatalf("kept=%d dropped=%d, want 3/2", rep.Kept, rep.Dropped)
	}

	st, msgs := readOutputMsgs(t, dst)
	if st.Msgs != 3 {
		t.Fatalf("state.json messages=%d, want 3 (patch failed?)", st.Msgs)
	}
	if len(msgs) != 3 {
		t.Fatalf("output has %d msgs, want 3", len(msgs))
	}
	// Renumbered from 1, contiguous, in source order.
	want := map[uint64]string{1: "keep.a", 2: "keep.c", 3: "keep.e"}
	for seq, subj := range want {
		if msgs[seq] != subj {
			t.Fatalf("msgs[%d]=%q, want %q (full=%v)", seq, msgs[seq], subj, msgs)
		}
	}
}

func TestStreamStateless_SinglePass_Identity(t *testing.T) {
	ctx := context.Background()
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	src := t.TempDir()
	dst := filepath.Join(t.TempDir(), "out")

	var in []srcMsg
	for i := uint64(1); i <= 50; i++ {
		in = append(in, srcMsg{seq: i, subject: fmt.Sprintf("s.%d", i), payload: fmt.Sprintf("payload-%d", i), ts: base.Add(time.Duration(i) * time.Second)})
	}
	writeTestBackup(t, src, in)

	rep, err := Filter(ctx, src, dst)
	if err != nil {
		t.Fatalf("Filter: %v", err)
	}
	if rep.Kept != 50 {
		t.Fatalf("kept=%d, want 50", rep.Kept)
	}

	st, msgs := readOutputMsgs(t, dst)
	if st.Msgs != 50 {
		t.Fatalf("state.json messages=%d, want 50", st.Msgs)
	}
	if len(msgs) != 50 {
		t.Fatalf("output msgs=%d, want 50", len(msgs))
	}
	for i := uint64(1); i <= 50; i++ {
		if msgs[i] != fmt.Sprintf("s.%d", i) {
			t.Fatalf("msgs[%d]=%q, want s.%d", i, msgs[i], i)
		}
	}
}

func TestStreamStateless_SinglePass_DropAll(t *testing.T) {
	ctx := context.Background()
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	src := t.TempDir()
	dst := filepath.Join(t.TempDir(), "out")

	writeTestBackup(t, src, []srcMsg{
		{seq: 1, subject: "a.1", payload: "p", ts: base},
		{seq: 2, subject: "a.2", payload: "p", ts: base.Add(time.Second)},
	})

	rep, err := Filter(ctx, src, dst, WithSubject("nomatch.>"))
	if err != nil {
		t.Fatalf("Filter: %v", err)
	}
	if rep.Kept != 0 || rep.Dropped != 2 {
		t.Fatalf("kept=%d dropped=%d, want 0/2", rep.Kept, rep.Dropped)
	}
	st, msgs := readOutputMsgs(t, dst)
	if st.Msgs != 0 {
		t.Fatalf("state.json messages=%d, want 0", st.Msgs)
	}
	if len(msgs) != 0 {
		t.Fatalf("output msgs=%d, want 0", len(msgs))
	}
}

func TestStreamStateful_KVCompact(t *testing.T) {
	ctx := context.Background()
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	src := t.TempDir()
	dst := filepath.Join(t.TempDir(), "out")

	// KV.a: 3 revisions -> keep latest (seq 6).
	// KV.b: 2 revisions then a DEL tombstone -> drop the whole subject.
	// KV.c: 1 revision -> keep (seq 4).
	writeTestBackup(t, src, []srcMsg{
		{seq: 1, subject: "KV.a", payload: "a1", ts: base.Add(1 * time.Second)},
		{seq: 2, subject: "KV.b", payload: "b1", ts: base.Add(2 * time.Second)},
		{seq: 3, subject: "KV.a", payload: "a2", ts: base.Add(3 * time.Second)},
		{seq: 4, subject: "KV.c", payload: "c1", ts: base.Add(4 * time.Second)},
		{seq: 5, subject: "KV.b", payload: "b2", ts: base.Add(5 * time.Second)},
		{seq: 6, subject: "KV.a", payload: "a3", ts: base.Add(6 * time.Second)},
		{seq: 7, subject: "KV.b", payload: "", ts: base.Add(7 * time.Second), kvOp: "DEL"},
	})

	rep, err := Filter(ctx, src, dst, WithKVCompact())
	if err != nil {
		t.Fatalf("Filter: %v", err)
	}
	if rep.HeaderParseFailures != 0 {
		t.Fatalf("header parse failures = %d, want 0 (tombstone header should decode)", rep.HeaderParseFailures)
	}
	if rep.Kept != 2 {
		t.Fatalf("kept=%d, want 2 (latest of KV.a and KV.c; KV.b dropped on tombstone)", rep.Kept)
	}

	st, msgs := readOutputMsgs(t, dst)
	if st.Msgs != 2 {
		t.Fatalf("state.json messages=%d, want 2", st.Msgs)
	}
	// Renumbered in ascending source-seq order: KV.c (seq4) -> 1, KV.a (seq6) -> 2.
	want := map[uint64]string{1: "KV.c", 2: "KV.a"}
	for seq, subj := range want {
		if msgs[seq] != subj {
			t.Fatalf("msgs[%d]=%q, want %q (full=%v)", seq, msgs[seq], subj, msgs)
		}
	}
}

func TestStreamStateful_LastN(t *testing.T) {
	ctx := context.Background()
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	src := t.TempDir()
	dst := filepath.Join(t.TempDir(), "out")

	// last-2 per subject:
	//   KV.a: seq 1,3,6  -> keep 3,6
	//   KV.b: seq 2,5    -> keep both
	//   KV.c: seq 4      -> keep
	writeTestBackup(t, src, []srcMsg{
		{seq: 1, subject: "KV.a", payload: "a1", ts: base.Add(1 * time.Second)},
		{seq: 2, subject: "KV.b", payload: "b1", ts: base.Add(2 * time.Second)},
		{seq: 3, subject: "KV.a", payload: "a2", ts: base.Add(3 * time.Second)},
		{seq: 4, subject: "KV.c", payload: "c1", ts: base.Add(4 * time.Second)},
		{seq: 5, subject: "KV.b", payload: "b2", ts: base.Add(5 * time.Second)},
		{seq: 6, subject: "KV.a", payload: "a3", ts: base.Add(6 * time.Second)},
	})

	rep, err := Filter(ctx, src, dst, WithLastNPerSubject(2))
	if err != nil {
		t.Fatalf("Filter: %v", err)
	}
	if rep.Kept != 5 || rep.Dropped != 1 {
		t.Fatalf("kept=%d dropped=%d, want 5/1 (KV.a seq1 evicted)", rep.Kept, rep.Dropped)
	}

	st, msgs := readOutputMsgs(t, dst)
	if st.Msgs != 5 {
		t.Fatalf("state.json messages=%d, want 5", st.Msgs)
	}
	// Ascending source-seq order: seq2(b),3(a),4(c),5(b),6(a) -> new 1..5.
	want := map[uint64]string{1: "KV.b", 2: "KV.a", 3: "KV.c", 4: "KV.b", 5: "KV.a"}
	for seq, subj := range want {
		if msgs[seq] != subj {
			t.Fatalf("msgs[%d]=%q, want %q (full=%v)", seq, msgs[seq], subj, msgs)
		}
	}
}

func TestStreamStateful_KVCompact_DryRunWritesNothing(t *testing.T) {
	ctx := context.Background()
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	src := t.TempDir()
	dst := filepath.Join(t.TempDir(), "out")

	writeTestBackup(t, src, []srcMsg{
		{seq: 1, subject: "KV.a", payload: "a1", ts: base.Add(1 * time.Second)},
		{seq: 2, subject: "KV.a", payload: "a2", ts: base.Add(2 * time.Second)},
		{seq: 3, subject: "KV.b", payload: "", ts: base.Add(3 * time.Second), kvOp: "PURGE"},
	})

	rep, err := Filter(ctx, src, dst, WithKVCompact(), WithDryRun())
	if err != nil {
		t.Fatalf("Filter: %v", err)
	}
	if rep.Kept != 1 {
		t.Fatalf("kept=%d, want 1 (latest KV.a; KV.b purged)", rep.Kept)
	}
	if _, err := os.Stat(dst); !os.IsNotExist(err) {
		t.Fatalf("dry-run wrote destination %s (err=%v)", dst, err)
	}
}

// TestStateJSON_SeqFields guards the restore regression: the output state.json
// must carry first_seq/last_seq matching the renumbered survivors. A stale
// first_seq=0 makes the server's restore cursor (first_seq-1, uint64) underflow,
// rejecting every message as "out of order".
func TestStateJSON_SeqFields(t *testing.T) {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	src := t.TempDir()
	dst := filepath.Join(t.TempDir(), "out")
	writeTestBackup(t, src, []srcMsg{
		{seq: 1, subject: "a.1", payload: "p", ts: base},
		{seq: 2, subject: "a.2", payload: "p", ts: base.Add(time.Second)},
		{seq: 3, subject: "a.3", payload: "p", ts: base.Add(2 * time.Second)},
	})

	// kept > 0: first_seq=1, last_seq=messages
	if _, err := Filter(context.Background(), src, dst); err != nil {
		t.Fatalf("Filter: %v", err)
	}
	st, _ := readOutputMsgs(t, dst)
	if st.Msgs != 3 || st.FirstSeq != 1 || st.LastSeq != 3 {
		t.Fatalf("kept>0: messages=%d first_seq=%d last_seq=%d, want 3/1/3", st.Msgs, st.FirstSeq, st.LastSeq)
	}

	// kept == 0 (drop all): first_seq=0, last_seq=0
	dst2 := filepath.Join(t.TempDir(), "empty")
	if _, err := Filter(context.Background(), src, dst2, WithSubject("nomatch.>")); err != nil {
		t.Fatalf("Filter drop-all: %v", err)
	}
	st2, _ := readOutputMsgs(t, dst2)
	if st2.Msgs != 0 || st2.FirstSeq != 0 || st2.LastSeq != 0 {
		t.Fatalf("kept=0: messages=%d first_seq=%d last_seq=%d, want 0/0/0", st2.Msgs, st2.FirstSeq, st2.LastSeq)
	}
}
