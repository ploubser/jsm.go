package backupedit

import (
	"context"
	"io"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"
)

// outputMtimes returns the msgs/* entry ModTimes (UnixNano) from a backup.
func outputMtimes(t *testing.T, dir string) []int64 {
	t.Helper()
	tr, err := openTar(filepath.Join(dir, "stream.tar.s2"))
	if err != nil {
		t.Fatalf("openTar: %v", err)
	}
	defer tr.Close()
	var ns []int64
	for {
		h, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("next: %v", err)
		}
		io.Copy(io.Discard, tr)
		if strings.HasPrefix(h.Name, "msgs/") {
			ns = append(ns, h.ModTime.UTC().UnixNano())
		}
	}
	sort.Slice(ns, func(i, j int) bool { return ns[i] < ns[j] })
	return ns
}

// distinct sub-second (nanosecond) timestamps to prove PAX sub-second survives
// the spill UnixNano round-trip and the FormatPAX output writer.
func TestMsgTime_PreservedNanosecond(t *testing.T) {
	base := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	// each ts carries a distinct nanosecond component
	mk := func(seq uint64, subj string, nanos int) srcMsg {
		return srcMsg{seq: seq, subject: subj, payload: "p", ts: base.Add(time.Duration(seq) * time.Second).Add(time.Duration(nanos) * time.Nanosecond)}
	}

	cases := []struct {
		name string
		opts []Option
	}{
		{"identity_stateless", nil},
		{"subject_stateless", []Option{WithSubject("KV.>")}},
		{"lastn_spill", []Option{WithLastNPerSubject(2)}},
		{"kvcompact_spill", []Option{WithKVCompact()}},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			src := t.TempDir()
			dst := filepath.Join(t.TempDir(), "out")
			msgs := []srcMsg{
				mk(1, "KV.a", 1),
				mk(2, "KV.b", 123456789),
				mk(3, "KV.a", 999999999),
				mk(4, "KV.c", 42),
				mk(5, "KV.b", 500000001),
				mk(6, "KV.a", 7),
			}
			writeTestBackup(t, src, msgs)

			rep, err := Filter(context.Background(), src, dst, c.opts...)
			if err != nil {
				t.Fatalf("Filter: %v", err)
			}

			// Expected survivor timestamps depend on the filter.
			var want []int64
			switch c.name {
			case "identity_stateless", "subject_stateless":
				for _, m := range msgs {
					want = append(want, m.ts.UnixNano())
				}
			case "lastn_spill": // last-2 per subject
				// KV.a: seq 3,6 ; KV.b: 2,5 ; KV.c: 4
				for _, m := range []srcMsg{msgs[2], msgs[5], msgs[1], msgs[4], msgs[3]} {
					want = append(want, m.ts.UnixNano())
				}
			case "kvcompact_spill": // latest per subject: KV.a seq6, KV.b seq5, KV.c seq4
				for _, m := range []srcMsg{msgs[5], msgs[4], msgs[3]} {
					want = append(want, m.ts.UnixNano())
				}
			}
			sort.Slice(want, func(i, j int) bool { return want[i] < want[j] })

			got := outputMtimes(t, dst)
			if len(got) != len(want) {
				t.Fatalf("kept=%d, got %d mtimes want %d", rep.Kept, len(got), len(want))
			}
			for i := range want {
				if got[i] != want[i] {
					t.Fatalf("mtime[%d]=%d want %d (Δ=%dns) — sub-second NOT preserved", i, got[i], want[i], got[i]-want[i])
				}
			}
			t.Logf("%s: %d timestamps preserved nanosecond-exact", c.name, len(got))
		})
	}
}
