package backupedit

import (
	"archive/tar"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/klauspost/compress/s2"

	"github.com/nats-io/jsm.go/api"
)

// outputWriter assembles the filtered backup and is shared by both single-pass
// paths (stateless, and per-subject emit-from-spill). The output leads with a
// patchable uncompressed state.json chunk; survivor messages are appended
// renumbered from 1; on commit the messages count is overwritten in place, the
// source is checked for concurrent modification, backup.json is written, and the
// staging dir is atomically renamed into place.
type outputWriter struct {
	dst    string
	tmpDst string
	f      *os.File
	s2w    *s2.Writer
	tw     *tar.Writer
	ps     *patchableState

	counter  uint64
	accBytes uint64
	firstTs  time.Time
	lastTs   time.Time
}

func newOutputWriter(dst string) (*outputWriter, error) {
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return nil, fmt.Errorf("mkdir dst parent: %w", err)
	}
	suffix, err := randSuffix()
	if err != nil {
		return nil, fmt.Errorf("random suffix: %w", err)
	}
	tmpDst := dst + ".tmp-" + suffix + "-" + strconv.Itoa(os.Getpid())
	if err := os.MkdirAll(tmpDst, 0o755); err != nil {
		return nil, fmt.Errorf("mkdir tmpDst: %w", err)
	}
	f, err := os.Create(filepath.Join(tmpDst, "stream.tar.s2"))
	if err != nil {
		os.RemoveAll(tmpDst)
		return nil, fmt.Errorf("create output tar: %w", err)
	}
	// state.json must lead the tar; written as a patchable uncompressed chunk
	// with messages reserved at a fixed width (overwritten on commit).
	ps, err := writePatchableState(f, buildNewState(0, 0, time.Time{}, time.Time{}))
	if err != nil {
		f.Close()
		os.RemoveAll(tmpDst)
		return nil, err
	}
	s2w := s2.NewWriter(f)
	return &outputWriter{
		dst:    dst,
		tmpDst: tmpDst,
		f:      f,
		s2w:    s2w,
		tw:     tar.NewWriter(s2w),
		ps:     ps,
	}, nil
}

// writeMsg copies one survivor body to the output, renumbered from 1. The
// subjLen/hlen/payloadLen feed the advisory bytes aggregate; ts becomes the
// entry ModTime (the message timestamp).
func (o *outputWriter) writeMsg(ts time.Time, subjLen, hlen, payloadLen int, body []byte) error {
	o.counter++
	o.accBytes += msgByteCost(subjLen, hlen, payloadLen)
	if o.firstTs.IsZero() || ts.Before(o.firstTs) {
		o.firstTs = ts
	}
	if ts.After(o.lastTs) {
		o.lastTs = ts
	}
	hdr := &tar.Header{
		Name:    "msgs/" + strconv.FormatUint(o.counter, 10),
		Mode:    0o600,
		Size:    int64(len(body)),
		ModTime: ts,
		Format:  tar.FormatPAX,
	}
	if err := o.tw.WriteHeader(hdr); err != nil {
		return fmt.Errorf("write tar header: %w", err)
	}
	if _, err := o.tw.Write(body); err != nil {
		return fmt.Errorf("write tar body: %w", err)
	}
	return nil
}

// abort tears down the half-written staging dir. Safe to call after commit
// (it no-ops once the file is closed and the dir renamed away).
func (o *outputWriter) abort() {
	if o.f != nil {
		o.f.Close()
		o.f = nil
	}
	os.RemoveAll(o.tmpDst)
}

// commit finalises the output: close writers, patch the messages count, verify
// the source did not move, write backup.json, and atomically rename into place.
func (o *outputWriter) commit(src string, pre *preflightInfo, backup *backupMeta) (api.StreamState, error) {
	if err := o.tw.Close(); err != nil {
		return api.StreamState{}, fmt.Errorf("close output tar: %w", err)
	}
	if err := o.s2w.Close(); err != nil {
		return api.StreamState{}, fmt.Errorf("close s2 writer: %w", err)
	}
	if err := o.ps.patch(o.f, o.counter); err != nil {
		return api.StreamState{}, err
	}
	if err := o.f.Sync(); err != nil {
		return api.StreamState{}, fmt.Errorf("sync output tar: %w", err)
	}
	if err := o.f.Close(); err != nil {
		return api.StreamState{}, fmt.Errorf("close output file: %w", err)
	}
	o.f = nil

	if err := verifySourceUnchanged(src, pre); err != nil {
		return api.StreamState{}, err
	}

	finalState := buildNewState(o.counter, o.accBytes, o.firstTs, o.lastTs)
	backup.State = finalState
	backup.MutatedBy = "backupedit at " + time.Now().UTC().Format(time.RFC3339)
	if err := writeBackupJSON(filepath.Join(o.tmpDst, "backup.json"), backup); err != nil {
		return api.StreamState{}, fmt.Errorf("write backup.json: %w", err)
	}
	if err := os.Rename(o.tmpDst, o.dst); err != nil {
		return api.StreamState{}, fmt.Errorf("rename tmpDst -> dst: %w", err)
	}
	return finalState, nil
}

// verifySourceUnchanged refuses the run if the source backup's size or mtime
// moved while we were reading it.
func verifySourceUnchanged(src string, pre *preflightInfo) error {
	nowJSON, err := os.Stat(filepath.Join(src, "backup.json"))
	if err != nil {
		return fmt.Errorf("re-stat backup.json: %w", err)
	}
	nowTar, err := os.Stat(filepath.Join(src, "stream.tar.s2"))
	if err != nil {
		return fmt.Errorf("re-stat stream.tar.s2: %w", err)
	}
	if nowJSON.Size() != pre.srcJSONSize || nowJSON.ModTime().UnixNano() != pre.srcJSONMtime ||
		nowTar.Size() != pre.srcTarSize || nowTar.ModTime().UnixNano() != pre.srcTarMtime {
		return ErrSourceModifiedDuringRead
	}
	return nil
}
