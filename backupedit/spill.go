package backupedit

import (
	"bufio"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"time"
)

// spill is an append-only scratch file holding every candidate message body for
// the per-subject single pass. Walk 1 appends [seq][ts][len][body] in ascending
// seq order; emit scans it sequentially and copies the survivors (those still in
// the keep set) to the output. Sequential write + sequential scan is exactly the
// access pattern the benchmark showed beats a second source decompression by
// ~10x and a bbolt body-store by a wide margin — so bodies live here, while the
// bbolt index holds only the cheap seqs.
//
// Record layout (big-endian): seq(8) | ts UnixNano(8) | len(4) | body(len).
type spill struct {
	f    *os.File
	path string
	log  *slog.Logger
	bw   *bufio.Writer
	hdr  [20]byte // reusable write header
}

const spillBufSize = 1 << 20

func openSpill(dir string, log *slog.Logger) (*spill, error) {
	if log == nil {
		log = slog.Default()
	}
	if dir == "" {
		dir = os.TempDir()
	}
	var rnd [8]byte
	if _, err := rand.Read(rnd[:]); err != nil {
		return nil, fmt.Errorf("backupedit: entropy unavailable: %w", err)
	}
	path := filepath.Join(dir, fmt.Sprintf("backupedit-spill-%s-%d.dat", hex.EncodeToString(rnd[:]), os.Getpid()))
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("backupedit: create spill: %w", err)
	}
	s := &spill{
		f:    f,
		path: path,
		log:  log.With("component", "backupedit.spill", "path", path),
		bw:   bufio.NewWriterSize(f, spillBufSize),
	}
	s.log.Info("spill opened", "pid", os.Getpid())
	return s, nil
}

// append writes one candidate record. body is written verbatim and copied to the
// output untouched on emit.
func (s *spill) append(seq uint64, ts time.Time, body []byte) error {
	binary.BigEndian.PutUint64(s.hdr[0:8], seq)
	binary.BigEndian.PutUint64(s.hdr[8:16], uint64(ts.UnixNano()))
	binary.BigEndian.PutUint32(s.hdr[16:20], uint32(len(body)))
	if _, err := s.bw.Write(s.hdr[:]); err != nil {
		return fmt.Errorf("backupedit: spill write header: %w", err)
	}
	if _, err := s.bw.Write(body); err != nil {
		return fmt.Errorf("backupedit: spill write body: %w", err)
	}
	return nil
}

// rewind flushes buffered writes and seeks to the start so forEach can scan.
func (s *spill) rewind() error {
	if err := s.bw.Flush(); err != nil {
		return fmt.Errorf("backupedit: spill flush: %w", err)
	}
	if _, err := s.f.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("backupedit: spill seek: %w", err)
	}
	return nil
}

// forEach scans every record in append (ascending seq) order. The body slice is
// reused between calls — fn must copy it if it retains the bytes past the call.
func (s *spill) forEach(fn func(seq uint64, ts time.Time, body []byte) error) error {
	br := bufio.NewReaderSize(s.f, spillBufSize)
	var hdr [20]byte
	buf := make([]byte, 0, 64<<10)
	for {
		_, err := io.ReadFull(br, hdr[:])
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return fmt.Errorf("backupedit: spill read header: %w", err)
		}
		seq := binary.BigEndian.Uint64(hdr[0:8])
		ts := time.Unix(0, int64(binary.BigEndian.Uint64(hdr[8:16]))).UTC()
		n := int(binary.BigEndian.Uint32(hdr[16:20]))
		if cap(buf) < n {
			buf = make([]byte, n)
		}
		buf = buf[:n]
		if _, err := io.ReadFull(br, buf); err != nil {
			return fmt.Errorf("backupedit: spill read body (seq %d, %d bytes): %w", seq, n, err)
		}
		if err := fn(seq, ts, buf); err != nil {
			return err
		}
	}
}

// Close closes and always removes the spill file. It is pure scratch — never
// kept, even when the index is (WithKeepIndex preserves the bbolt index only).
func (s *spill) Close() error {
	if s == nil || s.f == nil {
		return nil
	}
	_ = s.bw.Flush()
	closeErr := s.f.Close()
	s.f = nil
	removeErr := os.Remove(s.path)
	if removeErr != nil && errors.Is(removeErr, fs.ErrNotExist) {
		removeErr = nil
	}
	s.log.Info("spill closed", "close_err", errOrNil(closeErr), "remove_err", errOrNil(removeErr))
	return errors.Join(closeErr, removeErr)
}
