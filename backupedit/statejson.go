package backupedit

import (
	"archive/tar"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"os"
	"strconv"
	"time"

	"github.com/nats-io/jsm.go/api"
)

// The single-pass stateless path can't know the surviving-message count until
// the whole source has been read, but state.json must be the FIRST tar entry
// and its `messages` count is the one field the server validates on restore.
// Rather than walk the source twice (count, then copy), we write state.json as
// a single *uncompressed* s2 chunk at the head of the output, with the
// messages value reserved at a fixed character width. After the survivors have
// been streamed and counted we seek back and overwrite the digits in place,
// then fix that chunk's CRC. No recompression, no second source read.
//
// s2 framing format (snappy framing §4): the stream is a stream-identifier
// chunk followed by data chunks. An uncompressed chunk is
//
//	[0x01][len uint24 LE][crc uint32 LE][raw bytes]
//
// where len = 4 + len(raw) and crc is the masked CRC-32C of the raw bytes.
// Because the chunk is stored verbatim, patching its bytes only requires
// recomputing the 4-byte CRC — everything else stays byte-stable.

const (
	// s2StreamID is the s2 stream-identifier chunk (magic), 10 bytes. Readers
	// accept it appearing again mid-stream, so the remaining (compressed)
	// chunks may be produced by an ordinary s2.Writer after this prefix.
	s2StreamID = "\xff\x06\x00\x00S2sTwO"

	// s2ChunkUncompressed is the chunk type for verbatim data.
	s2ChunkUncompressed = 0x01

	// s2ChecksumLen is the per-chunk CRC width.
	s2ChecksumLen = 4

	// msgsCountWidth reserves space for state.json's messages value. A uint64
	// is at most 20 decimal digits; the value is written left-aligned and
	// padded with spaces (insignificant JSON whitespace before the next token),
	// so the field width — and therefore every later byte offset — never moves.
	msgsCountWidth = 20
)

var s2CRCTable = crc32.MakeTable(crc32.Castagnoli)

// s2MaskedCRC replicates klauspost/s2's framing checksum (snappy framing §3):
// a masked CRC-32C of the chunk's uncompressed data. Kept byte-for-byte
// identical to s2.crc so the reader accepts our hand-written chunk.
func s2MaskedCRC(b []byte) uint32 {
	c := crc32.Update(0, s2CRCTable, b)
	return c>>15 | c<<17+0xa282ead8
}

// patchableState holds the offsets needed to overwrite the count-bearing
// state.json fields in the leading uncompressed chunk once the survivor count
// is known. messages is what the server validates against the msgs/ entries;
// first_seq is load-bearing too — restore does store.Compact(first_seq) and
// inits its ordering cursor as first_seq-1, so a stale first_seq=0 underflows
// (uint64) and every message reads as "out of order". last_seq is patched for
// consistency (the server recomputes it on restore, but we keep it honest).
type patchableState struct {
	entry       []byte // full state.json tar entry bytes (header+json+padding), kept for CRC recompute
	msgsOff     int    // offset of the messages digit field within entry
	firstSeqOff int    // offset of the first_seq digit field
	lastSeqOff  int    // offset of the last_seq digit field
	fileCRC     int64  // file offset where the chunk CRC begins
	fileData    int64  // file offset where entry begins
}

// writePatchableState writes the s2 stream identifier and the leading
// uncompressed chunk holding state.json (with a fixed-width, space-padded
// messages field) to f, which must be positioned at the start of the file. It
// returns a handle used to patch the count once the survivors are written.
func writePatchableState(f *os.File, st api.StreamState) (*patchableState, error) {
	entry, msgsOff, firstSeqOff, lastSeqOff, err := buildStateEntry(st)
	if err != nil {
		return nil, err
	}

	if _, err := f.Write([]byte(s2StreamID)); err != nil {
		return nil, fmt.Errorf("write s2 stream id: %w", err)
	}

	chunkLen := s2ChecksumLen + len(entry) // fits 3 bytes: state.json is tiny
	var hdr [4]byte
	hdr[0] = s2ChunkUncompressed
	hdr[1] = byte(chunkLen)
	hdr[2] = byte(chunkLen >> 8)
	hdr[3] = byte(chunkLen >> 16)
	if _, err := f.Write(hdr[:]); err != nil {
		return nil, fmt.Errorf("write state chunk header: %w", err)
	}

	ps := &patchableState{entry: entry, msgsOff: msgsOff, firstSeqOff: firstSeqOff, lastSeqOff: lastSeqOff}
	ps.fileCRC = int64(len(s2StreamID) + len(hdr))
	ps.fileData = ps.fileCRC + s2ChecksumLen

	var crcBuf [4]byte
	binary.LittleEndian.PutUint32(crcBuf[:], s2MaskedCRC(entry))
	if _, err := f.Write(crcBuf[:]); err != nil {
		return nil, fmt.Errorf("write state chunk crc: %w", err)
	}
	if _, err := f.Write(entry); err != nil {
		return nil, fmt.Errorf("write state chunk: %w", err)
	}
	return ps, nil
}

// patch overwrites the reserved messages/first_seq/last_seq fields and fixes the
// chunk CRC in place. Call after the rest of the stream has been flushed to f.
// Output seqs are renumbered from 1, so first_seq is 1 (or 0 for an empty
// result) and last_seq equals the message count.
func (ps *patchableState) patch(f *os.File, msgs uint64) error {
	var firstSeq, lastSeq uint64
	if msgs > 0 {
		firstSeq, lastSeq = 1, msgs
	}
	for _, fld := range []struct {
		off int
		val uint64
	}{
		{ps.msgsOff, msgs},
		{ps.firstSeqOff, firstSeq},
		{ps.lastSeqOff, lastSeq},
	} {
		field, err := leftAlignNum(fld.val, msgsCountWidth)
		if err != nil {
			return err
		}
		copy(ps.entry[fld.off:fld.off+msgsCountWidth], field)
	}

	var crcBuf [4]byte
	binary.LittleEndian.PutUint32(crcBuf[:], s2MaskedCRC(ps.entry))
	if _, err := f.WriteAt(crcBuf[:], ps.fileCRC); err != nil {
		return fmt.Errorf("patch state chunk crc: %w", err)
	}
	if _, err := f.WriteAt(ps.entry, ps.fileData); err != nil {
		return fmt.Errorf("patch state.json: %w", err)
	}
	return nil
}

// patchedFields are the numeric state.json fields reserved at a fixed width so
// they can be overwritten in place once the survivor count is known.
var patchedFields = []string{`"messages":`, `"first_seq":`, `"last_seq":`}

// buildStateEntry marshals st (with the patched fields reserved at a fixed
// width) and wraps it in a single tar entry, returning the entry bytes and the
// digit-field offsets for messages/first_seq/last_seq within them. The tar entry
// carries no EOF trailer — the survivor entries and trailer are appended by the
// streaming writer.
func buildStateEntry(st api.StreamState) (entry []byte, msgsOff, firstSeqOff, lastSeqOff int, err error) {
	st.Msgs, st.FirstSeq, st.LastSeq = 0, 0, 0
	raw, err := json.Marshal(st)
	if err != nil {
		return nil, 0, 0, 0, fmt.Errorf("marshal state.json: %w", err)
	}

	// Reserve each field to a fixed width (placeholder 0 + trailing spaces).
	for _, marker := range patchedFields {
		raw, err = padField(raw, marker)
		if err != nil {
			return nil, 0, 0, 0, err
		}
	}

	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	hdr := &tar.Header{
		Name: "state.json",
		Mode: 0o600,
		Size: int64(len(raw)),
		// Truncate to whole seconds so archive/tar doesn't emit a PAX extended
		// header for sub-second mtime; keeps the entry a single USTAR block.
		ModTime: time.Now().UTC().Truncate(time.Second),
		Format:  tar.FormatPAX,
	}
	if err := tw.WriteHeader(hdr); err != nil {
		return nil, 0, 0, 0, fmt.Errorf("write state.json tar header: %w", err)
	}
	if _, err := tw.Write(raw); err != nil {
		return nil, 0, 0, 0, fmt.Errorf("write state.json tar body: %w", err)
	}
	// Flush pads the entry to a 512-byte boundary but, unlike Close, does NOT
	// write the two zero blocks that terminate a tar stream.
	if err := tw.Flush(); err != nil {
		return nil, 0, 0, 0, fmt.Errorf("flush state.json tar: %w", err)
	}
	entry = buf.Bytes()

	off := func(marker string) (int, error) {
		i := bytes.Index(entry, []byte(marker))
		if i < 0 {
			return 0, fmt.Errorf("backupedit: %s not found in tar entry", marker)
		}
		return i + len(marker), nil
	}
	if msgsOff, err = off(`"messages":`); err != nil {
		return nil, 0, 0, 0, err
	}
	if firstSeqOff, err = off(`"first_seq":`); err != nil {
		return nil, 0, 0, 0, err
	}
	if lastSeqOff, err = off(`"last_seq":`); err != nil {
		return nil, 0, 0, 0, err
	}
	return entry, msgsOff, firstSeqOff, lastSeqOff, nil
}

// padField replaces the numeric value following marker with a fixed-width,
// space-padded placeholder ("0" + spaces), so it can be patched in place later
// without shifting any byte offsets. The trailing spaces are insignificant JSON
// whitespace before the next token.
func padField(raw []byte, marker string) ([]byte, error) {
	m := []byte(marker)
	i := bytes.Index(raw, m)
	if i < 0 {
		return nil, fmt.Errorf("backupedit: %s not found in state.json", marker)
	}
	valStart := i + len(m)
	valEnd := valStart
	for valEnd < len(raw) && raw[valEnd] >= '0' && raw[valEnd] <= '9' {
		valEnd++
	}
	field, err := leftAlignNum(0, msgsCountWidth)
	if err != nil {
		return nil, err
	}
	out := make([]byte, 0, len(raw)+msgsCountWidth)
	out = append(out, raw[:valStart]...)
	out = append(out, field...)
	out = append(out, raw[valEnd:]...)
	return out, nil
}

// leftAlignNum renders n as decimal, left-aligned in a space-padded field of
// the given width. Errors if the decimal form is wider than width.
func leftAlignNum(n uint64, width int) ([]byte, error) {
	s := strconv.FormatUint(n, 10)
	if len(s) > width {
		return nil, fmt.Errorf("backupedit: number %d exceeds reserved width %d", n, width)
	}
	out := make([]byte, width)
	copy(out, s)
	for i := len(s); i < width; i++ {
		out[i] = ' '
	}
	return out, nil
}
