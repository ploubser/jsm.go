package backupedit

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strconv"
)

// Per-message tar entry body shape (from nats-server PR #7882
// stream_backup.go writeStoreMsg / parseSnapshotMessagePreamble):
//
//	<hlen> <slen> <subject>\r\n<headers bytes><body bytes>

var (
	errPreambleEOF        = errors.New("backupedit: unexpected EOF in message preamble")
	errPreambleSeparator  = errors.New("backupedit: missing space separator in message preamble")
	errPreambleTerminator = errors.New("backupedit: missing \\r\\n terminator in message preamble")
	errPreambleNegative   = errors.New("backupedit: negative length in message preamble")
)

func parsePreamble(r *bufio.Reader) (subject string, hlen int, err error) {
	var hlenI, slenI int
	if _, err = fmt.Fscanf(r, "%d %d", &hlenI, &slenI); err != nil {
		if err == io.EOF {
			return "", 0, errPreambleEOF
		}
		return "", 0, fmt.Errorf("backupedit: parsing preamble lengths: %w", err)
	}
	if hlenI < 0 || slenI < 0 {
		return "", 0, errPreambleNegative
	}
	sep, err := r.ReadByte()
	if err != nil {
		return "", 0, errPreambleEOF
	}
	if sep != ' ' {
		return "", 0, errPreambleSeparator
	}
	subjBytes := make([]byte, slenI)
	if _, err = io.ReadFull(r, subjBytes); err != nil {
		return "", 0, errPreambleEOF
	}
	var eol [2]byte
	if _, err = io.ReadFull(r, eol[:]); err != nil {
		return "", 0, errPreambleEOF
	}
	if eol[0] != '\r' || eol[1] != '\n' {
		return "", 0, errPreambleTerminator
	}
	return string(subjBytes), hlenI, nil
}

func preambleLen(subj string, hlen int) int {
	return len(strconv.Itoa(hlen)) + 1 + len(strconv.Itoa(len(subj))) + 1 + len(subj) + 2
}

// msgByteCost mirrors nats-server's fileStoreMsgSize for state.Bytes
// consistency post-restore. (msgHdrSize=22, recordHashSize=8.)
func msgByteCost(subjLen, hlen, payloadLen int) uint64 {
	return uint64(22 + subjLen + hlen + payloadLen + 8)
}
