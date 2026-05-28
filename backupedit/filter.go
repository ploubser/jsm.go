package backupedit

import (
	"archive/tar"
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/nats-io/jsm.go/api"
)

// Report summarises a Filter run; a non-nil *Report is returned even on error.
type Report struct {
	Kept                  uint64
	Dropped               uint64
	MatchedByFilter       map[string]uint64
	HeaderParseFailures   uint64
	ConsumersDroppedCount int
	NewState              api.StreamState
	MutatedBy             string

	// IndexPath is set only when WithKeepIndex() is used on a per-subject run.
	IndexPath string
}

var (
	ErrSourceNotFound           = errors.New("backupedit: source backup missing backup.json or stream.tar.s2")
	ErrInvalidBackup            = errors.New("backupedit: source backup is invalid")
	ErrUnsupportedV1Format      = errors.New("backupedit: backup is in v1 format; backupedit targets v2 only")
	ErrDestinationExists        = errors.New("backupedit: destination exists and is not empty, or stale .tmp-* sibling present")
	ErrSourceModifiedDuringRead = errors.New("backupedit: source backup was modified while being read")
	ErrConsumerCountMismatch    = errors.New("backupedit: state.json Consumers count does not match number of consumers/* entries")
	ErrMsgCountMismatch         = errors.New("backupedit: state.json Msgs count does not match number of msgs/* entries")
	ErrConflictingFilters       = errors.New("backupedit: WithKVCompact() and WithLastNPerSubject(n) are mutually exclusive")
)

// Filter reads the v2 backup at src and writes a filtered copy to dst, always
// returning a non-nil *Report. Stateless filters run in a single source pass:
// state.json leads the output as a patchable uncompressed s2 chunk whose
// messages count is overwritten in place once the survivors are counted.
// Per-subject filters (last-N, kv-compact) still walk twice — walk 1 builds the
// on-disk index, walk 2 copies the survivors.
func Filter(ctx context.Context, src, dst string, opts ...Option) (*Report, error) {
	report := &Report{MatchedByFilter: map[string]uint64{}}

	cfg := &config{}
	for _, opt := range opts {
		opt(cfg)
	}
	if err := cfg.validate(); err != nil {
		return report, err
	}
	if cfg.logger == nil {
		cfg.logger = slog.Default()
	}
	log := cfg.logger.With("component", "backupedit")

	pre, backup, err := preflight(src, dst)
	if err != nil {
		return report, err
	}

	log.Info("opening source tar",
		"src", src,
		"messages", backup.State.Msgs,
		"consumers", backup.State.Consumers,
		"first_seq", backup.State.FirstSeq,
		"last_seq", backup.State.LastSeq,
	)
	logActiveFilters(log, cfg)

	perSubjectActive := cfg.lastN > 0 || cfg.kvCompact

	if perSubjectActive {
		return filterPerSubject(ctx, src, dst, pre, backup, cfg, report, log)
	}

	// Stateless: single source pass. Dry-run counts only (no output, no index);
	// the real run streams survivors and patches state.json's count in place.
	if cfg.dryRun {
		kept, err := scanWalk1(ctx, src, cfg, report, backup, nil)
		if err != nil {
			return report, err
		}
		report.Kept = kept
		report.NewState = buildNewState(kept, 0, time.Time{}, time.Time{})
		log.Info("single pass complete", "kept", kept, "dropped", report.Dropped, "dry_run", true)
		log.Info("dry-run complete; no destination written")
		return report, nil
	}

	newState, kept, err := streamStateless(ctx, src, dst, pre, backup, cfg, report, log)
	if err != nil {
		return report, err
	}
	report.Kept = kept
	report.NewState = newState
	report.MutatedBy = backup.MutatedBy
	log.Info("single pass complete", "written", kept, "dropped", report.Dropped, "dst", dst, "first_seq", newState.FirstSeq, "last_seq", newState.LastSeq)
	return report, nil
}

// filterPerSubject runs the deferred-decision path (last-N, kv-compact): walk 1
// records per-subject candidates in the on-disk index, finalisation resolves
// them into the survivor set, walk 2 copies survivors renumbered.
func filterPerSubject(
	ctx context.Context,
	src, dst string,
	pre *preflightInfo,
	backup *backupMeta,
	cfg *config,
	report *Report,
	log *slog.Logger,
) (*Report, error) {
	idx, err := openIndex(indexOptions{Logger: cfg.logger, Dir: cfg.indexDir, Keep: cfg.keepIndex})
	if err != nil {
		return report, err
	}
	defer idx.Close()
	if cfg.keepIndex {
		report.IndexPath = idx.Path()
	}

	ringCap := cfg.lastN
	if cfg.kvCompact {
		ringCap = 1
	}
	filterKey := FilterKindLastN
	if cfg.kvCompact {
		filterKey = FilterKindKVCompact
	}

	// Dry-run: build the index and finalise to learn the kept count, but spill
	// nothing and write no output.
	if cfg.dryRun {
		var cbErr error
		passed, serr := scanWalk1(ctx, src, cfg, report, backup, func(oldSeq uint64, subj string, _ time.Time, hdrs nats.Header) {
			if cbErr == nil {
				cbErr = recordCandidate(idx, cfg, ringCap, oldSeq, subj, hdrs)
			}
		})
		if serr != nil {
			return report, serr
		}
		if cbErr != nil {
			return report, cbErr
		}
		kept, ferr := idx.finalizePerSubject(cfg.kvCompact)
		if ferr != nil {
			return report, ferr
		}
		evicted := passed - kept
		report.MatchedByFilter[filterKey] += evicted
		report.Dropped += evicted
		report.Kept = kept
		report.NewState = buildNewState(kept, 0, time.Time{}, time.Time{})
		log.Info("walk 1 complete", "kept", kept, "dropped", report.Dropped, "per_subject", true)
		log.Info("dry-run complete; no destination written")
		return report, nil
	}

	// Real run: single source pass. Spill every candidate body to flat scratch
	// and record survivor seqs in the index; finalise resolves the keep set;
	// then emit survivors by scanning the spill — no second source decompression.
	// The benchmark showed the second walk (s2 decode + per-message tar framing)
	// dwarfs a sequential spill write + scan, and that a bbolt body-store would
	// hand the win back, so bodies go to a flat file and the index holds seqs.
	sp, err := openSpill(cfg.indexDir, cfg.logger)
	if err != nil {
		return report, err
	}
	defer sp.Close()

	passed, err := walkSpillIndex(ctx, src, cfg, report, backup, idx, sp, ringCap)
	if err != nil {
		return report, err
	}

	kept, err := idx.finalizePerSubject(cfg.kvCompact)
	if err != nil {
		return report, err
	}
	evicted := passed - kept
	report.MatchedByFilter[filterKey] += evicted
	report.Dropped += evicted
	if err = idx.MarkComplete(); err != nil {
		return report, err
	}
	report.Kept = kept
	log.Info("walk 1 complete", "kept", kept, "dropped", report.Dropped, "per_subject", true)

	ow, err := newOutputWriter(dst)
	if err != nil {
		return report, err
	}
	committed := false
	defer func() {
		if !committed {
			ow.abort()
		}
	}()

	if err := sp.rewind(); err != nil {
		return report, err
	}
	emitErr := sp.forEach(func(seq uint64, ts time.Time, body []byte) error {
		if cerr := ctx.Err(); cerr != nil {
			return cerr
		}
		ok, kerr := idx.isKept(seq)
		if kerr != nil {
			return kerr
		}
		if !ok {
			return nil
		}
		subj, hlen, payloadLen, _, perr := parseMsg(body, seq, false, report)
		if perr != nil {
			return perr
		}
		return ow.writeMsg(ts, len(subj), hlen, payloadLen, body)
	})
	if emitErr != nil {
		return report, emitErr
	}
	if ow.counter != kept {
		return report, fmt.Errorf("backupedit: internal: emitted %d messages but finalised %d kept", ow.counter, kept)
	}

	newState, err := ow.commit(src, pre, backup)
	if err != nil {
		return report, err
	}
	committed = true
	report.NewState = newState
	report.MutatedBy = backup.MutatedBy
	log.Info("walk 2 complete", "written", kept, "dst", dst, "first_seq", newState.FirstSeq, "last_seq", newState.LastSeq)
	return report, nil
}

// recordCandidate records one stateless-survivor in the per-subject index:
// computes the kv-compact tombstone flag and routes to the latest or ring
// bucket by ringCap.
func recordCandidate(idx *index, cfg *config, ringCap int, seq uint64, subj string, hdrs nats.Header) error {
	isTomb := false
	if cfg.kvCompact && hdrs != nil {
		op := hdrs.Get("KV-Operation")
		isTomb = op == "DEL" || op == "PURGE"
	}
	slot := indexSlot{Seq: seq, IsTombstone: isTomb}
	if ringCap == 1 {
		return idx.setLatest(subj, slot)
	}
	return idx.pushRing(subj, ringCap, slot)
}

// walkSpillIndex is the per-subject real-run walk 1: read the source once, apply
// any stateless filters, and for each survivor append its body to the spill and
// record its candidacy in the index. Returns the count that passed the stateless
// filters (the population the per-subject filter then thins to the keep set).
func walkSpillIndex(
	ctx context.Context,
	src string,
	cfg *config,
	report *Report,
	backup *backupMeta,
	idx *index,
	sp *spill,
	ringCap int,
) (passed uint64, err error) {
	tr, err := openTar(filepath.Join(src, "stream.tar.s2"))
	if err != nil {
		return 0, fmt.Errorf("open source tar: %w", err)
	}
	defer tr.Close()

	hdr, err := tr.Next()
	if err != nil {
		return 0, fmt.Errorf("%w: read first entry: %s", ErrInvalidBackup, err.Error())
	}
	switch hdr.Name {
	case "state.json":
		if _, derr := io.Copy(io.Discard, tr); derr != nil {
			return 0, fmt.Errorf("%w: drain state.json: %s", ErrInvalidBackup, derr.Error())
		}
	case "meta.inf":
		return 0, ErrUnsupportedV1Format
	default:
		return 0, fmt.Errorf("%w: unexpected first tar entry %q", ErrInvalidBackup, hdr.Name)
	}

	var msgsSeen, consumersSeen int
	for {
		if cerr := ctx.Err(); cerr != nil {
			return 0, cerr
		}
		entry, nerr := tr.Next()
		if nerr == io.EOF {
			break
		}
		if nerr != nil {
			return 0, fmt.Errorf("%w: tar.Next: %s", ErrInvalidBackup, nerr.Error())
		}

		switch {
		case strings.HasPrefix(entry.Name, "consumers/"):
			consumersSeen++
			if _, derr := io.Copy(io.Discard, tr); derr != nil {
				return 0, fmt.Errorf("%w: drain consumer: %s", ErrInvalidBackup, derr.Error())
			}
			continue
		case !strings.HasPrefix(entry.Name, "msgs/"):
			if _, derr := io.Copy(io.Discard, tr); derr != nil {
				return 0, fmt.Errorf("%w: drain entry %s: %s", ErrInvalidBackup, entry.Name, derr.Error())
			}
			continue
		}

		msgsSeen++
		oldSeq, perr := strconv.ParseUint(strings.TrimPrefix(entry.Name, "msgs/"), 10, 64)
		if perr != nil {
			return 0, fmt.Errorf("%w: invalid msgs entry name %q", ErrInvalidBackup, entry.Name)
		}
		fullBuf, rerr := io.ReadAll(tr)
		if rerr != nil {
			return 0, fmt.Errorf("%w: msgs/%d: read entry: %s", ErrInvalidBackup, oldSeq, rerr.Error())
		}
		subj, hlen, _, hdrs, mperr := parseMsg(fullBuf, oldSeq, cfg.needsBody(), report)
		if mperr != nil {
			return 0, mperr
		}
		var payload []byte
		if cfg.needsBody() {
			payload = fullBuf[preambleLen(subj, hlen)+hlen:]
		}

		if rejects := cfg.evaluateAll(subj, entry.ModTime, hdrs, payload); len(rejects) > 0 {
			for _, k := range rejects {
				report.MatchedByFilter[k]++
			}
			report.Dropped++
			continue
		}

		if aerr := sp.append(oldSeq, entry.ModTime, fullBuf); aerr != nil {
			return 0, aerr
		}
		if rerr := recordCandidate(idx, cfg, ringCap, oldSeq, subj, hdrs); rerr != nil {
			return 0, rerr
		}
		passed++
	}

	if consumersSeen != backup.State.Consumers {
		return 0, fmt.Errorf("%w: state.json says %d, found %d", ErrConsumerCountMismatch, backup.State.Consumers, consumersSeen)
	}
	if uint64(msgsSeen) != backup.State.Msgs {
		return 0, fmt.Errorf("%w: state.json says %d, found %d", ErrMsgCountMismatch, backup.State.Msgs, msgsSeen)
	}
	report.ConsumersDroppedCount = consumersSeen
	return passed, nil
}

// streamStateless performs the stateless path in a single source pass: each
// message is read once, evaluated against the stateless filters, and survivors
// are copied (renumbered from 1) to the output. state.json leads the output as
// an uncompressed, patchable s2 chunk; its messages count is overwritten in
// place after the pass, so the source is never read a second time.
func streamStateless(
	ctx context.Context,
	src, dst string,
	pre *preflightInfo,
	backup *backupMeta,
	cfg *config,
	report *Report,
	log *slog.Logger,
) (api.StreamState, uint64, error) {
	ow, err := newOutputWriter(dst)
	if err != nil {
		return api.StreamState{}, 0, err
	}
	committed := false
	defer func() {
		if !committed {
			ow.abort()
		}
	}()

	tr, err := openTar(filepath.Join(src, "stream.tar.s2"))
	if err != nil {
		return api.StreamState{}, 0, fmt.Errorf("open source tar: %w", err)
	}
	defer tr.Close()

	hdr, err := tr.Next()
	if err != nil {
		return api.StreamState{}, 0, fmt.Errorf("%w: read first entry: %s", ErrInvalidBackup, err.Error())
	}
	switch hdr.Name {
	case "state.json":
		if _, derr := io.Copy(io.Discard, tr); derr != nil {
			return api.StreamState{}, 0, fmt.Errorf("%w: drain state.json: %s", ErrInvalidBackup, derr.Error())
		}
	case "meta.inf":
		return api.StreamState{}, 0, ErrUnsupportedV1Format
	default:
		return api.StreamState{}, 0, fmt.Errorf("%w: unexpected first tar entry %q", ErrInvalidBackup, hdr.Name)
	}

	var msgsSeen, consumersSeen int
	for {
		if cerr := ctx.Err(); cerr != nil {
			return api.StreamState{}, 0, cerr
		}
		entry, nerr := tr.Next()
		if nerr == io.EOF {
			break
		}
		if nerr != nil {
			return api.StreamState{}, 0, fmt.Errorf("%w: tar.Next: %s", ErrInvalidBackup, nerr.Error())
		}

		switch {
		case strings.HasPrefix(entry.Name, "consumers/"):
			consumersSeen++
			if _, derr := io.Copy(io.Discard, tr); derr != nil {
				return api.StreamState{}, 0, fmt.Errorf("%w: drain consumer: %s", ErrInvalidBackup, derr.Error())
			}
			continue
		case !strings.HasPrefix(entry.Name, "msgs/"):
			if _, derr := io.Copy(io.Discard, tr); derr != nil {
				return api.StreamState{}, 0, fmt.Errorf("%w: drain entry %s: %s", ErrInvalidBackup, entry.Name, derr.Error())
			}
			continue
		}

		msgsSeen++
		oldSeq, perr := strconv.ParseUint(strings.TrimPrefix(entry.Name, "msgs/"), 10, 64)
		if perr != nil {
			return api.StreamState{}, 0, fmt.Errorf("%w: invalid msgs entry name %q", ErrInvalidBackup, entry.Name)
		}

		// Read the whole entry once: needed to copy it out byte-for-byte and to
		// evaluate the filter. O(message size), not O(message count).
		fullBuf, rerr := io.ReadAll(tr)
		if rerr != nil {
			return api.StreamState{}, 0, fmt.Errorf("%w: msgs/%d: read entry: %s", ErrInvalidBackup, oldSeq, rerr.Error())
		}
		subj, hlen, payloadLen, hdrs, perr := parseMsg(fullBuf, oldSeq, cfg.needsBody(), report)
		if perr != nil {
			return api.StreamState{}, 0, perr
		}
		var payload []byte
		if cfg.needsBody() {
			payload = fullBuf[preambleLen(subj, hlen)+hlen:]
		}

		if rejects := cfg.evaluateAll(subj, entry.ModTime, hdrs, payload); len(rejects) > 0 {
			for _, k := range rejects {
				report.MatchedByFilter[k]++
			}
			report.Dropped++
			continue
		}

		if err := ow.writeMsg(entry.ModTime, len(subj), hlen, payloadLen, fullBuf); err != nil {
			return api.StreamState{}, 0, err
		}
	}

	if consumersSeen != backup.State.Consumers {
		return api.StreamState{}, 0, fmt.Errorf("%w: state.json says %d, found %d", ErrConsumerCountMismatch, backup.State.Consumers, consumersSeen)
	}
	if uint64(msgsSeen) != backup.State.Msgs {
		return api.StreamState{}, 0, fmt.Errorf("%w: state.json says %d, found %d", ErrMsgCountMismatch, backup.State.Msgs, msgsSeen)
	}
	report.ConsumersDroppedCount = consumersSeen

	newState, err := ow.commit(src, pre, backup)
	if err != nil {
		return api.StreamState{}, 0, err
	}
	committed = true
	return newState, ow.counter, nil
}

// parseMsg parses a message tar-entry body: subject + header length from the
// preamble, the payload length, and (when needBody) the decoded headers. A
// header decode failure is counted in report.HeaderParseFailures and yields nil
// headers rather than an error.
func parseMsg(fullBuf []byte, oldSeq uint64, needBody bool, report *Report) (subj string, hlen, payloadLen int, hdrs nats.Header, err error) {
	subj, hlen, perr := parsePreamble(bufio.NewReader(bytes.NewReader(fullBuf)))
	if perr != nil {
		return "", 0, 0, nil, fmt.Errorf("%w: msgs/%d: %s", ErrInvalidBackup, oldSeq, perr.Error())
	}
	pl := preambleLen(subj, hlen)
	payloadLen = len(fullBuf) - pl - hlen
	if payloadLen < 0 {
		return "", 0, 0, nil, fmt.Errorf("%w: msgs/%d: negative payload length", ErrInvalidBackup, oldSeq)
	}
	if needBody && hlen > 0 {
		mhdr := append(append([]byte{}, fullBuf[pl:pl+hlen]...), '\r', '\n')
		if hh, herr := nats.DecodeHeadersMsg(mhdr); herr != nil {
			report.HeaderParseFailures++
		} else {
			hdrs = hh
		}
	}
	return subj, hlen, payloadLen, hdrs, nil
}

func logActiveFilters(log *slog.Logger, cfg *config) {
	if len(cfg.subject) > 0 {
		var withs, withouts []string
		for _, cl := range cfg.subject {
			if cl.invert {
				withouts = append(withouts, cl.pattern)
			} else {
				withs = append(withs, cl.pattern)
			}
		}
		log.Info("applying filter", "kind", "subject", "with", withs, "without", withouts)
	}
	if len(cfg.header) > 0 {
		log.Info("applying filter", "kind", "header", "clauses", len(cfg.header))
	}
	if len(cfg.headerPresent) > 0 {
		log.Info("applying filter", "kind", "header_present", "clauses", len(cfg.headerPresent))
	}
	if cfg.hasSince || cfg.hasBefore {
		log.Info("applying filter", "kind", "time", "since", cfg.since, "before", cfg.before)
	}
	if len(cfg.payloadSub) > 0 || len(cfg.payloadRe) > 0 {
		log.Info("applying filter", "kind", "payload",
			"substring_clauses", len(cfg.payloadSub),
			"regex_clauses", len(cfg.payloadRe),
		)
	}
	if cfg.lastN > 0 {
		log.Info("applying filter", "kind", "last_n_per_subject", "n", cfg.lastN)
	}
	if cfg.kvCompact {
		log.Info("applying filter", "kind", "kv_compact")
	}
}

// scanWalk1 reads the source tar once, applies the stateless filters, validates
// entry counts against state.json, and calls onSurvivor for each msgs entry that
// passes. It accumulates only scalars in report (Dropped, MatchedByFilter,
// HeaderParseFailures, ConsumersDroppedCount) — never a per-message collection.
// Returns the number of messages that passed the stateless filters.
func scanWalk1(
	ctx context.Context,
	src string,
	cfg *config,
	report *Report,
	backup *backupMeta,
	onSurvivor func(oldSeq uint64, subj string, ts time.Time, hdrs nats.Header),
) (passed uint64, err error) {
	tr, err := openTar(filepath.Join(src, "stream.tar.s2"))
	if err != nil {
		return 0, fmt.Errorf("open source tar: %w", err)
	}
	defer tr.Close()

	hdr, err := tr.Next()
	if err != nil {
		return 0, fmt.Errorf("%w: read first entry: %s", ErrInvalidBackup, err.Error())
	}
	switch hdr.Name {
	case "state.json":
		if _, derr := io.Copy(io.Discard, tr); derr != nil {
			return 0, fmt.Errorf("%w: drain state.json: %s", ErrInvalidBackup, derr.Error())
		}
	case "meta.inf":
		return 0, ErrUnsupportedV1Format
	default:
		return 0, fmt.Errorf("%w: unexpected first tar entry %q", ErrInvalidBackup, hdr.Name)
	}

	var msgsSeen, consumersSeen int
	for {
		if cerr := ctx.Err(); cerr != nil {
			return 0, cerr
		}
		entry, nerr := tr.Next()
		if nerr == io.EOF {
			break
		}
		if nerr != nil {
			return 0, fmt.Errorf("%w: tar.Next: %s", ErrInvalidBackup, nerr.Error())
		}

		switch {
		case strings.HasPrefix(entry.Name, "consumers/"):
			consumersSeen++
			if _, derr := io.Copy(io.Discard, tr); derr != nil {
				return 0, fmt.Errorf("%w: drain consumer: %s", ErrInvalidBackup, derr.Error())
			}

		case strings.HasPrefix(entry.Name, "msgs/"):
			msgsSeen++
			oldSeq, perr := strconv.ParseUint(strings.TrimPrefix(entry.Name, "msgs/"), 10, 64)
			if perr != nil {
				return 0, fmt.Errorf("%w: invalid msgs entry name %q", ErrInvalidBackup, entry.Name)
			}
			br := bufio.NewReader(tr)
			subj, hlen, perr := parsePreamble(br)
			if perr != nil {
				return 0, fmt.Errorf("%w: msgs/%d: %s", ErrInvalidBackup, oldSeq, perr.Error())
			}
			if int(entry.Size)-preambleLen(subj, hlen)-hlen < 0 {
				return 0, fmt.Errorf("%w: msgs/%d: negative payload length", ErrInvalidBackup, oldSeq)
			}

			var hdrs nats.Header
			var payload []byte
			if cfg.needsBody() {
				body, derr := io.ReadAll(br)
				if derr != nil {
					return 0, fmt.Errorf("%w: msgs/%d: read body: %s", ErrInvalidBackup, oldSeq, derr.Error())
				}
				if hlen > len(body) {
					return 0, fmt.Errorf("%w: msgs/%d: hlen %d > body %d", ErrInvalidBackup, oldSeq, hlen, len(body))
				}
				payload = body[hlen:]
				if hlen > 0 {
					// Copy the header bytes before appending the CRLF so we
					// don't clobber the first payload bytes (they share body's
					// backing array).
					mhdr := append(append([]byte{}, body[:hlen]...), '\r', '\n')
					if hh, herr := nats.DecodeHeadersMsg(mhdr); herr != nil {
						report.HeaderParseFailures++
					} else {
						hdrs = hh
					}
				}
			} else {
				if _, derr := io.Copy(io.Discard, br); derr != nil {
					return 0, fmt.Errorf("%w: msgs/%d: discard body: %s", ErrInvalidBackup, oldSeq, derr.Error())
				}
			}

			if rejects := cfg.evaluateAll(subj, entry.ModTime, hdrs, payload); len(rejects) > 0 {
				for _, k := range rejects {
					report.MatchedByFilter[k]++
				}
				report.Dropped++
				continue
			}

			passed++
			if onSurvivor != nil {
				onSurvivor(oldSeq, subj, entry.ModTime, hdrs)
			}

		default:
			if _, derr := io.Copy(io.Discard, tr); derr != nil {
				return 0, fmt.Errorf("%w: drain entry %s: %s", ErrInvalidBackup, entry.Name, derr.Error())
			}
		}
	}

	if consumersSeen != backup.State.Consumers {
		return 0, fmt.Errorf("%w: state.json says %d, found %d", ErrConsumerCountMismatch, backup.State.Consumers, consumersSeen)
	}
	if uint64(msgsSeen) != backup.State.Msgs {
		return 0, fmt.Errorf("%w: state.json says %d, found %d", ErrMsgCountMismatch, backup.State.Msgs, msgsSeen)
	}
	report.ConsumersDroppedCount = consumersSeen
	return passed, nil
}

// buildNewState builds the output stream state. NumSubjects is always 0: the
// server recomputes it (and bytes/timestamps) from the restored messages.
func buildNewState(kept, accBytes uint64, firstTs, lastTs time.Time) api.StreamState {
	s := api.StreamState{
		Msgs:        kept,
		Bytes:       accBytes,
		NumSubjects: 0,
		NumDeleted:  0,
		Consumers:   0,
	}
	if kept > 0 {
		s.FirstSeq = 1
		s.LastSeq = kept
		s.FirstTime = firstTs
		s.LastTime = lastTs
	}
	return s
}

func writeStateJSON(tw *tarWriter, st api.StreamState) error {
	b, err := json.Marshal(st)
	if err != nil {
		return fmt.Errorf("marshal state.json: %w", err)
	}
	hdr := &tar.Header{
		Name:    "state.json",
		Mode:    0o600,
		Size:    int64(len(b)),
		ModTime: time.Now().UTC(),
		Format:  tar.FormatPAX,
	}
	if err := tw.WriteHeader(hdr); err != nil {
		return fmt.Errorf("write state.json header: %w", err)
	}
	if _, err := tw.Write(b); err != nil {
		return fmt.Errorf("write state.json body: %w", err)
	}
	return nil
}

func writeBackupJSON(path string, m *backupMeta) error {
	b, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func randSuffix() (string, error) {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}
