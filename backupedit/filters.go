package backupedit

import (
	"bytes"
	"log/slog"
	"regexp"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/nats-io/jsm.go"
)

// Filter combination: AND across kinds; within a kind, positives OR (must
// match at least one if any specified) and negatives AND-NOT (must not match
// any). With* / Without* both delegate to a single private helper.

type config struct {
	subject       []subjectClause
	header        []headerClause
	headerPresent []headerPresentClause
	payloadSub    []payloadSubClause
	payloadRe     []payloadReClause

	since     time.Time
	before    time.Time
	hasSince  bool
	hasBefore bool

	lastN     int
	kvCompact bool

	dryRun bool

	// Index lifecycle.
	keepIndex bool
	indexDir  string

	logger *slog.Logger
}

type subjectClause struct {
	invert  bool
	pattern string
}
type headerClause struct {
	invert bool
	name   string
	value  string
}
type headerPresentClause struct {
	invert bool
	name   string
}
type payloadSubClause struct {
	invert bool
	needle []byte
}
type payloadReClause struct {
	invert bool
	re     *regexp.Regexp
}

func (c *config) validate() error {
	if c.kvCompact && c.lastN > 0 {
		return ErrConflictingFilters
	}
	return nil
}

func (c *config) needsBody() bool {
	if c.kvCompact {
		return true
	}
	if len(c.header) > 0 || len(c.headerPresent) > 0 {
		return true
	}
	if len(c.payloadSub) > 0 || len(c.payloadRe) > 0 {
		return true
	}
	return false
}

// Option configures Filter.
type Option func(*config)

func WithSubject(patterns ...string) Option {
	return withSubject(false, patterns...)
}
func WithoutSubject(patterns ...string) Option {
	return withSubject(true, patterns...)
}
func withSubject(invert bool, patterns ...string) Option {
	return func(c *config) {
		for _, p := range patterns {
			c.subject = append(c.subject, subjectClause{invert: invert, pattern: p})
		}
	}
}

func WithHeader(name, value string) Option    { return withHeader(false, name, value) }
func WithoutHeader(name, value string) Option { return withHeader(true, name, value) }
func withHeader(invert bool, name, value string) Option {
	return func(c *config) {
		c.header = append(c.header, headerClause{invert: invert, name: name, value: value})
	}
}

func WithHeaderPresent(name string) Option    { return withHeaderPresent(false, name) }
func WithoutHeaderPresent(name string) Option { return withHeaderPresent(true, name) }
func withHeaderPresent(invert bool, name string) Option {
	return func(c *config) {
		c.headerPresent = append(c.headerPresent, headerPresentClause{invert: invert, name: name})
	}
}

func WithSince(t time.Time) Option {
	return func(c *config) { c.since = t; c.hasSince = true }
}
func WithBefore(t time.Time) Option {
	return func(c *config) { c.before = t; c.hasBefore = true }
}

func WithPayloadSubstring(needle []byte) Option    { return withPayloadSubstring(false, needle) }
func WithoutPayloadSubstring(needle []byte) Option { return withPayloadSubstring(true, needle) }
func withPayloadSubstring(invert bool, needle []byte) Option {
	return func(c *config) {
		c.payloadSub = append(c.payloadSub, payloadSubClause{invert: invert, needle: needle})
	}
}

func WithPayloadRegexp(re *regexp.Regexp) Option    { return withPayloadRegexp(false, re) }
func WithoutPayloadRegexp(re *regexp.Regexp) Option { return withPayloadRegexp(true, re) }
func withPayloadRegexp(invert bool, re *regexp.Regexp) Option {
	return func(c *config) {
		c.payloadRe = append(c.payloadRe, payloadReClause{invert: invert, re: re})
	}
}

func WithLastNPerSubject(n int) Option {
	return func(c *config) {
		if n > 0 {
			c.lastN = n
		}
	}
}
func WithKVCompact() Option { return func(c *config) { c.kvCompact = true } }

func WithDryRun() Option              { return func(c *config) { c.dryRun = true } }
func WithKeepIndex() Option           { return func(c *config) { c.keepIndex = true } }
func WithIndexDir(path string) Option { return func(c *config) { c.indexDir = path } }
func WithLogger(l *slog.Logger) Option {
	return func(c *config) { c.logger = l }
}

// Filter-kind keys used in Report.MatchedByFilter.
const (
	FilterKindSubject       = "subject"
	FilterKindHeader        = "header"
	FilterKindHeaderPresent = "header_present"
	FilterKindTime          = "time"
	FilterKindPayload       = "payload"
	FilterKindLastN         = "last_n_per_subject"
	FilterKindKVCompact     = "kv_compact"
)

// kindAccepts returns true when (positives OR / negatives AND-NOT) passes.
func kindAccepts(positiveCount, positiveMatched, negativeMatched int) bool {
	if negativeMatched > 0 {
		return false
	}
	if positiveCount > 0 && positiveMatched == 0 {
		return false
	}
	return true
}

// evaluateAll returns the set of filter-kind keys that reject the message.
// Empty slice == passes all stateless filters.
func (c *config) evaluateAll(subj string, ts time.Time, hdrs nats.Header, payload []byte) []string {
	var rejects []string

	if len(c.subject) > 0 {
		posCount, posMatched, negMatched := 0, 0, 0
		for _, cl := range c.subject {
			match := jsm.SubjectIsSubsetMatch(subj, cl.pattern)
			if cl.invert {
				if match {
					negMatched++
				}
			} else {
				posCount++
				if match {
					posMatched++
				}
			}
		}
		if !kindAccepts(posCount, posMatched, negMatched) {
			rejects = append(rejects, FilterKindSubject)
		}
	}

	if len(c.header) > 0 {
		posCount, posMatched, negMatched := 0, 0, 0
		for _, cl := range c.header {
			match := false
			if hdrs != nil {
				for _, v := range hdrs.Values(cl.name) {
					if v == cl.value {
						match = true
						break
					}
				}
			}
			if cl.invert {
				if match {
					negMatched++
				}
			} else {
				posCount++
				if match {
					posMatched++
				}
			}
		}
		if !kindAccepts(posCount, posMatched, negMatched) {
			rejects = append(rejects, FilterKindHeader)
		}
	}

	if len(c.headerPresent) > 0 {
		posCount, posMatched, negMatched := 0, 0, 0
		for _, cl := range c.headerPresent {
			match := false
			if hdrs != nil {
				if vs := hdrs.Values(cl.name); len(vs) > 0 {
					match = true
				}
			}
			if cl.invert {
				if match {
					negMatched++
				}
			} else {
				posCount++
				if match {
					posMatched++
				}
			}
		}
		if !kindAccepts(posCount, posMatched, negMatched) {
			rejects = append(rejects, FilterKindHeaderPresent)
		}
	}

	if c.hasSince || c.hasBefore {
		if c.hasSince && ts.Before(c.since) {
			rejects = append(rejects, FilterKindTime)
		} else if c.hasBefore && !ts.Before(c.before) {
			rejects = append(rejects, FilterKindTime)
		}
	}

	if len(c.payloadSub) > 0 || len(c.payloadRe) > 0 {
		posCount, posMatched, negMatched := 0, 0, 0
		for _, cl := range c.payloadSub {
			match := bytes.Contains(payload, cl.needle)
			if cl.invert {
				if match {
					negMatched++
				}
			} else {
				posCount++
				if match {
					posMatched++
				}
			}
		}
		for _, cl := range c.payloadRe {
			match := cl.re.Match(payload)
			if cl.invert {
				if match {
					negMatched++
				}
			} else {
				posCount++
				if match {
					posMatched++
				}
			}
		}
		if !kindAccepts(posCount, posMatched, negMatched) {
			rejects = append(rejects, FilterKindPayload)
		}
	}

	return rejects
}
