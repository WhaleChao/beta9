package watch

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"math"
	"slices"
	"time"
)

// outcomePoints is the most points a chart of outcomes is drawn with:
// machines are bought far less often than the market is sampled.
const outcomePoints = 60

// outcomesShownFrom is how many machines a time or a share is shown of.
// Fewer say little, and tell how many machines are bought.
const outcomesShownFrom = 3

var outcomeSteps = []time.Duration{
	5 * time.Minute, 15 * time.Minute, 30 * time.Minute, time.Hour, 3 * time.Hour,
	6 * time.Hour, 12 * time.Hour, 24 * time.Hour, 48 * time.Hour, 7 * 24 * time.Hour,
}

// OutcomesAnswer is a chart of what became of the machines bought over a
// time: how long they took to come up, and how often and how soon spot
// machines were taken back.
type OutcomesAnswer struct {
	GPU         string          `json:"gpu,omitempty"`
	Range       string          `json:"range,omitempty"`
	Start       time.Time       `json:"start"`
	End         time.Time       `json:"end"`
	StepSeconds int             `json:"step_seconds"`
	Cols        []string        `json:"cols"`
	Series      []OutcomeSeries `json:"series"`
	Truncated   bool            `json:"truncated,omitempty"`
}

// OutcomeColumns is what each point of an outcome series holds, in order.
// Times are in seconds, and a time or a share is null where fewer than
// outcomesShownFrom machines are behind it. Every machine counts in the step
// in which what is said of it happened.
//
//   - ts: when the step began, in milliseconds.
//   - boot_p50, boot_p90: how long the machines that came up took to, from
//     when they were bought.
//   - preempted_share: the share of the machines that ended, after they
//     came up, that their provider took back.
//   - preempted_after_p50: how long the machines that were taken back were
//     had for.
var OutcomeColumns = []string{"ts", "boot_p50", "boot_p90", "preempted_share", "preempted_after_p50"}

// OutcomeSeries is one line of a chart of outcomes, and what the whole time
// came to.
type OutcomeSeries struct {
	Series
	Summary OutcomeSummary `json:"summary"`
}

type OutcomeSummary struct {
	BootP50           *float64 `json:"boot_p50"`
	BootP90           *float64 `json:"boot_p90"`
	PreemptedShare    *float64 `json:"preempted_share"`
	PreemptedAfterP50 *float64 `json:"preempted_after_p50"`
}

// outcomeStepOf is how far apart the points of a chart of outcomes over a
// span are.
func outcomeStepOf(span time.Duration) time.Duration {
	for _, step := range outcomeSteps {
		if span <= outcomePoints*step {
			return step
		}
	}
	return outcomeSteps[len(outcomeSteps)-1]
}

// Outcomes answers what became of the machines bought over a time. It is
// asked, and leaves out and groups what it is of, as Series is.
func (r *Reader) Outcomes(ctx context.Context, q SeriesRequest) (*OutcomesAnswer, error) {
	if r.log == nil {
		return nil, ErrNoHistory
	}
	if err := q.normalize(r.clock.Now()); err != nil {
		return nil, err
	}
	step := outcomeStepOf(q.End.Sub(q.Start))
	return cache(r, "outcomes:"+q.cacheKey(step), seriesCachedFor, func() (*OutcomesAnswer, error) {
		return shared(ctx, r, "outcomes", q, func(ctx context.Context) (*OutcomesAnswer, error) {
			return r.outcomes(ctx, q, step)
		})
	})
}

func (r *Reader) outcomes(ctx context.Context, q SeriesRequest, step time.Duration) (*OutcomesAnswer, error) {
	outcomes, err := readOutcomes(ctx, r.log, r.streams.Outcomes(), q.Start.Truncate(step), q.End)
	if err != nil {
		return nil, fmt.Errorf("read what became of the machines bought: %w", err)
	}

	type tally struct {
		series OutcomeSeries
		steps  map[int64]*outcomeTally
		all    outcomeTally
	}
	tallies := map[string]*tally{}
	for _, outcome := range outcomes {
		listing := Listing{
			Provider: outcome.Provider, Cloud: outcome.Cloud, Region: outcome.Region,
			Geo: outcome.Geo, Spot: outcome.Spot, GPUs: outcome.GPUs,
		}
		if outcome.GPU == "" || q.GPU != "" && outcome.GPU != q.GPU ||
			!q.wants(listing) || !q.Admin && !r.sells(outcome.Provider) {
			continue
		}
		series := q.seriesOf(outcome.GPU, listing)
		at, ok := tallies[series.Key]
		if !ok {
			at = &tally{series: OutcomeSeries{Series: series}, steps: map[int64]*outcomeTally{}}
			tallies[series.Key] = at
		}
		begins := outcome.At.Truncate(step).UnixMilli()
		if at.steps[begins] == nil {
			at.steps[begins] = &outcomeTally{}
		}
		at.steps[begins].add(outcome)
		at.all.add(outcome)
	}

	out := &OutcomesAnswer{
		GPU: q.GPU, Range: q.Range, Start: q.Start, End: q.End, StepSeconds: int(step.Seconds()),
		Cols: OutcomeColumns, Series: []OutcomeSeries{},
	}
	for _, at := range tallies {
		series := at.series
		series.Summary = at.all.summary()
		series.Points = [][]any{}
		for _, begins := range slices.Sorted(maps.Keys(at.steps)) {
			if s := at.steps[begins].summary(); s != (OutcomeSummary{}) {
				series.Points = append(series.Points, []any{begins, s.BootP50, s.BootP90, s.PreemptedShare, s.PreemptedAfterP50})
			}
		}
		if series.Summary != (OutcomeSummary{}) {
			out.Series = append(out.Series, series)
		}
	}
	slices.SortFunc(out.Series, func(a, b OutcomeSeries) int { return cmp.Compare(a.Key, b.Key) })
	if len(out.Series) > seriesAtMost {
		out.Series, out.Truncated = out.Series[:seriesAtMost], true
	}
	return out, nil
}

// outcomeTally is what the outcomes of a step, or of a whole time, come to.
type outcomeTally struct {
	boots, preemptedAfter []time.Duration
	ended, preempted      int
}

func (t *outcomeTally) add(outcome Outcome) {
	switch {
	case outcome.Event == OutcomeActive:
		t.boots = append(t.boots, outcome.Took)
	case outcome.Event == OutcomeEnd && outcome.Result != ResultNeverUp:
		// Preempted share and time-to-preemption are defined for machines
		// that came up; exclude machines lost before reaching active.
		if outcome.Booted != nil && !*outcome.Booted {
			return
		}
		t.ended++
		if outcome.Result == ResultInterrupted {
			t.preempted++
			after := outcome.Took
			if outcome.Ran > 0 {
				after = outcome.Ran
			}
			t.preemptedAfter = append(t.preemptedAfter, after)
		}
	}
}

func (t *outcomeTally) summary() OutcomeSummary {
	slices.Sort(t.boots)
	slices.Sort(t.preemptedAfter)
	out := OutcomeSummary{
		BootP50:           percentile(t.boots, 0.5),
		BootP90:           percentile(t.boots, 0.9),
		PreemptedAfterP50: percentile(t.preemptedAfter, 0.5),
	}
	if t.ended >= outcomesShownFrom {
		preempted := share(float64(t.preempted) / float64(t.ended))
		out.PreemptedShare = &preempted
	}
	return out
}

// percentile is the nearest-rank percentile of durations in order, in
// seconds to the tenth, or nil when there are too few to show.
func percentile(sorted []time.Duration, p float64) *float64 {
	if len(sorted) < outcomesShownFrom {
		return nil
	}
	rank := int(math.Ceil(p*float64(len(sorted)))) - 1
	seconds := math.Round(sorted[max(rank, 0)].Seconds()*10) / 10
	return &seconds
}

// readOutcomes reads the outcomes written from from until to, in order of
// their time.
func readOutcomes(ctx context.Context, log Log, stream Stream, from, to time.Time) ([]Outcome, error) {
	records, err := log.Read(ctx, stream, from, to)
	if err != nil {
		return nil, err
	}
	outcomes := make([]Outcome, 0, len(records))
	for _, record := range records {
		var outcome Outcome
		if json.Unmarshal(record.Body, &outcome) == nil {
			outcomes = append(outcomes, outcome)
		}
	}
	slices.SortStableFunc(outcomes, func(a, b Outcome) int { return a.At.Compare(b.At) })
	return outcomes, nil
}
