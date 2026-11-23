package watch

import (
	"context"
	"encoding/json"
	"time"

	"github.com/rs/zerolog/log"
)

// spendHeld is how many spend records wait to be written before more are
// dropped. Spend is emitted at most once a minute per running machine.
const spendHeld = 2048

// Spend is one incremental billing window for a bought machine, written to the
// market's spend stream as the machine runs (and when it closes) so spend per
// provider, pool, and reservation includes still-running machines.
//
// The stream is for working out what machines cost and earn, not for
// accounts: a record that cannot be queued or written is logged and dropped,
// so its sums can come to less than what Redis counted against the spend
// caps.
//
// Every record is the control plane's: what it paid a provider and what it
// billed, by its own clock. Nothing a machine reports is in one, and no
// machine writes one, so a worker's account of its own usage can be held
// against the records of its machine here.
type Spend struct {
	// At is the end of the billing window (the S2 record timestamp).
	At time.Time `json:"-"`
	// From is the start of the billing window.
	From time.Time `json:"-"`

	Source   string `json:"source"`
	Provider string `json:"provider"`
	Cloud    string `json:"cloud,omitempty"`
	Region   string `json:"region,omitempty"`
	Geo      string `json:"geo,omitempty"`
	Offer    string `json:"offer,omitempty"`
	Zone     string `json:"zone,omitempty"`
	Spot     bool   `json:"spot,omitempty"`
	GPU      string `json:"gpu,omitempty"`
	GPUs     uint32 `json:"gpus,omitempty"`

	MachineID string `json:"machine_id,omitempty"`
	// Pool is the control-plane pool name for SourcePool spend. Omitted for
	// tenant reservations.
	Pool string `json:"pool,omitempty"`
	// Booted reports whether the machine had come up by the end of this window,
	// separating unproductive boot spend from active runtime spend.
	Booted bool `json:"booted,omitempty"`

	// HourlyMicros is what the provider charged for the machine per hour
	// (micro-USD).
	HourlyMicros int64 `json:"micros,omitempty"`
	// Duration is the length of the billing window (At - From).
	Duration time.Duration `json:"-"`
	// CostMicros is the provider spend accrued over Duration (micro-USD;
	// 10_000 micros = 1 cent, 1_000_000 micros = $1).
	CostMicros int64 `json:"cost_micros,omitempty"`
	// RevenueMicros is the customer revenue billed over Duration when known at
	// the machine level (SourceReservation). Serverless pool container revenue
	// is attributed per-container in OpenMeter.
	RevenueMicros int64 `json:"revenue_micros,omitempty"`
}

type spendWire struct {
	V      int   `json:"v"`
	TS     int64 `json:"ts"`
	FromTS int64 `json:"from_ts,omitempty"`
	spendFields
	DurationMS int64 `json:"duration_ms,omitempty"`
}

type spendFields Spend

func (s Spend) MarshalJSON() ([]byte, error) {
	var fromTS int64
	if !s.From.IsZero() {
		fromTS = s.From.UnixMilli()
	}
	return json.Marshal(spendWire{
		V:           recordVersion,
		TS:          s.At.UnixMilli(),
		FromTS:      fromTS,
		spendFields: spendFields(s),
		DurationMS:  s.Duration.Milliseconds(),
	})
}

func (s *Spend) UnmarshalJSON(data []byte) error {
	var in spendWire
	if err := json.Unmarshal(data, &in); err != nil {
		return err
	}
	*s = Spend(in.spendFields)
	s.At = time.UnixMilli(in.TS).UTC()
	if in.FromTS != 0 {
		s.From = time.UnixMilli(in.FromTS).UTC()
	}
	s.Duration = time.Duration(in.DurationMS) * time.Millisecond
	return nil
}

// SpendOf constructs an incremental Spend record from an Outcome's machine
// metadata and the billing window [from, to].
func SpendOf(outcome Outcome, from, to time.Time, costMicros, revenueMicros int64, booted bool) Spend {
	from = from.UTC()
	to = to.UTC()
	duration := max(0, to.Sub(from))
	pool := ""
	if outcome.Source == SourcePool {
		pool = outcome.Pool
	}
	return Spend{
		At:            to,
		From:          from,
		Source:        outcome.Source,
		Provider:      outcome.Provider,
		Cloud:         outcome.Cloud,
		Region:        outcome.Region,
		Geo:           outcome.Geo,
		Offer:         outcome.Offer,
		Zone:          outcome.Zone,
		Spot:          outcome.Spot,
		GPU:           outcome.GPU,
		GPUs:          outcome.GPUs,
		MachineID:     outcome.MachineID,
		Pool:          pool,
		Booted:        booted,
		HourlyMicros:  outcome.HourlyMicros,
		Duration:      duration,
		CostMicros:    max(0, costMicros),
		RevenueMicros: max(0, revenueMicros),
	}
}

// RecordSpend takes note of one incremental spend window, at the present time
// unless it says when. Recording never waits and never fails.
func (r *Recorder) RecordSpend(spend Spend) {
	if r == nil {
		return
	}
	if spend.Duration <= 0 && spend.CostMicros <= 0 && spend.RevenueMicros <= 0 {
		return
	}
	if spend.At.IsZero() {
		spend.At = r.clock.Now()
	}
	spend.At = spend.At.UTC()
	if !spend.From.IsZero() {
		spend.From = spend.From.UTC()
	}
	select {
	case r.spendQueue <- spend:
	default:
		if r.spendDropped.Add(1) == 1 {
			log.Warn().Msg("market: spend records are recorded faster than they are written; some are dropped")
		}
	}
}

// ReadSpend returns the spend records written from from until to, oldest first.
func (r *Recorder) ReadSpend(ctx context.Context, from, to time.Time) ([]Spend, error) {
	if r == nil {
		return nil, nil
	}
	records, err := r.log.Read(ctx, r.spendStream, from, to)
	spends := make([]Spend, 0, len(records))
	for _, record := range records {
		var spend Spend
		if json.Unmarshal(record.Body, &spend) == nil {
			spends = append(spends, spend)
		}
	}
	return spends, err
}

// FlushSpend writes the spend records that wait, after any it is given.
func (r *Recorder) FlushSpend(ctx context.Context, spends ...Spend) {
	if r == nil {
		return
	}
	for {
		for len(spends) < outcomesAtOnce {
			select {
			case spend := <-r.spendQueue:
				spends = append(spends, spend)
				continue
			default:
			}
			break
		}
		if len(spends) == 0 {
			return
		}
		records := make([]Record, 0, len(spends))
		for _, spend := range spends {
			body, err := json.Marshal(spend)
			if err != nil {
				continue
			}
			records = append(records, Record{At: spend.At, Body: body})
		}
		if err := r.log.Append(ctx, r.spendStream, records...); err != nil {
			log.Warn().Err(err).Int("records", len(records)).Msg("market: could not write machine spend to the market's history")
		}
		spends = spends[:0]
	}
}
