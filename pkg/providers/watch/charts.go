package watch

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/rs/zerolog/log"
)

const (
	// chartVersion is told apart in what a chart is kept under, so that a
	// gateway is never given a chart drawn as another version draws it.
	chartVersion = 1
	// chartGrace is how much longer than a range's Every its charts are
	// kept, so that they are worked out anew before they are let go.
	chartGrace = time.Minute
	// chartWorkAtMost is how long working out a chart may take.
	chartWorkAtMost = 2 * time.Minute
)

// ChartCache keeps the charts that every gateway answers alike, so that one
// gateway works each out and the rest are given it.
type ChartCache interface {
	// GetChart returns the chart kept under name, or nil when none is.
	GetChart(ctx context.Context, name string) ([]byte, error)
	// SetChart keeps a chart under name for ttl.
	SetChart(ctx context.Context, name string, chart []byte, ttl time.Duration) error
}

// Range is a time a chart can be asked about by name, which ends now. Its
// charts are worked out anew every Every: a tenant is told the same of it
// for that long.
type Range struct {
	Name  string
	Span  time.Duration
	Every time.Duration
}

var Ranges = []Range{
	{Name: "1h", Span: time.Hour, Every: time.Minute},
	{Name: "6h", Span: 6 * time.Hour, Every: 5 * time.Minute},
	{Name: "24h", Span: 24 * time.Hour, Every: 5 * time.Minute},
	{Name: "7d", Span: 7 * 24 * time.Hour, Every: 15 * time.Minute},
	{Name: "30d", Span: 30 * 24 * time.Hour, Every: 30 * time.Minute},
	{Name: "90d", Span: 90 * 24 * time.Hour, Every: time.Hour},
}

func rangeOf(name string) (Range, bool) {
	for _, rng := range Ranges {
		if rng.Name == name {
			return rng, true
		}
	}
	return Range{}, false
}

func rangeNames() string {
	names := make([]string, len(Ranges))
	for i, rng := range Ranges {
		names[i] = rng.Name
	}
	return strings.Join(names, ", ")
}

// chartName is what the answer to a normalized question is kept under.
func (q SeriesRequest) chartName(kind string) string {
	return fmt.Sprintf("%s:v%d:%s|%s|%v|%v|%s|%v", kind, chartVersion, q.Range, q.GPU, q.Counts, q.Geos, q.Pricing, q.Group)
}

// shared answers a normalized question from the chart every gateway keeps
// of it, and works out the chart and keeps it when there is none. Only a
// tenant's question about a range is shared: every tenant is told the same
// of it. Each gateway works out a chart once at a time, however many ask.
func shared[T any](ctx context.Context, r *Reader, kind string, q SeriesRequest, work func(context.Context) (T, error)) (T, error) {
	rng, ok := rangeOf(q.Range)
	if !ok || q.Admin || r.charts == nil {
		return work(ctx)
	}
	name := q.chartName(kind)
	var answer T
	kept, err := r.charts.GetChart(ctx, name)
	if err != nil {
		log.Warn().Err(err).Str("chart", name).Msg("market: could not read a shared chart")
	} else if kept != nil && json.Unmarshal(kept, &answer) == nil {
		return answer, nil
	}
	worked, err, _ := r.flights.Do(name, func() (any, error) {
		// Whoever asked first may stop waiting before the rest do.
		ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), chartWorkAtMost)
		defer cancel()
		answer, err := work(ctx)
		if err == nil {
			r.keepChart(ctx, name, rng, answer)
		}
		return answer, err
	})
	if err != nil {
		return answer, err
	}
	return worked.(T), nil
}

func (r *Reader) keepChart(ctx context.Context, name string, rng Range, answer any) {
	chart, err := json.Marshal(answer)
	if err == nil {
		err = r.charts.SetChart(ctx, name, chart, rng.Every+chartGrace)
	}
	if err != nil {
		log.Warn().Err(err).Str("chart", name).Msg("market: could not share a chart")
	}
}

// Warm works out anew the charts of every range that are due, of every GPU
// type however it is sold, which is what most tenants and the public pages
// ask, and keeps them for every gateway. The gateway that samples calls it
// after each sample; a call while another is still at work does nothing.
func (r *Reader) Warm(ctx context.Context) {
	if r.log == nil || r.charts == nil || !r.warming.TryLock() {
		return
	}
	defer r.warming.Unlock()
	now := r.clock.Now()
	for _, rng := range Ranges {
		if now.Sub(r.warmed[rng.Name]) < rng.Every-r.interval/2 {
			continue
		}
		for _, pricing := range []string{PricingAny, PricingOnDemand, PricingSpot} {
			q := SeriesRequest{Range: rng.Name, Pricing: pricing}
			if err := r.warm(ctx, q, rng, now); err != nil {
				log.Warn().Err(err).Str("range", rng.Name).Str("pricing", pricing).Msg("market: could not work out a chart anew")
			}
		}
		r.warmed[rng.Name] = now
	}
}

func (r *Reader) warm(ctx context.Context, q SeriesRequest, rng Range, now time.Time) error {
	ctx, cancel := context.WithTimeout(ctx, chartWorkAtMost)
	defer cancel()
	if err := q.normalize(now); err != nil {
		return err
	}
	tier, step := r.tierOf(q, now)
	series, err := r.series(ctx, q, tier, step, now)
	if err != nil {
		return err
	}
	r.keepChart(ctx, q.chartName("series"), rng, series)
	outcomes, err := r.outcomes(ctx, q, outcomeStepOf(q.End.Sub(q.Start)))
	if err != nil {
		return err
	}
	r.keepChart(ctx, q.chartName("outcomes"), rng, outcomes)
	return nil
}
