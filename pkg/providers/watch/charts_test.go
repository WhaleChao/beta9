package watch

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/providers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// memoryCharts is a ChartCache in memory, which keeps every chart until it
// is kept anew.
type memoryCharts struct {
	mu     sync.Mutex
	charts map[string][]byte
}

func (c *memoryCharts) GetChart(_ context.Context, name string) ([]byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.charts[name], nil
}

func (c *memoryCharts) SetChart(_ context.Context, name string, chart []byte, _ time.Duration) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.charts[name] = chart
	return nil
}

func (c *memoryCharts) kept() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.charts)
}

// countingLog is a history that can be made to fail.
type countingLog struct {
	Log
	fail error
}

func (l *countingLog) Read(ctx context.Context, stream Stream, from, to time.Time) ([]Record, error) {
	if l.fail != nil {
		return nil, l.fail
	}
	return l.Log.Read(ctx, stream, from, to)
}

func h100(id, region string, gpus uint32, micros int64, change ...func(*providers.Offer)) providers.Offer {
	return a10g(id, region, gpus, micros, append(change, func(offer *providers.Offer) { offer.GPU = "H100" })...)
}

func TestAChartOfEveryGPUTypeHasALineOfEachThatTenantsCanReserve(t *testing.T) {
	aws := providers.NewFake("aws", "test")
	aws.SetOffers(a10g("us-east-1b/g5.xlarge", "us-east-1", 1, 380_000, spot), a10g("us-east-1/g5.xlarge", "us-east-1", 1, 1_000_000))
	verda := providers.NewFake("verda", "test")
	verda.SetOffers(a10g("1A10.8V", "FIN-01", 1, 100_000), h100("1H100.80S", "FIN-01", 1, 2_000_000))
	market := newTestMarket(t, aws, verda)
	market.sample(market.sampler("gw-1"), noon)
	market.clock.Advance(time.Minute)

	answer, err := market.reader(notVerda).Series(context.Background(), SeriesRequest{Range: "1h"})
	require.NoError(t, err)
	assert.Equal(t, "1h", answer.Range)
	assert.Equal(t, noon.Add(-time.Hour+time.Minute), answer.Start)
	require.Len(t, answer.Series, 1, "verda sells the only H100, and tenants cannot reserve from it")
	assert.Equal(t, "A10G", answer.Series[0].GPU)
	assertPoints(t, [][]any{point(noon, 380_000, 380_000, 380_000, nil, nil, 1)}, answer.Series[0])

	answer, err = market.reader().Series(context.Background(), SeriesRequest{Range: "1h", Pricing: PricingOnDemand, Group: []string{ByGeo}})
	require.NoError(t, err)
	keys := []string{}
	for _, series := range answer.Series {
		keys = append(keys, series.Key)
	}
	assert.Equal(t, []string{"A10G|eu-north", "A10G|us-east", "H100|eu-north"}, keys, "every GPU type is its own line, whatever else tells them apart, the cheapest first")
}

func TestEveryGatewayIsGivenTheChartsTheSamplingGatewayWorksOut(t *testing.T) {
	ctx := context.Background()
	market := fourSellers(t)
	market.clock.Advance(time.Minute)
	charts := &memoryCharts{charts: map[string][]byte{}}
	sharing := func(opts *ReaderOptions) { opts.Charts = charts }
	broken := func(opts *ReaderOptions) { opts.Log = &countingLog{Log: market.log, fail: errors.New("s2 is down")} }

	market.reader(notVerda, sharing).Warm(ctx)
	assert.Equal(t, len(Ranges)*3*2, charts.kept(), "a chart of prices and one of outcomes, of every range, however it is sold")

	// A gateway that cannot read the history is told what it would have
	// worked out.
	other := market.reader(notVerda, sharing, broken)
	for _, q := range []SeriesRequest{{Range: "1h"}, {Range: "90d", Pricing: PricingSpot}} {
		want, err := market.reader(notVerda).Series(ctx, q)
		require.NoError(t, err)
		got, err := other.Series(ctx, q)
		require.NoError(t, err)
		assert.JSONEq(t, jsonOf(t, want), jsonOf(t, got), "%+v", q)
	}
	outcomes, err := other.Outcomes(ctx, SeriesRequest{Range: "7d"})
	require.NoError(t, err)
	assert.Equal(t, "7d", outcomes.Range)

	// What an admin asks, and what is not asked by range, is never shared.
	_, err = other.Series(ctx, SeriesRequest{Range: "1h", Admin: true})
	assert.ErrorContains(t, err, "s2 is down")
	_, err = other.Series(ctx, SeriesRequest{Start: noon})
	assert.ErrorContains(t, err, "s2 is down")
}

func TestARangesChartsAreWorkedOutAnewAsOftenAsTheRangeSays(t *testing.T) {
	ctx := context.Background()
	market := fourSellers(t)
	market.clock.Advance(time.Minute)
	charts := &memoryCharts{charts: map[string][]byte{}}
	sharing := func(opts *ReaderOptions) { opts.Charts = charts }
	sampling := market.reader(notVerda, sharing)
	sampling.Warm(ctx)

	sampler := market.sampler("gw-1")
	sampler.OnSampled(func() { sampling.Warm(ctx) })
	market.sample(sampler, noon.Add(2*time.Minute))
	market.clock.Advance(time.Minute)

	other := market.reader(notVerda, sharing, func(opts *ReaderOptions) {
		opts.Log = &countingLog{Log: market.log, fail: errors.New("s2 is down")}
	})
	hour, err := other.Series(ctx, SeriesRequest{Range: "1h"})
	require.NoError(t, err)
	assert.Equal(t, noon.Add(2*time.Minute), hour.End, "the last hour is worked out anew every minute")
	day, err := other.Series(ctx, SeriesRequest{Range: "24h"})
	require.NoError(t, err)
	assert.Equal(t, noon.Add(time.Minute), day.End, "the last day every five")
}

func TestARangeIsAskedInPlaceOfAStartAndAnEnd(t *testing.T) {
	reader := fourSellers(t).reader()

	for what, q := range map[string]SeriesRequest{
		`range must be one of 1h, 6h, 24h, 7d, 30d, 90d, not "2d"`: {Range: "2d"},
		"range is asked in place of start and end, not with them":  {Range: "24h", Start: noon},
	} {
		_, err := reader.Series(context.Background(), q)
		var wrong *RequestError
		require.ErrorAs(t, err, &wrong, what)
		assert.Equal(t, what, wrong.What)
	}
}
