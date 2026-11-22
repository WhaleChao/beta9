package watch

import (
	"context"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/providers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// boughtMachines is a market in which machines were bought from two
// providers, of which tenants cannot reserve from verda:
//
//   - five A10Gs on spot from aws at noon, of which four came up, in a
//     minute, a minute and a half, two minutes and five minutes, and one
//     did not. Two were taken back, an hour and three hours on; one was
//     given back after two hours, and one was lost.
//   - an H100 on demand from aws, which came up.
//   - three A10Gs on demand from verda, which came up in ten seconds.
func boughtMachines(t *testing.T) *testMarket {
	t.Helper()
	market := newTestMarket(t, providers.NewFake("aws", "test"), providers.NewFake("verda", "test"))
	recorder := NewRecorder(market.log, StreamsOf(market.config), market.clock)
	record := func(at time.Duration, outcome Outcome) {
		outcome.At = noon.Add(at)
		recorder.Record(outcome)
	}
	of := func(provider string, offer providers.Offer) Outcome {
		offer.Provider = provider
		return OutcomeOf(offer)
	}

	spotA10G := of("aws", a10g("us-east-1b/g5.xlarge", "us-east-1", 1, 380_000, spot))
	for i, boot := range []time.Duration{time.Minute, 90 * time.Second, 2 * time.Minute, 5 * time.Minute} {
		record(time.Duration(i)*time.Second, spotA10G.Bought(nil, time.Second))
		record(boot, spotA10G.CameUp(boot))
	}
	record(4*time.Second, spotA10G.Bought(nil, time.Second))
	record(10*time.Minute, spotA10G.Ended(ResultNeverUp, 10*time.Minute))
	record(time.Hour, spotA10G.Ended(ResultInterrupted, time.Hour))
	record(2*time.Hour, spotA10G.Ended(ResultReleased, 2*time.Hour))
	record(3*time.Hour, spotA10G.Ended(ResultInterrupted, 3*time.Hour))
	record(4*time.Hour, spotA10G.Ended(ResultLost, 4*time.Hour))

	onDemandH100 := of("aws", h100("us-east-1a/p5.48xlarge", "us-east-1", 8, 16_000_000))
	record(0, onDemandH100.Bought(nil, time.Second))
	record(200*time.Second, onDemandH100.CameUp(200*time.Second))

	verda := of("verda", a10g("1A10.8V", "FIN-01", 1, 100_000))
	for range 3 {
		record(0, verda.Bought(nil, time.Second))
		record(10*time.Second, verda.CameUp(10*time.Second))
	}
	recorder.Flush(context.Background())
	market.clock.Advance(6 * time.Hour)
	return market
}

func seconds(value float64) *float64 { return &value }

func TestATenantIsToldHowLongMachinesTookToComeUpAndHowSoonSpotOnesWereTakenBack(t *testing.T) {
	market := boughtMachines(t)

	answer, err := market.reader(notVerda).Outcomes(context.Background(), SeriesRequest{Range: "24h"})
	require.NoError(t, err)
	assert.Equal(t, OutcomeColumns, answer.Cols)
	assert.Equal(t, 30*60, answer.StepSeconds)
	require.Len(t, answer.Series, 1, "too few H100s were bought to say anything of, and verda's are not a tenant's to know of")

	a10g := answer.Series[0]
	assert.Equal(t, "A10G", a10g.GPU)
	assert.Equal(t, OutcomeSummary{
		BootP50:        seconds(90),
		BootP90:        seconds(300),
		PreemptedShare: seconds(0.5),
	}, a10g.Summary, "two machines taken back are too few to say how soon")
	assert.JSONEq(t, jsonOf(t, [][]any{
		{noon.UnixMilli(), 90, 300, nil, nil},
	}), jsonOf(t, a10g.Points), "a step with too few machines in it says nothing")
}

func TestOutcomesAreLeftOutAndGroupedAsPricesAre(t *testing.T) {
	market := boughtMachines(t)
	ask := func(q SeriesRequest) []string {
		q.Range = "24h"
		answer, err := market.reader().Outcomes(context.Background(), q)
		require.NoError(t, err)
		keys := []string{}
		for _, series := range answer.Series {
			keys = append(keys, series.Key)
		}
		return keys
	}

	assert.Equal(t, []string{"on_demand", "spot"}, ask(SeriesRequest{GPU: "A10G"}))
	assert.Equal(t, []string{"A10G"}, ask(SeriesRequest{Pricing: PricingSpot}))
	assert.Equal(t, []string{"A10G|eu-north"}, ask(SeriesRequest{Geos: []string{"eu-north"}, Group: []string{ByGeo}}))
	assert.Equal(t, []string{"all"}, ask(SeriesRequest{GPU: "A10G", Group: []string{}}))
	assert.Equal(t, []string{"A10G|aws", "A10G|verda"}, ask(SeriesRequest{Admin: true, Group: []string{ByProvider}}))

	_, err := market.reader().Outcomes(context.Background(), SeriesRequest{Range: "24h", Group: []string{ByProvider}})
	assert.ErrorIs(t, err, ErrAdminOnly)
}
