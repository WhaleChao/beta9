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

func TestPreBootInterruptionsDoNotSkewPreemptedShareOrTimeAfterSpinup(t *testing.T) {
	market := newTestMarket(t, providers.NewFake("nebius", "test"))
	recorder := NewRecorder(market.log, StreamsOf(market.config), market.clock)
	base := OutcomeOf(h100("1gpu-h100", "eu-north1", 1, 1_000_000, spot))
	base.Provider = "nebius"
	base.Source = SourcePool
	base.Pool = "spot-h100-capacity"

	record := func(at time.Duration, o Outcome) {
		o.At = noon.Add(at)
		recorder.Record(o)
	}

	// Ten machines taken back 15s into boot before ever becoming active.
	for i := range 10 {
		o := base
		o.MachineID = "preboot-" + string(rune('a'+i))
		record(time.Duration(i)*time.Second, o.Bought(nil, time.Second))
		record(time.Duration(i)*time.Second+15*time.Second, o.EndedAfter(ResultInterrupted, 15*time.Second, 0, 0, false))
	}

	// Four machines that booted in 3m: three preempted after 20m, 40m, 60m of
	// running (total life 23m, 43m, 63m), and one released cleanly after 2h.
	for i, ran := range []time.Duration{20 * time.Minute, 40 * time.Minute, 60 * time.Minute} {
		o := base
		o.MachineID = "ran-preempt-" + string(rune('a'+i))
		record(time.Minute, o.CameUp(3*time.Minute))
		record(3*time.Minute+ran, o.EndedAfter(ResultInterrupted, 3*time.Minute+ran, 3*time.Minute, ran, true))
	}
	released := base
	released.MachineID = "ran-released"
	record(time.Minute, released.CameUp(3*time.Minute))
	record(2*time.Hour+3*time.Minute, released.EndedAfter(ResultReleased, 2*time.Hour+3*time.Minute, 3*time.Minute, 2*time.Hour, true))

	recorder.Flush(context.Background())
	market.clock.Advance(4 * time.Hour)

	answer, err := market.reader().Outcomes(context.Background(), SeriesRequest{Range: "24h", Admin: true, Group: []string{ByProvider}})
	require.NoError(t, err)
	require.Len(t, answer.Series, 1)
	assert.Equal(t, OutcomeSummary{
		BootP50:           seconds(180),
		BootP90:           seconds(180),
		PreemptedShare:    seconds(0.75), // 3 of 4 booted machines, not 13 of 14
		PreemptedAfterP50: seconds(2400), // 40m running after spin-up, not 15s and not 43m
	}, answer.Series[0].Summary)

	// Verify new wire fields round-trip cleanly through S2 storage.
	readBack, err := recorder.Read(context.Background(), noon.Add(-time.Minute), noon.Add(4*time.Hour))
	require.NoError(t, err)
	var sawPreBoot, sawBooted bool
	for _, o := range readBack {
		if o.MachineID == "preboot-a" && o.Event == OutcomeEnd {
			sawPreBoot = true
			require.Equal(t, "spot-h100-capacity", o.Pool)
			require.NotNil(t, o.Booted)
			require.False(t, *o.Booted)
			require.Zero(t, o.Boot)
			require.Zero(t, o.Ran)
		}
		if o.MachineID == "ran-preempt-b" && o.Event == OutcomeEnd {
			sawBooted = true
			require.NotNil(t, o.Booted)
			require.True(t, *o.Booted)
			require.Equal(t, 3*time.Minute, o.Boot)
			require.Equal(t, 40*time.Minute, o.Ran)
			require.Equal(t, 43*time.Minute, o.Took)
		}
	}
	require.True(t, sawPreBoot)
	require.True(t, sawBooted)
}
