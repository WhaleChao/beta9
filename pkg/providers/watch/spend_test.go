package watch

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/stretchr/testify/require"
)

func TestSpendRoundTripAndStreamRecording(t *testing.T) {
	require.Zero(t, Streams{SnapshotsFor: time.Hour, SamplesFor: time.Hour}.Spend().Keep, "spend is kept for good, whatever the samples are kept for")

	memLog := NewMemoryLog()
	clock := common.NewFakeClock(noon)
	recorder := NewRecorder(memLog, Streams{}, clock)

	poolOutcome := OutcomeOf(h100("1H100.80S", "FIN-01", 1, 2_680_000, spot))
	poolOutcome.Provider = "verda"
	poolOutcome.Cloud = "verda"
	poolOutcome.Source = SourcePool
	poolOutcome.Pool = "spot-h100-capacity"
	poolOutcome.MachineID = "machine-1"

	from := noon
	to := noon.Add(time.Minute)
	poolSpend := SpendOf(poolOutcome, from, to, 44_667, 0, true)

	resOutcome := poolOutcome
	resOutcome.Source = SourceReservation
	resOutcome.Pool = "tenant-private-pool"
	resOutcome.MachineID = "machine-2"
	resSpend := SpendOf(resOutcome, from, to, 44_667, 49_133, true)
	require.Empty(t, resSpend.Pool, "reservation spend omits tenant pool names")

	recorder.RecordSpend(poolSpend)
	recorder.RecordSpend(resSpend)
	recorder.FlushSpend(context.Background())

	raw := memLog.All("spend")
	require.Len(t, raw, 2)

	// Verify wire JSON fields directly.
	var wire map[string]any
	require.NoError(t, json.Unmarshal(raw[0].Body, &wire))
	require.Equal(t, float64(recordVersion), wire["v"])
	require.Equal(t, float64(to.UnixMilli()), wire["ts"])
	require.Equal(t, float64(from.UnixMilli()), wire["from_ts"])
	require.Equal(t, float64(time.Minute.Milliseconds()), wire["duration_ms"])
	require.Equal(t, "spot-h100-capacity", wire["pool"])
	require.Equal(t, "machine-1", wire["machine_id"])
	require.Equal(t, true, wire["booted"])

	got, err := recorder.ReadSpend(context.Background(), noon.Add(-time.Minute), noon.Add(2*time.Minute))
	require.NoError(t, err)
	require.Equal(t, []Spend{poolSpend, resSpend}, got)
}
