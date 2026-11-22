package repository

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAMarketChartIsKeptForAsLongAsItIsGiven(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	require.NoError(t, err)
	charts := NewMarketChartRedisRepository(rdb)
	ctx := context.Background()

	chart, err := charts.GetChart(ctx, "series:v1:24h")
	require.NoError(t, err)
	assert.Nil(t, chart, "none is kept yet")

	require.NoError(t, charts.SetChart(ctx, "series:v1:24h", []byte(`{"series":[]}`), time.Minute))
	chart, err = charts.GetChart(ctx, "series:v1:24h")
	require.NoError(t, err)
	assert.Equal(t, `{"series":[]}`, string(chart))

	ttl, err := rdb.TTL(ctx, "market:chart:series:v1:24h").Result()
	require.NoError(t, err)
	assert.Equal(t, time.Minute, ttl)
}
