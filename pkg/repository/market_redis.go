package repository

import (
	"context"
	"errors"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/providers/watch"
	redis "github.com/redis/go-redis/v9"
)

// MarketChartRedisRepository keeps the market's charts in Redis, where every
// gateway is given them.
type MarketChartRedisRepository struct {
	rdb *common.RedisClient
}

var _ watch.ChartCache = (*MarketChartRedisRepository)(nil)

func NewMarketChartRedisRepository(rdb *common.RedisClient) *MarketChartRedisRepository {
	return &MarketChartRedisRepository{rdb: rdb}
}

func (r *MarketChartRedisRepository) GetChart(ctx context.Context, name string) ([]byte, error) {
	chart, err := r.rdb.Get(ctx, common.RedisKeys.MarketChart(name)).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	return chart, err
}

func (r *MarketChartRedisRepository) SetChart(ctx context.Context, name string, chart []byte, ttl time.Duration) error {
	return r.rdb.Set(ctx, common.RedisKeys.MarketChart(name), chart, ttl).Err()
}
