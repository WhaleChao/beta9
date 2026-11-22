package apiv1

import (
	"cmp"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/providers/watch"
	"github.com/labstack/echo/v4"
	"github.com/rs/zerolog/log"
)

// MarketGroup answers what the providers sell and what they sold for, from
// the record the market's sampler keeps.
//
// Anyone signed in may ask. What they are told depends on who they are: a
// tenant is told what it would pay, by GPU, part of the world and how the
// machine is sold; a cluster admin is also told who sells it, and what it
// costs.
type MarketGroup struct {
	routerGroup *echo.Group
	reader      *watch.Reader
}

// NewMarketGroup serves the market from reader. A nil reader is a cluster
// that does not sample the market.
func NewMarketGroup(g *echo.Group, reader *watch.Reader) *MarketGroup {
	group := &MarketGroup{routerGroup: g, reader: reader}

	g.GET("/now", auth.WithAuth(group.Now))
	g.GET("/series", auth.WithAuth(group.Series))
	g.GET("/outcomes", auth.WithAuth(group.Outcomes))

	return group
}

// NewPublicMarketGroup serves the market to anyone, signed in or not, for
// pages that show it in public. They are told what a tenant is, of every
// GPU type, and may ask by range and pricing only: those charts are shared
// by every gateway and worked out anew as the market is sampled.
func NewPublicMarketGroup(g *echo.Group, reader *watch.Reader) {
	g.Use(func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(ctx echo.Context) error {
			if reader == nil {
				return NewHTTPError(http.StatusServiceUnavailable, "The market is not sampled on this cluster")
			}
			ctx.Response().Header().Set("Cache-Control", "public, max-age=30")
			return next(ctx)
		}
	})
	g.GET("/now", func(ctx echo.Context) error {
		return marketNowOf(ctx, reader, false)
	})
	g.GET("/series", func(ctx echo.Context) error {
		answer, err := reader.Series(ctx.Request().Context(), publicMarketRequest(ctx))
		return marketAnswer(ctx, answer, err, "what GPUs sold for")
	})
	g.GET("/outcomes", func(ctx echo.Context) error {
		answer, err := reader.Outcomes(ctx.Request().Context(), publicMarketRequest(ctx))
		return marketAnswer(ctx, answer, err, "what became of the machines bought")
	})
}

// publicMarketRequest is what the public may ask: a chart of every GPU type
// over a range, the last day unless given, sold one way or any.
func publicMarketRequest(ctx echo.Context) watch.SeriesRequest {
	return watch.SeriesRequest{
		Range:   cmp.Or(strings.TrimSpace(ctx.QueryParam("range")), "24h"),
		Pricing: strings.ToLower(strings.TrimSpace(ctx.QueryParam("pricing"))),
		Group:   []string{watch.ByGPU},
	}
}

// marketNow is what is for sale, and whether the asker was told as an admin.
type marketNow struct {
	*watch.Now
	Admin bool `json:"admin"`
}

// Now answers what is for sale at present.
func (g *MarketGroup) Now(ctx echo.Context) error {
	if g.reader == nil {
		return NewHTTPError(http.StatusServiceUnavailable, "The market is not sampled on this cluster")
	}
	cc, _ := ctx.(*auth.HttpAuthContext)
	return marketNowOf(ctx, g.reader, isClusterAdmin(authInfoFromContext(cc)))
}

func marketNowOf(ctx echo.Context, reader *watch.Reader, admin bool) error {
	now, err := reader.Now(ctx.Request().Context(), admin)
	if err != nil {
		if IsRequestCanceled(ctx, err) {
			return HTTPClientClosedRequest(ctx)
		}
		log.Error().Err(err).Msg("market: could not read what is for sale")
		return HTTPInternalServerError("Failed to read the market")
	}
	return ctx.JSON(http.StatusOK, marketNow{Now: now, Admin: admin})
}

// Series answers what GPU types sold for over a time.
//
// The question is in the query:
//
//   - gpu: the GPU type. Every one, each its own series, unless given.
//   - gpu_count, geo: the numbers of GPUs and the parts of the world to
//     keep, separated by commas. All of them unless given.
//   - pricing: on_demand, spot or any. Any unless given.
//   - group: what the series are told apart by, of gpu, pricing, geo and
//     gpus, separated by commas, or none for one series of everything. By
//     pricing for one GPU type, and by GPU type for all of them, unless
//     given.
//   - range: 1h, 6h, 24h, 7d, 30d or 90d, ending now, in place of start and
//     end. A tenant's chart of a range is shared by every gateway.
//   - start, end: the time asked about, as RFC 3339 or as milliseconds. The
//     last day unless given.
//
// And for cluster admins only: provider, cloud and region to keep; provider,
// cloud and region to group by; and raw=true for what the providers charge
// in place of what a tenant pays.
func (g *MarketGroup) Series(ctx echo.Context) error {
	return g.answer(ctx, "what GPUs sold for", func(q watch.SeriesRequest) (any, error) {
		return g.reader.Series(ctx.Request().Context(), q)
	})
}

// Outcomes answers what became of the machines bought over a time: how long
// they took to come up, and how often and how soon spot machines were taken
// back. It is asked as Series is.
func (g *MarketGroup) Outcomes(ctx echo.Context) error {
	return g.answer(ctx, "what became of the machines bought", func(q watch.SeriesRequest) (any, error) {
		return g.reader.Outcomes(ctx.Request().Context(), q)
	})
}

func (g *MarketGroup) answer(ctx echo.Context, what string, ask func(watch.SeriesRequest) (any, error)) error {
	if g.reader == nil {
		return NewHTTPError(http.StatusServiceUnavailable, "The market is not sampled on this cluster")
	}
	cc, _ := ctx.(*auth.HttpAuthContext)

	query, err := marketSeriesRequest(ctx)
	if err != nil {
		return HTTPBadRequest(err.Error())
	}
	query.Admin = isClusterAdmin(authInfoFromContext(cc))

	answer, err := ask(query)
	return marketAnswer(ctx, answer, err, what)
}

func marketAnswer(ctx echo.Context, answer any, err error, what string) error {
	var wrong *watch.RequestError
	switch {
	case err == nil:
		return ctx.JSON(http.StatusOK, answer)
	case errors.As(err, &wrong):
		return HTTPBadRequest(wrong.What)
	case errors.Is(err, watch.ErrAdminOnly):
		return HTTPForbidden("Which provider sells what is for cluster admins only")
	case errors.Is(err, watch.ErrNoHistory):
		return NewHTTPError(http.StatusServiceUnavailable, "The market's history is not kept on this cluster")
	case IsRequestCanceled(ctx, err):
		return HTTPClientClosedRequest(ctx)
	}
	log.Error().Err(err).Str("query", ctx.QueryString()).Msgf("market: could not read %s", what)
	return HTTPInternalServerError("Failed to read the market's history")
}

func marketSeriesRequest(ctx echo.Context) (watch.SeriesRequest, error) {
	query := watch.SeriesRequest{
		GPU:       ctx.QueryParam("gpu"),
		Range:     strings.TrimSpace(ctx.QueryParam("range")),
		Geos:      listParam(ctx, "geo"),
		Pricing:   strings.ToLower(strings.TrimSpace(ctx.QueryParam("pricing"))),
		Providers: listParam(ctx, "provider"),
		Clouds:    listParam(ctx, "cloud"),
		Regions:   listParam(ctx, "region"),
	}
	for _, value := range listParam(ctx, "gpu_count") {
		count, err := strconv.ParseUint(value, 10, 32)
		if err != nil || count == 0 {
			return query, &watch.RequestError{What: "gpu_count must be whole numbers, not " + strconv.Quote(value)}
		}
		query.Counts = append(query.Counts, uint32(count))
	}
	if ctx.QueryParams().Has("group") {
		// Grouped by nothing is one series of everything, which is not the
		// same as not saying what to group by.
		query.Group = []string{}
		for _, by := range listParam(ctx, "group") {
			if by != "none" {
				query.Group = append(query.Group, strings.ToLower(by))
			}
		}
	}
	if value := strings.TrimSpace(ctx.QueryParam("raw")); value != "" {
		raw, err := strconv.ParseBool(value)
		if err != nil {
			return query, &watch.RequestError{What: "raw must be true or false, not " + strconv.Quote(value)}
		}
		query.Raw = raw
	}

	var err error
	if query.Start, err = timeParam(ctx, "start"); err != nil {
		return query, err
	}
	if query.End, err = timeParam(ctx, "end"); err != nil {
		return query, err
	}
	return query, nil
}

// listParam reads a parameter that holds several values, given once with
// commas between them or once for each.
func listParam(ctx echo.Context, name string) []string {
	var values []string
	for _, given := range ctx.QueryParams()[name] {
		for _, value := range strings.Split(given, ",") {
			if value = strings.TrimSpace(value); value != "" {
				values = append(values, value)
			}
		}
	}
	return values
}

// timeParam reads a time given as RFC 3339 or as milliseconds since 1970.
// One that is not given is the zero time.
func timeParam(ctx echo.Context, name string) (time.Time, error) {
	value := strings.TrimSpace(ctx.QueryParam(name))
	if value == "" {
		return time.Time{}, nil
	}
	if millis, err := strconv.ParseInt(value, 10, 64); err == nil {
		return time.UnixMilli(millis).UTC(), nil
	}
	at, err := time.Parse(time.RFC3339, value)
	if err != nil {
		return time.Time{}, &watch.RequestError{What: name + " must be a time as RFC 3339 or in milliseconds, not " + strconv.Quote(value)}
	}
	return at.UTC(), nil
}
