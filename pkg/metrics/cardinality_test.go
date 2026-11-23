package metrics

import (
	"bytes"
	"slices"
	"strings"
	"testing"

	vmetrics "github.com/VictoriaMetrics/metrics"
	"github.com/beam-cloud/beta9/pkg/types"
)

func TestSchedulerMetricsAggregateByResources(t *testing.T) {
	// The recorders use the global registry; remove only metrics created here.
	before := vmetrics.ListMetricNames()
	t.Cleanup(func() {
		for _, name := range vmetrics.ListMetricNames() {
			if !slices.Contains(before, name) {
				vmetrics.UnregisterMetric(name)
			}
		}
	})

	requests := []*types.ContainerRequest{
		{ContainerId: "first", Cpu: 1000, Memory: 1024},
		{ContainerId: "second", Cpu: 1000, Memory: 1024, RetryCount: 1},
		{ContainerId: "third", Cpu: 2000, Memory: 2048, Gpu: "A10G", GpuCount: 1, GpuRequest: []string{"A10G"}},
	}
	for _, request := range requests {
		RecordRequestRetry(request)
		RecordRequestScheduleFailure(request)
	}

	var output bytes.Buffer
	vmetrics.GetDefaultSet().WritePrometheus(&output)
	lines := strings.Split(output.String(), "\n")
	for name, want := range map[string][]string{
		metricRequestRetries: {
			`scheduler_request_retries{gpu="none",requested_gpus="",gpu_count="0",cpu="1000",memory="1024"} 2`,
			`scheduler_request_retries{gpu="A10G",requested_gpus="A10G",gpu_count="1",cpu="2000",memory="2048"} 1`,
		},
		metricRequestScheduleFailure: {
			`scheduler_request_schedule_failure{gpu="none",requested_gpus="",gpu_count="0",cpu="1000",memory="1024"} 2`,
			`scheduler_request_schedule_failure{gpu="A10G",requested_gpus="A10G",gpu_count="1",cpu="2000",memory="2048"} 1`,
		},
	} {
		var got []string
		for _, line := range lines {
			if strings.HasPrefix(line, name+"{") {
				got = append(got, line)
			}
		}
		slices.Sort(got)
		slices.Sort(want)
		if !slices.Equal(got, want) {
			t.Errorf("%s: got %v; want %v", name, got, want)
		}
	}
}
