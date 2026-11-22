package scheduler

import (
	"testing"
	"time"

	repo "github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
)

// What a waiting request says it waits for, where the simulator's scenarios
// (sim/pending_test.go) do not reach: a pinned machine, the Spot CPU tier,
// and how often it is stored.

// countPendingStores counts the times a container's pending phase is stored.
type countPendingStores struct {
	repo.ContainerRepository
	n int
}

func (c *countPendingStores) SetContainerPending(containerId string, phase types.ContainerPendingPhase, reason types.ContainerPendingReason, since time.Time) (bool, error) {
	c.n++
	return c.ContainerRepository.SetContainerPending(containerId, phase, reason, since)
}

func pendingReason(t *testing.T, s *Scheduler, containerID string) types.ContainerPendingReason {
	t.Helper()
	state, err := s.containerRepo.GetContainerState(containerID)
	require.NoError(t, err)
	require.Equal(t, types.ContainerPendingWaitingForCapacity, state.PendingPhase)
	return state.PendingReason
}

// A request that may run on one machine only waits for room there, and one
// for the Spot CPU tier for capacity nothing else is using. Neither has a
// pool add a worker, and each says which it is.
func TestRequestThatWaitsForRoomSaysWhichRoom(t *testing.T) {
	scheduler, err := NewSchedulerForTest()
	require.NoError(t, err)
	scheduler.workerPoolManager.SetPool("shared", sharedPool, &LocalWorkerPoolControllerForTest{ctx: scheduler.ctx, name: "shared", config: scheduler.config, workerRepo: scheduler.workerRepo})

	pinned := &types.ContainerRequest{Cpu: 1000, Memory: 1024, MachineId: "machine-1", Timestamp: time.Now()}
	spot := spotRequest()
	spot.Timestamp = time.Now()
	setPendingSchedulerRequests(t, scheduler, pinned, spot)

	newSchedulingAttempt(scheduler, pinned, nil).run()
	newSchedulingAttempt(scheduler, spot, nil).run()

	require.Equal(t, types.ContainerPendingMachineBusy, pendingReason(t, scheduler, pinned.ContainerId))
	require.Equal(t, types.ContainerPendingNoSpareCapacity, pendingReason(t, scheduler, spot.ContainerId))
	require.Equal(t, int64(2), scheduler.requestBacklog.Len())
}

// A waiting request passes several times a second. It stores what it waits
// for when that changes, and not on every pass.
func TestWaitingRequestStoresWhatItWaitsForOnlyWhenItChanges(t *testing.T) {
	scheduler, err := NewSchedulerForTest()
	require.NoError(t, err)
	stores := &countPendingStores{ContainerRepository: scheduler.containerRepo}
	scheduler.containerRepo = stores
	request := &types.ContainerRequest{Cpu: 1000, Memory: 1024, MachineId: "machine-1", Timestamp: time.Now()}
	setPendingSchedulerRequests(t, scheduler, request)

	for range 5 {
		newSchedulingAttempt(scheduler, request, nil).run()
	}
	require.Equal(t, 1, stores.n)

	attempt := newSchedulingAttempt(scheduler, request, nil)
	attempt.waitsFor(types.ContainerPendingWaitingForCapacity, types.ContainerPendingNoCapacity)
	attempt.waitsFor(types.ContainerPendingWaitingForCapacity, types.ContainerPendingNoCapacity)
	require.Equal(t, 2, stores.n)

	// A request that a worker had and gave back is stamped anew. It says
	// what it waits for again, and since when, though that is the same.
	back := *scheduler.prepareWorkerRequest(&types.Worker{Id: "worker-1"}, request)
	back.Timestamp = back.Timestamp.Add(time.Second)
	attempt = newSchedulingAttempt(scheduler, &back, nil)
	attempt.waitsFor(types.ContainerPendingWaitingForCapacity, types.ContainerPendingNoCapacity)
	attempt.waitsFor(types.ContainerPendingWaitingForCapacity, types.ContainerPendingNoCapacity)
	require.Equal(t, 3, stores.n)
}

func TestRouteBackup(t *testing.T) {
	pool := func(name, gpu string, priority int32, alsoServes ...string) *WorkerPool {
		return &WorkerPool{Name: name, Config: types.WorkerPoolConfig{GPUType: gpu, Priority: priority, AlsoServes: alsoServes}}
	}
	r := newRoute(&types.ContainerRequest{Gpu: "A10G", GpuCount: 1}, []*WorkerPool{
		pool("a10g-a", "A10G", 10),
		pool("a10g-b", "A10G", 10),
		pool("a10g-low", "A10G", 1),
		pool("l40s", "L40S", 50, "A10G"),
		pool("t4", "T4", 50),
	}, "")

	require.False(t, r.backup("a10g-a"))
	require.False(t, r.backup("a10g-b"), "differs from the first pool by name alone")
	require.True(t, r.backup("a10g-low"), "tried after the pools of a higher priority")
	require.True(t, r.backup("l40s"), "another GPU type that also serves the request")
	require.False(t, r.backup("t4"), "not in the route")
	require.False(t, route{}.backup("a10g-a"))
}
