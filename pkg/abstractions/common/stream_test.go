package abstractions

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type testContainerStreamClient struct {
	started  chan struct{}
	attach   chan struct{}
	log      common.OutputMsg
	delay    time.Duration
	trailing []common.OutputMsg
}

type pendingContainerStreamRepo struct {
	repository.ContainerRepository
	ready     chan struct{}
	attempted chan struct{}
}

func (r *pendingContainerStreamRepo) GetWorkerAddress(context.Context, string) (string, error) {
	select {
	case <-r.attempted:
	default:
		close(r.attempted)
	}
	select {
	case <-r.ready:
		return "worker.internal", nil
	default:
		return "", errors.New("worker address not assigned")
	}
}

func (r *pendingContainerStreamRepo) GetContainerState(string) (*types.ContainerState, error) {
	return &types.ContainerState{Status: types.ContainerStatusPending}, nil
}

func TestContainerStreamWaitsForPendingContainerWorker(t *testing.T) {
	repo := &pendingContainerStreamRepo{ready: make(chan struct{}), attempted: make(chan struct{})}
	stream := &ContainerStream{containerRepo: repo}
	type addressResult struct {
		host string
		err  error
	}
	result := make(chan addressResult, 1)

	go func() {
		host, err := stream.waitForWorkerAddress(context.Background(), "container-1")
		result <- addressResult{host: host, err: err}
	}()

	<-repo.attempted
	select {
	case <-result:
		t.Fatal("returned before the pending container received a worker")
	case <-time.After(20 * time.Millisecond):
	}

	close(repo.ready)
	got := <-result
	require.NoError(t, got.err)
	require.Equal(t, "worker.internal", got.host)
}

// pendingStream is a stream that waits for a worker to take a container, on
// the repository the gateway uses: asking it for a worker's address does not
// return until there is one.
type pendingStream struct {
	t    *testing.T
	repo repository.ContainerRepository
	// told receives what the client is told.
	told chan types.Pending
	// done receives how the wait ended.
	done chan pendingStreamResult
}

type pendingStreamResult struct {
	host string
	err  error
}

const pendingStreamContainer = "container-1"

// waitForPendingContainer starts the wait for a container that the scheduler
// has not placed. callback, if given, stands in for the client.
func waitForPendingContainer(t *testing.T, callback func(types.Pending) error) *pendingStream {
	t.Helper()
	read, longest := pendingReadInterval, maxPendingReadInterval
	pendingReadInterval, maxPendingReadInterval = 5*time.Millisecond, 10*time.Millisecond
	t.Cleanup(func() { pendingReadInterval, maxPendingReadInterval = read, longest })

	rdb, err := repository.NewRedisClientForTest()
	require.NoError(t, err)
	s := &pendingStream{
		t:    t,
		repo: repository.NewContainerRedisRepositoryForTest(rdb),
		told: make(chan types.Pending, 100),
		done: make(chan pendingStreamResult, 1),
	}
	require.NoError(t, s.repo.SetContainerState(pendingStreamContainer, &types.ContainerState{
		ContainerId: pendingStreamContainer,
		StubId:      "stub-1",
		Status:      types.ContainerStatusPending,
		ScheduledAt: time.Now().Add(-90 * time.Second).Unix(),
	}))

	if callback == nil {
		callback = func(pending types.Pending) error {
			s.told <- pending
			return nil
		}
	}
	stream := &ContainerStream{containerRepo: s.repo, pendingCallback: callback}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() {
		host, err := stream.waitForWorker(ctx, pendingStreamContainer)
		s.done <- pendingStreamResult{host: host, err: err}
	}()
	return s
}

// waitsFor has the scheduler store what the container waits for, and
// returns once the client has been told it.
func (s *pendingStream) waitsFor(phase types.ContainerPendingPhase, reason types.ContainerPendingReason, since time.Time) types.Pending {
	s.t.Helper()
	stored, err := s.repo.SetContainerPending(pendingStreamContainer, phase, reason, since)
	require.NoError(s.t, err)
	require.True(s.t, stored)
	for {
		if told := s.nextTold(); told.Phase == phase && told.Reason == reason {
			return told
		}
	}
}

// nextTold is the next thing the client is told.
func (s *pendingStream) nextTold() types.Pending {
	s.t.Helper()
	select {
	case pending := <-s.told:
		return pending
	case result := <-s.done:
		s.t.Fatalf("the wait ended before the client was told anything more: %+v", result)
	case <-time.After(5 * time.Second):
		s.t.Fatal("the client was not told what the container waits for")
	}
	return types.Pending{}
}

// toldNothingMore checks that the client is told nothing over several reads
// of the container's state.
func (s *pendingStream) toldNothingMore() {
	s.t.Helper()
	select {
	case pending := <-s.told:
		s.t.Fatalf("the client was told %+v again", pending)
	case <-time.After(10 * maxPendingReadInterval):
	}
}

func (s *pendingStream) result() pendingStreamResult {
	s.t.Helper()
	select {
	case result := <-s.done:
		return result
	case <-time.After(5 * time.Second):
		s.t.Fatal("the wait for a worker did not end")
	}
	return pendingStreamResult{}
}

func TestContainerStreamTellsWhatAPendingContainerWaitsFor(t *testing.T) {
	s := waitForPendingContainer(t, nil)

	// The scheduler has said nothing yet.
	queued := s.nextTold()
	require.Equal(t, types.ContainerPendingQueued, queued.Phase)
	require.InDelta(t, 90, queued.WaitedSeconds, 2)

	waiting := s.waitsFor(types.ContainerPendingWaitingForCapacity, types.ContainerPendingNoCapacity, time.Now().Add(-40*time.Second))
	require.InDelta(t, 40, waiting.WaitedSeconds, 2)
	require.Equal(t, "No capacity available right now, waiting for some", waiting.Message)
	// What has not changed is not told again.
	s.toldNothingMore()

	s.waitsFor(types.ContainerPendingProvisioning, types.ContainerPendingWorkerStarting, time.Now())
	s.waitsFor(types.ContainerPendingProvisioning, types.ContainerPendingBackupWorkerStarting, time.Now())

	// A worker has it: whatever it waited for, it starts now.
	require.NoError(t, s.repo.SetWorkerAddress(pendingStreamContainer, "worker.internal"))
	require.Equal(t, types.Pending{Phase: types.ContainerPendingStarting, Message: "Placed on a machine, starting the container"}, s.nextTold())
	require.Equal(t, pendingStreamResult{host: "worker.internal"}, s.result())
	s.toldNothingMore()
}

// A client hears what its container waits for again while that stays the
// same, with how long it has been: that is how it knows the wait goes on.
func TestContainerStreamTellsAgainWhatAContainerStillWaitsFor(t *testing.T) {
	retell := pendingRetellInterval
	pendingRetellInterval = 50 * time.Millisecond
	t.Cleanup(func() { pendingRetellInterval = retell })

	s := waitForPendingContainer(t, nil)
	s.waitsFor(types.ContainerPendingWaitingForCapacity, types.ContainerPendingNoCapacity, time.Now().Add(-40*time.Second))

	first := time.Now()
	for range 3 {
		again := s.nextTold()
		require.Equal(t, types.ContainerPendingNoCapacity, again.Reason)
		require.InDelta(t, 40, again.WaitedSeconds, 2)
	}
	// Three more in no less than three intervals, less the one under way.
	require.GreaterOrEqual(t, time.Since(first), 2*pendingRetellInterval)
}

// A container's state is read only to tell a client what the container
// waits for: not for a container that a worker takes at once, and not for a
// stream with nobody to tell.
func TestContainerStreamReadsNoStateItHasNoUseFor(t *testing.T) {
	read, longest := pendingReadInterval, maxPendingReadInterval
	pendingReadInterval, maxPendingReadInterval = 5*time.Millisecond, 10*time.Millisecond
	t.Cleanup(func() { pendingReadInterval, maxPendingReadInterval = read, longest })

	for name, placed := range map[string]bool{"a worker has the container": true, "nobody to tell": false} {
		t.Run(name, func(t *testing.T) {
			rdb, counts, err := repository.NewRedisClientForTestWithCounts()
			require.NoError(t, err)
			repo := repository.NewContainerRedisRepositoryForTest(rdb)
			stream := &ContainerStream{containerRepo: repo}
			if placed {
				require.NoError(t, repo.SetWorkerAddress(pendingStreamContainer, "worker.internal"))
				stream.pendingCallback = func(types.Pending) error {
					t.Error("told what a container waits for when a worker already has it")
					return nil
				}
			} else {
				require.NoError(t, repo.SetContainerState(pendingStreamContainer, &types.ContainerState{ContainerId: pendingStreamContainer, Status: types.ContainerStatusPending}))
				time.AfterFunc(10*maxPendingReadInterval, func() { repo.SetWorkerAddress(pendingStreamContainer, "worker.internal") })
			}

			counts.Start()
			host, err := stream.waitForWorker(context.Background(), pendingStreamContainer)
			commands, _ := counts.Stop()
			require.NoError(t, err)
			require.Equal(t, "worker.internal", host)
			require.NotZero(t, commands["GET"])
			require.Zero(t, commands["HGETALL"])
		})
	}
}

// The wait ends when the container will not get a worker, and says why when
// the scheduler kept why.
func TestContainerStreamStopsWaitingForAContainerThatWillNotStart(t *testing.T) {
	const stopped = "container stopped before it was assigned to a worker"
	failure := types.ContainerRequestFailure{Reason: types.ContainerSchedulingFailureWorkerCapacityTimeout, Gpus: []string{"A10G"}, WaitedSeconds: 1200}

	for name, tc := range map[string]struct {
		end  func(t *testing.T, repo repository.ContainerRepository)
		want string
		code codes.Code
	}{
		"the scheduler gives up on it": {
			end: func(t *testing.T, repo repository.ContainerRepository) {
				require.NoError(t, repo.SetContainerRequestFailure(pendingStreamContainer, failure))
				require.NoError(t, repo.DeleteContainerState(pendingStreamContainer))
				require.NoError(t, repo.SetContainerRequestStatus(pendingStreamContainer, types.ContainerRequestStatusFailed))
			},
			want: "No A10G capacity became available in 20 minutes.",
			code: codes.ResourceExhausted,
		},
		"its request failed with no reason kept": {
			end: func(t *testing.T, repo repository.ContainerRepository) {
				require.NoError(t, repo.DeleteContainerState(pendingStreamContainer))
				require.NoError(t, repo.SetContainerRequestStatus(pendingStreamContainer, types.ContainerRequestStatusFailed))
			},
			want: stopped,
			code: codes.Unknown,
		},
		// Nothing tells the repository that a container was stopped, so its
		// answer would come only when it stops waiting itself, minutes later.
		"it is stopped: its state says it is stopping": {
			end: func(t *testing.T, repo repository.ContainerRepository) {
				marked, err := repo.MarkPendingContainerStoppingIfUnassigned(pendingStreamContainer, 60, 0)
				require.NoError(t, err)
				require.True(t, marked)
			},
			want: stopped,
			code: codes.Unknown,
		},
		"it is stopped: its state is removed": {
			end: func(t *testing.T, repo repository.ContainerRepository) {
				require.NoError(t, repo.DeleteContainerState(pendingStreamContainer))
			},
			want: stopped,
			code: codes.Unknown,
		},
	} {
		t.Run(name, func(t *testing.T) {
			s := waitForPendingContainer(t, nil)
			s.waitsFor(types.ContainerPendingWaitingForCapacity, types.ContainerPendingNoCapacity, time.Now())

			tc.end(t, s.repo)

			err := s.result().err
			require.EqualError(t, err, tc.want)
			require.Equal(t, tc.code, status.Code(err))
			var notScheduled *ContainerNotScheduledError
			if errors.As(err, &notScheduled) {
				require.Equal(t, failure, notScheduled.Failure)
			} else {
				require.Equal(t, stopped, tc.want)
			}
		})
	}

	// A client that has gone ends the wait too.
	gone := errors.New("client went away")
	require.ErrorIs(t, waitForPendingContainer(t, func(types.Pending) error { return gone }).result().err, gone)
}

func (c *testContainerStreamClient) StreamLogsWithReady(_ context.Context, _ string, output chan common.OutputMsg, ready func()) error {
	close(c.started)
	<-c.attach
	output <- c.log
	ready()
	time.Sleep(c.delay)
	for _, message := range c.trailing {
		output <- message
	}
	return nil
}

func (c *testContainerStreamClient) SyncWorkspace(context.Context, *pb.SyncContainerWorkspaceRequest) (*pb.SyncContainerWorkspaceResponse, error) {
	return &pb.SyncContainerWorkspaceResponse{}, nil
}

type testStreamContainerRepo struct {
	repository.ContainerRepository
	exitCode int
}

func (r testStreamContainerRepo) GetContainerExitCode(string) (int, error) {
	return r.exitCode, nil
}

func TestContainerStreamWaitsForLogAttachmentBeforeExit(t *testing.T) {
	output := make(chan common.OutputMsg, 1)
	exitEvents := make(chan common.KeyEvent, 1)
	clientResults := make(chan containerClientResult, 1)
	started := make(chan struct{})
	attach := make(chan struct{})
	exited := make(chan int32, 1)

	stream := &ContainerStream{
		sendCallback: func(message common.OutputMsg) error {
			output <- message
			return nil
		},
		exitCallback: func(exitCode int32) error {
			exited <- exitCode
			return nil
		},
		containerRepo: testStreamContainerRepo{exitCode: 7},
	}
	clientResults <- containerClientResult{client: &testContainerStreamClient{
		started: started,
		attach:  attach,
		log:     common.OutputMsg{Msg: "buffered output"},
	}}
	exitEvents <- common.KeyEvent{}

	done := make(chan error, 1)
	go func() {
		done <- stream.handleStreams(context.Background(), "container-1", make(chan common.OutputMsg, 1), exitEvents, clientResults)
	}()

	<-started
	select {
	case <-exited:
		t.Fatal("processed exit before log attachment")
	case <-time.After(20 * time.Millisecond):
	}

	close(attach)
	require.Equal(t, "buffered output", (<-output).Msg)
	require.Equal(t, int32(7), <-exited)
	require.NoError(t, <-done)
}

func TestContainerStreamDrainsBacklogThroughEOF(t *testing.T) {
	attach := make(chan struct{})
	close(attach)
	clients := make(chan containerClientResult, 1)
	clients <- containerClientResult{client: &testContainerStreamClient{
		started: make(chan struct{}), attach: attach, log: common.OutputMsg{Msg: "first"},
		delay: 750 * time.Millisecond, trailing: []common.OutputMsg{{Msg: "last"}},
	}}
	events := make(chan common.KeyEvent, 1)
	events <- common.KeyEvent{}
	var output []string
	exited := false
	stream := &ContainerStream{
		containerRepo: testStreamContainerRepo{},
		sendCallback:  func(message common.OutputMsg) error { output = append(output, message.Msg); return nil },
		exitCallback:  func(int32) error { exited = true; require.Equal(t, []string{"first", "last"}, output); return nil },
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	require.NoError(t, stream.handleStreams(ctx, "container-1", make(chan common.OutputMsg, 1000), events, clients))
	require.True(t, exited)
}

func TestContainerStreamEOFRecoversLostExitNotification(t *testing.T) {
	attach := make(chan struct{})
	close(attach)
	clients := make(chan containerClientResult, 1)
	clients <- containerClientResult{client: &testContainerStreamClient{
		started: make(chan struct{}), attach: attach, log: common.OutputMsg{Msg: "first"},
		trailing: []common.OutputMsg{{Msg: "last"}},
	}}
	var output []string
	exited := false
	stream := &ContainerStream{
		containerRepo: testStreamContainerRepo{exitCode: int(types.ContainerExitCodeUnknownError)},
		sendCallback:  func(message common.OutputMsg) error { output = append(output, message.Msg); return nil },
		exitCallback: func(code int32) error {
			exited = true
			require.EqualValues(t, types.ContainerExitCodeUnknownError, code)
			require.Equal(t, []string{"first", "last"}, output)
			return nil
		},
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, stream.handleStreams(ctx, "container", make(chan common.OutputMsg, 1000), make(chan common.KeyEvent), clients))
	require.True(t, exited)
}
