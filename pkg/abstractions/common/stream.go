package abstractions

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/network"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	flushLogsTimeout          = 10 * time.Second
	workerAddressPollInterval = time.Second
)

// A stream that tells its client what its container waits for reads that
// after pendingReadInterval, and then after twice as long each time, up to
// maxPendingReadInterval. So a container that a worker takes at once costs no
// read, and one that waits for a machine costs one every few seconds.
//
// The client is told each change, and what has not changed once more every
// pendingRetellInterval: a client that hears nothing cannot tell how long it
// has waited, or decide that it has waited long enough.
//
// Tests shorten these.
var (
	pendingReadInterval    = time.Second
	maxPendingReadInterval = 5 * time.Second
	pendingRetellInterval  = 10 * time.Second
)

// ErrWorkerUnreachable wraps a failure to connect to the worker running a container.
var ErrWorkerUnreachable = errors.New("worker unreachable")

type ContainerStreamOpts struct {
	SendCallback func(o common.OutputMsg) error
	ExitCallback func(exitCode int32) error
	// PendingCallback is optional. It is told what the container waits for
	// until a worker has it.
	PendingCallback func(pending types.Pending) error
	ContainerRepo   repository.ContainerRepository
	Tailscale       *network.Tailscale
	Config          types.AppConfig
	KeyEventManager *common.KeyEventManager
	SyncQueue       chan *pb.SyncContainerWorkspaceRequest
}

func NewContainerStream(opts ContainerStreamOpts) (*ContainerStream, error) {
	return &ContainerStream{
		sendCallback:    opts.SendCallback,
		exitCallback:    opts.ExitCallback,
		pendingCallback: opts.PendingCallback,
		containerRepo:   opts.ContainerRepo,
		tailscale:       opts.Tailscale,
		config:          opts.Config,
		keyEventManager: opts.KeyEventManager,
		syncQueue:       opts.SyncQueue,
	}, nil
}

type ContainerStream struct {
	sendCallback    func(o common.OutputMsg) error
	exitCallback    func(exitCode int32) error
	pendingCallback func(pending types.Pending) error
	containerRepo   repository.ContainerRepository
	tailscale       *network.Tailscale
	config          types.AppConfig
	keyEventManager *common.KeyEventManager
	syncQueue       chan *pb.SyncContainerWorkspaceRequest
}

type containerStreamClient interface {
	StreamLogsWithReady(context.Context, string, chan common.OutputMsg, func()) error
	SyncWorkspace(context.Context, *pb.SyncContainerWorkspaceRequest) (*pb.SyncContainerWorkspaceResponse, error)
}

type containerClientResult struct {
	client containerStreamClient
	err    error
}

func (l *ContainerStream) Stream(ctx context.Context, authInfo *auth.AuthInfo, containerId string) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	outputChan := make(chan common.OutputMsg, 1000)
	keyEventChan := make(chan common.KeyEvent, 1000)
	if err := l.keyEventManager.ListenForKey(ctx, common.RedisKeys.SchedulerContainerExitCode(containerId), keyEventChan); err != nil {
		return err
	}

	clientResult := make(chan containerClientResult, 1)
	go func() {
		hostname, err := l.waitForWorker(ctx, containerId)
		if err != nil {
			clientResult <- containerClientResult{err: err}
			return
		}

		conn, err := network.ConnectToBackend(ctx, hostname, 30*time.Second, l.tailscale, l.config.Tailscale, l.containerRepo)
		if err != nil {
			clientResult <- containerClientResult{err: fmt.Errorf("%w: %w", ErrWorkerUnreachable, err)}
			return
		}

		client, err := common.NewContainerClient(hostname, conn)
		clientResult <- containerClientResult{client: client, err: err}
	}()

	return l.handleStreams(ctx, containerId, outputChan, keyEventChan, clientResult)
}

// notAssigned is how the wait ends for a container that will not get a
// worker: with why the scheduler gave up on it, if it did.
func (l *ContainerStream) notAssigned(containerID string) error {
	if failure, err := l.containerRepo.GetContainerRequestFailure(containerID); err == nil && failure != nil {
		return &ContainerNotScheduledError{Failure: *failure}
	}
	return fmt.Errorf("container stopped before it was assigned to a worker")
}

// waitForWorker waits until a worker has the container, and returns where to
// reach it. A stream with a PendingCallback tells its client what the
// container waits for meanwhile. It also stops waiting for a container that
// is stopped before a worker has it, which waitForWorkerAddress goes on
// waiting for until its repository gives up, minutes later.
func (l *ContainerStream) waitForWorker(ctx context.Context, containerID string) (string, error) {
	if l.pendingCallback == nil {
		return l.waitForWorkerAddress(ctx, containerID)
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	type answer struct {
		hostname string
		err      error
	}
	found := make(chan answer, 1)
	go func() {
		hostname, err := l.waitForWorkerAddress(ctx, containerID)
		found <- answer{hostname, err}
	}()

	// told is what the client was told last, and toldAt when.
	var told types.Pending
	var toldAt time.Time
	tell := func(pending types.Pending) error {
		if pending.Phase == told.Phase && pending.Reason == told.Reason && time.Since(toldAt) < pendingRetellInterval {
			return nil
		}
		told, toldAt = pending, time.Now()
		return l.pendingCallback(pending)
	}

	readIn := pendingReadInterval
	read := time.NewTimer(readIn)
	defer read.Stop()
	for {
		select {
		case a := <-found:
			// A client that was told the container waits is told that a
			// worker has it. From here the container's output says how it
			// gets on.
			if a.err == nil && told.Phase != "" {
				starting := types.ContainerPendingStarting
				if err := tell(types.Pending{Phase: starting, Message: starting.Message("")}); err != nil {
					return "", err
				}
			}
			return a.hostname, a.err
		case <-read.C:
			state, err := l.containerRepo.GetContainerState(containerID)
			var notFound *types.ErrContainerStateNotFound
			switch {
			case errors.As(err, &notFound), err == nil && state.Status == types.ContainerStatusStopping:
				// The container was stopped, or the scheduler gave up on it.
				select {
				case a := <-found:
					return a.hostname, a.err
				default:
					return "", l.notAssigned(containerID)
				}
			case err == nil:
				if pending := state.Pending(time.Now()); pending != nil {
					if err := tell(*pending); err != nil {
						return "", err
					}
				}
			}
			// A state that cannot be read says nothing.
			readIn = min(2*readIn, maxPendingReadInterval)
			read.Reset(readIn)
		}
	}
}

func (l *ContainerStream) waitForWorkerAddress(ctx context.Context, containerID string) (string, error) {
	ticker := time.NewTicker(workerAddressPollInterval)
	defer ticker.Stop()

	for {
		hostname, err := l.containerRepo.GetWorkerAddress(ctx, containerID)
		if err == nil && hostname != "" {
			return hostname, nil
		}

		state, stateErr := l.containerRepo.GetContainerState(containerID)
		if stateErr != nil || state == nil || state.Status == types.ContainerStatusStopping {
			return "", l.notAssigned(containerID)
		}

		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-ticker.C:
		}
	}
}

func (l *ContainerStream) handleStreams(
	ctx context.Context,
	containerId string,
	outputChan chan common.OutputMsg,
	keyEventChan chan common.KeyEvent,
	clientResult chan containerClientResult,
) error {
	var lastMessage common.OutputMsg
	var client containerStreamClient
	var exitEvents <-chan common.KeyEvent
	var logStreamReady <-chan struct{}
	var syncQueue <-chan *pb.SyncContainerWorkspaceRequest
	logErrors := make(chan error, 1)
	logsFinished := false

_stream:
	for {
		select {
		case result := <-clientResult:
			clientResult = nil
			if result.err != nil {
				return result.err
			}
			client = result.client
			syncQueue = l.syncQueue
			ready := make(chan struct{})
			logStreamReady = ready
			go func() {
				readyCallback := func() { close(ready) }
				logErrors <- client.StreamLogsWithReady(ctx, containerId, outputChan, readyCallback)
			}()
		case err := <-logErrors:
			logErrors = nil
			if err != nil {
				return err
			}
			logsFinished = true
			// EOF is durable evidence that this log buffer closed. A lost exit
			// notification must not leave an invocation waiting forever.
			for len(outputChan) > 0 {
				if err := l.sendCallback(<-outputChan); err != nil {
					return err
				}
			}
			exitCode, err := l.containerRepo.GetContainerExitCode(containerId)
			if err != nil {
				return status.Error(codes.Unavailable, "container exit status is not yet available")
			}
			return l.exitCallback(int32(exitCode))
		case <-logStreamReady:
			logStreamReady = nil
			exitEvents = keyEventChan
		case req := <-syncQueue:
			_, err := client.SyncWorkspace(ctx, req)
			if err != nil {
				continue
			}
		case o := <-outputChan:
			if err := l.sendCallback(o); err != nil {
				return err
			}

			if o.Done {
				lastMessage = o
				break _stream
			}
		case <-exitEvents:
			exitCode, err := l.containerRepo.GetContainerExitCode(containerId)
			if err != nil {
				exitCode = -1
			}

			// Drain through EOF before delivering the result. A completed
			// invocation may have accumulated logs during a gateway outage.
			flushLogsTimer := time.NewTimer(flushLogsTimeout)
			defer flushLogsTimer.Stop()

			for !logsFinished || len(outputChan) > 0 {
				select {
				case o := <-outputChan:
					if err := l.sendCallback(o); err != nil {
						return err
					}
				case err := <-logErrors:
					logErrors = nil
					if err != nil {
						return err
					}
					logsFinished = true
				case <-flushLogsTimer.C:
					return status.Error(codes.Unavailable, "container logs are still flushing")
				case <-ctx.Done():
					return ctx.Err()
				}
			}

			if err := l.exitCallback(int32(exitCode)); err != nil {
				return err
			}
			return nil

		case <-ctx.Done():
			return ctx.Err()
		}
	}

	if !lastMessage.Success {
		return errors.New("failed")
	}

	return nil
}
