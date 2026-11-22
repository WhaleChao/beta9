package types

import (
	"encoding/json"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCapacityMemoryReservesNativeVMOverhead(t *testing.T) {
	for _, tc := range []struct {
		memory, expected int64
		useVM            bool
	}{
		{0, 0, true}, {-1, -1, true}, {1, 2, false}, {1024, 1280, false},
		{128, 896, true}, {512, 1280, true}, {1025, 1794, true}, {4096, 5120, true},
	} {
		request := &ContainerRequest{Memory: tc.memory, UseVM: tc.useVM}
		require.Equal(t, tc.expected, request.CapacityMemory())
		require.Equal(t, tc.memory, request.Memory, "billing and the application budget remain unchanged")
	}
}

func TestSandboxPrivateWorkerRuntimeTokenOptIn(t *testing.T) {
	for _, kind := range []StubType{StubType(StubTypeSandbox), StubType(StubTypeVM)} {
		request := &ContainerRequest{Stub: StubWithRelated{Stub: Stub{Type: kind, Config: `{}`}}, Env: []string{"SAFE=value"}}
		require.False(t, request.PrivateWorkerRequest().RuntimeTokenRequired)
		request.Env = append(request.Env, "BETA9_TOKEN=explicit-secret")
		request.Stub.Config = `{"secrets":[{"name":"BETA9_TOKEN"}]}`
		private := request.PrivateWorkerRequest()
		require.False(t, private.RuntimeTokenRequired)
		require.Equal(t, []string{"BETA9_TOKEN"}, private.RuntimeSecretNames)
		require.Equal(t, []string{"SAFE=value"}, private.Env)
	}
}

func TestKeylessCheckpointMayEvict(t *testing.T) {
	request := &ContainerRequest{Gpu: "RTX4090",
		Checkpoint: &Checkpoint{Status: string(CheckpointStatusAvailable), Runtime: "gvisor"}}
	require.False(t, request.MayEvict())
	request.Checkpoint.CompatibilityKey = "v1:host"
	require.True(t, request.MayEvict())
	request.Checkpoint.CompatibilityKey = ""
	request.Checkpoint.Runtime = CheckpointRuntimeFilesystem
	require.True(t, request.MayEvict())
	request.Checkpoint.Runtime = "gvisor"
	request.Gpu = string(NO_GPU)
	require.True(t, request.MayEvict())
	request.Gpu = "RTX4090"
	request.Stub.Type = StubType(StubTypeASGIDeployment)
	require.True(t, request.MayEvict())
}

func TestWorkerStartConcurrencyUsesPoolAndRuntime(t *testing.T) {
	worker := &Worker{
		TotalCpu: 16000,
		PoolName: "default",
		Runtime:  ContainerRuntimeRunc.String(),
	}
	config := WorkerConfig{
		Pools: map[string]WorkerPoolConfig{
			"default": {
				ContainerRuntime:          ContainerRuntimeRunc.String(),
				ContainerStartConcurrency: 32,
			},
		},
	}

	require.Equal(t, 32, WorkerStartConcurrency(config, worker))
}

func TestWorkerStartConcurrencyCapsByWorkerCPU(t *testing.T) {
	worker := &Worker{
		TotalCpu: 1000,
		PoolName: "default",
		Runtime:  ContainerRuntimeRunc.String(),
	}
	config := WorkerConfig{
		Pools: map[string]WorkerPoolConfig{
			"default": {
				ContainerRuntime:          ContainerRuntimeRunc.String(),
				ContainerStartConcurrency: 32,
			},
		},
	}

	require.Equal(t, 2, WorkerStartConcurrency(config, worker))
}

func TestWorkerProtoRoundTripPreservesRuntimeAndPoolSelector(t *testing.T) {
	worker := &Worker{
		Id:           "worker-1",
		Runtime:      ContainerRuntimeRunc.String(),
		PoolSelector: "private-cpu",
	}

	roundTrip := NewWorkerFromProto(worker.ToProto())
	require.Equal(t, ContainerRuntimeRunc.String(), roundTrip.Runtime)
	require.Equal(t, "private-cpu", roundTrip.PoolSelector)
}

func TestContainerRequestProtoRoundTripPreservesTaskID(t *testing.T) {
	request := &ContainerRequest{TaskId: "task-1"}

	roundTrip := NewContainerRequestFromProto(request.ToProto())
	require.Equal(t, request.TaskId, roundTrip.TaskId)
}

func TestCheckpointProtoRoundTripPreservesRuntime(t *testing.T) {
	checkpoint := &Checkpoint{CheckpointId: "checkpoint-1", Runtime: ContainerRuntimeGvisor.String()}

	require.Equal(t, checkpoint.Runtime, NewCheckpointFromProto(checkpoint.ToProto()).Runtime)
	require.False(t, checkpoint.IsFilesystemOnly())
	checkpoint.Runtime = " FILESYSTEM "
	require.True(t, checkpoint.IsFilesystemOnly())
}

func TestPrivateWorkerRequestRemovesControlPlaneCredentials(t *testing.T) {
	signingKey := "workspace-signing-key"
	storageID := uint(1)
	storageSecret := "storage-secret"
	mountConfig := &MountPointConfig{
		BucketName: "external-bucket",
		AccessKey:  "external-access",
		SecretKey:  "external-secret",
	}
	stubConfig, err := json.Marshal(StubConfigV1{
		Secrets: []Secret{{Name: "SECRET"}},
	})
	require.NoError(t, err)

	request := &ContainerRequest{
		ContainerId: "container-1",
		EntryPoint:  []string{"python"},
		Env:         []string{"BETA9_TOKEN=user-token", "SECRET=value", "SAFE=value"},
		ImageId:     "image-1",
		Stub: StubWithRelated{
			Stub: Stub{Config: string(stubConfig)},
		},
		Workspace: Workspace{
			Name:       "workspace",
			SigningKey: &signingKey,
			Storage: &WorkspaceStorage{
				Id:        &storageID,
				SecretKey: &storageSecret,
			},
		},
		Mounts: []Mount{{
			MountPath:        "/mnt/data",
			MountPointConfig: mountConfig,
		}},
		ImageCredentials:         "image-creds",
		BuildRegistryCredentials: "build-creds",
		BuildOptions: BuildOptions{
			SourceImageCreds: "source-image-creds",
			BuildSecrets:     []string{"SECRET=value"},
		},
	}

	privateRequest := request.PrivateWorkerRequest()

	require.Nil(t, privateRequest.Workspace.SigningKey)
	require.Empty(t, privateRequest.ImageCredentials)
	require.Empty(t, privateRequest.BuildRegistryCredentials)
	require.Empty(t, privateRequest.BuildOptions.SourceImageCreds)
	require.Empty(t, privateRequest.BuildOptions.BuildSecrets)
	require.Equal(t, []string{"SAFE=value"}, privateRequest.Env)
	require.NotNil(t, privateRequest.Workspace.Storage)
	require.Nil(t, privateRequest.Workspace.Storage.SecretKey)
	require.NotSame(t, request.Mounts[0].MountPointConfig, privateRequest.Mounts[0].MountPointConfig)
	require.Empty(t, privateRequest.Mounts[0].MountPointConfig.AccessKey)
	require.Empty(t, privateRequest.Mounts[0].MountPointConfig.SecretKey)

	privateRequest.Mounts[0].MountPointConfig.SecretKey = "changed"
	require.Equal(t, "external-secret", request.Mounts[0].MountPointConfig.SecretKey)

	proto := privateRequest.ToProto()
	require.Empty(t, proto.Workspace.SigningKey)
	require.Empty(t, proto.Workspace.Storage.AccessKey)
	require.Empty(t, proto.Workspace.Storage.SecretKey)
}

func TestContainerRequestProtoRoundTripPreservesGitSource(t *testing.T) {
	dockerfile := ""
	request := &ContainerRequest{
		BuildOptions: BuildOptions{
			Dockerfile: &dockerfile,
			GitSource:  &GitSource{RepoURL: "https://github.com/acme/app", Ref: "main", Commit: "abc", Token: "tok", WorkingDir: "api"},
		},
	}
	require.True(t, request.IsBuildRequest())

	restored := NewContainerRequestFromProto(request.ToProto())
	require.True(t, restored.IsBuildRequest(), "an empty Dockerfile string travels as nil; the git source alone marks a build")
	require.Equal(t, request.BuildOptions.GitSource, restored.BuildOptions.GitSource)

	private := request.PrivateWorkerRequest()
	require.Empty(t, private.BuildOptions.GitSource.Token)
	require.Equal(t, "tok", request.BuildOptions.GitSource.Token, "sanitizing the private copy leaves the original alone")
}

func TestBrokeredSecretWorkerBoundary(t *testing.T) {
	request := &ContainerRequest{Env: []string{"ALIAS=beam_secret_a"}, SecretBindings: []SecretBinding{{EnvName: "ALIAS", SecretName: "KEY", Placeholder: "beam_secret_a", Value: "real-value", Hosts: []string{"api.example.com"}}}, Stub: StubWithRelated{Stub: Stub{Config: `{"secrets":[{"name":"KEY","env_name":"ALIAS"}]}`}}, BlockNetwork: true}
	private := request.PrivateWorkerRequest()
	require.Equal(t, request.Env, private.Env)
	require.Empty(t, private.SecretBindings[0].Value)
	require.Equal(t, "real-value", request.SecretBindings[0].Value)
	roundTrip := NewContainerRequestFromProto(private.ToProto())
	require.Equal(t, private.SecretBindings, roundTrip.SecretBindings)
	require.Equal(t, []string{"api.example.com"}, roundTrip.EgressAllowList())
	roundTrip.AllowList = []string{"other.example.com"}
	require.Equal(t, []string{"other.example.com"}, roundTrip.EgressAllowList())
}

func TestCPUShareMultiplier(t *testing.T) {
	for _, valid := range []CPUShareMultiplier{0, 1, 1.1, 1.25, 16} {
		require.NoError(t, valid.Validate(), "%v", valid)
	}
	for _, invalid := range []CPUShareMultiplier{-1, 0.5, 1.005, 16.01, CPUShareMultiplier(math.NaN())} {
		require.Error(t, invalid.Validate(), "%v", invalid)
	}
	require.EqualValues(t, 16000, CPUShareMultiplier(0).Schedulable(16000))
	require.EqualValues(t, 24000, CPUShareMultiplier(1.5).Schedulable(16000))
	require.EqualValues(t, 22050, CPUShareMultiplier(1.4).Schedulable(15750))
}

func TestContainerStateSaysWhatItWaitsFor(t *testing.T) {
	now := time.Unix(1_000_000, 0)
	stored := func(state ContainerState) ContainerState {
		state.PendingPhase, state.PendingReason, state.PendingSince = ContainerPendingProvisioning, ContainerPendingWorkerStarting, now.Unix()-40
		return state
	}
	for _, tc := range []struct {
		name  string
		state ContainerState
		want  *Pending
	}{
		{
			name:  "nothing stored yet: queued since it was asked for",
			state: ContainerState{Status: ContainerStatusPending, ScheduledAt: now.Unix() - 3},
			want:  &Pending{Phase: ContainerPendingQueued, WaitedSeconds: 3, Message: "Waiting to be placed on a machine"},
		},
		{
			name:  "what the scheduler stored",
			state: stored(ContainerState{Status: ContainerStatusPending}),
			want:  &Pending{Phase: ContainerPendingProvisioning, Reason: ContainerPendingWorkerStarting, WaitedSeconds: 40, Message: "No warm machine, starting one"},
		},
		{
			// When the worker took it is not kept, so how long is not said.
			name:  "a worker has it: starting, whatever it waited for",
			state: stored(ContainerState{Status: ContainerStatusPending, WorkerId: "worker-1"}),
			want:  &Pending{Phase: ContainerPendingStarting, Message: "Placed on a machine, starting the container"},
		},
		{
			name:  "a clock behind the scheduler's does not make the wait negative",
			state: ContainerState{Status: ContainerStatusPending, ScheduledAt: now.Unix() + 5},
			want:  &Pending{Phase: ContainerPendingQueued, Message: "Waiting to be placed on a machine"},
		},
		{name: "running", state: stored(ContainerState{Status: ContainerStatusRunning, WorkerId: "worker-1"})},
		{name: "stopping before a worker had it", state: stored(ContainerState{Status: ContainerStatusStopping})},
	} {
		t.Run(tc.name, func(t *testing.T) {
			state := tc.state
			state.SettlePending()
			require.Equal(t, tc.want, state.Pending(now))

			// A container that does not wait says nothing about waiting.
			encoded, err := json.Marshal(state)
			require.NoError(t, err)
			require.Equal(t, tc.want != nil, strings.Contains(string(encoded), "pending_phase"))
		})
	}
}

func TestPendingMessages(t *testing.T) {
	for _, tc := range []struct {
		phase  ContainerPendingPhase
		reason ContainerPendingReason
		want   string
	}{
		{ContainerPendingQueued, "", "Waiting to be placed on a machine"},
		{ContainerPendingProvisioning, "", "No warm machine, starting one"},
		{ContainerPendingProvisioning, ContainerPendingWorkerStarting, "No warm machine, starting one"},
		{ContainerPendingProvisioning, ContainerPendingBackupWorkerStarting, "Preferred capacity is unavailable, starting a machine on backup capacity"},
		{ContainerPendingWaitingForCapacity, "", "No capacity available right now, waiting for some"},
		{ContainerPendingWaitingForCapacity, ContainerPendingNoCapacity, "No capacity available right now, waiting for some"},
		{ContainerPendingWaitingForCapacity, ContainerPendingMachineBusy, "The machine it is pinned to is full, waiting for room"},
		{ContainerPendingWaitingForCapacity, ContainerPendingNoSpareCapacity, "The Spot tier has no spare capacity right now, waiting for some"},
		{ContainerPendingStarting, "", "Placed on a machine, starting the container"},
		{"later", "", ""},
	} {
		require.Equal(t, tc.want, tc.phase.Message(tc.reason), "%s/%s", tc.phase, tc.reason)
	}
}

func TestPendingOf(t *testing.T) {
	now := time.Unix(1_000_000, 0)
	pending := func(id string, phase ContainerPendingPhase, reason ContainerPendingReason, waited int64) ContainerState {
		return ContainerState{ContainerId: id, Status: ContainerStatusPending, PendingPhase: phase, PendingReason: reason, PendingSince: now.Unix() - waited}
	}
	noCapacity := pending("no-capacity", ContainerPendingWaitingForCapacity, ContainerPendingNoCapacity, 300)
	queued := pending("queued", ContainerPendingQueued, "", 2)
	provisioning := pending("provisioning", ContainerPendingProvisioning, ContainerPendingWorkerStarting, 40)
	provisioningLonger := pending("provisioning-longer", ContainerPendingProvisioning, ContainerPendingBackupWorkerStarting, 90)
	starting := ContainerState{ContainerId: "starting", Status: ContainerStatusPending, PendingPhase: ContainerPendingStarting}
	running := ContainerState{ContainerId: "running", Status: ContainerStatusRunning}
	stopping := ContainerState{ContainerId: "stopping", Status: ContainerStatusStopping}

	// Work that any of the containers can take: an endpoint or a task queue.
	require.Nil(t, PendingOf(nil, "", now))
	require.Nil(t, PendingOf([]ContainerState{stopping}, "", now), "a container that is going away is not on its way")
	require.Nil(t, PendingOf([]ContainerState{noCapacity, running}, "", now), "work waits its turn on the container that runs")
	require.Equal(t, ContainerPendingWaitingForCapacity, PendingOf([]ContainerState{noCapacity, stopping}, "", now).Phase)
	require.Equal(t, ContainerPendingQueued, PendingOf([]ContainerState{noCapacity, queued}, "", now).Phase)
	require.Equal(t, ContainerPendingProvisioning, PendingOf([]ContainerState{noCapacity, provisioning, queued}, "", now).Phase)
	require.Equal(t, ContainerPendingStarting, PendingOf([]ContainerState{provisioning, starting, noCapacity}, "", now).Phase)
	longest := PendingOf([]ContainerState{provisioning, provisioningLonger}, "", now)
	require.Equal(t, ContainerPendingBackupWorkerStarting, longest.Reason)
	require.EqualValues(t, 90, longest.WaitedSeconds)

	// Work with a container of its own: a function call. Its stub's other
	// containers run other calls.
	all := []ContainerState{running, starting, noCapacity}
	require.Equal(t, ContainerPendingWaitingForCapacity, PendingOf(all, "no-capacity", now).Phase)
	require.Nil(t, PendingOf(all, "running", now))
	require.Nil(t, PendingOf(all, "gone", now))
}

func TestContainerRequestFailure(t *testing.T) {
	for reason, noCapacity := range map[ContainerSchedulingFailureReason]bool{
		ContainerSchedulingFailureProvisioningLimit:               true,
		ContainerSchedulingFailureWorkerCapacityTimeout:           true,
		ContainerSchedulingFailureRetryLimit:                      true,
		ContainerSchedulingFailureManagedFallbackNoCapacity:       true,
		ContainerSchedulingFailureNoOpportunisticCapacity:         true,
		ContainerSchedulingFailureNoController:                    false,
		ContainerSchedulingFailureManagedFallbackConcurrencyLimit: false,
	} {
		require.Equal(t, noCapacity, ContainerRequestFailure{Reason: reason}.NoCapacity(), reason)
	}

	require.Empty(t, (&ContainerRequest{}).RequestedGpus())
	require.Empty(t, (&ContainerRequest{Gpu: string(NO_GPU)}).RequestedGpus())
	require.Equal(t, []string{"A10G"}, (&ContainerRequest{Gpu: "A10G"}).RequestedGpus())
	require.Equal(t, []string{"A10G", "T4"}, (&ContainerRequest{GpuRequest: []string{"A10G", "T4", "A10G", ""}, Gpu: "A10G"}).RequestedGpus())
}
