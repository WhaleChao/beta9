package compute

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/s2-streamstore/s2-sdk-go/s2"
)

type fakeTelemetryCredentialIssuer struct {
	args []s2.IssueAccessTokenArgs
}

func (f *fakeTelemetryCredentialIssuer) Issue(_ context.Context, args s2.IssueAccessTokenArgs) (*s2.IssueAccessTokenResponse, error) {
	f.args = append(f.args, args)
	return &s2.IssueAccessTokenResponse{AccessToken: "token-" + string(args.ID)}, nil
}

func TestScopedTelemetryConfigUsesWorkspaceWriteOnlyPrefixes(t *testing.T) {
	issuer := &fakeTelemetryCredentialIssuer{}
	service := &Service{
		appConfig: types.AppConfig{
			Database: types.DatabaseConfig{
				S2: types.S2Config{
					ApiKey: "root-token",
					Basin:  "events-basin",
				},
			},
		},
		telemetryCredentials: issuer,
	}

	config, err := service.agentTelemetryConfig(context.Background(), "workspace-123", false)
	if err != nil {
		t.Fatalf("agentTelemetryConfig() error = %v", err)
	}
	if config == nil || !config.Enabled {
		t.Fatalf("expected enabled scoped telemetry config, got %#v", config)
	}
	if config.Logs == nil {
		t.Fatal("expected logs sink")
	}
	if got, want := config.Logs.StreamPrefix, "events/logs/workspaces/workspace-123"; got != want {
		t.Fatalf("log prefix = %q, want %q", got, want)
	}
	if got, want := config.Logs.Destination, "events-basin"; got != want {
		t.Fatalf("log destination = %q, want %q", got, want)
	}
	if config.Events == nil {
		t.Fatal("expected events sink")
	}
	if got, want := config.Events.StreamPrefix, "events/workspaces/workspace-123"; got != want {
		t.Fatalf("event prefix = %q, want %q", got, want)
	}
	if got, want := config.Events.Destination, "events-basin"; got != want {
		t.Fatalf("event destination = %q, want %q", got, want)
	}
	if len(issuer.args) != 2 {
		t.Fatalf("expected two issued tokens, got %d", len(issuer.args))
	}

	wantOps := []string{s2.OperationAppend}
	for _, args := range issuer.args {
		if args.Scope.Basins == nil || args.Scope.Basins.Exact == nil || *args.Scope.Basins.Exact != "events-basin" {
			t.Fatalf("unexpected basin scope: %#v", args.Scope.Basins)
		}
		if !reflect.DeepEqual(args.Scope.Ops, wantOps) {
			t.Fatalf("ops = %#v, want %#v", args.Scope.Ops, wantOps)
		}
		for _, forbidden := range []string{s2.OperationRead, s2.OperationDeleteStream, s2.OperationListBasins, s2.OperationIssueAccessToken} {
			for _, op := range args.Scope.Ops {
				if op == forbidden {
					t.Fatalf("scoped token includes forbidden op %q", forbidden)
				}
			}
		}
	}
}

// What was bought, what became of it and what it cost are read back from the
// market's history, so that history is the control plane's alone to write. A
// token issued to a machine that is not trusted is over the streams of its
// own workspace, whatever the workspace is called, and so over no stream of
// the market's, which pkg/repository keeps under "<prefix>/market/".
func TestScopedTelemetryTokensAreOverNoMarketStream(t *testing.T) {
	for _, workspace := range []string{"workspace-123", "market", "../market", "/market/spend", "a/../../market"} {
		issuer := &fakeTelemetryCredentialIssuer{}
		service := &Service{
			appConfig:            types.AppConfig{Database: types.DatabaseConfig{S2: types.S2Config{ApiKey: "root-token", Basin: "events-basin"}}},
			telemetryCredentials: issuer,
		}
		config, err := service.agentTelemetryConfig(context.Background(), workspace, false)
		if err != nil || len(issuer.args) != 2 {
			t.Fatalf("workspace %q: agentTelemetryConfig() issued %d tokens, error = %v", workspace, len(issuer.args), err)
		}
		if config.Logs.Credential == "root-token" || config.Events.Credential == "root-token" {
			t.Fatalf("workspace %q: a machine that is not trusted was given the cluster's credential", workspace)
		}
		for _, args := range issuer.args {
			if args.Scope.Streams == nil || args.Scope.Streams.Prefix == nil {
				t.Fatalf("workspace %q: token is not scoped to a stream prefix: %#v", workspace, args.Scope.Streams)
			}
			prefix := *args.Scope.Streams.Prefix
			own, ok := strings.CutPrefix(prefix, "events/logs/workspaces/")
			if !ok {
				own, ok = strings.CutPrefix(prefix, "events/workspaces/")
			}
			if !ok || own == "" || strings.Contains(own, "/") {
				t.Fatalf("workspace %q: token is over %q, which is not one workspace's streams", workspace, prefix)
			}
			for _, market := range []string{"events/market/spend", "events/market/outcomes"} {
				if strings.HasPrefix(market, prefix) {
					t.Fatalf("workspace %q: token over %q can write %q", workspace, prefix, market)
				}
			}
		}
	}
}

func TestAgentTelemetryConfigUsesClusterCredentialsForPlatformPools(t *testing.T) {
	issuer := &fakeTelemetryCredentialIssuer{}
	service := &Service{
		appConfig: types.AppConfig{Database: types.DatabaseConfig{S2: types.S2Config{
			ApiKey: "root-token",
			Basin:  "events-basin",
		}}},
		telemetryCredentials: issuer,
	}

	config, err := service.agentTelemetryConfig(context.Background(), "admin-workspace", true)
	if err != nil {
		t.Fatalf("agentTelemetryConfig() error = %v", err)
	}
	if got, want := config.Logs.Credential, "root-token"; got != want {
		t.Fatalf("log credential = %q, want %q", got, want)
	}
	if got, want := config.Logs.StreamPrefix, "events/logs/workspaces"; got != want {
		t.Fatalf("log prefix = %q, want %q", got, want)
	}
	if got, want := config.Events.Credential, "root-token"; got != want {
		t.Fatalf("event credential = %q, want %q", got, want)
	}
	if got, want := config.Events.StreamPrefix, "events/workspaces"; got != want {
		t.Fatalf("event prefix = %q, want %q", got, want)
	}
	if len(issuer.args) != 0 {
		t.Fatalf("platform workers should use the cluster S2 config, issued %d scoped tokens", len(issuer.args))
	}
}
