package daemon

import (
	"context"
	"errors"
	"testing"
	"time"

	upstream "github.com/actions/scaleset"
	"github.com/meza/github-scaler/internal/scaler"
)

func TestJITGeneratorRemovesStaleRunnerAfterNameConflict(t *testing.T) {
	generatedRunnerNames := []string{}
	runnerNameLookups := []string{}
	removedRunnerIDs := []int64{}
	client := fakeScaleSetJITClient{
		generateJitRunnerConfig: func(
			_ context.Context,
			setting *upstream.RunnerScaleSetJitRunnerSetting,
			_ int,
		) (*upstream.RunnerScaleSetJitRunnerConfig, error) {
			generatedRunnerNames = append(generatedRunnerNames, setting.Name)
			if len(generatedRunnerNames) == 1 {
				return nil, upstream.RunnerExistsError
			}
			return &upstream.RunnerScaleSetJitRunnerConfig{
				EncodedJITConfig: "encoded",
				Runner:           &upstream.RunnerReference{ID: 100, Name: "linux-1"},
			}, nil
		},
		getRunnerByName: func(_ context.Context, runnerName string) (*upstream.RunnerReference, error) {
			runnerNameLookups = append(runnerNameLookups, runnerName)
			return &upstream.RunnerReference{ID: 99, Name: "linux-1"}, nil
		},
		removeRunner: func(_ context.Context, runnerID int64) error {
			removedRunnerIDs = append(removedRunnerIDs, runnerID)
			return nil
		},
	}
	generator := scalesetJITGenerator{Client: client, ScaleSetID: 12}
	jitConfig, err := generator.Generate(context.Background(), "linux-1")
	if err != nil {
		t.Fatalf("expected jit success after stale runner cleanup, got %v", err)
	}
	if jitConfig.Runner.ID != 100 || jitConfig.JITConfig != "encoded" {
		t.Fatalf("expected retried jit config, got %#v", jitConfig)
	}
	if len(runnerNameLookups) != 1 || runnerNameLookups[0] != "linux-1" {
		t.Fatalf("expected runner name lookup, got %#v", runnerNameLookups)
	}
	if len(removedRunnerIDs) != 1 || removedRunnerIDs[0] != 99 {
		t.Fatalf("expected stale runner removal, got %#v", removedRunnerIDs)
	}
	if len(generatedRunnerNames) != 2 {
		t.Fatalf("expected one retry after stale runner cleanup, got %#v", generatedRunnerNames)
	}
}

func TestJITGeneratorSurfacesStaleRunnerLookupFailure(t *testing.T) {
	client := fakeScaleSetJITClient{
		generateJitRunnerConfig: func(context.Context, *upstream.RunnerScaleSetJitRunnerSetting, int) (*upstream.RunnerScaleSetJitRunnerConfig, error) {
			return nil, upstream.RunnerExistsError
		},
		getRunnerByName: func(context.Context, string) (*upstream.RunnerReference, error) {
			return nil, errors.New("lookup failed")
		},
	}
	generator := scalesetJITGenerator{Client: client}
	if _, err := generator.Generate(context.Background(), "linux-1"); err == nil {
		t.Fatal("expected lookup error")
	}
}

func TestJITGeneratorSurfacesMissingStaleRunner(t *testing.T) {
	var missingRunner *upstream.RunnerReference
	client := fakeScaleSetJITClient{
		generateJitRunnerConfig: func(context.Context, *upstream.RunnerScaleSetJitRunnerSetting, int) (*upstream.RunnerScaleSetJitRunnerConfig, error) {
			return nil, upstream.RunnerExistsError
		},
		getRunnerByName: func(context.Context, string) (*upstream.RunnerReference, error) {
			return missingRunner, nil
		},
	}
	generator := scalesetJITGenerator{Client: client}
	if _, err := generator.Generate(context.Background(), "linux-1"); err == nil {
		t.Fatal("expected missing stale runner error")
	}
}

func TestJITGeneratorSurfacesStaleRunnerRemovalFailure(t *testing.T) {
	client := fakeScaleSetJITClient{
		generateJitRunnerConfig: func(context.Context, *upstream.RunnerScaleSetJitRunnerSetting, int) (*upstream.RunnerScaleSetJitRunnerConfig, error) {
			return nil, upstream.RunnerExistsError
		},
		getRunnerByName: func(context.Context, string) (*upstream.RunnerReference, error) {
			return &upstream.RunnerReference{ID: 99, Name: "linux-1"}, nil
		},
		removeRunner: func(context.Context, int64) error {
			return errors.New("remove failed")
		},
	}
	generator := scalesetJITGenerator{Client: client}
	if _, err := generator.Generate(context.Background(), "linux-1"); err == nil {
		t.Fatal("expected remove error")
	}
}

func TestJITGeneratorSurfacesRetryFailureAfterStaleRunnerCleanup(t *testing.T) {
	generatedRunnerNames := []string{}
	removedRunnerIDs := []int64{}
	client := fakeScaleSetJITClient{
		generateJitRunnerConfig: func(
			_ context.Context,
			setting *upstream.RunnerScaleSetJitRunnerSetting,
			_ int,
		) (*upstream.RunnerScaleSetJitRunnerConfig, error) {
			generatedRunnerNames = append(generatedRunnerNames, setting.Name)
			if len(generatedRunnerNames) == 1 {
				return nil, upstream.RunnerExistsError
			}
			return nil, errors.New("retry failed")
		},
		getRunnerByName: func(context.Context, string) (*upstream.RunnerReference, error) {
			return &upstream.RunnerReference{ID: 99, Name: "linux-1"}, nil
		},
		removeRunner: func(_ context.Context, runnerID int64) error {
			removedRunnerIDs = append(removedRunnerIDs, runnerID)
			return nil
		},
	}
	generator := scalesetJITGenerator{Client: client}

	if _, err := generator.Generate(context.Background(), "linux-1"); err == nil {
		t.Fatal("expected retry error")
	}
	if len(removedRunnerIDs) != 1 || removedRunnerIDs[0] != 99 {
		t.Fatalf("expected stale runner removal, got %#v", removedRunnerIDs)
	}
	if len(generatedRunnerNames) != 2 {
		t.Fatalf("expected one retry after stale runner cleanup, got %#v", generatedRunnerNames)
	}
}

func TestJITGeneratorHandlesMissingRunnerReference(t *testing.T) {
	client := fakeScaleSetJITClient{generateJitRunnerConfig: func(
		context.Context,
		*upstream.RunnerScaleSetJitRunnerSetting,
		int,
	) (*upstream.RunnerScaleSetJitRunnerConfig, error) {
		return &upstream.RunnerScaleSetJitRunnerConfig{EncodedJITConfig: "encoded"}, nil
	}}
	generator := scalesetJITGenerator{Client: client, ScaleSetID: 12}
	jitConfig, err := generator.Generate(context.Background(), "linux-1")
	if err != nil {
		t.Fatalf("expected jit success, got %v", err)
	}
	if jitConfig.Runner.ID != 0 || jitConfig.Runner.Name != "" {
		t.Fatalf("expected empty runner reference, got %#v", jitConfig.Runner)
	}
}

func TestJITGeneratorRemovesGeneratedRunner(t *testing.T) {
	removedRunnerIDs := []int64{}
	client := fakeScaleSetJITClient{removeRunner: func(_ context.Context, runnerID int64) error {
		removedRunnerIDs = append(removedRunnerIDs, runnerID)
		return nil
	}}
	generator := scalesetJITGenerator{Client: client}
	if err := generator.Remove(context.Background(), scaler.JITRunner{ID: 99, Name: "linux-1"}); err != nil {
		t.Fatalf("expected remove success, got %v", err)
	}
	if len(removedRunnerIDs) != 1 || removedRunnerIDs[0] != 99 {
		t.Fatalf("expected runner removal, got %#v", removedRunnerIDs)
	}
}

func TestJITGeneratorIgnoresMissingRunnerReference(t *testing.T) {
	removedRunnerIDs := []int64{}
	client := fakeScaleSetJITClient{removeRunner: func(_ context.Context, runnerID int64) error {
		removedRunnerIDs = append(removedRunnerIDs, runnerID)
		return nil
	}}
	generator := scalesetJITGenerator{Client: client}
	if err := generator.Remove(context.Background(), scaler.JITRunner{}); err != nil {
		t.Fatalf("expected missing runner reference success, got %v", err)
	}
	if len(removedRunnerIDs) != 0 {
		t.Fatalf("expected no runner removal, got %#v", removedRunnerIDs)
	}
}

func TestJITGeneratorSurfacesRemoveFailure(t *testing.T) {
	client := fakeScaleSetJITClient{removeRunner: func(context.Context, int64) error {
		return errors.New("remove failed")
	}}
	generator := scalesetJITGenerator{Client: client}
	if err := generator.Remove(context.Background(), scaler.JITRunner{ID: 99, Name: "linux-1"}); err == nil {
		t.Fatal("expected remove error")
	}
}

func waitForPoll(t *testing.T, messageClient *fakeMessageClient) {
	t.Helper()
	select {
	case <-messageClient.polled:
	case <-time.After(time.Second):
		t.Fatal("expected poll")
	}
}

func assertScaleSetLabels(t *testing.T, labels []upstream.Label, expectedNames []string) {
	t.Helper()
	if len(labels) != len(expectedNames) {
		t.Fatalf("expected labels %v, got %#v", expectedNames, labels)
	}
	for labelIndex, expectedName := range expectedNames {
		if labels[labelIndex].Name != expectedName || labels[labelIndex].Type != "System" {
			t.Fatalf("expected label %q at index %d, got %#v", expectedName, labelIndex, labels[labelIndex])
		}
	}
}
