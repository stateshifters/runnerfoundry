package daemon

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	upstream "github.com/actions/scaleset"
	"github.com/meza/github-scaler/internal/scaler"
)

func TestScaleSetMessageClientRequiresConfirmedJobAcquisition(t *testing.T) {
	if err := (scaleSetMessageClient{Client: &blockingMessageClient{}}).Acquire(context.Background(), 1); err == nil {
		t.Fatal("expected unsupported acquisition error")
	}
	if err := (scaleSetMessageClient{Client: &fakeMessageClient{acquireErr: errors.New("acquire failed")}}).Acquire(context.Background(), 1); err == nil {
		t.Fatal("expected acquisition request error")
	}
	if err := (scaleSetMessageClient{Client: &emptyAcquisitionClient{}}).Acquire(context.Background(), 1); err == nil {
		t.Fatal("expected unconfirmed acquisition error")
	}
}

func TestUnconfirmedJobAcquisitionPreservesDemandWithoutStartingRunner(t *testing.T) {
	provider := &fakeScalerProvider{trackStarted: true}
	coordinator := scaler.NewCoordinator(1)
	coordinator.AddRepository("repo", coordinatedRepositoryWorker{
		Reconciler: scaler.Reconciler{ScaleSet: scaler.ScaleSetRuntime{
			Name: "local", MaxRunners: 1, Provider: provider, JITConfig: fakeJITConfigGenerator{},
		}},
		MessageClient: scaleSetMessageClient{Client: &emptyAcquisitionClient{}},
	})
	handler := coordinatedScaleSetHandler{Coordinator: coordinator, Repository: "repo"}
	err := handler.HandleMessage(context.Background(), &upstream.RunnerScaleSetMessage{
		Statistics: &upstream.RunnerScaleSetStatistic{},
		JobAvailableMessages: []*upstream.JobAvailable{{JobMessageBase: upstream.JobMessageBase{
			RunnerRequestID: 51, QueueTime: time.Now(),
		}}},
	})
	if err == nil || !strings.Contains(err.Error(), "job 51 was not acquired") {
		t.Fatalf("expected unconfirmed acquisition failure, got %v", err)
	}
	if len(provider.started) != 0 {
		t.Fatalf("expected no runner for unconfirmed acquisition, got %#v", provider.started)
	}

	recoveryWorker := &recordingRecoveryWorker{}
	coordinator.AddRepository("repo", recoveryWorker)
	if err := coordinator.Schedule(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(recoveryWorker.acquired) != 1 || recoveryWorker.acquired[0] != 51 || len(recoveryWorker.started) != 1 {
		t.Fatalf("expected preserved demand to remain recoverable, acquired=%#v started=%#v", recoveryWorker.acquired, recoveryWorker.started)
	}
}
