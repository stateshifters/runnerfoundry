package daemon

import (
	"context"
	"errors"
	"testing"

	upstream "github.com/actions/scaleset"
	"github.com/meza/github-scaler/internal/config"
)

func TestScaleSetMessageClientForwardsMessages(t *testing.T) {
	message := &upstream.RunnerScaleSetMessage{MessageID: 7}
	client := scaleSetMessageClient{Client: &fakeMessageClient{message: message}}
	receivedMessage, err := client.GetMessage(context.Background(), 1, 2)
	if err != nil {
		t.Fatalf("expected message success, got %v", err)
	}
	if receivedMessage.MessageID != 7 {
		t.Fatalf("expected forwarded message, got %#v", receivedMessage)
	}
	if err := client.DeleteMessage(context.Background(), 7); err != nil {
		t.Fatalf("expected delete success, got %v", err)
	}
	if err := client.Close(context.Background()); err != nil {
		t.Fatalf("expected close success, got %v", err)
	}
}

func TestScaleSetMessageClientWrapsErrors(t *testing.T) {
	client := scaleSetMessageClient{Client: &fakeMessageClient{
		getMessageErr: errors.New("get failed"),
		deleteErr:     errors.New("delete failed"),
		closeErr:      errors.New("close failed"),
	}}
	if _, err := client.GetMessage(context.Background(), 1, 2); err == nil {
		t.Fatal("expected get error")
	}
	if err := client.DeleteMessage(context.Background(), 7); err == nil {
		t.Fatal("expected delete error")
	}
	if err := client.Close(context.Background()); err == nil {
		t.Fatal("expected close error")
	}
}

func TestEnsureRunnerScaleSetReturnsExistingOrCreatesNew(t *testing.T) {
	existing, err := ensureRunnerScaleSet(context.Background(), &fakeScaleSetClient{
		runnerScaleSet: &upstream.RunnerScaleSet{ID: 1},
	}, 10, config.ScaleSetConfig{Name: "linux"})
	if err != nil {
		t.Fatalf("expected existing success, got %v", err)
	}
	if existing.ID != 1 {
		t.Fatalf("expected existing scale set, got %#v", existing)
	}

	client := &fakeScaleSetClient{createdScaleSet: &upstream.RunnerScaleSet{ID: 2}}
	created, err := ensureRunnerScaleSet(context.Background(), client, 10, config.ScaleSetConfig{Name: "linux"})
	if err != nil {
		t.Fatalf("expected create success, got %v", err)
	}
	if created.ID != 2 {
		t.Fatalf("expected created scale set, got %#v", created)
	}
}

func TestEnsureRunnerScaleSetUsesConfiguredLabels(t *testing.T) {
	client := &fakeScaleSetClient{createdScaleSet: &upstream.RunnerScaleSet{ID: 2}}
	_, err := ensureRunnerScaleSet(context.Background(), client, 10, config.ScaleSetConfig{Name: "linux"})
	if err != nil {
		t.Fatalf("expected create success, got %v", err)
	}
	assertScaleSetLabels(t, client.createdScaleSetRequest.Labels, []string{"linux"})
}

func TestRunnerHostnameReturnsHostName(t *testing.T) {
	if runnerHostname() == "" {
		t.Fatal("expected hostname")
	}
}

func TestRunnerHostnameFallsBackWhenHostNameFails(t *testing.T) {
	originalHostname := operatingSystemHostname
	defer func() {
		operatingSystemHostname = originalHostname
	}()
	operatingSystemHostname = func() (string, error) {
		return "", errors.New("hostname failed")
	}
	if runnerHostname() != "github-scaler" {
		t.Fatal("expected fallback hostname")
	}
}
