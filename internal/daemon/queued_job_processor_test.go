package daemon

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stateshifters/runnerfoundry/internal/core"
	runtimeengine "github.com/stateshifters/runnerfoundry/internal/runtime"
)

func TestQueuedJobProcessorStartsImmediatelyAndJoinsOnStop(t *testing.T) {
	requests := make(chan core.RunnerPool, 1)
	requester := queuedJobRequesterFake{request: func(context.Context, core.RunnerPool) (runtimeengine.ProcessResult, error) {
		requests <- "linux"
		return runtimeengine.ProcessResult{Outcome: runtimeengine.ProcessNoWork}, nil
	}}
	gate := newQueuedJobEligibilityGate()
	processor := newQueuedJobProcessorForPools(requester, gate, nil, []core.RunnerPool{"linux"})

	if err := processor.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	select {
	case runnerPool := <-requests:
		t.Fatalf("Start() requested work for ineligible Runner Pool %q", runnerPool)
	default:
	}
	markQueuedJobPoolsEligible(t, gate, "linux")
	processor.Wake("linux")
	if runnerPool := <-requests; runnerPool != "linux" {
		t.Fatalf("woken Runner Pool = %q, want linux", runnerPool)
	}
	if err := processor.Stop(context.Background()); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}
}

func TestQueuedJobProcessorCoalescesWakeupsPerPool(t *testing.T) {
	requests := make(chan queuedJobRequest, 4)
	requester := queuedJobRequesterFake{request: func(
		ctx context.Context,
		runnerPool core.RunnerPool,
	) (runtimeengine.ProcessResult, error) {
		request := queuedJobRequest{runnerPool: runnerPool, response: make(chan queuedJobResponse, 1)}
		requests <- request
		select {
		case response := <-request.response:
			return response.result, response.err
		case <-ctx.Done():
			return runtimeengine.ProcessResult{}, ctx.Err()
		}
	}}
	processor := startQueuedJobTestProcessor(t, requester, nil, "linux")
	processor.Wake("linux")
	linux := nextQueuedJobRequest(t, requests, "linux")
	for range 20 {
		processor.Wake("linux")
	}
	linux.response <- queuedJobResponse{result: runtimeengine.ProcessResult{Outcome: runtimeengine.ProcessNoWork}}
	requeuedLinux := nextQueuedJobRequest(t, requests, "linux")
	requeuedLinux.response <- queuedJobResponse{result: runtimeengine.ProcessResult{Outcome: runtimeengine.ProcessNoWork}}
	assertNoQueuedJobRequest(t, requests)
}

func TestQueuedJobProcessorProcessesDifferentPoolsConcurrently(t *testing.T) {
	requests := make(chan queuedJobRequest, 2)
	requester := queuedJobRequesterFake{request: func(
		ctx context.Context,
		runnerPool core.RunnerPool,
	) (runtimeengine.ProcessResult, error) {
		request := queuedJobRequest{runnerPool: runnerPool, response: make(chan queuedJobResponse, 1)}
		requests <- request
		select {
		case response := <-request.response:
			return response.result, response.err
		case <-ctx.Done():
			return runtimeengine.ProcessResult{}, ctx.Err()
		}
	}}
	processor := startQueuedJobTestProcessor(t, requester, nil, "linux", "windows")

	processor.Wake("linux")
	linux := nextQueuedJobRequest(t, requests, "linux")
	processor.Wake("windows")
	windows := nextQueuedJobRequest(t, requests, "windows")
	windows.response <- queuedJobResponse{result: runtimeengine.ProcessResult{Outcome: runtimeengine.ProcessNoWork}}
	linux.response <- queuedJobResponse{result: runtimeengine.ProcessResult{Outcome: runtimeengine.ProcessNoWork}}
}

func TestQueuedJobProcessorInventoryRefreshAndPoolMaintenanceSelectEligibility(t *testing.T) {
	requests := make(chan core.RunnerPool, 2)
	requester := queuedJobRequesterFake{request: func(
		_ context.Context,
		runnerPool core.RunnerPool,
	) (runtimeengine.ProcessResult, error) {
		requests <- runnerPool
		return runtimeengine.ProcessResult{Outcome: runtimeengine.ProcessNoWork}, nil
	}}
	gate := newQueuedJobEligibilityGate()
	processor := newQueuedJobProcessorForPools(requester, gate, nil, []core.RunnerPool{"linux", "windows"})
	if err := processor.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() { stopQueuedJobProcessor(t, processor) })

	markQueuedJobPoolsEligible(t, gate, "windows")
	if err := gate.RefreshInventory(func() error { return nil }); err != nil {
		t.Fatalf("refresh inventory: %v", err)
	}
	for _, runnerPool := range []core.RunnerPool{"linux", "windows"} {
		_, requested, err := gate.requestIfEligible(context.Background(), runnerPool, requester)
		if err != nil || requested {
			t.Fatalf("post-refresh %s request = requested %t, error %v; want ineligible", runnerPool, requested, err)
		}
	}
	markQueuedJobPoolsEligible(t, gate, "windows")
	processor.Wake("windows")
	if runnerPool := <-requests; runnerPool != "windows" {
		t.Fatalf("eligible Runner Pool = %q, want windows", runnerPool)
	}
}

func TestQueuedJobProcessorUsesCallerCancellationAndCanRestartAfterJoin(t *testing.T) {
	requestStarted := make(chan struct{}, 2)
	requester := queuedJobRequesterFake{request: func(
		ctx context.Context,
		_ core.RunnerPool,
	) (runtimeengine.ProcessResult, error) {
		requestStarted <- struct{}{}
		<-ctx.Done()
		return runtimeengine.ProcessResult{}, fmt.Errorf("wait for request cancellation: %w", ctx.Err())
	}}
	gate := newQueuedJobEligibilityGate()
	markQueuedJobPoolsEligible(t, gate, "linux")
	processor := newQueuedJobProcessorForPools(requester, gate, nil, []core.RunnerPool{"linux"})
	runContext, cancel := context.WithCancel(context.Background())
	if err := processor.Start(runContext); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	if err := processor.Start(context.Background()); err == nil {
		t.Fatal("duplicate Start() unexpectedly succeeded")
	}
	processor.Wake("linux")
	<-requestStarted
	cancel()
	if err := processor.Stop(context.Background()); err != nil {
		t.Fatalf("Stop() after caller cancellation error = %v", err)
	}
	if err := processor.Start(context.Background()); err != nil {
		t.Fatalf("Start() after join error = %v", err)
	}
	if err := processor.Stop(context.Background()); err != nil {
		t.Fatalf("second Stop() error = %v", err)
	}
}

func TestQueuedJobProcessorStopRemainsRetryableUntilWorkersJoin(t *testing.T) {
	requestStarted := make(chan struct{})
	releaseRequest := make(chan struct{})
	requester := queuedJobRequesterFake{request: func(
		context.Context,
		core.RunnerPool,
	) (runtimeengine.ProcessResult, error) {
		close(requestStarted)
		<-releaseRequest
		return runtimeengine.ProcessResult{}, nil
	}}
	processor := startQueuedJobTestProcessor(t, requester, nil, "linux")
	processor.Wake("linux")
	awaitTestSignal(t, requestStarted, "queued-job request")

	timedOut, cancel := context.WithCancel(context.Background())
	cancel()
	if err := processor.Stop(timedOut); !errors.Is(err, context.Canceled) {
		t.Fatalf("first Stop() error = %v, want cancellation", err)
	}
	stopDone := make(chan error, 1)
	go func() { stopDone <- processor.Stop(context.Background()) }()
	select {
	case err := <-stopDone:
		t.Fatalf("retry Stop() returned before worker joined: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(releaseRequest)
	assertNoTestError(t, <-stopDone, "retry queued-job processor stop")
}

func TestQueuedJobProcessorReportsFailureAndRecoveryAsOperationalEvents(t *testing.T) {
	responses := make(chan error, 2)
	events := make(chan operationalEvent, 2)
	requester := queuedJobRequesterFake{request: func(
		context.Context,
		core.RunnerPool,
	) (runtimeengine.ProcessResult, error) {
		return runtimeengine.ProcessResult{Outcome: runtimeengine.ProcessNoWork}, <-responses
	}}
	processor := startQueuedJobTestProcessor(t, requester, collectQueuedJobOperationalEvents(t, events), "linux")

	wantErr := errors.New("request failed")
	responses <- wantErr
	processor.Wake("linux")
	failure := <-events
	if failure.operation != "job_processing" || failure.outcome != "failed" || failure.runnerPool != "linux" ||
		!errors.Is(failure.err, wantErr) {
		t.Fatalf("failure event = %#v", failure)
	}
	responses <- nil
	processor.Wake("linux")
	recovery := <-events
	if recovery.operation != "job_processing" || recovery.outcome != "recovered" || recovery.runnerPool != "linux" ||
		recovery.err != nil {
		t.Fatalf("recovery event = %#v", recovery)
	}
}

func TestQueuedJobProcessorImmediatelyRetriesDurablyRequeuedFailure(t *testing.T) {
	requests := make(chan int, 2)
	requestCount := 0
	requester := queuedJobRequesterFake{request: func(
		context.Context,
		core.RunnerPool,
	) (runtimeengine.ProcessResult, error) {
		requestCount++
		requests <- requestCount
		if requestCount == 1 {
			return runtimeengine.ProcessResult{
				Outcome: runtimeengine.ProcessRetry, RetryOutcome: core.RetryRequeued,
			}, errors.New("start failed after safe requeue")
		}
		return runtimeengine.ProcessResult{Outcome: runtimeengine.ProcessNoWork}, nil
	}}
	processor := startQueuedJobTestProcessor(t, requester, func(error) {}, "linux")

	processor.Wake("linux")
	if request := <-requests; request != 1 {
		t.Fatalf("first request = %d", request)
	}
	if request := <-requests; request != 2 {
		t.Fatalf("immediate retry request = %d", request)
	}
}

func TestQueuedJobProcessorContinuesAfterStartingRunner(t *testing.T) {
	assertQueuedJobProcessorContinuesAfter(t, runtimeengine.ProcessRunnerStarted)
}

func TestQueuedJobProcessorContinuesAfterReportingProvisioningFailure(t *testing.T) {
	assertQueuedJobProcessorContinuesAfter(t, runtimeengine.ProcessProvisioningFailureReported)
}

func assertQueuedJobProcessorContinuesAfter(t *testing.T, outcome runtimeengine.ProcessOutcome) {
	t.Helper()
	requests := make(chan int, 2)
	requestCount := 0
	requester := queuedJobRequesterFake{request: func(
		context.Context,
		core.RunnerPool,
	) (runtimeengine.ProcessResult, error) {
		requestCount++
		requests <- requestCount
		if requestCount == 1 {
			return runtimeengine.ProcessResult{Outcome: outcome}, nil
		}
		return runtimeengine.ProcessResult{Outcome: runtimeengine.ProcessNoWork}, nil
	}}
	processor := startQueuedJobTestProcessor(t, requester, nil, "linux")

	processor.Wake("linux")
	if request := <-requests; request != 1 {
		t.Fatalf("first request = %d", request)
	}
	if request := <-requests; request != 2 {
		t.Fatalf("continued request = %d", request)
	}
}

type queuedJobRequest struct {
	runnerPool core.RunnerPool
	response   chan queuedJobResponse
}

type queuedJobResponse struct {
	result runtimeengine.ProcessResult
	err    error
}

func assertNoQueuedJobRequest(t *testing.T, requests <-chan queuedJobRequest) {
	t.Helper()
	select {
	case request := <-requests:
		t.Fatalf("unexpected request for Runner Pool %q", request.runnerPool)
	case <-time.After(20 * time.Millisecond):
	}
}

func stopQueuedJobProcessor(t *testing.T, processor *queuedJobProcessor) {
	t.Helper()
	if err := processor.Stop(context.Background()); err != nil {
		t.Errorf("Stop() error = %v", err)
	}
}

func startQueuedJobTestProcessor(
	t *testing.T,
	requester queuedJobRequester,
	report func(error),
	eligible ...core.RunnerPool,
) *queuedJobProcessor {
	t.Helper()
	gate := newQueuedJobEligibilityGate()
	markQueuedJobPoolsEligible(t, gate, eligible...)
	processor := newQueuedJobProcessorForPools(requester, gate, report, eligible)
	if err := processor.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() { stopQueuedJobProcessor(t, processor) })
	return processor
}

func markQueuedJobPoolsEligible(
	t *testing.T,
	gate *queuedJobEligibilityGate,
	runnerPools ...core.RunnerPool,
) {
	t.Helper()
	for _, runnerPool := range runnerPools {
		if err := gate.MaintainPool(runnerPool, func() (bool, error) { return true, nil }); err != nil {
			t.Fatalf("mark Runner Pool %q eligible: %v", runnerPool, err)
		}
	}
}

func nextQueuedJobRequest(
	t *testing.T,
	requests <-chan queuedJobRequest,
	want core.RunnerPool,
) queuedJobRequest {
	t.Helper()
	request := <-requests
	if request.runnerPool != want {
		t.Fatalf("requested Runner Pool = %q, want %q", request.runnerPool, want)
	}
	return request
}

func collectQueuedJobOperationalEvents(
	t *testing.T,
	events chan<- operationalEvent,
) func(error) {
	t.Helper()
	return func(err error) {
		if err == nil {
			t.Error("processor used report(nil)")
			return
		}
		var envelope operationalEventEnvelope
		if !errors.As(err, &envelope) {
			t.Errorf("report = %T, want operational event", err)
			return
		}
		events <- envelope.event
	}
}

type queuedJobRequesterFake struct {
	request func(context.Context, core.RunnerPool) (runtimeengine.ProcessResult, error)
}

func (requester queuedJobRequesterFake) RequestWork(
	ctx context.Context,
	runnerPool core.RunnerPool,
) (runtimeengine.ProcessResult, error) {
	return requester.request(ctx, runnerPool)
}
