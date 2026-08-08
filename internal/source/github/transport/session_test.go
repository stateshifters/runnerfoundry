package transport

import (
	"context"
	"errors"
	"testing"

	upstream "github.com/actions/scaleset"
	"github.com/google/uuid"
)

func TestSessionAcquireRequestsReturnsAcquiredSubset(t *testing.T) {
	t.Parallel()

	messageAPI := &messageSessionAPIFake{acquiredIDs: []int64{42}}
	session := &Session{messageAPI: messageAPI}

	acquired, err := session.AcquireRequests(context.Background(), []int64{41, 42})
	if err != nil {
		t.Fatalf("acquire requests: %v", err)
	}
	if len(messageAPI.requestedIDs) != 2 || messageAPI.requestedIDs[0] != 41 || messageAPI.requestedIDs[1] != 42 {
		t.Fatalf("requested IDs = %#v, want [41 42]", messageAPI.requestedIDs)
	}
	if len(acquired) != 1 || acquired[0] != 42 {
		t.Fatalf("acquired IDs = %#v, want [42]", acquired)
	}
}

func TestClientGenerateBootstrapReturnsOpaqueJITConfiguration(t *testing.T) {
	t.Parallel()

	jit := &jitAPIFake{config: &upstream.RunnerScaleSetJitRunnerConfig{EncodedJITConfig: "opaque-jit"}}
	client := &Client{jit: jit}

	bootstrap, err := client.GenerateBootstrap(context.Background(), Registration{ID: 17}, "linux-42")
	if err != nil {
		t.Fatalf("generate bootstrap: %v", err)
	}
	if string(bootstrap) != "opaque-jit" {
		t.Fatalf("bootstrap = %q", bootstrap)
	}
	if jit.runnerScaleSetID != 17 || jit.runnerName != "linux-42" {
		t.Fatalf("JIT request = scale set %d runner %q", jit.runnerScaleSetID, jit.runnerName)
	}
}

func TestOpenSessionExposesRepositoryAndRunnerPoolRoutingKey(t *testing.T) {
	t.Parallel()

	client := &Client{
		repositoryURL: "https://github.com/acme/widgets",
		openSession: func(context.Context, int, string) (messageSessionAPI, error) {
			return &messageSessionAPIFake{}, nil
		},
	}
	session, err := client.OpenSession(context.Background(), Registration{ID: 17, Name: "linux"}, "member-1")
	if err != nil {
		t.Fatalf("open session: %v", err)
	}
	if session.Key() != (SessionKey{Repository: "https://github.com/acme/widgets", RunnerPool: "linux"}) {
		t.Fatalf("session key = %#v", session.Key())
	}
}

func TestOpenSessionCopiesInitialStatisticsAndSessionIdentity(t *testing.T) {
	t.Parallel()

	sessionID := uuid.MustParse("1d2da680-4390-4c6f-9f04-2f12c518ac34")
	upstreamStatistics := &upstream.RunnerScaleSetStatistic{TotalAssignedJobs: 3}
	client := &Client{
		openSession: func(context.Context, int, string) (messageSessionAPI, error) {
			return upstreamMessageSession{sessionID: sessionID, initialStatistics: upstreamStatistics}, nil
		},
	}
	session, err := client.OpenSession(context.Background(), Registration{ID: 17}, "member-1")
	if err != nil {
		t.Fatalf("open session: %v", err)
	}
	upstreamStatistics.TotalAssignedJobs = 8
	statistics := session.InitialStatistics()
	if session.SessionID() != sessionID || statistics == nil || statistics.TotalAssignedJobs != 3 {
		t.Fatalf("initial session = (%s, %#v), want (%s, 3 assigned)",
			session.SessionID(), statistics, sessionID)
	}
	statistics.TotalAssignedJobs = 12
	if session.InitialStatistics().TotalAssignedJobs != 3 {
		t.Fatal("caller mutated retained initial statistics")
	}
}

func TestSessionPreservesErrNoMessageFromMessageBoundary(t *testing.T) {
	t.Parallel()

	session := &Session{messageAPI: &messageSessionAPIFake{}}
	message, err := session.GetMessage(context.Background(), 5, 3)
	if !errors.Is(err, ErrNoMessage) || message != nil {
		t.Fatalf("GetMessage = (%#v, %v), want nil ErrNoMessage", message, err)
	}
}

func TestUpstreamMessageSessionNormalizesAndWrapsBoundaryResults(t *testing.T) {
	t.Parallel()

	message := &upstream.RunnerScaleSetMessage{MessageID: 7}
	adapter := upstreamMessageSession{
		getMessage: func(context.Context, int, int) (*upstream.RunnerScaleSetMessage, error) {
			return message, nil
		},
		deleteMessage: func(context.Context, int) error { return nil },
		acquireJobs:   func(context.Context, []int64) ([]int64, error) { return []int64{42}, nil },
		closeSession:  func(context.Context) error { return nil },
	}
	if got, err := adapter.GetMessage(context.Background(), 0, 1); err != nil || got != message {
		t.Fatalf("GetMessage = (%#v, %v)", got, err)
	}
	if err := adapter.DeleteMessage(context.Background(), 7); err != nil {
		t.Fatalf("DeleteMessage: %v", err)
	}
	if acquired, err := adapter.AcquireJobs(context.Background(), []int64{42}); err != nil || len(acquired) != 1 {
		t.Fatalf("AcquireJobs = (%#v, %v)", acquired, err)
	}
	if err := adapter.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestUpstreamMessageSessionNormalizesNoMessage(t *testing.T) {
	t.Parallel()

	adapter := upstreamMessageSession{getMessage: (messageResult{}).get}
	if _, err := adapter.GetMessage(context.Background(), 0, 1); !errors.Is(err, ErrNoMessage) {
		t.Fatalf("empty GetMessage error = %v", err)
	}
}

func TestUpstreamMessageSessionWrapsFailures(t *testing.T) {
	t.Parallel()

	expectedErr := errors.New("upstream failed")
	adapter := upstreamMessageSession{
		getMessage:    (messageResult{err: expectedErr}).get,
		deleteMessage: func(context.Context, int) error { return expectedErr },
		acquireJobs:   func(context.Context, []int64) ([]int64, error) { return nil, expectedErr },
		closeSession:  func(context.Context) error { return expectedErr },
	}
	if _, err := adapter.GetMessage(context.Background(), 0, 1); !errors.Is(err, expectedErr) {
		t.Fatalf("GetMessage error = %v", err)
	}
	if err := adapter.DeleteMessage(context.Background(), 7); !errors.Is(err, expectedErr) {
		t.Fatalf("DeleteMessage error = %v", err)
	}
	if _, err := adapter.AcquireJobs(context.Background(), []int64{42}); !errors.Is(err, expectedErr) {
		t.Fatalf("AcquireJobs error = %v", err)
	}
	if err := adapter.Close(context.Background()); !errors.Is(err, expectedErr) {
		t.Fatalf("Close error = %v", err)
	}
}

type messageResult struct {
	message *upstream.RunnerScaleSetMessage
	err     error
}

func (result messageResult) get(
	context.Context,
	int,
	int,
) (*upstream.RunnerScaleSetMessage, error) {
	return result.message, result.err
}

type messageSessionAPIFake struct {
	requestedIDs []int64
	acquiredIDs  []int64
	message      *upstream.RunnerScaleSetMessage
	messageErr   error
	deleteErr    error
	acquireErr   error
	closeErr     error
}

func (api *messageSessionAPIFake) GetMessage(context.Context, int, int) (*upstream.RunnerScaleSetMessage, error) {
	if api.message == nil && api.messageErr == nil {
		return nil, ErrNoMessage
	}
	return api.message, api.messageErr
}

func (api *messageSessionAPIFake) DeleteMessage(context.Context, int) error { return api.deleteErr }

func (api *messageSessionAPIFake) Close(context.Context) error { return api.closeErr }

func (api *messageSessionAPIFake) AcquireJobs(_ context.Context, requestIDs []int64) ([]int64, error) {
	api.requestedIDs = append(api.requestedIDs, requestIDs...)
	return api.acquiredIDs, api.acquireErr
}

type jitAPIFake struct {
	config           *upstream.RunnerScaleSetJitRunnerConfig
	err              error
	runnerName       string
	runnerScaleSetID int
}

func (api *jitAPIFake) GenerateJitRunnerConfig(
	_ context.Context,
	setting *upstream.RunnerScaleSetJitRunnerSetting,
	runnerScaleSetID int,
) (*upstream.RunnerScaleSetJitRunnerConfig, error) {
	api.runnerName = setting.Name
	api.runnerScaleSetID = runnerScaleSetID
	return api.config, api.err
}

func TestSessionReportsMessageFailuresAndSuccess(t *testing.T) {
	t.Parallel()

	expectedErr := errors.New("transport failed")
	message := &upstream.RunnerScaleSetMessage{MessageID: 7}
	session := &Session{messageAPI: &messageSessionAPIFake{message: message}}
	gotMessage, err := session.GetMessage(context.Background(), 0, 2)
	if err != nil || gotMessage != message {
		t.Fatalf("GetMessage = (%#v, %v)", gotMessage, err)
	}

	session.messageAPI = &messageSessionAPIFake{messageErr: expectedErr, deleteErr: expectedErr, closeErr: expectedErr}
	if _, err := session.GetMessage(context.Background(), 0, 2); !errors.Is(err, expectedErr) {
		t.Fatalf("GetMessage error = %v", err)
	}
	if err := session.DeleteMessage(context.Background(), 7); !errors.Is(err, expectedErr) {
		t.Fatalf("DeleteMessage error = %v", err)
	}
	if err := session.Close(context.Background()); !errors.Is(err, expectedErr) {
		t.Fatalf("Close error = %v", err)
	}

	session.messageAPI = &messageSessionAPIFake{}
	if err := session.DeleteMessage(context.Background(), 7); err != nil {
		t.Fatalf("DeleteMessage: %v", err)
	}
	if err := session.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestSessionReportsAcquisitionFailures(t *testing.T) {
	t.Parallel()

	expectedErr := errors.New("transport failed")

	for _, testCase := range []struct {
		name       string
		requestIDs []int64
		messageAPI messageSessionAPI
	}{
		{name: "invalid request", requestIDs: []int64{42, 0}, messageAPI: &messageSessionAPIFake{}},
		{name: "acquire failure", requestIDs: []int64{42}, messageAPI: &messageSessionAPIFake{acquireErr: expectedErr}},
	} {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			testSession := &Session{messageAPI: testCase.messageAPI}
			if _, err := testSession.AcquireRequests(context.Background(), testCase.requestIDs); err == nil {
				t.Fatal("AcquireRequests succeeded")
			}
		})
	}
}

func TestClientReportsBootstrapFailures(t *testing.T) {
	t.Parallel()

	expectedErr := errors.New("transport failed")
	if _, err := (*Client)(nil).GenerateBootstrap(context.Background(), Registration{}, "runner"); err == nil {
		t.Fatal("nil client generated bootstrap")
	}

	for _, jit := range []jitAPI{&jitAPIFake{err: expectedErr}, &jitAPIFake{}} {
		client := &Client{jit: jit}
		if _, err := client.GenerateBootstrap(context.Background(), Registration{ID: 17}, "runner"); err == nil {
			t.Fatal("GenerateBootstrap succeeded")
		}
	}
}

func TestOpenSessionReportsConfigurationAndBoundaryFailures(t *testing.T) {
	t.Parallel()

	if _, err := (*Client)(nil).OpenSession(context.Background(), Registration{}, "owner"); err == nil {
		t.Fatal("nil client opened session")
	}
	expectedErr := errors.New("open failed")
	for _, testCase := range []struct {
		name        string
		openSession func(context.Context, int, string) (messageSessionAPI, error)
	}{
		{name: "open failure", openSession: func(context.Context, int, string) (messageSessionAPI, error) {
			return nil, expectedErr
		}},
	} {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			client := &Client{openSession: testCase.openSession}
			if _, err := client.OpenSession(context.Background(), Registration{ID: 17}, "owner"); err == nil {
				t.Fatal("OpenSession succeeded")
			}
		})
	}
}
