package transport

import (
	"context"
	"errors"
	"net/http"
	"testing"

	upstream "github.com/actions/scaleset"
	"github.com/google/uuid"
)

func TestSessionPreservesBoundaryErrorClassificationsAndClosesIdleConnections(t *testing.T) {
	t.Parallel()
	for _, expected := range []error{nil, upstream.UnauthorizedError, upstream.NotFoundError, upstream.ConflictError, errors.New("rate limited"), errors.New("server unavailable")} {
		exerciseSessionBoundaryError(t, expected)
	}
}

func exerciseSessionBoundaryError(t *testing.T, expected error) {
	t.Helper()
	boundary := &idleConnectionBoundary{}
	session := &Session{messageAPI: &messageSessionAPIFake{messageErr: expected, deleteErr: expected, acquireErr: expected, closeErr: expected}, httpClient: &http.Client{Transport: boundary}}
	_, acquireErr := session.AcquireRequests(t.Context(), []int64{42})
	for _, actual := range []error{acquireErr, session.DeleteMessage(t.Context(), 7), session.Close(t.Context())} {
		if !errors.Is(actual, expected) {
			t.Fatalf("session error = %v, want %v", actual, expected)
		}
	}
	if boundary.closed != 1 {
		t.Fatalf("idle cleanup calls = %d, want 1 after close attempt", boundary.closed)
	}
	if expected != nil {
		if _, err := session.GetMessage(t.Context(), 0, 1); !errors.Is(err, expected) {
			t.Fatalf("poll error = %v, want %v", err, expected)
		}
	}
}

type idleConnectionBoundary struct{ closed int }

func (boundary *idleConnectionBoundary) RoundTrip(*http.Request) (*http.Response, error) {
	return nil, errors.New("unexpected request")
}
func (boundary *idleConnectionBoundary) CloseIdleConnections() { boundary.closed++ }

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
		openSession: func(context.Context, int, string, ...upstream.HTTPOption) (messageSessionAPI, error) {
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
		openSession: func(context.Context, int, string, ...upstream.HTTPOption) (messageSessionAPI, error) {
			return &messageSessionAPIFake{metadata: upstream.RunnerScaleSetSession{SessionID: sessionID, Statistics: upstreamStatistics}}, nil
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

func TestSessionNormalizesNilMessageSuccess(t *testing.T) {
	t.Parallel()

	session := &Session{messageAPI: &messageSessionAPIFake{}}
	message, err := session.GetMessage(context.Background(), 5, 3)
	if !errors.Is(err, ErrNoMessage) || message != nil {
		t.Fatalf("GetMessage = (%#v, %v), want nil ErrNoMessage", message, err)
	}
}

type messageSessionAPIFake struct {
	metadata     upstream.RunnerScaleSetSession
	requestedIDs []int64
	acquiredIDs  []int64
	message      *upstream.RunnerScaleSetMessage
	messageErr   error
	deleteErr    error
	acquireErr   error
	closeErr     error
}

func (api *messageSessionAPIFake) Session() upstream.RunnerScaleSetSession { return api.metadata }

func (api *messageSessionAPIFake) GetMessage(context.Context, int, int) (*upstream.RunnerScaleSetMessage, error) {
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
	runner           *upstream.RunnerReference
	removedRunnerID  int64
	lookupErr        error
	removeErr        error
}

func (api *jitAPIFake) GetRunnerByName(context.Context, string) (*upstream.RunnerReference, error) {
	return api.runner, api.lookupErr
}

func (api *jitAPIFake) RemoveRunner(_ context.Context, runnerID int64) error {
	api.removedRunnerID = runnerID
	return api.removeErr
}

func TestCleanupRunnerRegistrationIsIdempotent(t *testing.T) {
	t.Parallel()
	if err := (*Client)(nil).CleanupRunner(context.Background(), "runner"); err == nil {
		t.Fatal("nil client cleanup was accepted")
	}
	api := &jitAPIFake{runner: &upstream.RunnerReference{ID: 42}}
	client := &Client{jit: api}
	if err := client.CleanupRunner(context.Background(), "runner-attempt"); err != nil || api.removedRunnerID != 42 {
		t.Fatalf("cleanup = %v, removed runner = %d", err, api.removedRunnerID)
	}
	api.runner = nil
	if err := client.CleanupRunner(context.Background(), "missing"); err != nil {
		t.Fatalf("missing cleanup = %v", err)
	}
	api.runner = &upstream.RunnerReference{ID: 43}
	api.removeErr = upstream.NotFoundError
	if err := client.CleanupRunner(context.Background(), "raced"); err != nil {
		t.Fatalf("raced cleanup = %v", err)
	}
	wantErr := errors.New("upstream failed")
	api.lookupErr, api.removeErr = wantErr, nil
	if err := client.CleanupRunner(context.Background(), "lookup-failed"); !errors.Is(err, wantErr) {
		t.Fatalf("lookup error = %v", err)
	}
	api.lookupErr, api.removeErr = nil, wantErr
	if err := client.CleanupRunner(context.Background(), "remove-failed"); !errors.Is(err, wantErr) {
		t.Fatalf("remove error = %v", err)
	}
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
		openSession func(context.Context, int, string, ...upstream.HTTPOption) (messageSessionAPI, error)
	}{
		{name: "open failure", openSession: func(context.Context, int, string, ...upstream.HTTPOption) (messageSessionAPI, error) {
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
