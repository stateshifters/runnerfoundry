package transport

import (
	"context"
	"errors"
	"fmt"

	upstream "github.com/actions/scaleset"
)

var ErrNoMessage = errors.New("no GitHub scale-set message")

type SessionKey struct {
	Repository string
	RunnerPool string
}

// Session owns one GitHub runner scale-set message session. Close must be
// called when polling ends.
type Session struct {
	messageAPI messageSessionAPI
	key        SessionKey
}

// OpenSession creates the message session used to poll one registered Runner Pool.
func (client *Client) OpenSession(
	ctx context.Context,
	registration Registration,
	owner string,
) (*Session, error) {
	if client == nil || client.openSession == nil {
		return nil, errors.New("GitHub session APIs are required")
	}
	messageAPI, err := client.openSession(ctx, registration.ID, owner)
	if err != nil {
		return nil, fmt.Errorf("open message session for runner scale set %d: %w", registration.ID, err)
	}
	return &Session{
		messageAPI: messageAPI,
		key:        SessionKey{Repository: client.repositoryURL, RunnerPool: registration.Name},
	}, nil
}

func (session *Session) Key() SessionKey {
	return session.key
}

type upstreamMessageSession struct {
	getMessage    func(context.Context, int, int) (*upstream.RunnerScaleSetMessage, error)
	deleteMessage func(context.Context, int) error
	acquireJobs   func(context.Context, []int64) ([]int64, error)
	closeSession  func(context.Context) error
}

func adaptUpstreamMessageSession(client *upstream.MessageSessionClient) upstreamMessageSession {
	return upstreamMessageSession{
		getMessage:    client.GetMessage,
		deleteMessage: client.DeleteMessage,
		acquireJobs:   client.AcquireJobs,
		closeSession:  client.Close,
	}
}

func (session upstreamMessageSession) GetMessage(
	ctx context.Context,
	lastMessageID int,
	maxCapacity int,
) (*upstream.RunnerScaleSetMessage, error) {
	message, err := session.getMessage(ctx, lastMessageID, maxCapacity)
	if err == nil && message == nil {
		return nil, ErrNoMessage
	}
	if err != nil {
		return nil, fmt.Errorf("receive upstream scale-set message: %w", err)
	}
	return message, nil
}

func (session upstreamMessageSession) DeleteMessage(ctx context.Context, messageID int) error {
	if err := session.deleteMessage(ctx, messageID); err != nil {
		return fmt.Errorf("delete upstream scale-set message: %w", err)
	}
	return nil
}

func (session upstreamMessageSession) AcquireJobs(ctx context.Context, requestIDs []int64) ([]int64, error) {
	acquiredIDs, err := session.acquireJobs(ctx, requestIDs)
	if err != nil {
		return nil, fmt.Errorf("acquire upstream jobs: %w", err)
	}
	return acquiredIDs, nil
}

func (session upstreamMessageSession) Close(ctx context.Context) error {
	if err := session.closeSession(ctx); err != nil {
		return fmt.Errorf("close upstream message session: %w", err)
	}
	return nil
}

func (session *Session) GetMessage(
	ctx context.Context,
	lastMessageID int,
	maxCapacity int,
) (*upstream.RunnerScaleSetMessage, error) {
	message, err := session.messageAPI.GetMessage(ctx, lastMessageID, maxCapacity)
	if err != nil {
		return nil, fmt.Errorf("get GitHub scale-set message: %w", err)
	}
	return message, nil
}

func (session *Session) DeleteMessage(ctx context.Context, messageID int) error {
	if err := session.messageAPI.DeleteMessage(ctx, messageID); err != nil {
		return fmt.Errorf("delete GitHub scale-set message %d: %w", messageID, err)
	}
	return nil
}

func (session *Session) Close(ctx context.Context) error {
	if err := session.messageAPI.Close(ctx); err != nil {
		return fmt.Errorf("close GitHub scale-set message session: %w", err)
	}
	return nil
}

// AcquireRequest associates one already-routed GitHub request with this
// session. The caller must route by SessionKey; this method never retries a
// rejected claim or acquires any other request.
func (session *Session) AcquireRequest(
	ctx context.Context,
	requestID int64,
) error {
	if requestID <= 0 {
		return fmt.Errorf("GitHub request ID %d must be positive", requestID)
	}
	acquiredIDs, err := session.messageAPI.AcquireJobs(ctx, []int64{requestID})
	if err != nil {
		return fmt.Errorf("acquire GitHub request %d: %w", requestID, err)
	}
	if !containsRequest(acquiredIDs, requestID) {
		return fmt.Errorf("GitHub request %d was not acquired", requestID)
	}
	return nil
}

// GenerateBootstrap asks GitHub for opaque JIT runner configuration without
// requiring ownership of a message session.
func (client *Client) GenerateBootstrap(
	ctx context.Context,
	registration Registration,
	runnerName string,
) ([]byte, error) {
	if client == nil || client.jit == nil {
		return nil, errors.New("GitHub JIT API is required")
	}
	jitConfig, err := client.jit.GenerateJitRunnerConfig(ctx, &upstream.RunnerScaleSetJitRunnerSetting{
		Name: runnerName,
	}, registration.ID)
	if err != nil {
		return nil, fmt.Errorf("generate JIT bootstrap for runner %q: %w", runnerName, err)
	}
	if jitConfig == nil || jitConfig.EncodedJITConfig == "" {
		return nil, fmt.Errorf("generate JIT bootstrap for runner %q returned empty configuration", runnerName)
	}
	return []byte(jitConfig.EncodedJITConfig), nil
}

func containsRequest(acquiredIDs []int64, requestID int64) bool {
	for _, acquiredID := range acquiredIDs {
		if acquiredID == requestID {
			return true
		}
	}
	return false
}
