package daemon

import (
	"errors"
	"time"

	upstream "github.com/actions/scaleset"
	"github.com/meza/RunnerFoundry/internal/scaler"
)

func scaleSetMessageUpdate(
	repository string,
	scaleSet string,
	message *upstream.RunnerScaleSetMessage,
	observedAt time.Time,
) (scaler.MessageUpdate, error) {
	update := scaler.MessageUpdate{
		Repository: repository, ScaleSet: scaleSet, AssignedJobs: message.Statistics.TotalAssignedJobs,
		ObservedAt: observedAt,
	}
	var err error
	update.Available, err = availableMessageDemands(repository, message.JobAvailableMessages)
	if err != nil {
		return scaler.MessageUpdate{}, err
	}
	update.Started, err = startedMessageIdentities(message.JobStartedMessages)
	if err != nil {
		return scaler.MessageUpdate{}, err
	}
	update.Completed = completedMessageIdentities(message.JobCompletedMessages)
	return update, nil
}

func availableMessageDemands(repository string, messages []*upstream.JobAvailable) ([]scaler.JobDemand, error) {
	demands := make([]scaler.JobDemand, 0, len(messages))
	for _, available := range messages {
		if available == nil || available.RunnerRequestID <= 0 || available.QueueTime.IsZero() {
			return nil, errors.New("job available message requires a positive runner request ID and queue time")
		}
		demands = append(demands, scaler.JobDemand{
			Repository: repository, RequestID: available.RunnerRequestID, QueueTime: available.QueueTime,
		})
	}
	return demands, nil
}

func startedMessageIdentities(messages []*upstream.JobStarted) ([]scaler.RunnerMessageIdentity, error) {
	identities := make([]scaler.RunnerMessageIdentity, 0, len(messages))
	for _, started := range messages {
		if !jobStartedHasIdentity(started) {
			return nil, errors.New("job started message requires a positive runner request ID or runner name")
		}
		identities = append(identities, scaler.RunnerMessageIdentity{
			RunnerIdentity: scaler.RunnerIdentity{RequestID: started.RunnerRequestID, RunnerName: started.RunnerName},
			GitHubRunnerID: int64(started.RunnerID),
		})
	}
	return identities, nil
}

func completedMessageIdentities(messages []*upstream.JobCompleted) []scaler.RunnerMessageIdentity {
	identities := make([]scaler.RunnerMessageIdentity, 0, len(messages))
	for _, completed := range messages {
		if completed == nil || (completed.RunnerRequestID <= 0 && completed.RunnerName == "") {
			continue
		}
		identities = append(identities, scaler.RunnerMessageIdentity{
			RunnerIdentity: scaler.RunnerIdentity{RequestID: completed.RunnerRequestID, RunnerName: completed.RunnerName},
			GitHubRunnerID: int64(completed.RunnerID),
		})
	}
	return identities
}
