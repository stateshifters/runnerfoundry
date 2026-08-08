package daemon

import (
	"log/slog"
	"time"

	githubsource "github.com/meza/RunnerFoundry/internal/source/github"
)

func newGitHubManagerObserver(report func(error), currentTime func() time.Time) githubsource.ManagerObserver {
	return githubsource.ManagerObserver{
		CurrentTime: currentTime,
		Report: func(activity githubsource.ManagerActivity) {
			reportGitHubManagerActivity(report, activity)
		},
	}
}

func reportGitHubManagerActivity(report func(error), activity githubsource.ManagerActivity) {
	event := githubActivityEvent(activity)
	if activity.Err != nil {
		reportGitHubActivityFailure(report, event)
		return
	}
	level := slog.LevelInfo
	if activity.Visibility == githubsource.ActivityDebug {
		level = slog.LevelDebug
	}
	reportProgress(report, level, event)
	if activity.Stage == "reconciliation" && activity.Outcome == "completed" {
		reportOperational(report, operationalEvent{operation: activity.Operation, outcome: "recovered"})
	}
}

func githubActivityEvent(activity githubsource.ManagerActivity) operationalEvent {
	return operationalEvent{
		operation:         activity.Operation,
		stage:             activity.Stage,
		outcome:           activity.Outcome,
		runnerPool:        activity.RunnerPool,
		runnerName:        activity.RunnerName,
		repository:        activity.Repository,
		sourceRequestID:   activity.SourceRequestID,
		jobID:             activity.JobID,
		installationID:    activity.InstallationID,
		runnerGroupID:     activity.RunnerGroupID,
		scaleSetID:        activity.ScaleSetID,
		messageID:         activity.MessageID,
		githubEventType:   activity.EventType,
		stateBackend:      activity.StateBackend,
		stateOperation:    activity.StateOperation,
		sourcePC:          activity.OriginPC,
		count:             activity.Count,
		repositoryCount:   activity.RepositoryCount,
		installationCount: activity.InstallationCount,
		current:           activity.Current,
		total:             activity.Total,
		duration:          activity.Duration,
		err:               activity.Err,
	}
}

func reportGitHubActivityFailure(report func(error), event operationalEvent) {
	err := event.err
	if event.stateBackend == "" {
		event.diagnostic = err.Error()
	}
	reportOperational(report, event)
}
