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
		reportGitHubActivityFailure(report, activity.Visibility, event)
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
		repository:        activity.Repository,
		sourceRequestID:   activity.SourceRequestID,
		count:             activity.Count,
		repositoryCount:   activity.RepositoryCount,
		installationCount: activity.InstallationCount,
		current:           activity.Current,
		total:             activity.Total,
		duration:          activity.Duration,
		err:               activity.Err,
	}
}

func reportGitHubActivityFailure(report func(error), visibility githubsource.ActivityVisibility, event operationalEvent) {
	err := event.err
	event.err = nil
	event.diagnostic = err.Error()
	reportProgress(report, slog.LevelDebug, event)
	if visibility != githubsource.ActivityInfo {
		return
	}
	event.diagnostic = ""
	event.err = err
	reportOperational(report, event)
}
