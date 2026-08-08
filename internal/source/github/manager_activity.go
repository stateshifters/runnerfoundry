package github

import (
	"runtime"
	"time"

	"github.com/meza/RunnerFoundry/internal/core"
)

type ActivityVisibility string

const (
	ActivityInfo  ActivityVisibility = "info"
	ActivityDebug ActivityVisibility = "debug"
)

// ManagerActivity describes observable progress through GitHub-owned lifecycle work.
// It never contains credentials, request headers, or message payloads.
type ManagerActivity struct {
	Visibility         ActivityVisibility
	Operation          string
	Stage              string
	Outcome            string
	Repository         string
	RunnerPool         core.RunnerPool
	SourceRequestID    int64
	SourceRequestIDSet bool
	RunnerName         core.RunnerName
	EventType          string
	GitHubJobID        string
	WorkflowRunID      int64
	GitHubRunnerID     int64
	IdentityReason     string
	JobID              core.JobID
	InstallationID     int64
	RunnerGroupID      int64
	ScaleSetID         int64
	MessageID          int64
	StateBackend       string
	StateOperation     string
	Current            int
	Total              int
	Count              int
	RepositoryCount    int
	InstallationCount  int
	Duration           time.Duration
	Err                error
	OriginPC           uintptr
}

// ManagerObserver receives lifecycle observations serialized by Manager. Report
// must return promptly because active repository work waits for it. CurrentTime
// may be called concurrently and must be safe for parallel repository work.
type ManagerObserver struct {
	Report      func(ManagerActivity)
	CurrentTime func() time.Time
}

func (observer ManagerObserver) report(activity ManagerActivity) {
	if observer.Report != nil {
		captureManagerActivityOrigin(&activity, 1)
		observer.Report(activity)
	}
}

func (observer ManagerObserver) currentTime() time.Time {
	if observer.CurrentTime == nil {
		return time.Time{}
	}
	return observer.CurrentTime()
}

func (manager *Manager) report(activity ManagerActivity) {
	captureManagerActivityOrigin(&activity, 1)
	manager.activityMu.Lock()
	defer manager.activityMu.Unlock()
	manager.observer.report(activity)
}

func captureManagerActivityOrigin(activity *ManagerActivity, skip int) {
	if activity.OriginPC != 0 {
		return
	}
	programCounters := make([]uintptr, 1)
	if runtime.Callers(skip+2, programCounters) != 0 {
		activity.OriginPC = programCounters[0]
	}
}

func shouldReportManagerProgress(current int, total int) bool {
	if current == 1 || current == total {
		return true
	}
	return current*20/total > (current-1)*20/total
}
