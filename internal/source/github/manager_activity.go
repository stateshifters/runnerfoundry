package github

import (
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
	Visibility        ActivityVisibility
	Operation         string
	Stage             string
	Outcome           string
	Repository        string
	RunnerPool        core.RunnerPool
	SourceRequestID   int64
	Current           int
	Total             int
	Count             int
	RepositoryCount   int
	InstallationCount int
	Duration          time.Duration
	Err               error
}

// ManagerObserver receives synchronous lifecycle observations. Report must return
// promptly because manager work remains serialized. CurrentTime makes durations
// deterministic for callers and tests.
type ManagerObserver struct {
	Report      func(ManagerActivity)
	CurrentTime func() time.Time
}

func (observer ManagerObserver) report(activity ManagerActivity) {
	if observer.Report != nil {
		observer.Report(activity)
	}
}

func (observer ManagerObserver) currentTime() time.Time {
	if observer.CurrentTime == nil {
		return time.Time{}
	}
	return observer.CurrentTime()
}

func shouldReportManagerProgress(current int, total int) bool {
	if current == 1 || current == total {
		return true
	}
	return current*20/total > (current-1)*20/total
}
