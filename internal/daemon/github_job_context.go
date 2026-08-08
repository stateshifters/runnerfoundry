package daemon

import (
	"fmt"

	"github.com/meza/RunnerFoundry/internal/core"
	githubsource "github.com/meza/RunnerFoundry/internal/source/github"
)

func addGitHubJobContext(report func(error), event operationalEvent, job core.Job) operationalEvent {
	if job.Source.Adapter == "" {
		return event
	}
	if job.Source.Adapter != githubsource.AdapterName {
		return event
	}
	identity, err := githubsource.DecodeSourceID(job.Source.ID)
	if err == nil && identity.RunnerPool != job.RunnerPool {
		err = fmt.Errorf("GitHub source Runner Pool %q does not match durable Runner Pool %q",
			identity.RunnerPool, job.RunnerPool)
	}
	if err != nil {
		failure := event
		failure.component = "github_identity"
		failure.operation = "job_context"
		failure.stage = "decode_source_identity"
		failure.outcome = "failed"
		failure.err = err
		failure.diagnostic = err.Error()
		reportOperational(report, failure)
		return event
	}
	event.repository = identity.Repository
	event.sourceRequestID = identity.RequestID
	return event
}
