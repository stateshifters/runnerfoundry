package supervisor

import githubsource "github.com/stateshifters/runnerfoundry/internal/source/github"

type reportedActivityError struct{ cause error }

func (reported reportedActivityError) Error() string      { return reported.cause.Error() }
func (reported reportedActivityError) Unwrap() error      { return reported.cause }
func (reportedActivityError) OperationallyReported() bool { return true }

func markOperationallyReported(err error) error {
	if err == nil {
		return nil
	}
	return reportedActivityError{cause: err}
}

func (source *Runtime) report(activity githubsource.ManagerActivity) {
	source.observer.ReportActivity(activity)
}
