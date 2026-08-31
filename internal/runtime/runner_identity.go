package runtime

import "github.com/stateshifters/runnerfoundry/internal/core"

func processingRunnerName(processing core.ProcessingJob) core.RunnerName {
	return processing.RunnerName
}

func isRunnerFoundryName(name core.RunnerName) bool {
	return core.IsRunnerFoundryRunnerName(name)
}
