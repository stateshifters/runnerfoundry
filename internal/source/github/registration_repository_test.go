package github_test

import (
	githubsource "github.com/stateshifters/runnerfoundry/internal/source/github"
	"github.com/stateshifters/runnerfoundry/internal/source/github/transport"
)

func upsertActiveRecord(records []githubsource.Registration, replacement githubsource.Registration) []githubsource.Registration {
	for index, record := range records {
		if record.RepositoryURL == replacement.RepositoryURL && record.RunnerPool == replacement.RunnerPool {
			result := append([]githubsource.Registration(nil), records...)
			result[index] = replacement
			return result
		}
	}
	return append(append([]githubsource.Registration(nil), records...), replacement)
}

func upsertOwnedRecord(records []githubsource.Registration, replacement githubsource.Registration) []githubsource.Registration {
	for index, record := range records {
		if record.RepositoryURL == replacement.RepositoryURL && record.ScaleSetID == replacement.ScaleSetID {
			result := append([]githubsource.Registration(nil), records...)
			result[index] = replacement
			return result
		}
	}
	return append(append([]githubsource.Registration(nil), records...), replacement)
}

func removeExactRecord(records []githubsource.Registration, removal githubsource.Registration) []githubsource.Registration {
	result := make([]githubsource.Registration, 0, len(records))
	for _, record := range records {
		if record != removal {
			result = append(result, record)
		}
	}
	return result
}

func removeRepositoryRecords(
	records []githubsource.Registration,
	repositoryURL string,
	installationID transport.InstallationID,
) []githubsource.Registration {
	result := make([]githubsource.Registration, 0, len(records))
	for _, record := range records {
		if record.RepositoryURL != repositoryURL || record.InstallationID != installationID {
			result = append(result, record)
		}
	}
	return result
}
