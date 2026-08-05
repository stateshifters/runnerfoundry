package cluster

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/meza/RunnerFoundry/internal/scaler"
)

func clusterLifecycleAdvisoryKey(
	scaleSet, repository string,
	identity scaler.RunnerIdentity,
) string {
	identityKey := strings.TrimSpace(identity.RunnerName)
	if identity.RequestID > 0 {
		identityKey = strconv.FormatInt(identity.RequestID, 10)
	}
	return strings.Join([]string{scaleSet, repository, identityKey}, "\x1f")
}

func clusterLifecycleLockedStatement(lockParameter int, mutation string) string {
	return fmt.Sprintf(`WITH locked AS MATERIALIZED (
		SELECT pg_advisory_xact_lock(hashtextextended($%d, 0))
	)
	%s`, lockParameter, mutation)
}
