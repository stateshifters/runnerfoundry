package postgres

import (
	"github.com/google/uuid"
	"github.com/stateshifters/runnerfoundry/internal/core"
)

var (
	testJobID1    = uuid.MustParse("00000000-0000-0000-0000-000000000001")
	testMemberIDA = core.MemberID("runner-a.example.test")
)
