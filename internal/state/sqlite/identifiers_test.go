package sqlite

import (
	"github.com/google/uuid"
	"github.com/stateshifters/runnerfoundry/internal/core"
)

const (
	testJobIDText    = "00000000-0000-4000-8000-000000000007"
	testMemberIDText = "runner-a.example.test"
)

var (
	testJobID    = uuid.MustParse(testJobIDText)
	testMemberID = core.MemberID(testMemberIDText)
)
