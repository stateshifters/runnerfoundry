package sqlite

import "github.com/google/uuid"

const (
	testJobIDText    = "00000000-0000-4000-8000-000000000007"
	testMemberIDText = "00000000-0000-4000-8000-00000000000a"
)

var (
	testJobID    = uuid.MustParse(testJobIDText)
	testMemberID = uuid.MustParse(testMemberIDText)
)
