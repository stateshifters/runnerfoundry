package postgres

import "github.com/google/uuid"

var (
	testJobID1    = uuid.MustParse("00000000-0000-0000-0000-000000000001")
	testJobID2    = uuid.MustParse("00000000-0000-0000-0000-000000000002")
	testJobID3    = uuid.MustParse("00000000-0000-0000-0000-000000000003")
	testJobID4    = uuid.MustParse("00000000-0000-0000-0000-000000000004")
	testJobID7    = uuid.MustParse("00000000-0000-0000-0000-000000000007")
	testJobID8    = uuid.MustParse("00000000-0000-0000-0000-000000000008")
	testMemberIDA = uuid.MustParse("00000000-0000-0000-0000-0000000000a1")
	testMemberIDB = uuid.MustParse("00000000-0000-0000-0000-0000000000b2")
)

func validNullUUID(identifier uuid.UUID) uuid.NullUUID {
	return uuid.NullUUID{UUID: identifier, Valid: true}
}

func invalidNullUUID() uuid.NullUUID {
	return uuid.NullUUID{}
}
