package types

import (
	"math"

	"github.com/google/uuid"
)

type Timestamp = int64

const MaxTimestamp = Timestamp(math.MaxInt64)

type UniqueID uuid.UUID

func NewUniqueID() UniqueID {
	return UniqueID(uuid.New())
}

func (id UniqueID) String() string {
	return uuid.UUID(id).String()
}

func MustParse(s string) UniqueID {
	return UniqueID(uuid.MustParse(s))
}

func Parse(s string) (UniqueID, error) {
	id, err := uuid.Parse(s)
	return UniqueID(id), err
}

func NilUniqueID() UniqueID {
	return UniqueID(uuid.Nil)
}

func ToUniqueID(idString *string) (UniqueID, error) {
	if idString != nil {
		id, err := Parse(*idString)
		if err != nil {
			return NilUniqueID(), err
		} else {
			return id, nil
		}
	} else {
		return NilUniqueID(), nil
	}
}

func FromUniqueID(id UniqueID) *string {
	var idStringPointer *string
	if id != NilUniqueID() {
		idString := id.String()
		idStringPointer = &idString
	} else {
		idStringPointer = nil
	}
	return idStringPointer
}
