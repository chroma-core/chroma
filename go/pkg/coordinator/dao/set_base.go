package dao

import (
	"errors"
	"github.com/google/uuid"
	"reflect"
)

type MSetBase interface {
	SetParentID(uuid2 uuid.UUID)
	ParentID() (uuid.UUID, bool)
	SetID(uuid2 uuid.UUID)
	ID() (uuid.UUID, bool)
	SetName(string)
	Name() (string, bool)
	SetCreatedAt(int64)
	CreatedAt() (int64, bool)
	SetUpdatedAt(int64)
	UpdatedAt() (int64, bool)
	SetDeletedAt(int64)
	DeletedAt() (int64, bool)
	SetVersion(int)
	Version() (int, bool)
}

func getValueByName(object interface{}, name string) (interface{}, error) {
	field := reflect.ValueOf(object).FieldByName(name)
	if field.IsValid() && field.CanInterface() {
		return field.Interface(), nil
	}
	return nil, errors.New("field not found")
}

func SetBase[K any](m MSetBase, base *K) error {
	parentID, err := getValueByName(*base, "ParentID")
	if err != nil {
		return err
	}
	if parentID.(uuid.UUID) != uuid.Nil {
		m.SetParentID(parentID.(uuid.UUID))
	}

	id, err := getValueByName(*base, "ID")
	if err != nil {
		return err
	}
	if id.(uuid.UUID) != uuid.Nil {
		m.SetID(id.(uuid.UUID))
	}

	name, err := getValueByName(*base, "Name")
	if err != nil {
		return err
	}
	if name.(*string) != nil {
		m.SetName(*name.(*string))
	}

	createdAt, err := getValueByName(*base, "CreatedAt")
	if err != nil {
		return err
	}
	if createdAt.(int64) != 0 {
		m.SetCreatedAt(createdAt.(int64))
	}

	updatedAt, err := getValueByName(*base, "UpdatedAt")
	if err != nil {
		return err
	}
	if updatedAt.(int64) != 0 {
		m.SetUpdatedAt(updatedAt.(int64))
	}

	deletedAt, err := getValueByName(*base, "DeletedAt")
	if err != nil {
		return err
	}
	if deletedAt.(int64) != 0 {
		m.SetDeletedAt(deletedAt.(int64))
	}

	version, err := getValueByName(*base, "Version")
	if err != nil {
		return err
	}
	if version.(int) != 0 {
		m.SetVersion(version.(int))
	}

	return nil
}
