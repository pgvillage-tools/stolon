package util

import (
	"reflect"

	"github.com/mitchellh/copystructure"
)

// ToPtr returns the value as a pointer
func ToPtr[T any](v T) *T {
	return &v
}

// DeepCopy returns a copy of the KeeperInfo resource
func DeepCopy[T any](org *T) (new *T) {
	var ok bool
	if org == nil {
		return nil
	}
	if nk, err := copystructure.Copy(org); err != nil {
		panic(err)
	} else if !reflect.DeepEqual(org, nk) {
		panic("not equal")
	} else if new, ok = nk.(*T); !ok {
		panic("different type after copy")
	}
	return new
}
