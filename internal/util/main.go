package util

// ToPtr returns the value as a pointer
func ToPtr[T any](v T) *T {
	return &v
}
