package konsumer

import "slices"

func Decorate[T any](base T, decorators ...func(next T) T) T {
	for _, decorator := range slices.Backward(decorators) {
		base = decorator(base)
	}
	return base
}
