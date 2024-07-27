package slicesext

func GroupBy[T any, K comparable](xs []T, f func(T) K) map[K][]T {
	groups := make(map[K][]T, 0)

	for _, x := range xs {
		key := f(x)
		groups[key] = append(groups[key], x)
	}

	return groups
}

func Map[T any, U any](xs []T, f func(T) U) []U {
	out := make([]U, 0, len(xs))

	for _, x := range xs {
		out = append(out, f(x))
	}

	return out
}

func MapIndex[T any, U any](xs []T, f func(int, T) U) []U {
	out := make([]U, 0, len(xs))

	for i, x := range xs {
		out = append(out, f(i, x))
	}

	return out
}

func Find[T any](xs []T, predicate func(T) bool) *T {
	for _, x := range xs {
		if predicate(x) {
			return &x
		}
	}

	return nil
}
