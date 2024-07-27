package mapsext

func Values[K comparable, V any](m map[K]V) []V {
	out := make([]V, 0, len(m))

	for _, v := range m {
		out = append(out, v)
	}

	return out
}

func Map[K comparable, V any, KK comparable, VV any](m map[K]V, f func(K, V) (KK, VV)) map[KK]VV {
	out := make(map[KK]VV, len(m))

	for k, v := range m {
		newK, newV := f(k, v)
		out[newK] = newV
	}

	return out
}
