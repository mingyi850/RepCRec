package utils

func GetRange(start int, end int, interval int) []int {
	result := make([]int, (end-start)/interval+1)
	for i := 0; i < len(result); i++ {
		result[i] = start + i*interval
	}
	return result
}

func GetMapKeys[K comparable, V any](m map[K]V) []K {
	keys := make([]K, len(m))
	i := 0
	for k := range m {
		keys[i] = k
		i++
	}
	return keys
}

func AddIfAbsent[K comparable, V any](m map[K]V, key K, value V) {
	if _, ok := m[key]; !ok {
		m[key] = value
	}
}
