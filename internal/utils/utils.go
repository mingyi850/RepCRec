package utils

func GetRange(start int, end int, interval int) []int {
	result := make([]int, (end-start)/interval+1)
	for i := 0; i < len(result); i++ {
		result[i] = start + i*interval
	}
	return result
}
