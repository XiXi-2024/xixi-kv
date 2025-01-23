package utils

import "strconv"

// FloatFromBytes 将给定[]byte类型转换为float64
func FloatFromBytes(val []byte) float64 {
	f, _ := strconv.ParseFloat(string(val), 64)
	return f
}

// Float64ToBytes 将给定float64类型转换为[]byte
func Float64ToBytes(val float64) []byte {
	return []byte(strconv.FormatFloat(val, 'f', -1, 64))
}
