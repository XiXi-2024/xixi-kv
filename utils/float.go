package utils

import "strconv"

// FloatFromBytes 将 []byte 数据转换为 float64 类型
func FloatFromBytes(val []byte) float64 {
	f, _ := strconv.ParseFloat(string(val), 64)
	return f
}

// Float64ToBytes 将 float64 数据转换为 []byte 类型
func Float64ToBytes(val float64) []byte {
	return []byte(strconv.FormatFloat(val, 'f', -1, 64))
}
