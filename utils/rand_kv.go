package utils

import (
	"fmt"
	"math/rand"
	"time"
)

var (
	randStr = rand.New(rand.NewSource(time.Now().Unix()))
	letters = []byte("Wx234234234sdlfjlasWWDSFDFsdgfn2342352342345dfgasW")
)

// GetTestKey 根据 n 生成key 供测试使用
func GetTestKey(n int) []byte {
	return []byte(fmt.Sprintf("bitcask-go-key-%09d", n))
}

// RandomValue 生成长度为 n 的随机value 供测试使用
func RandomValue(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[randStr.Intn(len(letters))]
	}
	return []byte("bitcask-go-value" + string(b))
}
