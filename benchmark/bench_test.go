package benchmark

import (
	xixi_kv "github.com/XiXi-2024/xixi-kv"
	"github.com/XiXi-2024/xixi-kv/utils"
	"os"
	"testing"
)

var n = 10000000
var keys [][]byte
var value []byte

func init() {
	keys = make([][]byte, n)
	value = utils.RandomValue(1024)
	for i := 0; i < n; i++ {
		keys[i] = utils.GetTestKey(i)
	}
}

func BenchmarkDB_Put(b *testing.B) {
	db, dir := getDataAndDir()
	defer func() {
		_ = db.Close()
		_ = os.RemoveAll(dir)
	}()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = db.Put(keys[i], value)
	}
	b.StopTimer()
}

func BenchmarkDB_Put_Parallel(b *testing.B) {
	db, dir := getDataAndDir()
	defer func() {
		_ = db.Close()
		_ = os.RemoveAll(dir)
	}()
	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		idx := 0
		for pb.Next() {
			_ = db.Put(keys[idx], value)
			idx++
		}
	})

	b.StopTimer()
}

func BenchmarkDB_Get(b *testing.B) {
	db, dir := getDataAndDir()
	defer func() {
		_ = db.Close()
		_ = os.RemoveAll(dir)
	}()
	for i := 0; i < b.N; i++ {
		_ = db.Put(keys[i], value)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = db.Get(keys[i])
	}
	b.StopTimer()
}

func BenchmarkDB_Get_Parallel(b *testing.B) {
	db, dir := getDataAndDir()
	defer func() {
		_ = db.Close()
		_ = os.RemoveAll(dir)
	}()
	for i := 0; i < b.N; i++ {
		_ = db.Put(keys[i], value)
	}
	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		idx := 0
		for pb.Next() {
			_, _ = db.Get(keys[idx])
			idx++
		}
	})

	b.StopTimer()
}

func BenchmarkDB_Delete(b *testing.B) {
	db, dir := getDataAndDir()
	defer func() {
		_ = db.Close()
		_ = os.RemoveAll(dir)
	}()
	for i := 0; i < b.N; i++ {
		_ = db.Put(keys[i], value)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = db.Delete(keys[i])
	}
	b.StopTimer()
}

func BenchmarkDB_Delete_Parallel(b *testing.B) {
	db, dir := getDataAndDir()
	defer func() {
		_ = db.Close()
		_ = os.RemoveAll(dir)
	}()
	for i := 0; i < b.N; i++ {
		_ = db.Put(keys[i], value)
	}
	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		idx := 0
		for pb.Next() {
			_ = db.Delete(keys[idx])
			idx++
		}
	})

	b.StopTimer()
}

func BenchmarkBatch_Put(b *testing.B) {
	db, dir := getDataAndDir()
	batch := db.NewBatch(xixi_kv.DefaultBatchOptions)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := batch.Put(keys[i], value)
		if err != nil {
			panic(err)
		}
	}
	err := batch.Commit()
	if err != nil {
		panic(err)
	}
	b.StopTimer()
	_ = db.Close()
	_ = os.RemoveAll(dir)
}

func BenchmarkBatch_Delete(b *testing.B) {
	db, dir := getDataAndDir()
	for i := 0; i < b.N; i++ {
		_ = db.Put(keys[i], value)
	}
	var batch *xixi_kv.Batch
	batch = db.NewBatch(xixi_kv.DefaultBatchOptions)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := batch.Delete(keys[i])
		if err != nil {
			panic(err)
		}
	}
	err := batch.Commit()
	if err != nil {
		panic(err)
	}
	_ = db.Close()
	_ = os.RemoveAll(dir)
}

func getDataAndDir() (*xixi_kv.DB, string) {
	opts := xixi_kv.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-benchmark")
	opts.DirPath = dir
	var err error
	db, err := xixi_kv.Open(opts)
	if err != nil {
		panic(err)
	}
	return db, dir
}
