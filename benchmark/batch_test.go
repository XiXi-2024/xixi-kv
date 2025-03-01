package benchmark

import (
	kv "github.com/XiXi-2024/xixi-kv"
	"math/rand"
	"os"
	"testing"
)

func BenchmarkBatch_Put(b *testing.B) {
	db, dir := getDataAndDir()
	batch := db.NewBatch(kv.DefaultBatchOptions)
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

func BenchmarkBatch_Put_Parallel(b *testing.B) {
	db, dir := getDataAndDir()
	batch := db.NewBatch(kv.DefaultBatchOptions)
	defer func() {
		_ = db.Close()
		_ = os.RemoveAll(dir)
	}()

	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		idx := 0
		for pb.Next() {
			_ = batch.Put(keys[idx], value)
			idx++
		}
	})

	b.StopTimer()

	err := batch.Commit()
	if err != nil {
		panic(err)
	}
}

func BenchmarkBatch_Get(b *testing.B) {
	db, dir := getDataAndDir()
	defer func() {
		_ = db.Close()
		_ = os.RemoveAll(dir)
	}()
	for i := 0; i < b.N; i++ {
		x := rand.Intn(2)
		if x == 0 {
			_ = db.Put(keys[i], value)
		}
	}
	batch := db.NewBatch(kv.DefaultBatchOptions)
	for i := 0; i < b.N; i++ {
		x := rand.Intn(2)
		if x == 0 {
			_ = batch.Put(keys[i], value)
		}
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, _ = batch.Get(keys[i])
	}

	b.StopTimer()

	err := batch.Commit()
	if err != nil {
		panic(err)
	}
}

func BenchmarkBatch_Get_Parallel(b *testing.B) {
	db, dir := getDataAndDir()
	defer func() {
		_ = db.Close()
		_ = os.RemoveAll(dir)
	}()
	for i := 0; i < b.N; i++ {
		x := rand.Intn(2)
		if x == 0 {
			_ = db.Put(keys[i], value)
		}
	}
	batch := db.NewBatch(kv.DefaultBatchOptions)
	for i := 0; i < b.N; i++ {
		x := rand.Intn(2)
		if x == 0 {
			_ = batch.Put(keys[i], value)
		}
	}

	b.ReportAllocs()
	b.ResetTimer()

	idx := 0
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _ = batch.Get(keys[idx])
			idx++
		}
	})

	b.StopTimer()

	err := batch.Commit()
	if err != nil {
		panic(err)
	}
}

func BenchmarkBatch_Delete(b *testing.B) {
	db, dir := getDataAndDir()
	defer func() {
		_ = db.Close()
		_ = os.RemoveAll(dir)
	}()
	for i := 0; i < b.N; i++ {
		x := rand.Intn(2)
		if x == 0 {
			_ = db.Put(keys[i], value)
		}
	}
	batch := db.NewBatch(kv.DefaultBatchOptions)
	for i := 0; i < b.N; i++ {
		x := rand.Intn(2)
		if x == 0 {
			_ = batch.Put(keys[i], value)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := batch.Delete(keys[i])
		if err != nil {
			panic(err)
		}
	}
	b.StopTimer()
	err := batch.Commit()
	if err != nil {
		panic(err)
	}
}

func BenchmarkBatch_Delete_Parallel(b *testing.B) {
	db, dir := getDataAndDir()
	defer func() {
		_ = db.Close()
		_ = os.RemoveAll(dir)
	}()
	for i := 0; i < b.N; i++ {
		x := rand.Intn(2)
		if x == 0 {
			_ = db.Put(keys[i], value)
		}
	}
	batch := db.NewBatch(kv.DefaultBatchOptions)
	for i := 0; i < b.N; i++ {
		x := rand.Intn(2)
		if x == 0 {
			_ = batch.Put(keys[i], value)
		}
	}

	b.ReportAllocs()
	b.ResetTimer()

	idx := 0
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = batch.Delete(keys[idx])
			idx++
		}
	})

	b.StopTimer()

	err := batch.Commit()
	if err != nil {
		panic(err)
	}
}
