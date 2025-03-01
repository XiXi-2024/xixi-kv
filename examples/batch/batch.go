package main

import (
	"fmt"
	kv "github.com/XiXi-2024/xixi-kv"
	"log"
)

func main() {
	db, err := kv.Open(kv.DefaultOptions)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		_ = db.Close()
	}()

	// 新建批处理客户端
	batch := db.NewBatch(kv.DefaultBatchOptions)

	// 新增
	key, value := []byte("key"), []byte("value")
	err = batch.Put(key, value)
	if err != nil {
		log.Fatal(err)
	}

	// 读取
	val, err := batch.Get(key)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("%s\n", val)

	// 删除
	err = batch.Delete(key)
	if err != nil {
		log.Fatal(err)
	}

	// 提交
	err = batch.Commit()
	if err != nil {
		log.Fatal(err)
	}
}
