package main

import (
	"fmt"
	kv "github.com/XiXi-2024/xixi-kv"
	"log"
)

func main() {
	opts := kv.DefaultOptions
	db, err := kv.Open(opts)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// 新增
	key, value := []byte("key"), []byte("value")
	err = db.Put(key, value)
	if err != nil {
		log.Fatal(err)
	}

	// 获取
	val, err := db.Get(key)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("%s\n", val)

	// 删除
	err = db.Delete(key)
	if err != nil {
		log.Fatal(err)
	}
}
