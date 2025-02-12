```text
██   ██ ██ ██   ██ ██       ██   ██ ██    ██ 
 ██ ██  ██  ██ ██  ██       ██  ██  ██    ██ 
  ███   ██   ███   ██ █████ █████   ██    ██ 
 ██ ██  ██  ██ ██  ██       ██  ██   ██  ██  
██   ██ ██ ██   ██ ██       ██   ██   ████                                                
```
![GitHub top language](https://img.shields.io/github/languages/top/XiXi-2024/xixi-kv)   [![Go Reference](https://pkg.go.dev/badge/github.com/XiXi-2024/xixi-kv)](https://pkg.go.dev/github.com/XiXi-2024/xixi-kv)   ![LICENSE](https://img.shields.io/github/license/XiXi-2024/xixi-kv)   ![GitHub stars](https://img.shields.io/github/stars/XiXi-2024/xixi-kv)   ![GitHub forks](https://img.shields.io/github/forks/XiXi-2024/xixi-kv)   [![Go Report Card](https://goreportcard.com/badge/github.com/XiXi-2024/xixi-kv)](https://goreportcard.com/report/github.com/XiXi-2024/xixi-kv)![GitHub go.mod Go version (subdirectory of monorepo)](https://img.shields.io/github/go-mod/go-version/XiXi-2024/xixi-kv)![GitHub contributors](https://img.shields.io/github/contributors/XiXi-2024/xixi-kv)

xixi-kv 是基于 Bitcask 模型的轻量级 kv 存储引擎，具备读写低时延、高吞吐、超越内存容量的数据存储能力等核心特性。
### 特性
- 支持 B 树、可持久化 B+ 树、自适应基数树索引实现，用户可权衡操作效率与存储能力灵活选择合适的索引方案。
- 支持批量事务写入，通过全局锁和数据库启动识别机制实现事务的原子性与隔离性。
- 设计定制化的日志记录数据格式，采用变长字段并自实现相应的解编码器，优化日志记录的存储效率。
- 运用迭代器模式，在索引层和数据库层分别定义统一的迭代器接口，实现对日志记录的有序遍历和附加操作。
- 引入内存文件映射（MMap）IO管理实现加速索引构建，在数据量不超过可用内存大小的场景下提升启动速度。
### 快速入门
完整示例详见：[basic_operation.go](examples/basic_operation.go)
#### 安装
安装`Go`并运行`go get`命令
```shell
$ go get -u github.com/XiXi-2024/xixi-kv
```
#### 打开数据库
xixi-kv 的核心对象是`DB`，如果需要打开或创建数据库请使用`Open`方法
```go
package main

import (
	kv "github.com/XiXi-2024/xixi-kv"
	"log"
)

// 使用示例
func main() {
	// 提供默认用户配置项, 默认路径为系统临时目录
	opts := kv.DefaultOptions
	db, err := kv.Open(opts)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	// ...
}
```
#### 基础操作
```go
// 新增
err = db.Put(key, value)

// 获取
val, err := db.Get(key)

// 删除
err = db.Delete(key)
```
### 单线程基准测试
MacOS系统，默认配置

| 接口   | QPS (os.File) | 响应时间 (os.File) | QPS (mmap) | 相应时间 (mmap) |
|--------|---------------|----------------|------------|-------------|
| Put    | 53703         | 27745          | 855710     | 1725        |
| Get    | 323540        | 3731           | 1767753    | 716.7       |
| Delete | 415341        | 2921           | 2121006    | 627.7       |
### 问题
`Windows`系统中运行时需要确保所有打开的 `DB` 实例或文件显式关闭才能删除文件，否则会出现类似`The process cannot access the file because it is being used by another process.`的错误\
建议在 Mac 或 Linux 环境下运行或在 Windows 系统下测试时手动删除生成的文件。
### 贡献
项目仍不完善，非常欢迎 issue 和 pr，我会第一时间响应！