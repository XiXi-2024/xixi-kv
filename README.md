![xixi-kv-logo.jpg](./assets/images/xixi-kv-logo.jpg)\
![GitHub top language](https://img.shields.io/github/languages/top/XiXi-2024/xixi-kv)   [![Go Reference](https://pkg.go.dev/badge/github.com/XiXi-2024/xixi-kv)](https://pkg.go.dev/github.com/XiXi-2024/xixi-kv)   ![LICENSE](https://img.shields.io/github/license/XiXi-2024/xixi-kv)   ![GitHub stars](https://img.shields.io/github/stars/XiXi-2024/xixi-kv)   ![GitHub forks](https://img.shields.io/github/forks/XiXi-2024/xixi-kv)   [![Go Report Card](https://goreportcard.com/badge/github.com/XiXi-2024/xixi-kv)](https://goreportcard.com/report/github.com/XiXi-2024/xixi-kv)![GitHub go.mod Go version (subdirectory of monorepo)](https://img.shields.io/github/go-mod/go-version/XiXi-2024/xixi-kv)![GitHub contributors](https://img.shields.io/github/contributors/XiXi-2024/xixi-kv)

English | [简体中文](README_CN.md)

xixi-kv is a concurrent-safe key-value storage engine based on the Bitcask model, featuring low read/write latency, high throughput, and data storage capacity that exceeds memory limitations.
### Features

For more features and usage information, please refer to：[issues](https://github.com/XiXi-2024/xixi-kv/issues)

* Supports concurrent-safe sharded index implementation with multiple underlying index configuration options, including B-tree, skip list, and map.
* Provides high-performance batch processing with no maximum operation limit, guaranteeing atomicity, durability, and consistency.
* Supports both standard file I/O and memory-mapped file (MMap) implementations with corresponding configuration options, suitable for different data file capacity scenarios.
* Offers database-level iterator functionality with customizable iterator configuration options, allowing users to flexibly control data traversal methods.

### Quick Start
For a complete example, see：[main.go](examples/db/main.go)

#### Installation
Install Go and run the go get command:
```shell
go get -u github.com/XiXi-2024/xixi-kv
```
#### Opening the Database
The core object of xixi-kv is DB , which provides default configuration options via DefaultOptions . To open or create a database, use the Open method:
```go
package main

import kv "github.com/XiXi-2024/xixi-kv"

func main() {
	db, err := kv.Open(kv.DefaultOptions)
    // ...
}

```
#### Basic Operations
```go
// Insert
err = db.Put(key, logRecord)

// Retrieve
val, err := db.Get(key)

// Delete
err = db.Delete(key)
```
### Advanced Configuration

xixi-kv offers various configuration options that can be adjusted according to specific requirements:

```go
opts := kv.Options{
    DirPath:            "/path/to/data",    // Data directory 
    DataFileSize:       256 * 1024 * 1024,  // Data file size limit
    SyncStrategy:       kv.Threshold,       // Synchronization strategy
    BytesPerSync:       8 * 1024 * 1024,    // Bytes written before synchronization
    IndexType:          kv.BPTree,          // Index type
    ShardNum:           16,                 // Number of index shards
    FileIOType:         kv.MemoryMap,       // I/O type
    DataFileMergeRatio: 0.5,                // Merge trigger ratio
    EnableBackgroundMerge: true,            // Enable background merging
}
```

### Benchmark Tests

For complete testing details, see: [db_test.go](benchmark/db_test.go)

#### Environment

```go
goos: darwin
goarch: arm64
cpu: Apple M1
```

#### os.File

| Interface   | QPS（Single Thread） | QPS（Multi-Thread） |
| ------ | ------------- | ------------- |
| Put    | 444279        | 376621        |
| Get    | 1002304       | 2370576       |
| Delete | 1297602       | 1006764       |

#### mmap

| Interface   | QPS（Single Thread） | QPS（Multi-Thread） |
| ------ | ------------- | ------------- |
| Put    | 1004450       | 1000326       |
| Get    | 2210830       | 8933606       |
| Delete | 6813520       | 4509661       |

### Notes

When running on Windows systems, ensure that all open DB instances or files are explicitly closed before attempting to delete files, otherwise you may encounter the following error:

```plaintext
The process cannot access the file because it is being used by another process.
```

It is recommended to run on macOS or Linux environments, or manually delete generated files when testing on Windows systems.

### Contribution

As the project continues to grow, I recognize the limitations of individual effort. There are still many issues to resolve and challenging features to implement. If you're interested in this project, I warmly welcome your issues and pull requests, and I'll respond promptly!