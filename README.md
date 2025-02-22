```text
██   ██ ██ ██   ██ ██       ██   ██ ██    ██ 
 ██ ██  ██  ██ ██  ██       ██  ██  ██    ██ 
  ███   ██   ███   ██ █████ █████   ██    ██ 
 ██ ██  ██  ██ ██  ██       ██  ██   ██  ██  
██   ██ ██ ██   ██ ██       ██   ██   ████                                                
```
![GitHub top language](https://img.shields.io/github/languages/top/XiXi-2024/xixi-kv)   [![Go Reference](https://pkg.go.dev/badge/github.com/XiXi-2024/xixi-kv)](https://pkg.go.dev/github.com/XiXi-2024/xixi-kv)   ![LICENSE](https://img.shields.io/github/license/XiXi-2024/xixi-kv)   ![GitHub stars](https://img.shields.io/github/stars/XiXi-2024/xixi-kv)   ![GitHub forks](https://img.shields.io/github/forks/XiXi-2024/xixi-kv)   [![Go Report Card](https://goreportcard.com/badge/github.com/XiXi-2024/xixi-kv)](https://goreportcard.com/report/github.com/XiXi-2024/xixi-kv)![GitHub go.mod Go version (subdirectory of monorepo)](https://img.shields.io/github/go-mod/go-version/XiXi-2024/xixi-kv)![GitHub contributors](https://img.shields.io/github/contributors/XiXi-2024/xixi-kv)

English | [简体中文](README_CN.md)

xixi-kv is a lightweight key-logRecord storage engine based on the Bitcask model, featuring low-latency read/write operations, high throughput, and the ability to store data volumes exceeding available memory.

### Features
- Supports multiple idx implementations including B-Tree, persistent B+ Tree, and adaptive radix tree, allowing users to balance operational efficiency with storage capacity based on their needs.
- Implements batch transaction writes with atomicity and isolation through global locking and database startup identification mechanisms.
- Features a custom log record format with variable-length fields and self-implemented encoders/decoders to optimize storage efficiency.
- Employs the iterator pattern with unified interfaces at both idx and database layers for ordered traversal and extended operations on log records.
- Utilizes memory-mapped (MMap) IO management to accelerate idx building, improving startup speed when data volume is within available memory limits.

### Quick Start
For a complete example, see: [basic_operation.go](examples/basic_operation.go)

#### Installation
Install `Go` and run the `go get` command:
```shell
$ go get -u github.com/XiXi-2024/xixi-kv
```

#### Opening the Database
The core object in xixi-kv is `DB`. Use the `Open` method to create or open a database:
```go
package main

import (
	kv "github.com/XiXi-2024/xixi-kv"
	"log"
)

// Usage example
func main() {
	// Use default options, default path is system temp directory
	opts := kv.DefaultOptions
	db, err := kv.Open(opts)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	// ...
}
```

#### Basic Operations
```go
// Put
err = db.Put(key, logRecord)

// Get
val, err := db.Get(key)

// Delete
err = db.Delete(key)
```

### Single-Thread Benchmark
MacOS system with default configuration

| Operation | QPS (os.File) | Latency (os.File) | QPS (mmap) | Latency (mmap) |
|-----------|---------------|------------------|------------|----------------|
| Put       | 53703         | 27745            | 855710     | 1725          |
| Get       | 323540        | 3731             | 1767753    | 716.7         |
| Delete    | 415341        | 2921             | 2121006    | 627.7         |

### Known Issues
When running on `Windows` systems, ensure all open `DB` instances and files are explicitly closed before deletion. Otherwise, you may encounter errors like `The process cannot access the file because it is being used by another process.`

It is recommended to run on Mac or Linux environments, or manually delete generated files when testing on Windows.

### Contributions
This project is still under development, and contributions are very welcome! Feel free to submit issues and pull requests—I will respond as quickly as possible!
