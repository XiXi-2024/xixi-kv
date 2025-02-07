# xixi-bitcask-kv
[简体中文](README_CN.md)

A lightweight key-value storage engine based on the Bitcask model. It offers low-latency read/write operations, high throughput, and the capability to store data volumes exceeding available memory. Written in Golang.

# Highlights
- Supports B-Tree, persistent B+ Tree, and adaptive radix tree indexes, allowing you to balance operational efficiency with storage capacity and select the most appropriate indexing scheme for your needs.
- Ensures transaction atomicity and isolation through a global lock and a database startup identification mechanism.
- Utilizes a custom log record format with variable-length fields and bespoke encoders/decoders to maximize storage efficiency.
- Defines unified iterator interfaces at both the index and database layers, enabling ordered traversal and extended operations on log records.
- Leverages memory-mapped IO (MMap) to speed up index building, improving startup times when the data size is within available memory limits.
# Known Issues
On Windows systems, you must explicitly close all open database instances or files before attempting to delete them. Failing to do so may result in errors like:
```
The process cannot access the file because it is being used by another process.
```
For a smoother experience, consider running the application on macOS or Linux, or manually delete generated files when testing on Windows.
# Contributions
This project is still under development, and contributions are very welcome. Please feel free to submit issues or pull requests—I will respond as quickly as possible!
