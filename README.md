# Kvstore: A High-Performance Distributed Key-Value Engine

Kvstore is a lightweight, horizontally scalable, distributed Key-Value database written entirely from scratch in Go. 

It bypasses standard Go networking libraries to implement a custom, single-threaded **epoll event loop** using raw Linux syscalls, capable of processing over **1.25 Million requests per second** on a single core. It features a custom RESP (REdis Serialization Protocol) parser, asynchronous Master-Follower replication, and a Consistent Hashing topology for infinite horizontal scale.

## 🚀 Architectural Highlights

* **Pure `epoll` Multiplexer:** Uses `golang.org/x/sys/unix` to manipulate raw Linux File Descriptors, avoiding Go's `net.Conn` goroutine-per-connection overhead.
* **Zero-Allocation Parsing:** The RESP parsing engine utilizes in-place slice manipulation and zero-allocation ASCII uppercase shifting to virtually eliminate Garbage Collection pauses during heavy benchmarks.
* **Asynchronous WAL Shipping:** Master nodes instantly stream Write-Ahead Log (WAL) RESP bytes to connected replicas via non-blocking TCP sockets, providing Read-Scaling and Durability without compromising write latency.
* **Consistent Hashing Mesh:** Implements a decentralized ring topology using `hash/crc32` and Virtual Nodes (VNodes) to prevent data hotspots.
* **Native Redirection:** Nodes actively intercept misrouted keys and reply with standard `-MOVED <slot> <ip:port>` errors, allowing seamless integration with standard `redis-cli -c` cluster clients.

## 🛠️ Getting Started

### Prerequisites
* Go 1.20+
* Linux OS (required for native `epoll` syscalls)
* `redis-cli` (for testing)

### Booting the Cluster

You can easily spin up a 3-node cluster mesh locally using the provided `Makefile`. Open three separate terminals:

**Terminal 1:**
```bash
make node1