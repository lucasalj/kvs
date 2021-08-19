<h1 align="center">kvs</h1>
<p align="center">A Log-Structured Key-value store that implements a minimal user interface.
<br />This project is based on the Rust course on <a href="https://github.com/pingcap/talent-plan/tree/master/courses/rust">PingCAP Talent-Plan</a></p>

<p align="center"><img src="https://img.shields.io/github/last-commit/lucasalj/kvs/main" /></p>

<p align="center">
 <a href="#objective">Objective</a> • 
 <a href="#features">Features</a> • 
 <a href="#how-to-run-it">How to run it</a>
</p>

<h4 align="center"> 
	🚧  Key value store 🚀 Under development...  🚧
</h4>

## Objective

This projects aims to be a Log-Structured key-value store with a minimal and user friendly interface. It was meant to teach myself how to implement a simplified version of a storage engine with replication and distributed consensus using Rust language. 

## Features

- [X] Storage engine:
  - [X] Automatic compaction
  - [ ] Asynchronous file I/O
  - [ ] Replicaiton and Raft Consensus
- [X] Client app
  - [X] Command Line Interface
    - [X] Insertion/Update (set)
    - [X] Read (get)
    - [X] Remove (rm)
  - [X] Server communication through hand-maid protocol over TCP/IP 
  - [ ] Asynchronous communication
- [X] Server app
  - [X] Command Line Interface
  - [X] Suppport for choosing between 2 storage engines: kvs (hand-maid), sled (real world engine)
  - [X] Multi-threaded execution
    - [X] Parallel execution by enabling lock-free reads
  - [X] Interval log checks for triggering compaction
  - [ ] Asynchronous communication
 
## How to run it

1. To be able to run it, first you need to compile the project using the **Cargo** packet manager.
2. Then you should start the server app.
3. Then you may send commands to the server and see the results using the client app.

### Server

* To display the help menu, type:
```
$ kvs-server --help
```

* To run the server on the current shell session with default IP and port (127.0.0.1:4000) with kvs engine:
```
$ kvs-server
```

* To run the server on the current shell session with IP 127.0.0.1 and port 4001 with kvs engine:
```
$ kvs-server --addr '127.0.0.1:4001'
```

### Client

* To display the help menu, type:
```
$ kvs-client --help
```

* To send a command **set** to the server with key **key0** and value **value0**:
```
$ kvs-client set key0 value0
```

* To send a command **get** to the server and receive the value associated with **key0** back:
```
$ kvs-client get key0
```

* To send a command **rm** to the server and remove the value associated with **key0**:
```
$ kvs-client rm key0
```
