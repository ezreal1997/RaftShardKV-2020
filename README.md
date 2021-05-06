# RaftShardKV

[6.824 - Spring 2020](http://nil.lcs.mit.edu/6.824/2020/schedule.html)

- [x] Lab1 MapReduce
- [x] Lab2 Raft
  - [x] Part A: Raft leader election and heartbeats
  - [x] Part B: Append new log entries
  - [x] Part C: Persistent state for Reboot
- [x] Lab3 Fault-tolerant Key/Value Service
  - [x] Part A: Key/value service without log compaction
  - [x] Part B: Key/value service with log compaction (Snapshot)
- [x] Lab4 Sharded Key/Value Service
  - [x] Part A: Shard Master (Config Server)
  - [x] Part B: Sharded Key/Value Server
  - [x] Challenge I: Garbage collection of state
  - [x] Challenge II: Client requests during configuration changes

```bash
GO111MODULE=off
go version go1.14.3 darwin/amd64
```

- Good

  Lab 1-4 使用脚本测试 500 次未出错。

- Bad

  Raft 没有实现 [no-op entry](https://github.com/hashicorp/raft/blob/master/raft.go#L442)，虽然出问题的触发条件极为严格，但也是个隐患。

  
