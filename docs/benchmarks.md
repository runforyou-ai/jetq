# 基准测试

`natsq/bench_test.go` 测量领取路径（`Consumer.Receive`）在账本中保留记录数不同时的耗时，用来说明领取成本随**保留中**的记录数增长，而不是随可运行任务数增长。

这些是在单机内存 nats-server 上的微基准：它们**不**测量吞吐量、端到端延迟或生产环境的稳定性，也不构成容量承诺。

## 运行

```sh
go test -run '^$' -bench Receive -benchtime 30x ./natsq
```

## 测量内容

- `BenchmarkReceiveEmptyLedger`：空账本上每次领取。
- `BenchmarkReceiveThousandRetained`：账本中另有 1,000 条其他队列、永不可领取的记录时每次领取。

两者都请求 1 条投递且无任务可领取，因此测的是扫描与读取候选记录的开销，不包含 handler 执行。

## 一次参考结果

在 Apple M5 Max（darwin/arm64）、Go 1.27、内存 nats-server 上：

```
BenchmarkReceiveEmptyLedger-18         30   52,149,272 ns/op
BenchmarkReceiveThousandRetained-18    30  107,233,336 ns/op
```

即约 52 ms 与约 107 ms 每次领取。空账本上的约 50 ms 主要不是在扫描，而是 `Receive` 在「本次扫描没有可投递任务」后等待下一次 wake 或扫描（`receiveWait`）的超时，这与任何驱动都必须等待新工作的语义一致；其中空转扫描本身的开销很小。保留 1,000 条记录让每次领取多出约 55 ms，这部分是逐条读取保留记录的真实扫描成本，约为每 1,000 条保留记录 50 ms 量级。按此量级外推，十万条保留记录意味着每轮扫描约 5 秒，而空闲等待也会随之变成扫描间隔；这是当前实现为正确性优先付出的代价，也是调整 `JobRetention` 与按业务拆分命名空间的现实理由。

## 适用范围与已知限制

- 单机内存服务器，无集群、无磁盘、无网络抖动。
- 只有 30 次迭代；尚未覆盖并发 worker、持续写入或高提交速率。
- 未比较不同 `ScanInterval`、`ClaimLease` 或保留期配置。
- 未测量提交路径、结算路径或调度器。

如需在生产负载下评估，应在接近真实拓扑与保留策略的环境中测量队列深度、领取延迟和 CPU 占用，而不是依赖这些数字。
