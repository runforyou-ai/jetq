# jetq

[![CI](https://github.com/runforyou-ai/jetq/actions/workflows/ci.yml/badge.svg)](https://github.com/runforyou-ai/jetq/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/runforyou-ai/jetq.svg)](https://pkg.go.dev/github.com/runforyou-ai/jetq)

[English](README.md)

jetq 是 Go 语言的持久化后台任务队列。每个任务由显式的任务名和 JSON 载荷组成；生产者与消费者是相互独立的进程，只共享存储后端。根包在驱动中立契约之上定义提交、带类型 handler、重试、超时、Snooze、取消、唯一性、状态查询和 redrive。`natsq` 包以 NATS JetStream 实现该契约：KeyValue 账本是每个任务及其决定的权威来源，wake 流只用于缩短新任务被领取前的等待。

需要 Go 1.27+；NATS 驱动需要启用 JetStream 的 nats-server 2.14+。

## 安装

```sh
go get github.com/runforyou-ai/jetq
```

## 使用

可直接编译的完整示例见 [example_test.go](example_test.go)。

```go
type Email struct {
	To      string `json:"to"`
	Subject string `json:"subject"`
}

// Task 是稳定任务名加默认策略；它不绑定队列、连接或驱动。
var SendEmail = jetq.MustTask[Email]("email.send", jetq.Policy{
	MaxAttempts: 3,
	Timeout:     jetq.TimeoutOf(30 * time.Second),
	Backoff:     jetq.ExponentialBackoff(time.Second, time.Minute),
})

// Provision 每个部署执行一次；Provision 与 Open 都会校验共享参数。
cfg := natsq.Config{Namespace: "prod", Replicas: 3}
if _, err := natsq.Provision(ctx, js, cfg); err != nil { ... }
driver, err := natsq.Open(ctx, js, cfg)

// 生产者。
client, err := jetq.NewClient(driver, jetq.WithMaxPayloadBytes(256<<10))
res, err := jetq.SubmitTask(ctx, client, SendEmail,
	Email{To: "a@example.com", Subject: "Welcome"},
	jetq.Submission{Queue: "mail", EnqueueKey: "welcome:a@example.com"})
// errors.Is(err, jetq.ErrEnqueueKeyConflict)：该 key 已被不同内容占用。
// 首次提交 res.Status == jetq.Accepted，幂等重试为 jetq.AlreadyAccepted。

// 消费者，可以在独立进程中。
worker := client.NewWorker(jetq.Queue{Name: "mail", Concurrency: 4})
jetq.Handle(worker, SendEmail, func(ctx context.Context, e Email) error {
	return send(ctx, e)
})
err = worker.Run(ctx) // 阻塞，直到 ctx 取消且运行中的 handler 返回
```

### 同一任务投递到多个队列

任务名是查找 handler 的键，队列在每次提交时选择。上面的 `SendEmail` 可以提交到 `mail`、`bulk` 和 `priority`；同一个 worker 可以注册同一个 handler 并服务多个队列（`client.NewWorker(jetq.Queue{Name: "mail"}, jetq.Queue{Name: "bulk"})`）。不同生产者也可以用 `WithDefaultQueue`、`WithDefaultPolicy` 设置各自的默认队列与策略。

### Raw 与动态任务名

带类型载荷是常规用法；以下两种形式共享同一 Envelope、执行策略、重试与准入路径：

```go
// 任务名在运行时才知道，载荷已经是 JSON。
client.Submit(ctx, "legacy.import", json.RawMessage(`{"id":7}`), jetq.Submission{})
worker := client.NewWorker(jetq.Queue{Name: "imports"})
jetq.HandleTask(worker, "legacy.import", func(ctx context.Context, raw json.RawMessage) error { ... })

// 或者使用带默认策略的 raw 任务值。
raw := jetq.Raw("legacy.import")
client.SubmitRaw(ctx, raw, json.RawMessage(`{"id":7}`), jetq.Submission{})
```

Raw JSON 按字节原样保存，包括空白和 `<` 等字符；空 raw 载荷保存为 `null`。`WithMaxPayloadBytes` 对每条路径（typed、raw、动态、prepared）检查编码后的载荷大小，超限返回 `jetq.ErrPayloadTooLarge`，不会进入驱动。

### 准备一次、幂等重试

`Prepare` / `PrepareTask` 在写入前确定 JobID、绝对运行时间和完整策略。对同一值重复 `SubmitPrepared` 就是 outbox 重试的方式：运行时间不会移动，`EnqueueKey` 保证幂等。

```go
job, err := jetq.PrepareTask(client, SendEmail, email,
	jetq.Submission{Queue: "mail", Delay: 10 * time.Minute, EnqueueKey: "digest:42"})
res1, _ := client.SubmitPrepared(ctx, job) // 记录为 now + 10m
res2, _ := client.SubmitPrepared(ctx, job) // AlreadyAccepted，运行时间不变
```

没有 JobID 且没有 ID 生成器时 `Prepare` 返回错误而不是 panic；同时设置 `RunAt` 与 `Delay`，或 Delay 为负，都会被拒绝。

### 策略优先级

解析后的策略由三层合并，优先级从高到低：提交时的 `Policy`、任务默认策略、生产者客户端默认策略（`WithDefaultPolicy`）。零值字段继承下一层；超时例外：`jetq.NoTimeout()` 显式表示“无超时”，不继承。解析后的策略写入 Envelope，消费者严格按生产者解析的结果执行。

### Handler 返回值

| 返回 | 效果 |
|---|---|
| `nil` | 成功，任务终态。 |
| error | 按解析后的 backoff 重试，直到 `MaxAttempts`；之后失败。 |
| `jetq.Permanent(err)` | 立即失败。 |
| `jetq.RetryAfter(d, err)` | 在 `d` 之后重试。 |
| `jetq.Snooze(d)` | 推迟到 `d`；准入预算退回，Snooze 不消耗尝试次数。 |
| panic | 捕获后按错误处理。 |
| 超时 | handler 的 ctx 以 `jetq.ErrTimeout` 为 cause 取消；无论 handler 返回什么（包括 Snooze 和 Permanent）该次尝试都算失败。 |

忽略取消的 handler 会在关闭后继续运行，直到 worker 的放弃窗口；它不会结算该任务。

### 超时、关闭与取消

每次尝试都在解析后的超时下运行（`jetq.NoTimeout()` 关闭）。长时间运行的尝试会自动续租，不会仅因为耗时长而被重新投递。`Run` 返回时，worker 在 `SetShutdownTimeout` 内等待运行中的 handler：完成的正常结算，仍在运行的释放但不结算（尝试次数不退回），已领取但未准入的投递被释放。

`Cancel` 只在首次准入前生效：等待中、已排期或 snooze 中的任务变为 canceled，不再投递；运行中的任务不会被中断。

### 状态查询与 redrive

```go
snap, found, err := client.Inspect(ctx, jobID)   // 状态、等待原因、预算、尝试、投递、snooze、最后错误、待完成决定
page, err := client.ListJobs(ctx, jetq.JobQuery{Queue: "mail", FailedOnly: true, Limit: 50})
stats, err := client.Stat(ctx)                    // 每队列 ready/running/delayed/failed 计数
res, err := client.Redrive(ctx, failedID, jetq.RedriveEnqueueKey("redrive:"+failedID))
```

快照不包含后端标识：没有 stream sequence，也没有 KV revision。redrive 不修改原任务；新任务使用新的 `JobID`，记录 `ParentJobID`，并默认继承原任务的任务名、载荷、队列、策略、元数据和唯一规则（可用选项覆盖），但绝不继承原 EnqueueKey。

使用相对 `RedriveDelay` 的 `Redrive` 在调用时就固定了运行时间。要幂等重试这种 redrive，准备一次后重复提交：

```go
job, err := client.PrepareRedrive(ctx, failedID,
	jetq.RedriveDelay(time.Hour), jetq.RedriveEnqueueKey("redrive:"+failedID))
_, err = client.SubmitPrepared(ctx, job) // 重试时再次提交：同一个 replacement，同一运行时间
```

### 周期调度（NATS 驱动）

调度是对常规提交路径的周期性提交，本身不是任务。每个调度集合由一个 owner 名称拥有，并带有配置版本；同步时必须声明所期望的版本，因此仍运行旧集合的进程无法删除新部署新增的调度。

```go
var v natsq.ScheduleVersion
v, err = driver.SyncSchedules(ctx, "billing", v,
	natsq.Schedule{Key: "daily", Cron: "0 2 * * *", Timezone: "Asia/Shanghai",
		Task: "invoice.close", Payload: json.RawMessage(`{"region":"cn"}`), Queue: "billing"})
if errors.Is(err, natsq.ErrVersionConflict) {
	v, err = driver.CurrentScheduleVersion(ctx, "billing") // 再基于它重新同步
}

worker.SetScheduler(driver.Scheduler("billing")) // worker 运行调度器并随关闭一起停止
// 或：driver.Scheduler("billing").Run(ctx)
```

- `expected` 为 0 时创建集合；owner 已持有不同集合时冲突。
- `expected` 非 0 时仅在版本匹配时更新，否则返回 `natsq.ErrVersionConflict` 且不写入。
- 同步已存储的定义是幂等的，返回当前版本。
- 只有变更或新增的调度会丢失游标；新增一条调度不会跳过另一条已到期的发生。
- 触发不会改变版本。
- 每个发生（occurrence）以固定的 JobID、EnqueueKey 和运行时间持久 claim。claim 与提交之间崩溃时，恢复用同一份快照提交；之后修改或删除调度不影响已 claim 的发生。
- `CatchUp: false`（默认）跳过无调度器运行期间错过的发生并触发到期的那一次；`CatchUp: true` 会从最早开始补发最多 `MaxCatchUp` 次。

### 部署、连接所有权与故障模式

`natsq.Provision` 创建命名空间的账本桶、wake 流与调度支持；`natsq.Open` 校验它们：账本存储类型与副本数必须与配置一致，账本不得允许 direct（follower）读取，wake 流的 subject、retention 和存储必须匹配。`Open` 不会改写不匹配的部署。二者都接收调用方创建、仍归调用方所有的 `jetstream.JetStream`；`Driver.Close` 停止驱动自身的 goroutine，不关闭连接。

KV 账本是权威：wake 消息因发布丢失或崩溃而缺失时，周期账本扫描会发现任务。`Recover`（worker 中由 `Maintain` 调用，也可直接调用）完成已记录的决定、接管 lease 过期的任务、完成丢失的提交，并连同 intent、唯一锁和调度 claim 一起回收过期的终态任务。

## 保证与限制

**至少一次。** 结算结果丢失的任务会再次执行；handler 对其副作用必须幂等。准入预算限制执行次数：预算耗尽的任务直接失败，不再运行 handler。

**提交结果。** `Accepted` 表示任务已持久化；`AlreadyAccepted` 表示同一 `EnqueueKey` 已持有相同内容；`Suppressed` 表示被唯一规则抑制；`Unknown`（伴随 `ErrUncertain`）表示结果未知，用同一 `EnqueueKey` 和同一 prepared 值重试即可。

**EnqueueKey 保留。** 已接受的 key 随任务一起保留和删除；保留期内的重试返回 `AlreadyAccepted`，之后相同内容就是新工作。请使用稳定、由内容派生的 key，并在提交确认前保留 outbox 行。

**唯一规则。** `UniqueModeWindow` 从持有者提交时起抑制窗口时长，无论持有者是否仍运行；`UniqueModeUntilDone` 抑制到持有者成功、失败、被取消或提交被明确拒绝。被抑制的持久回执有独立的短期保留期，不会作为任务出现在查询结果中。

**保留期。** 终态任务保留 `JobRetention`（默认 24h）供查询和 redrive，之后回收。辅助记录（intent、唯一锁、调度 claim）依赖其任务，任务回收时一并删除；无法删除的 claim 会让任务保留到下一轮，避免已回收的任务被重新创建。

**扫描开销。** 驱动没有可投递索引：`Receive` 会列出账本中的任务 key 并读取候选记录，因此领取开销随*保留中*的任务数增长，而不是随可运行任务数增长。默认 24h 终态保留意味着，存有数百万已完成任务的命名空间在每次接收时都要为它们付出成本。领取可见性由扫描间隔和 wake 流决定，lease 限制崩溃 worker 任务被占用的时长。该设计面向账本规模在数十万条记录以内的工作负载；未做吞吐量压测。可复现的微基准及其范围见 [docs/benchmarks.md](docs/benchmarks.md)。

**未测量的内容。** 本文不声明吞吐量、延迟或生产稳定性。

## 设计与验证

- [docs/design.md](docs/design.md)：驱动契约、core 与驱动的职责、NATS 的提交/准入/结算/调度协议与安全 GC，以及 PostgreSQL 事务式草图。
- [docs/validation.md](docs/validation.md)：每条保证对应的测试组，包括进程 kill 测试和每个驱动都运行的 conformance 套件。
- [CHANGELOG.md](CHANGELOG.md)：本版本变更。

`make test` 运行全套测试（race 检测，180 秒超时），`make lint` 运行静态检查。

## 许可证

[MIT](LICENSE)
