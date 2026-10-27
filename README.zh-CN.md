# jobq

[English](README.md)

jobq 是 Go 的后台任务队列，支持可替换的存储 driver：[NATS JetStream](https://docs.nats.io/nats-concepts/jetstream)、PostgreSQL 和内存。任务由稳定名称和普通 JSON 输入组成，处理函数是普通 Go 函数，支持重试、退避、延迟、唯一任务、失败回调和 cron 定时任务。

- **一套 API，多个 driver**：换存储只需要换传给 `jobq.New` 的 driver。重试、退避、Snooze、超时和停机逻辑只在核心里实现一次，所有 driver 行为一致。
- **强类型处理函数**：输入是普通结构体，`EnqueueNamed` 与 `HandleNamed` 使用同一稳定任务名称，也可以让类型通过 `JobName()` 自带名称。
- **重试**：按队列设置最大尝试次数和退避；`Permanent` 直接失败，`RetryAfter` 指定下次时间，`Snooze` 推迟执行且不计入尝试次数。
- **延迟与定时**：`Delay`、`At` 和 cron 定时任务，应用里不需要调度进程。
- **唯一任务**：`Unique(key)` 在任务结束前一直占用 key；`UniqueFor(key, d)` 在一个时间窗口内占用。
- **长任务**：处理中自动续租；worker 崩溃后，任务在租约过期后被重新认领；租约丢失会取消处理函数的 context。
- **死信**：尝试次数用完的任务连同最后一次错误一起保留，可以通过 client 列出、重试或删除。
- **验收套件**：`jobq/drivertest` 用同一份合约检查每个 driver。

要求 Go 1.27+。

## 模块

| 模块 | 用途 | 依赖 |
|---|---|---|
| `github.com/runforyou-ai/jobq` | Client、worker、driver 合约、`memq`、`drivertest` | 仅标准库 |
| `github.com/runforyou-ai/jobq/natsq` | NATS JetStream driver（nats-server 2.14+） | nats.go |
| `github.com/runforyou-ai/jobq/pgq` | PostgreSQL driver（PostgreSQL 14+） | pgx/v5 |

```sh
go get github.com/runforyou-ai/jobq
go get github.com/runforyou-ai/jobq/pgq   # 和/或 natsq
```

## 用法

```go
type WelcomeEmail struct {
	UserID int64 `json:"user_id"`
}

func (WelcomeEmail) JobName() string { return "welcome-email" }

// 选择 driver。
driver, err := pgq.New(ctx, pool)       // pool 是 *pgxpool.Pool
// driver, err := natsq.New(ctx, js)    // js 是 jetstream.JetStream
// driver := memq.New()                 // 测试和示例
defer driver.Close()

q := jobq.New(driver)

id, err := q.Enqueue(ctx, WelcomeEmail{UserID: 42},
	jobq.OnQueue("mail"),
	jobq.Delay(10*time.Minute),      // 或 jobq.At(t)
	jobq.Unique("welcome-42"),       // 任务结束前再入队返回 ErrDuplicate
	jobq.MaxAttempts(5),
	jobq.Timeout(time.Minute),
)
// errors.Is(err, jobq.ErrUncertain)：任务可能已存储，也可能没有，同时返回 id。
// 用 jobq.JobID(id) 重新入队：如果第一份已存储，会得到 ErrJobIDInUse。
_ = q.Cancel(ctx, id)

// 定时任务：每个实例在启动时传入同一份完整列表。
err = q.SyncSchedules(ctx,
	jobq.Schedule{Key: "daily-report", Cron: "0 2 * * *", Timezone: "Asia/Shanghai", Job: DailyReport{}},
	jobq.Schedule{Key: "sweep", Cron: "@every 30s", Job: Sweep{}, Options: []jobq.EnqueueOption{jobq.OnQueue("maintenance")}},
)

// Worker。
w := q.NewWorker(
	jobq.Queue{Name: "default"},
	jobq.Queue{Name: "mail", Concurrency: 8, MaxAttempts: 5, Backoff: jobq.Exponential(10*time.Second, time.Hour), Timeout: 5 * time.Minute},
)
jobq.Handle(w, func(ctx context.Context, job WelcomeEmail) error {
	info, _ := jobq.JobInfo(ctx) // id、尝试次数、队列、header……
	return send(ctx, job.UserID)
}, jobq.OnFailure(func(ctx context.Context, job WelcomeEmail, err error) {
	// 记录最终失败。
}))
w.Use(loggingMiddleware)
err = w.Run(ctx) // 阻塞；取消后等待运行中的任务
```

任务名只在运行时才知道时，用 `EnqueueNamed(ctx, name, payload)` 配合 `HandleNamed`，或用 `jobq.RawJob` 配合 `w.HandleRaw`，原样接收 JSON 输入。

### 查看队列

`q.Stats(ctx)` 按队列统计 ready、scheduled（延迟、重试或 Snooze 中）、running 和死信的数量。`q.DeadLetters(ctx, jobq.DeadLetterQuery{...})` 按时间倒序分页列出死信，`q.RetryDead(ctx, id)` 和 `q.DeleteDead(ctx, id)` 操作单个死信。driver 不支持时返回 `jobq.ErrUnsupported`。

### 处理函数的返回值

| 返回 | 效果 |
|---|---|
| `nil` | 任务完成。 |
| 任意 error | 按队列退避重试；次数用完进入死信。 |
| `jobq.RetryAfter(d, err)` | 过 `d` 后重试，替代退避。 |
| `jobq.Permanent(err)` | 立即进入死信。 |
| `jobq.Snooze(d)` | 过 `d` 后再次运行，不计入尝试次数（`Info.Snoozes` 记录次数）。 |
| panic | 当作 error 处理。 |
| 超时 | context 以 `jobq.ErrTimeout` 为 cause 被取消，本次尝试失败。 |

没有对应处理函数的任务会被放回，交给其他 worker，不消耗尝试次数；超过 `w.SetUnknownJobTimeout`（默认 1 小时）后进入死信。停机超时中断的任务会被放回且不消耗尝试次数；不响应取消的处理函数在 10 秒后被放弃。

投递语义是**至少一次**：处理函数必须幂等。

## Driver

| | memq | natsq | pgq |
|---|---|---|---|
| 存储 | 进程内存 | JetStream stream 与 KV bucket | 一个 PostgreSQL schema |
| 延迟任务 | 支持 | 服务端消息调度 | 按数据库时钟的 `run_at` |
| 定时任务 | 支持 | 服务端消息调度 | 用 `SKIP LOCKED` 抢占的行 |
| 查看与死信 | 支持 | 支持 | 支持 |
| 事务内入队 | 不支持 | 不支持 | `pgq.Driver.EnqueueTx` |

### pgq

`pgq.New` 会创建或迁移 schema（默认 `jobq`）。`EnqueueTx` 把任务存进你自己的事务，随你的数据一起提交或回滚：

```go
tx, _ := pool.Begin(ctx)
// ... 写入你的数据 ...
msg, _ := jobq.NewMessage("welcome-email", WelcomeEmail{UserID: 42}, jobq.OnQueue("mail"))
err := driver.EnqueueTx(ctx, tx, msg)
err = tx.Commit(ctx)
```

注意：

- 等待中的 worker 由 `LISTEN/NOTIFY` 唤醒。监听器在 driver 存活期间**一直占用连接池里的一个连接**，所以连接池至少要 2 个连接，worker 多时要更多。PgBouncer 事务模式下请用 `pgq.WithoutListen()`，此时 worker 改为轮询（`pgq.WithPollInterval`）。
- 所有时间都按数据库时钟计算，应用主机之间的时钟偏差不会让延迟偏移。
- 成功和取消的任务保留 24 小时（`WithRetention`），死信保留 14 天（`WithDeadRetention`）。

### natsq

`natsq.New` 会创建工作 stream、状态 bucket 和死信 stream。共用一个 NATS 账号的应用需要使用不同的前缀（`natsq.WithPrefix`）。租约是否过期按各进程的本地时钟判断，请让机器时钟同步到几秒以内。状态记录如何拦住过期的 worker，见 [docs/design.md](docs/design.md#nats-driver-natsq)。

### 编写 driver

实现 `jobq.Driver`（可选实现 `jobq.Scheduler` 和 `jobq.Inspector`），然后运行验收套件：

```go
func TestConformance(t *testing.T) {
	drivertest.Run(t, func(t *testing.T) jobq.Driver { return newMyDriver(t) }, drivertest.Options{})
}
```

## 开发

```sh
make test   # 所有模块；pgq 的测试需要 DATABASE_URL，没有设置时会跳过
make lint
```

## 设计

driver 合约、worker 语义和各 driver 的存储结构见 [docs/design.md](docs/design.md)。

## 许可证

[MIT](LICENSE)
