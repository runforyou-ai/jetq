# jetq

[English](README.md)

jetq 是基于 [NATS JetStream](https://docs.nats.io/nats-concepts/jetstream) 的 Go 后台任务队列，用法贴近 Laravel 队列：任务是结构体，处理函数是普通函数，重试、退避、延迟、唯一任务、失败处理和 cron 定时任务都按你熟悉的方式工作。

- **强类型任务**：任务是实现 `JobName()` 的结构体，处理函数直接拿到解码后的结构体。
- **重试**：按队列设置最大尝试次数和退避；`Permanent` 直接失败，`RetryAfter` 指定下次时间，`Snooze` 推迟执行且不计入尝试次数。
- **延迟与定时由服务端调度**：使用 JetStream 消息调度，应用里不需要调度进程、轮询或选主。
- **长任务**：处理中自动续期；worker 崩溃后，任务在 `AckWait` 后重新投递；`Queue.Timeout` 与 `jetq.Timeout(d)` 限制单次尝试的执行时间。
- **死信**：尝试次数用完的任务进入死信 stream，带最后一次错误，并调用 `OnFailure` 回调。
- **接口小**：一个 stream，每个队列一个 consumer，直接使用 `nats.go`。嵌入式还是独立 NATS 由应用决定。

要求 nats-server **2.14+**（推荐 2.15）、Go 1.27+。

## 安装

```sh
go get github.com/runforyou-ai/jetq
```

## 用法

```go
type WelcomeEmail struct {
	UserID int64 `json:"user_id"`
}

func (WelcomeEmail) JobName() string { return "welcome-email" }

q, err := jetq.New(ctx, js) // js 为 jetstream.JetStream，按需创建 stream

// 在数据库事务提交后投递。
id, err := q.Enqueue(ctx, WelcomeEmail{UserID: 42},
	jetq.OnQueue("mail"),
	jetq.Delay(10*time.Minute),   // 或 jetq.At(t)
	jetq.UniqueUntilDone("welcome-42"), // 同键任务结束前重复投递返回 ErrDuplicate；jetq.Unique(key) 按时间窗口去重
	// jetq.JobID(id) 自行指定任务编号，例如投递前先保存它
)
_ = q.Cancel(ctx, id)            // 取消尚未到点的延迟任务

// 定时任务：各实例启动时传入同一份完整清单。
err = q.SyncSchedules(ctx,
	jetq.Cron("daily-report", "0 2 * * *", DailyReport{}).In("Asia/Shanghai"),
	jetq.Cron("sweep", "@every 30s", Sweep{}, jetq.OnQueue("maintenance")),
)

// worker
w := q.NewWorker(
	jetq.Queue{Name: "default"},
	jetq.Queue{Name: "mail", Concurrency: 8, MaxAttempts: 5},
)
jetq.Handle(w, func(ctx context.Context, job WelcomeEmail) error {
	info, _ := jetq.JobInfo(ctx) // 任务编号、尝试次数、队列、消息头……
	return send(ctx, job.UserID)
}, jetq.OnFailure(func(ctx context.Context, job WelcomeEmail, err error) {
	// 相当于 Laravel 的 failed()
}))
err = w.Run(ctx) // 阻塞；取消后等待正在执行的任务
```

### 运行时指定任务名

任务名只能在运行时确定时（例如对接已有的任务系统），投递 `jetq.RawJob{Name: name, Payload: json}`，并用 `w.HandleRaw(name, fn, jetq.OnRawFailure(...))` 注册处理函数。载荷必须是合法 JSON，按原字节传递。

### 查看队列

`q.Stats(ctx)` 返回每个队列尚未投递（`Ready`）、已投递未结算（`InFlight`，主要是执行中）和死信的任务数，以及待执行的延迟任务数（含等待重试与推迟的任务）和已安装的定时任务数；`q.DeadLetters(ctx, jetq.DeadLetterQuery{...})` 按时间倒序分页读取死信及最后一次错误。

### 日志上下文

`w.SetLogContext(func(ctx, info) context.Context)` 为 jetq 自身的任务日志和失败回调派生 context，日志处理器可据此附加串联编号、租户等字段。

### 处理函数的返回值

| 返回 | 效果 |
|---|---|
| `nil` | 完成，从队列移除 |
| 普通错误 | 按退避重试；次数用完进入死信 |
| `jetq.RetryAfter(d, err)` | 在 `d` 后重试 |
| `jetq.Permanent(err)` | 直接进入死信 |
| `jetq.Snooze(d)` | `d` 后再执行，不计入尝试次数（`Info.Snoozes` 记录推迟次数） |
| panic | 按错误处理 |
| 超时 | context 以 `jetq.ErrTimeout` 为原因取消，本次尝试失败 |

等待重试或推迟的任务以延迟任务的形式保存，不会占住队列，也可以用 `Cancel` 取消。worker 没有注册处理函数的任务（例如滚动部署期间）会放回给其他 worker 且不计入尝试次数，超过 `w.SetUnknownJobTimeout`（默认 1 小时）后进入死信。停机超时中断的任务放回队列且不计入尝试次数；不响应取消的处理函数在 10 秒后被放弃。不足 1 秒的重试延迟通过重投实现；延迟任务最早约 250ms 后触发。

投递语义是**至少一次**，处理函数必须幂等。

## 与 Laravel 对照

| Laravel | jetq |
|---|---|
| `implements ShouldQueue` | `JobName() string` |
| `handle()` | `jetq.Handle(w, fn)` |
| `Job::dispatch()->onQueue('mail')->delay(...)` | `q.Enqueue(ctx, job, jetq.OnQueue("mail"), jetq.Delay(d))` |
| `$tries`、`backoff()` | `Queue.MaxAttempts`、`Queue.Backoff`、`jetq.MaxAttempts(n)` |
| `$timeout` | `Queue.Timeout`、`jetq.Timeout(d)` |
| `$this->release($delay)` | `return jetq.Snooze(d)` |
| `$this->fail()` | `return jetq.Permanent(err)` |
| `failed()`、`failed_jobs` | `jetq.OnFailure`、`w.OnFailed`、死信 stream |
| `ShouldBeUnique` | `jetq.UniqueUntilDone(key)`（任务结束前）或 `jetq.Unique(key)`（去重窗口内） |
| `$schedule->job(...)->cron(...)->timezone(...)` | `jetq.Cron(key, spec, job).In(tz)` + `q.SyncSchedules` |
| `queue:work` | `w.Run(ctx)` |
| `after_commit` | 事务提交后调用 `Enqueue` |

## 部署

jetq 只需要一个 `jetstream.JetStream`：

- **单台服务器**：在进程内嵌入 `nats-server`（见 [docs/design.md](docs/design.md#embedding-nats)）。
- **多台服务器**：使用独立 NATS 或集群；三节点集群配合 `jetq.WithReplicas(3)`。

## 设计

stream 与主题、投递语义和路线图见 [docs/design.md](docs/design.md)。

## 许可证

[MIT](LICENSE)
