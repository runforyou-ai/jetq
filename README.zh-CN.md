# jetq

[English](README.md)

jetq 是基于 [NATS JetStream](https://docs.nats.io/nats-concepts/jetstream) 的 Go 后台任务队列，任务由稳定名称和普通 JSON 输入组成，处理函数是普通 Go 函数，支持重试、退避、延迟、唯一任务、失败处理和 cron 定时任务。

- **强类型处理函数**：输入是普通结构体，`EnqueueNamed` 与 `HandleNamed` 使用同一稳定任务名称。
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

0.4 版本删除 `Cron` 和 `Schedule.In`；升级前请阅读[迁移说明](CHANGELOG.md#v040)。

## 用法

```go
type WelcomeEmail struct {
	UserID int64 `json:"user_id"`
}

q, err := jetq.New(ctx, js) // js 为 jetstream.JetStream，按需创建 stream

// 在数据库事务提交后投递。
id, err := q.EnqueueNamed(ctx, "welcome-email", WelcomeEmail{UserID: 42},
	jetq.OnQueue("mail"),
	jetq.Delay(10*time.Minute),   // 或 jetq.At(t)
	jetq.UniqueUntilDone("welcome-42"), // 同键任务结束前重复投递返回 ErrDuplicate；jetq.Unique(key) 按时间窗口去重
	// jetq.JobID(id) 自行指定任务编号，例如投递前先保存它
)
// errors.Is(err, jetq.ErrUncertain)：任务可能已投递也可能没有（超时、连接断开）；此时仍返回 id。
// UniqueUntilDone 任务会再发布一次以确定结果（ctx 已取消也会），仍无法确定时保留键直到任务结束或锁过期。
_ = q.Cancel(ctx, id)            // 取消尚未到点的延迟任务

// 定时任务：各实例启动时传入同一份完整清单。
err = q.SyncSchedules(ctx,
	jetq.Schedule{Key: "daily-report", Cron: "0 2 * * *", Name: "daily-report", Payload: DailyReport{}, Timezone: "Asia/Shanghai"},
	jetq.Schedule{Key: "sweep", Cron: "@every 30s", Name: "sweep", Payload: Sweep{}, Queue: "maintenance"},
)

// worker
w := q.NewWorker(
	jetq.Queue{Name: "default"},
	jetq.Queue{Name: "mail", Concurrency: 8, MaxAttempts: 5},
)
jetq.HandleNamed(w, "welcome-email", func(ctx context.Context, job WelcomeEmail) error {
	info, _ := jetq.JobInfo(ctx) // 任务编号、尝试次数、队列、消息头……
	return send(ctx, job.UserID)
}, jetq.OnFailure(func(ctx context.Context, job WelcomeEmail, err error) {
	// 记录最终失败。
}))
err = w.Run(ctx) // 阻塞；取消后等待正在执行的任务
```

### 预配置资源与事务 outbox

`jetq.ResourceConfigs(options...)` 返回任务流、死信流与状态桶定义，不写入 NATS。部署工具完成初始化后，应用进程通过 `jetq.Attach(ctx, js, options...)` 读取资源。Attach 校验主题、队列语义和最低保留期限，并保留副本、存储、放置与容量策略。`New` 继续提供自动创建或更新资源的便利入口。

outbox 重试必须使用相同载荷、绝对执行时间、`jetq.JobID(id)` 与 `jetq.MsgID(publicationID)`。MsgID 使用 JetStream 去重，并允许同一任务恢复其持有的 UniqueUntilDone 锁；它要求显式 JobID，不能与 Unique 或相对 Delay 组合，固定排期使用 At。重复调用确认同一次发布，任务可能已执行完或被取消；取消标记在状态桶 TTL 内对同一发布持续生效，包括超过去重窗口后的重试。收到 ErrUncertain 时保留 outbox 登记，仅在确认发布成功后删除。去重受流的 duplicate window 限制，更晚的重复执行由任务处理器保证业务幂等。


### 运行时指定任务名

用 `EnqueueNamed(ctx, name, payload)` 投递普通输入，用 `HandleNamed(w, name, fn, OnFailure(...))` 注册强类型处理函数。需要按原字节读取时，投递 `json.RawMessage` 并用 `w.HandleRaw(name, fn, jetq.OnRawFailure(...))` 注册；载荷必须是合法 JSON。

### 类型声明任务名称

`Enqueue` 与 `Handle` 也支持实现 `JobName() string` 的类型，适合由输入类型统一声明稳定协议名称的场景。`EnqueueNamed` 与 `HandleNamed` 直接接收名称，普通输入类型无需实现方法。

`json.RawMessage` 校验后按原字节传递，空值和 nil 输入编码为 `null`。其他值遵循 `encoding/json`，包括将 `[]byte` 编码为 base64 JSON 字符串。`null` 解码到结构体时得到零值，业务必填字段由处理函数校验。

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

## 部署

jetq 只需要一个 `jetstream.JetStream`：

- **单台服务器**：在进程内嵌入 `nats-server`（见 [docs/design.md](docs/design.md#embedding-nats)）。
- **多台服务器**：使用独立 NATS 或集群；三节点集群配合 `jetq.WithReplicas(3)`。

## 设计

stream 与主题、投递语义和路线图见 [docs/design.md](docs/design.md)。

## 许可证

[MIT](LICENSE)
