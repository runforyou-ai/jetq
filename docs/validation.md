# 验证与覆盖

本文把最终行为保证映射到当前测试。测试是断言来源：`review_*_test.go` 是独立编写的回归测试，断言不得为了通过而修改。

## 运行方式

```sh
gofmt -l .                                  # 无输出
go vet ./...                                # exit 0
go test -race -count=1 -timeout 180s ./...  # exit 0
golangci-lint run                           # exit 0
go list -deps . | grep -i nats              # 无输出（根包不依赖 NATS）
```

`make test` 与 `make lint` 执行同样的检查。

## 测试文件分工

| 文件 | 关注点 |
|---|---|
| `policy_test.go` | 策略合并优先级、backoff 校验与序列化、Envelope 深拷贝与指纹、Prepare 校验 |
| `worker_test.go` | 执行引擎：typed/raw 同路径、重试耗尽、panic、Snooze 退款、超时、未知任务、部分接收、停机 |
| `prepare_test.go` | `PrepareTask` 固定身份与相对 Delay、`SubmitRaw` 使用任务默认策略、缺失 JobID 的可解释错误 |
| `redrive_test.go` | `PrepareRedrive` 跨重试固定 Delay/ID/策略、拒绝继承原 key、payload 上限全路径 |
| `envelope_json_test.go` | Envelope/PreparedJob 值与指针 JSON 往返后载荷按字节稳定、指纹仍有效 |
| `review_core_test.go` 等 `review_*_test.go` | 独立回归：任务默认值、冻结 backoff、超时优先、失败观察者载荷、结算期续租、长任务续租、阻塞观察者不挂住关闭 |
| `fakedriver_test.go` | 引擎测试用的驱动替身 |
| `drivertest/drivertest.go` | 驱动符合性套件（任一实现可运行） |
| `natsq/driver_test.go` | NATS 提交/领取/准入/结算/恢复/唯一/取消/命名空间与配置冲突 |
| `natsq/conformance_test.go` | 在真实 NATS 上运行符合性套件，含 redrive |
| `natsq/e2e_test.go` | 跨进程形态：typed producer + worker、取消与唯一 |
| `natsq/extensions_test.go` | Inspector 分页/统计/失败过滤、redrive E2E、调度同步与并发触发、leader 一致读、部署冲突 |
| `natsq/outcome_test.go` | 提交结果规范化：suppressed 终态、恢复不激活被抑制候选 |
| `natsq/schedule_version_test.go` | 陈旧同步不删除新集合、触发不改版本、新增调度保留其余游标、发生内容冲突被报告 |
| `natsq/schedule_recovery_test.go` | 迟到轮询只触发一次、claim 快照不受清单变更影响、claim 响应丢失、保留期回收 claim 与 job |
| `natsq/sigkill_test.go` | 真实进程 SIGKILL 崩溃恢复 |
| `natsq/review_*_test.go` | 独立回归：提交协议、唯一、并发恢复、结算续租、准入响应丢失、调度（owner/游标/补发/载荷）、检查器隐藏抑制回执与末页游标、载荷字节、调度恢复 |

## 保证 → 测试

| 保证 | 覆盖 |
|---|---|
| 根包不依赖 NATS | `go list -deps .` 检查（CI 与本地）；根包测试全部不导入 NATS |
| 普通结构体、raw、动态任务名使用同一 Envelope 与策略 | `TestTypedAndRawUseTheSamePath`、`TestSubmitRawUsesTaskDefaults`、`natsq/TestEndToEndTypedProducerAndWorker`、`example_test.go` |
| 策略优先级（提交 > 任务 > 生产者；零值继承；NoTimeout 不继承） | `TestResolvePolicyPriority`、`TestResolvePolicyZeroTimeoutIsNotInherit`、`TestPolicyPriority`、`TestReviewSubmitUsesTaskDefaults` |
| 载荷按原始字节保存（含空白与 `<`） | `TestEnvelopeJSONKeepsPayloadBytes`、`TestReviewPreparedJSONPreservesPayloadAndFingerprint`、`natsq/TestReviewDriverPreservesRawJSONBytes`、`natsq/TestReviewSchedulePreservesPrettyRawJSONBytes`、`natsq/TestReviewSchedulePreservesRawPayload` |
| 编码不等于重编码：往返后指纹仍有效，outbox 重试不会指纹不匹配 | 同上文件；`natsq/TestSubmitRejectsTamperedFingerprint` |
| 业务载荷上限，超限不进驱动 | `TestMaxPayloadBytes` |
| EnqueueKey 幂等与内容冲突；显式 JobID 已占用是确定错误而非 Unknown | `natsq/TestEnqueueKeyIdempotencyAndConflict`、`TestConcurrentEnqueueKeyRetriesConverge`、`TestAlreadyAcceptedKeepsOriginalRunAt`、`drivertest` `SubmitJobIDInUse` |
| 提交结果区分 Accepted/AlreadyAccepted/Suppressed/Unknown | `natsq/outcome_test.go`、`TestSubmitCreateResponseLost`、`TestStalledIntentIsCompletedNotReplaced`、`TestReviewIntentMarkerFailureKeepsAcceptedJob` |
| 相对 Delay 准备时固定，重复提交不变时间 | `TestPrepareFixesRunAtAndFingerprint`、`TestPrepareTaskFixesIdentityAndDelay`、`TestPrepareRedriveFixesDelayAcrossRetries`、`drivertest` redrive 用例 |
| 准入消耗预算、响应丢失不重复计费、上限不可越过 | `TestReviewAdmissionResponseLossAtBudgetLimit`、`TestLeaseExpiryRedeliversAndKeepsBudgetSpent`、`TestExhaustedBudgetAfterRedeliveryDoesNotRun` |
| 失败观察者看到的尝试次数与预算一致（耗尽预算、恢复的失败决定、正常准入） | `TestFailureObserverSeesBudgetOnExhaustedPath`、`TestFailureObserverSeesBudgetOnPendingFail`、`TestObserverAttemptAfterAdmission` |
| Snooze 幂等退款（只退一次） | `TestSnoozeRefundsBudget`、`natsq/TestSnoozeRefundsBudgetOnce` |
| Release 退款、退还后不再运行 handler | `TestReleaseKeepsBudget`、`TestShutdownReleasesUnadmittedDelivery` |
| 决定已持久化则不重跑业务 | `natsq/TestRecordedDecisionIsResumedNotRerun`、`TestRecoveryCompletesRecordedDecision`、`TestDecisionWriteResponseLost`、`TestRecordedJobBeatsUnmarkedIntent`、`TestPendingDecisionResumesWithoutHandler` |
| 结算结果区分 NotRecorded/Done/清洁未完成 | `TestSettlementRetriedWhenNotRecorded`、`natsq/review_settle_renew_test.go` |
| 结算期间续租不被阻塞 | `TestReviewExtendNotBlockedBySettlement`、`TestReviewSettlementKeepsLease` |
| 长任务续租覆盖执行与关闭窗口 | `TestLongJobIsKeptAlive`、`TestSlowLogContextKeepsLease`、`TestReviewNoTimeoutJobRenewsPastShutdownBudget` |
| 超时优先于 nil/Snooze/Permanent | `TestTimeoutThenNilIsTimeout`、`TestReviewTimeoutOverridesSpecialErrors` |
| panic 视为错误 | `TestRetryExhaustionAndPanic` |
| 未知 handler 暂缓且不消耗预算，超时后失败 | `TestUnknownTaskDefersWithoutBudget`、`TestUnknownTaskFailsAfterAge` |
| 停机：完成的结算、未开始的释放、忽略取消有界放弃 | `TestShutdownGraceCompletesRunningJob`、`TestHandlerIgnoringCancelIsAbandoned`、`TestReviewBlockingFailureObserverDoesNotHangShutdown`、`TestPartialReceiveProcessesAll` |
| 取消仅作用于首次准入前，与领取竞争可解释 | `natsq/TestCancel`、`TestDelayedJobIsNotDeliveredEarly`、`TestCancelReportsWriteFailureAndKeepsLock`、`TestCanceledBeforeAdmissionSkipsHandler`、`TestEndToEndCancelAndUnique` |
| 唯一窗口与 UntilDone 的抑制、失效与回收 | `natsq/TestUniqueWindow`、`TestUniqueUntilDoneReleasedOnSuccess`、`TestUniqueUntilDoneLockRecoveredOnCrash`、`TestReviewWindowSurvivesSuccessfulJob`、`TestReviewUniqueMarkerFailurePreservesActiveJob`、`TestReviewConcurrentRecoveryRespectsUnique`、`TestReviewSuppressedEnqueueKeySurvivesNewCandidateID` |
| 被抑制的提交回执是终态且不可激活 | `natsq/TestSuppressedOutcomeIsFinalAndExpires`、`TestRecoveryDoesNotCreateSuppressedCandidate`、`TestHelperSuppressedThenOwnerEndsDoesNotActivate`、`TestReviewSuppressedIntentDoesNotRunAfterRecovery`、`TestReviewInspectorHidesSuppressedReceipt` |
| 检查器：分页、失败过滤、统计、无后端标识 | `natsq/TestInspectSnapshotAndPages`、`TestReviewInspectorLastPageHasNoCursor`、`TestEndToEndSnapshot` |
| redrive：原任务不变、新 ID、ParentJobID、拒绝继承 key、可重试 | `TestPrepareRedriveRejects`、`natsq/TestRedriveEndToEnd`、`drivertest.Redrive` |
| 调度 owner 隔离、版本 CAS、陈旧同步不删除新集合 | `natsq/TestScheduleSyncOwnership`、`TestScheduleStaleSyncCannotDeleteNewerSet`、`TestReviewScheduleOwnerOccurrenceIsolation` |
| 调度游标：触发不改版本、未变更保留游标、补发策略 | `TestScheduleFiringDoesNotChangeVersion`、`TestScheduleAddingKeepsUnchangedCursor`、`TestReviewScheduleCatchUpFiresEveryMissedOccurrence`、`TestReviewScheduleWithoutCatchUpSkipsMissedBacklog`、`TestScheduleLatePollFiresOnce` |
| 发生 claim 是唯一提交点、崩溃窗口可修复、快照不受清单变更影响 | `natsq/TestScheduleConcurrentSyncAndFiring`、`TestScheduleClaimSnapshotSurvivesChange`、`TestScheduleLostClaimResponseFiresOnce`、`TestReviewClaimedOccurrenceKeepsItsOriginalDefinition`、`TestReviewScheduleCursorResponseLossDoesNotLoseOccurrence`、`TestReviewScheduleFailedEnqueueIsRecoveredAfterCursorAdvance` |
| 发生内容冲突可见、不静默覆盖 | `TestScheduleOccurrenceConflictIsReported` |
| 安全回收：claim、intent、幂等决定先于 job 删除 | `TestScheduleRetentionCollectsClaimWithJob`、`TestReviewOccurrenceReceiptCannotReplayACollectedJob`、`TestUniqueUntilDoneLifecycle` 族 |
| 调度随 worker 生命周期启停 | `TestScheduleFiresThroughWorker` |
| 部署参数校验、leader 一致读、连接所有权 | `natsq/TestOpenRequiresProvisionedResources`、`TestConfigConflictAtProvision`、`TestOpenRejectsDeploymentConflicts`、`TestLedgerReadsAreLeaderConsistent`、`TestOpenRejectsDeploymentConflicts` |
| 命名空间隔离 | `natsq/TestNamespaceIsolation` |
| 真实进程崩溃后的恢复 | `natsq/TestSigkillAfterAdmission`、`TestSigkillAfterDecision`、`TestSigkillAfterSnooze` |
| 任一驱动符合契约 | `drivertest.Run`（提交幂等与冲突、唯一、准入预算与租约、Snooze 退款、取消、检查）、`natsq/TestConformance`、`TestConformanceRedrive` |

## SIGKILL 实验

`natsq/sigkill_test.go` 用本测试二进制再执行一个子进程作为 worker：

- 父进程独立启动 NATS server（tempdir 存储、独立 namespace），子进程独立连接并 Open 同一命名空间。
- hook 只用于标记边界并阻塞；父进程读取子进程 stdout 的 `BOUNDARY` 行后调用 `Process.Kill()`（SIGKILL）并 `Wait`，确认进程非正常退出。
- 三个场景：
  1. **准入后崩溃**：budget 已持久化并消耗；重复 kill 两次后恢复不运行 handler，任务失败，budget 等于 MaxAttempts，未越过上限。
  2. **决定已持久化**：成功决定已落盘；恢复不重跑 handler，任务为 succeeded，budget 为 1。
  3. **Snooze 后崩溃**：snooze 决定落盘；恢复退款恰好一次（Snoozes=1，budget=1），随后执行一次并成功。

不使用业务数据库；子进程使用独立临时工作目录。

## 故障注入

除进程 kill 外，测试通过驱动 hook 注入可控故障：准入响应丢失（`afterAdmit`）、决定写入响应丢失（`afterDecisionWrite`）、清理未完成（`afterCleanup`）、KV 写入前失败（`beforeWrite`）、KV 包装（`kv`，注入 timeout/关闭连接）、注入时钟（`now`）。这些注入点只制造故障，不替代真实崩溃路径——崩溃由上面的进程 kill 覆盖。

允许重复的窗口：决定写入前崩溃、准入响应丢失后重试、租约过期后接管、wake 消息丢失。禁止状态：预算超过上限后运行 handler、同一 transition 重复应用副作用、被抑制候选被激活、已回收任务被 claim 重放、陈旧清单删除新集合。
