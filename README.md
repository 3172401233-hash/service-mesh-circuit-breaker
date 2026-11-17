# service-mesh-circuit-breaker

JavaScript 服务网格熔断器核心：endpoint 隔离、半开探测、重试预算与渐进恢复。
所有时间由显式 `tick()` 推进，不使用任何真实计时器，行为完全可复现。

## 快速开始

```bash
npm test        # node --test
```

```js
const { MeshPool } = require('./index');

const pool = new MeshPool({
  failureThreshold: 3,        // 连续错误达到阈值即熔断
  errorRateThreshold: 0.5,    // 健康窗口内错误率阈值（慢失败）
  windowMinRequests: 5,       // 窗口最小样本量
  openDuration: 5,            // 熔断后冷却 tick 数
  halfOpenMaxProbes: 2,       // 半开状态允许的并发探测数
  halfOpenSuccessThreshold: 2,// 关闭熔断所需的连续探测成功数
  rampUpTicks: 3,             // 恢复后流量逐步放大的 tick 数
  maxConcurrency: 10,         // 单 endpoint 并发上限
  retryBudgetPerTick: 4,      // 每 tick 补充的重试令牌
});

pool.addEndpoint('a');
pool.addEndpoint('b');

const result = pool.execute((endpointId) => callUpstream(endpointId));
pool.tick(); // 推进逻辑时间：冷却计时、恢复爬坡、重试预算补充
```

## 状态机

- **closed**：正常通行，受 `maxConcurrency` 并发上限约束。连续错误达到
  `failureThreshold`（快速失败），或滑动健康窗口内错误率达到
  `errorRateThreshold` 且样本量不少于 `windowMinRequests`（慢失败）时熔断。
- **open**：拒绝全部流量（`capacity() === 0`），持续 `openDuration` 个 tick。
  这解决了"实例恢复后路由仍被关闭"的问题：冷却到期后自动进入半开，
  无需外部探测器。
- **half-open**：只允许 `halfOpenMaxProbes` 个并发探测。任意一个探测失败立即
  重新打开；连续 `halfOpenSuccessThreshold` 个探测成功后关闭。
- **恢复爬坡**：关闭后不立即放全量流量，并发上限在 `rampUpTicks` 个 tick 内
  从探测水位线性恢复到 `maxConcurrency`，避免刚恢复的实例被流量打垮。

## 熔断与重试的交互

两者解决不同层面的问题，必须协同而不是互相放大：

1. **首次尝试不消耗重试预算**，只有重试消耗令牌。预算每 tick 补充到
   `retryBudgetPerTick`，令牌耗尽时抛出 `RetryBudgetExhaustedError`。
   这防止"客户端无限重试"：重试总量被网格侧显式限流，故障期间重试风暴
   不会压垮正在恢复的 endpoint。
2. **熔断器让重试 fail-fast**。endpoint 打开后 `route()` 不再选中它，重试
   会被导向健康 peer；全部 endpoint 都打开时立即抛出 `PoolExhaustedError`，
   而不是空转等待。
3. **重试发生在熔断器外层**：`execute()` 的每次尝试独立 `tryAcquire` /
   `release`，失败会计入对应 endpoint 的健康统计，因此重试本身会加速
   真正故障实例的熔断（eject），形成负反馈闭环。
4. **每请求重试上限** `maxRetries` 与全局预算双重约束：预算管总量，
   上限管单请求，两者任一耗尽即停止。

## 测试

`node --test` 覆盖：快速失败、慢失败（窗口错误率）、半开并发竞争、
冷却计时、恢复爬坡、并发上限、endpoint 被剔除后重试落到健康 peer、
池耗尽 fail-fast、重试预算耗尽与按 tick 补充。所有用例仅通过
`tick()` 推进时间，无真实定时器。
