# Go Fan-out 模式学习笔记

参考实现：`../gmaker/internal/engine/engine_dispatch.go`

---

## 一、Fan-out 的两种形态

### 形态 A：任务分发（work distribution）

多个 worker 抢同一个 channel，谁空闲谁拿。适合无状态任务（下载、图片压缩等）。

```
producer ──jobs──► worker 1
                ├──► worker 2
                └──► worker 3
```

问题：同一条"队伍"有多个消费者，输出顺序不确定。

### 形态 B：路由 / 广播（demultiplexing）

一个 goroutine 读取统一的事件流，按 key 分发到各自专属的 channel。每个下游只收属于自己的事件，顺序保证。适合有状态的下游，比如每个市场维护自己的挂单状态。

```
producer ──src──► 路由 goroutine ──► worker A（只处理 A 的事件）
                                 ├──► worker B
                                 └──► worker C
```

gmaker 用的是形态 B。

---

## 二、形态 A 的实现要点（step1）

### WaitGroup + 匿名 goroutine 关闭 results

```
producer ──jobs──► worker 1 ──┐
                 ├──► worker 2 ──┼──► results ──► main（range 打印）
                 └──► worker 3 ──┘
```

收尾时面临一个循环等待：

- `range results` 要等 `results` 被 close 才结束
- `close(results)` 要等 3 个 worker 全部写完
- 两件事不能在同一个 goroutine 里按顺序执行

解法：把"等待并 close"单独交给一个 goroutine，main 只负责读：

```go
go func () {
wg.Wait()
close(results) // 计数归零后 close，通知 range 退出
}()
for v := range results {
fmt.Println(v)
}
```

### close 的原则

只有写入方才能 close channel，而且只能由一个写入方 close。有多个写入方时，用 WaitGroup 汇总成"全部写完"这一个时刻，再 close
一次。

### 为什么 wg.Add 要在 go 之前调用

如果在 goroutine 里调用 Add，main 可能在 Add 执行前就到了 Wait，此时计数还是 0，Wait 直接返回。

---

## 三、形态 B 的实现要点（step2）

### 路由 goroutine 负责 close 下游 channel

路由是 key channel 的唯一写入方，所以由它负责 close：

```go
go func () {
for event := range origin {
switch event.Key { ... }
}
// range 结束 = origin 已关闭且读空，路由不会再写，安全 close
close(chs[0])
close(chs[1])
close(chs[2])
}()
```

注意：`for event := range origin` 在 origin 关闭后**直接结束循环**，不会把零值事件交给循环体。想通过零值事件触发 default
分支来 close 下游，是不正确的。

### 为什么同一个 key 不会乱序

同一个 key 的事件经过的每一步都只有一个 goroutine，且先进先出：

1. producer 按 seq 顺序写进 origin
2. 只有一个路由 goroutine 读 origin，按读到的顺序写进 key channel
3. 每个 key channel 只有一个 worker 在读

sleep 只能让不同 key 之间交错，同一个 key 的事件只能排队。形态 A 会乱序，是因为多个 worker 抢同一个 channel。

---

## 四、慢消费者与背压（step3）

### 慢消费者会通过路由 goroutine 拖慢所有人

无缓冲 channel 要求发送方和接收方同时就绪。如果 A 的 worker 在 sleep，路由写 `chs[0] <- event` 就会阻塞。由于路由是所有 key
共用的，它卡在 A 上时，B 和 C 也拿不到新事件。

### 解法：每个 key 一个中转 goroutine（隔离舱）

在路由和 worker 之间加一层带缓冲的 dispatch channel 和一个中转 goroutine（relay）：

```
路由 ──dispatch[i]（有缓冲）──► relay ──chs[i]──► worker
```

relay 因为 A 的 worker 慢而阻塞时，只影响 A 自己的 relay，路由不受影响，可以继续给 B、C 投递。对应 gmaker 的
`runMarketDispatcher`。

### 缓冲只是推迟阻塞，不能根本解决

| 缓冲容量    | 能装下的事件数 | 路由阻塞时机   |
|-------------|----------------|----------------|
| 大（如 16） | 能装下全部事件 | 从不阻塞       |
| 小（如 3）  | 只能装 3 个    | 缓冲满之后阻塞 |
| 0（无缓冲） | 0              | 立即阻塞       |

实验结果：dispatch 容量为 3，A 的链路能同时容纳 `1（worker 处理中）+ 1（relay 拿着）+ 3（缓冲区）= 5` 个事件。A 有 10 个事件，路由要等
A 腾出位置才能投递后 5 个，这需要 `5 × 500ms = 2.5 秒`。B、C 在 2.5 秒多一点结束，而不是它们本来的 0.6 秒左右。

### 背压策略：按事件类型决定阻塞还是丢弃

缓冲满了只剩两条路：阻塞（拖慢上游）或丢弃（丢数据）。按事件类型选择：

| 事件类型               | 策略                | 理由                         |
|------------------------|---------------------|------------------------------|
| 关键事件（成交、撤单） | 阻塞等待，一定送到  | 丢了会导致订单状态出错       |
| 非关键事件（价格变化） | 队列超过 3/4 时丢弃 | 很快会有新的覆盖，丢了无所谓 |

```go
func relay(key string, in <-chan Event, out chan<- Event) {
    for event := range in {
        if event.Critical {
            // 先非阻塞试一次（快路径）；失败再阻塞等待
            select {
                case out <- event:
                default:
                out <- event // 阻塞，直到有位置
            }
            continue
        }
        // 非关键事件：超过 3/4 水位就丢弃
        if len(out) >= cap(out)*3/4 {
            continue // 丢弃
        }
        // len 是瞬间快照，判断后队列仍可能被写满，用 select+default 避免意外阻塞
        select {
            case out <- event:
            default: // 丢弃
        }
    }
    close(out)
}
```

**为什么"先试一次、再阻塞"好过"直接阻塞"**：正常情况下零等待；只在真发生拥堵时才打日志，拥堵日志因此有实际意义，不是噪声。

**为什么判断后还要用 select + default**：`len(out)` 只是某一时刻的快照，判断通过后、执行写入前，另一个 goroutine
可能刚好也写了一个进去，直接 `out <- event` 会在意想不到的时机阻塞，破坏"非关键事件不阻塞"的承诺。

**3/4 水位的含义**：非关键事件最多占用 3/4 的容量，剩下 1/4
专门留给关键事件。这是经验值，可以通过观察"等待投递"日志和丢弃计数来调整，不是算出来的。

注意整数运算的顺序：`cap(out)*3/4` 是先乘后除（正确），`cap(out)*(3/4)` 中 `3/4` 在整数运算里等于 0（结果错误，非关键事件全部被丢弃）。

### 背压策略的代价

如果慢消费者收到的关键事件很多，relay 会频繁阻塞等待，阻塞还是会传回路由 goroutine。这是不可避免的：关键事件不能丢，慢消费者的速度就是系统的瓶颈。

---

## 五、close 链条

每一步的写入方负责 close 自己的输出 channel，形成一条从 producer 到 worker 的 close 传递链：

```
producer 关 origin
    → 路由 goroutine 关 dispatch[i]
        → relay 关 chs[i]
            → worker 退出
                → wg.Wait() 返回
                    → main 退出
```

### WaitGroup 只等 worker，不等 relay

worker 能退出，说明 `chs[i]` 已被 close，也就说明 relay 已经走完了它的 `range` 循环。relay 不需要加进 WaitGroup。

---

## 六、对应 gmaker 的位置

| 本项目                     | gmaker                                                  |
|----------------------------|---------------------------------------------------------|
| `producer`                 | `client.Subscribe` 的 WebSocket 读取                    |
| `路由 goroutine`           | `engine.fanOut`（`engine_dispatch.go:90`）              |
| `dispatch[i]`              | `engine.dispatchChans[marketID]`（容量 4096）           |
| `relay`                    | `engine.runMarketDispatcher`（`engine_dispatch.go:50`） |
| `chs[i]`                   | `engine.workerChans[marketID]`（容量 4096）             |
| `worker`                   | `MarketWorker.Run`                                      |
| `registerMarketChanLocked` | 运行时动态注册 marketID，支持市场热插拔                 |

gmaker 的 fanOut 比 step3 多了两件事：

1. **广播**：`EventBook`、`EventPriceChange` 同时发给 marketdata 和 features 两个独立的 channel。
2. **动态路由表**：market 可以在运行中增删，路由表由 `sync.RWMutex` 保护。读锁下查表，持锁时间极短。
