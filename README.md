# Fan-out 模式学习项目

通过三步动手实验学习 Go 的 fan-out 并发模式，参考真实项目 `../gmaker/internal/engine/engine_dispatch.go` 的实现。

> 本项目目录为 `go-fan-out/`。

## 三步

| 目录     | 主题                 | 核心内容                                        |
|----------|----------------------|-------------------------------------------------|
| `step1/` | worker pool + fan-in | WaitGroup 收尾、谁来 close、`wg.Add` 时机       |
| `step2/` | 按 key 路由          | 路由 goroutine 负责 close 下游、顺序保证        |
| `step3/` | 慢消费者与背压       | 中转 goroutine 隔离、3/4 水位丢弃、关键事件阻塞 |

## 学习顺序

按 step1 → step2 → step3 顺序。每步在上一步基础上增加难度。

## 文档

- [`docs/fan-out-pattern.md`](docs/fan-out-pattern.md) — 三步讨论整理的学习笔记，涵盖所有设计要点和对应 gmaker 的代码位置

## 前置要求

- 会写基本的 Go 程序
- 了解 goroutine 和 channel 的基本用法

## 之后

可以继续 `../go-concurrency/` 课程，覆盖 Mutex、atomic、context、errgroup 等更多并发主题。

