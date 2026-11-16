// step3：慢消费者与背压。
//
// 数据流：
//
//	producer ──origin（无缓冲）──► 路由 goroutine
//	                                 ├──dispatch[0]（缓冲 3）──► relay A ──chs[0]（缓冲 4）──► worker A（慢，500ms/个）
//	                                 ├──dispatch[1]（缓冲 3）──► relay B ──chs[1]（缓冲 4）──► worker B
//	                                 └──dispatch[2]（缓冲 3）──► relay C ──chs[2]（缓冲 4）──► worker C
//
// 每个 key 有独立的 relay（中转 goroutine），慢 worker 的阻塞只会卡住自己的 relay，
// 不会卡住共用的路由 goroutine。relay 按事件类型做背压：关键事件一定送到，
// 非关键事件在队列快满时丢弃。对应 gmaker 的 internal/engine/engine_dispatch.go。
package main

import (
	"fmt"
	"math/rand/v2"
	"sync"
	"time"
)

type Event struct {
	Key      string
	Seq      int  // 该 key 下的序号，从 1 开始，用来检查同一个 key 是否乱序
	Critical bool // 关键事件（类比成交、撤单）：不能丢；非关键事件（类比价格变化）可以丢
}

var keys = []string{"A", "B", "C"}

// counters 是每个 key 的序号计数器。只有 producer 一个 goroutine 读写它，所以不需要加锁。
var counters = []int{0, 0, 0}

// producer 按 A、B、C 轮换生成 30 个事件，每个 key 的第 5、10 个是关键事件。
// 它是 origin 唯一的写入方，所以由它 close origin。
func producer(ch chan<- Event) {
	for i := range 30 {
		counter := counters[i%3] + 1
		counters[i%3] = counter
		ch <- Event{
			Key:      keys[i%3],
			Seq:      counter,
			Critical: counter%5 == 0,
		}
		time.Sleep(100 * time.Millisecond)
	}
	close(ch)
}

// worker 只处理自己 key 的事件。A 故意很慢，用来模拟慢消费者。
// 同一个 key 只有一个 worker 顺序处理，所以这个 key 的事件不会乱序。
func worker(id int, start time.Time, wg *sync.WaitGroup, ch <-chan Event) {
	defer wg.Done()
	received, critical := 0, 0
	// range 在 channel 被 close 且读空后才结束，close 由 relay 负责。
	for event := range ch {
		received++
		if event.Critical {
			critical++
		}
		if event.Key == "A" {
			time.Sleep(500 * time.Millisecond) // 慢消费者
		} else {
			time.Sleep(time.Duration(10+rand.IntN(91)) * time.Millisecond) // 10–100ms
		}
		fmt.Printf("Worker %d: %v -> seq %d\n", id, event.Key, event.Seq)
	}
	fmt.Printf("Worker %d done at %v, 收到 %d 个, 其中关键事件 %d 个\n", id, time.Since(start), received, critical)
}

// relay 是路由 goroutine 和 worker 之间的中转，对应 gmaker 的 runMarketDispatcher。
// 每个事件只走三条路之一：直接送到、等待后送到（仅关键事件）、丢弃（仅非关键事件）。
// relay 是 out 唯一的写入方，所以由它 close out。
func relay(key string, in <-chan Event, out chan<- Event) {
	dropped := 0
	for event := range in {
		if event.Critical {
			// 关键事件：先非阻塞试一次。成功就没有任何延迟；
			// 失败说明队列满了，打一条日志让人知道发生了拥堵，然后阻塞等待，一定送到。
			select {
			case out <- event:
			default:
				fmt.Printf("relay %s: 队列满，关键事件 seq %d 等待投递\n", key, event.Seq)
				out <- event
			}
			continue
		}
		// 非关键事件：队列用量达到 3/4 就丢弃，把最后 1/4 留给关键事件。
		// 注意先乘后除：cap(out)*(3/4) 在整数运算里会变成 0。
		if len(out) >= cap(out)*3/4 {
			dropped++
			continue
		}
		// len 只是一个瞬间的快照，判断之后、写入之前队列仍可能被写满，
		// 所以这里用 select + default 写入，写不进去也算丢弃，绝不阻塞。
		select {
		case out <- event:
		default:
			dropped++
		}
	}
	// 走到这里说明 in 已被 close 且读空，relay 不会再写 out，可以安全 close。
	close(out)
	fmt.Printf("relay %s: 丢弃 %d 个非关键事件\n", key, dropped)
}

func main() {
	start := time.Now()
	origin := make(chan Event)
	// worker 的输入队列。必须有缓冲：无缓冲时 cap 为 0，3/4 水位判断会让非关键事件全部被丢掉。
	chs := []chan Event{
		make(chan Event, 4),
		make(chan Event, 4),
		make(chan Event, 4),
	}
	// 路由写入的 dispatch 队列。每个 key 一个，A 堵住时只会填满 dispatch[0]。
	dispatch := []chan Event{
		make(chan Event, 3),
		make(chan Event, 3),
		make(chan Event, 3),
	}
	wg := new(sync.WaitGroup)

	go producer(origin)

	// 路由 goroutine（对应 gmaker 的 fanOut）：唯一读取 origin 的 goroutine，
	// 按 Key 把事件写进对应的 dispatch 队列。
	go func() {
		for event := range origin {
			switch event.Key {
			case "A":
				dispatch[0] <- event
			case "B":
				dispatch[1] <- event
			case "C":
				dispatch[2] <- event
			}
		}
		// range 结束说明 origin 已关闭且读空，路由不会再写 dispatch，在这里 close。
		close(dispatch[0])
		close(dispatch[1])
		close(dispatch[2])
	}()

	// 每个 key 启动一个 worker 和一个 relay。
	// WaitGroup 只等 worker：worker 能退出，说明 relay 已经 close 了 chs[i]，也就已经结束了。
	for i := range 3 {
		wg.Add(1) // 在 go 之前 Add，避免 main 先执行到 Wait 时计数还是 0
		go worker(i, start, wg, chs[i])
		go relay(keys[i], dispatch[i], chs[i])
	}

	// close 链条：producer 关 origin → 路由关 dispatch → relay 关 chs → worker 退出 → Wait 返回。
	wg.Wait()
}
