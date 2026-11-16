package main

import (
	"fmt"
	"math/rand/v2"
	"sync"
	"time"
)

type Event struct {
	Key string
	Seq int
}

var keys = []string{"A", "B", "C"}
var counters = []int{0, 0, 0}

func producer(ch chan<- Event) {
	for i := range 30 {
		counter := counters[i%3] + 1
		counters[i%3] = counter
		ch <- Event{Key: keys[i%3], Seq: counter}
	}
	close(ch)
}

func worker(id int, wg *sync.WaitGroup, ch <-chan Event) {
	defer wg.Done()
	for event := range ch {
		time.Sleep(time.Duration(10+rand.IntN(91)) * time.Millisecond) // 10–100ms
		fmt.Printf("Worker %d: %v -> seq %d\n", id, event.Key, event.Seq)
	}
	fmt.Printf("Worker %d done\n", id)
}

func main() {
	origin := make(chan Event)
	chs := []chan Event{
		make(chan Event),
		make(chan Event),
		make(chan Event),
	}
	wg := new(sync.WaitGroup)

	go producer(origin)

	// 路由goroutine
	go func() {
		for event := range origin {
			switch event.Key {
			case "A":
				chs[0] <- event
			case "B":
				chs[1] <- event
			case "C":
				chs[2] <- event
			}
		}
		close(chs[0])
		close(chs[1])
		close(chs[2])
	}()

	// worker
	for i := range 3 {
		wg.Add(1)
		go worker(i, wg, chs[i])
	}

	wg.Wait()
}
