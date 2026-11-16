package main

import (
	"fmt"
	"math/rand/v2"
	"sync"
	"time"
)

func producer(jobs chan<- int) {
	for i := range 20 {
		jobs <- i + 1
	}
	close(jobs)
}

func worker(id int, wg *sync.WaitGroup, jobs <-chan int, results chan<- string) {
	defer wg.Done()
	for v := range jobs {
		time.Sleep(time.Duration(10+rand.IntN(91)) * time.Millisecond) // 10–100ms
		results <- fmt.Sprintf("worker %v: %d -> %d", id, v, v*v)
	}
}

func main() {
	jobs := make(chan int)
	results := make(chan string)
	go producer(jobs)

	wg := new(sync.WaitGroup)
	for i := 1; i <= 3; i++ {
		wg.Add(1)
		go worker(i, wg, jobs, results)
	}
	go func() {
		wg.Wait()
		close(results)
	}()
	for v := range results {
		fmt.Println(v)
	}
}
