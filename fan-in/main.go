package main

import (
	"fmt"
	"sync"
)

func main() {
	// Create the source channel
	in := gen(2, 3)

	// Distribute work across two goroutines that both read from the in channel
	c1 := sq(in)
	c2 := sq(in)

	// Consume the merged output from c1 and c2
	for n := range merge(c1, c2) {
		fmt.Println(n)
	}
}

// This function takes a list of integers and emits them on a channel
func gen(nums ...int) <-chan int {
	out := make(chan int)
	go func() {
		for _, n := range nums {
			out <- n
		}
		close(out)
	}()
	return out
}

// This function receives integers on a channel and squares each integer in turn,
// emitting the output on a channel
func sq(in <-chan int) <-chan int {
	out := make(chan int)
	go func() {
		for n := range in {
			out <- n * n
		}
		close(out)
	}()
	return out
}

func merge(cs ...<-chan int) <-chan int {
	var wg sync.WaitGroup
	out := make(chan int)

	// Start an output goroutine for each input channel c in cs
	// The output function reads values from channel c, putting them onto the out
	// channel until channel c is closed, after which it calls wg.Done
	output := func(c <-chan int) {
		for n := range c {
			out <- n
		}
		wg.Done()
	}
	wg.Add(len(cs))
	for _, c := range cs {
		go output(c)
	}

	// Start a goroutine to close out once all the output goroutines have finished
	go func() {
		wg.Wait()
		close(out)
	}()
	return out
}
