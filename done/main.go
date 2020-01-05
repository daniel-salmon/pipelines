package main

import (
	"fmt"
	"sync"
)

func main() {
	// Create a done channel that's shared by all stages of the pipeline
	// Close that channel when the pipeline exits to signal all goroutines to exit
	done := make(chan struct{})
	defer close(done)

	// Create the source channel
	in := gen(2, 3)

	// Distribute work across two goroutines that both read from the in channel
	c1 := sq(done, in)
	c2 := sq(done, in)

	// Consume the first value produced by merge
	out := merge(done, c1, c2)
	fmt.Println(<-out)
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
func sq(done <-chan struct{}, in <-chan int) <-chan int {
	out := make(chan int)
	go func() {
		defer close(out)
		for n := range in {
			select {
			case out <- n * n:
			case <-done:
				return
			}
		}
	}()
	return out
}

func merge(done <-chan struct{}, cs ...<-chan int) <-chan int {
	var wg sync.WaitGroup
	out := make(chan int)

	// Start an output goroutine for each input channel c in cs
	// The output function reads values from channel c, putting them onto the out
	// channel until channel c is closed or until it receives a value from done,
	// after which it calls wg.Done
	output := func(c <-chan int) {
		defer wg.Done()
		for n := range c {
			select {
			case out <- n:
			case <-done:
				return
			}
		}
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
