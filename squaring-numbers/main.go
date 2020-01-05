package main

import "fmt"

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

func main() {
	// Create the source
	c := gen(2, 3)

	// Create the second stage consumer which squares each number
	out := sq(c)

	// Create the output consumer which prints each square as it receives them
	fmt.Println(<-out)
	fmt.Println(<-out)
}
