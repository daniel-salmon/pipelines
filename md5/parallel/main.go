package main

import (
	"crypto/md5"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

type result struct {
	path string
	sum  [md5.Size]byte
	err  error
}

func main() {
	wd, err := os.Getwd()
	if err != nil {
		log.Fatal(err)
	}

	var (
		dir = flag.String("dir", wd, "Directory for which to recursively calculate file MD5s")

		paths []string
	)
	flag.Parse()

	// Obtain a map of filepaths to the file's md5 hash
	m, err := MD5All(*dir)
	if err != nil {
		log.Fatal(err)
	}

	// Get a list of all of the filepaths so we can sort them for nice printing at the end
	for path := range m {
		paths = append(paths, path)
	}
	sort.Strings(paths)

	log.Println("here are your md5s:")
	for _, path := range paths {
		fmt.Printf("%x %s\n", m[path], path)
	}
}

func sumFiles(done <-chan struct{}, root string) (<-chan result, <-chan error) {
	// Start a goroutine for each regular file encountered as we walk the filepath
	// The goroutine sums the file and sends the result on output channel c.
	// Send any errors encountered during the walk on channel errc
	c := make(chan result)
	errc := make(chan error, 1)

	go func() {
		var wg sync.WaitGroup

		err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}

			if !info.Mode().IsRegular() {
				return nil
			}

			wg.Add(1)
			go func() {
				data, err := ioutil.ReadFile(path)
				select {
				case c <- result{path, md5.Sum(data), err}:
				case <-done:
				}
				wg.Done()
			}()

			// Abort the walk if done is closed
			select {
			case <-done:
				return errors.New("walk cancelled")
			default:
				return nil
			}
		})

		// The walk has returned so all calls to wg.Add(1) have been executed.
		// Start a goroutine to close c once all the sends / calls to wg.Done() have happened
		go func() {
			wg.Wait()
			close(c)
		}()

		// No select needed here since errc is buffered
		errc <- err
	}()

	return c, errc
}

func MD5All(root string) (map[string][md5.Size]byte, error) {
	done := make(chan struct{})
	defer close(done)

	c, errc := sumFiles(done, root)

	m := make(map[string][md5.Size]byte)
	for r := range c {
		if r.err != nil {
			return nil, r.err
		}
		m[r.path] = r.sum
	}
	if err := <-errc; err != nil {
		return nil, err
	}
	return m, nil
}
