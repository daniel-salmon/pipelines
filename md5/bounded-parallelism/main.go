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
		dir        = flag.String("dir", wd, "Directory for which to recursively calculate the md5 hash of files")
		numWorkers = flag.Int("num-workers", 5, "Number of concurrent workers")

		paths []string
	)
	flag.Parse()

	m, err := MD5All(*dir, *numWorkers)
	if err != nil {
		log.Fatal(err)
	}

	for path := range m {
		paths = append(paths, path)
	}
	sort.Strings(paths)

	log.Println("here are your md5s:")
	for _, path := range paths {
		fmt.Printf("%x %s\n", m[path], path)
	}
}

// Emit filepaths on the paths channel as we walk the directory tree
func walkFiles(done <-chan struct{}, root string) (<-chan string, <-chan error) {
	paths := make(chan string)
	errc := make(chan error, 1)

	go func() {
		defer close(paths)

		errc <- filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}

			if !info.Mode().IsRegular() {
				return nil
			}

			select {
			case paths <- path:
			case <-done:
				return errors.New("walk cancelled")
			}

			return nil
		})
	}()

	return paths, errc
}

// The digester reads in filepaths as they are emitted on the channel,
// calculating the md5 hash of the file and emitting the hash on the results channel
func digester(done <-chan struct{}, paths <-chan string, c chan<- result) {
	for path := range paths {
		data, err := ioutil.ReadFile(path)
		select {
		case c <- result{path, md5.Sum(data), err}:
		case <-done:
			return
		}
	}
}

func MD5All(root string, numWorkers int) (map[string][md5.Size]byte, error) {
	var m map[string][md5.Size]byte

	done := make(chan struct{})
	defer close(done)

	paths, errc := walkFiles(done, root)

	// Start a fixed number of workers to digest the files at filepaths published on the paths channel
	c := make(chan result)
	var wg sync.WaitGroup
	wg.Add(numWorkers)
	for i := 0; i < numWorkers; i++ {
		go func() {
			digester(done, paths, c)
			wg.Done()
		}()
	}

	go func() {
		wg.Wait()
		close(c)
	}()

	// Receive and process results
	m = make(map[string][md5.Size]byte)
	for r := range c {
		if r.err != nil {
			return nil, r.err
		}
		m[r.path] = r.sum
	}

	// Check for any errors encountered during the walk
	if err := <-errc; err != nil {
		return nil, err
	}

	return m, nil
}
