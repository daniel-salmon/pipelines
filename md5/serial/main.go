package main

import (
	"crypto/md5"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"sort"
)

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

func MD5All(root string) (map[string][md5.Size]byte, error) {
	m := make(map[string][md5.Size]byte)

	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// We only want to hash regular files on our system
		if !info.Mode().IsRegular() {
			return nil
		}

		data, err := ioutil.ReadFile(path)
		if err != nil {
			return err
		}

		m[path] = md5.Sum(data)

		return nil
	})
	if err != nil {
		return nil, err
	}

	return m, nil
}
