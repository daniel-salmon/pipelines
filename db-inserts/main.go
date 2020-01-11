package main

import (
	"bufio"
	"database/sql"
	"encoding/json"
	"flag"
	"log"
	"os"
	"sync"
	"sync/atomic"

	_ "github.com/go-sql-driver/mysql"
)

type Document struct {
	Type         string `json:"Type"`
	LineID       int    `json:"line_id"`
	PlayName     string `json:"play_name"`
	SpeechNumber int    `json:"speech_number"`
	LineNumber   string `json:"line_number"`
	Speaker      string `json:"speaker"`
	Line         string `json:"line"`
}

var wg sync.WaitGroup
var docCounter uint64

func main() {
	var (
		batchSize             = flag.Int("batch-size", 500, "Number of documents for each worker to process at any given time")
		file                  = flag.String("file", "shakespeare.json", "Name of file to parse")
		mysqlConnectionString = flag.String("mysql-connection", "root:pickles@tcp(172.18.0.1:3307)/shakespeare", "MySQL database connection string")
		numWorkers            = flag.Int("num-workers", 5, "Number of concurrent workers to use for processing")
	)
	flag.Parse()

	// Create the database interface
	db, err := sql.Open("mysql", *mysqlConnectionString)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Ping the database to establish the connection and verify it is available
	err = db.Ping()
	if err != nil {
		log.Fatal(err)
	}
	log.Println("db connection successful!")

	// Create the channel on which we'll pass the Shakespeare Documents
	docsc := make(chan Document)

	// Read the file
	// Emit each line as a Document on the docsc channel
	go func() {
		defer close(docsc)

		f, err := os.Open(*file)
		if err != nil {
			log.Fatal(err)
		}

		s := bufio.NewScanner(f)
		for s.Scan() {
			var doc Document
			if err := json.Unmarshal(s.Bytes(), &doc); err != nil {
				log.Fatal(err)
			}

			docsc <- doc
		}
	}()

	// Start the workers that will process the reads
	wg.Add(*numWorkers)
	for i := 0; i < *numWorkers; i++ {
		go insert(docsc, db, *batchSize)
	}

	wg.Wait()
}

func insert(docsc chan Document, db *sql.DB, batchSize int) {
	var (
		b            int
		insertedDocs uint64
		tx           *sql.Tx
		stmt         *sql.Stmt
		err          error
		rollbackErr  error
	)

	b = 1
	insertedDocs = 0
	for doc := range docsc {
		if b == 1 {
			// Begin a transaction for this batch
			// We open a transaction so that the worker invoking this method has a single,
			// isolated connection in the connection pool
			tx, err = db.Begin()
			if err != nil {
				log.Fatal(err)
			}

			stmt, err = tx.Prepare(`
				INSERT INTO document (
					type, line_id, play_name, speech_number, line_number, speaker, line
				)
				VALUES (?, ?, ?, ?, ?, ?, ?);
			`)
			if err != nil {
				log.Fatal(err)
			}
		}

		// Insert the document
		_, err = stmt.Exec(
			doc.Type,
			doc.LineID,
			doc.PlayName,
			doc.SpeechNumber,
			doc.LineNumber,
			doc.Speaker,
			doc.Line,
		)
		if err != nil {
			if rollbackErr = tx.Rollback(); rollbackErr != nil {
				log.Fatalf("Doc insert failed: %v, unable to rollback: %v", err, rollbackErr)
			} else {
				log.Fatal(err)
			}
		}

		// Increment the number of docs inserted during the current transaction
		insertedDocs++

		if b == batchSize {
			// Close the statement and commit the transaction
			if err = stmt.Close(); err != nil {
				if rollbackErr = tx.Rollback(); rollbackErr != nil {
					log.Fatalf("Unable to close prepared statement: %v, unable to rollback: %v", err, rollbackErr)
				}
				log.Fatalf("Unable to close the prepared statement: %v", err)
			}

			if err = tx.Commit(); err != nil {
				log.Fatalf("Unable to commit transaction: %v", err)
			}

			// Atomically increment the total number of documents that have been inserted by the job
			// We must ensure that each goroutine has exclusive access to the docCounter variable
			// Otherwise we would have a race condition
			atomic.AddUint64(&docCounter, insertedDocs)
			nDocs := atomic.LoadUint64(&docCounter)
			log.Printf("Successfully inserted a new batch of documents. Total docs: %d", nDocs)

			// Reset the batch offset and number of docs inserted in the current transaction
			b = 1
			insertedDocs = 0
		} else {
			b++
		}
	}

	// Commit the final transaction
	// These are the statements mod batch size
	if err = stmt.Close(); err != nil {
		if rollbackErr = tx.Rollback(); rollbackErr != nil {
			log.Fatalf("Unable to close prepared statement: %v, unable to rollback: %v", err, rollbackErr)
		}
		log.Fatalf("Unable to close prepared statement: %v", err)
	}

	if err = tx.Commit(); err != nil {
		log.Fatalf("Unable to commit transaction: %v", err)
	}

	// Increment the total number of documents inserted by the job
	atomic.AddUint64(&docCounter, insertedDocs)
	nDocs := atomic.LoadUint64(&docCounter)
	log.Printf("Successfully inserted the final batch of documents for this worker. Total docs: %d", nDocs)

	wg.Done()
}
