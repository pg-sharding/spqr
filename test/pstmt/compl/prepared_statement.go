package main

import (
	"context"
	"flag"
	"log"
	"sync"

	"github.com/jmoiron/sqlx"

	"time"

	_ "github.com/lib/pq"
)

var Q = `
SELECT 1
`
var concurrency int
var sleepTo int
var updTx bool

// nolint
func prep(wg *sync.WaitGroup) {
	ctx, cf := context.WithTimeout(context.Background(), 2*time.Second)
	defer cf()
	defer wg.Done()

	var err error
	db, err := sqlx.ConnectContext(ctx, "postgres", "host=localhost port=6432 user=user1 dbname=db1 sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}

	defer db.Close()

	if updTx {
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			log.Fatal(err)
		}

		defer tx.Rollback()

		stmt, err := tx.Prepare(Q)

		if err != nil {
			log.Printf("Not ok: %v\n", err)
			return
		}

		for i := 0; i < 10; i++ {
			_, err = stmt.Exec()
			if err != nil {
				log.Printf("Not ok: %v\n", err)
				return
			}
		}

		if err := tx.Commit(); err != nil {
			log.Printf("Not ok: %v\n", err)
			return
		}

		log.Println("OK")
	} else {
		stmt, err := db.Prepare(Q)

		if err != nil {
			log.Printf("Not ok: %v\n", err)
			return
		}

		for i := 0; i < 10; i++ {
			_, err = stmt.Exec()
			if err != nil {
				log.Printf("Not ok: %v\n", err)
				return
			}
		}

		log.Println("OK")
	}
}

func main() {
	flag.IntVar(&concurrency, "concurrency", 1, "help msg")
	flag.IntVar(&sleepTo, "sleep-to", 1, "help msg")
	flag.BoolVar(&updTx, "upd-tx", true, "help msg")

	flag.Parse()

	for {
		wg := sync.WaitGroup{}

		cnt := concurrency
		wg.Add(cnt)
		for i := 0; i < cnt; i++ {
			go prep(&wg)
		}
		wg.Wait()

		log.Println("ITER OK")

		time.Sleep(time.Duration(sleepTo) * time.Second)
	}
}
