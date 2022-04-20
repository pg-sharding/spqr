package main

import (
	"log"
	"github.com/jmoiron/sqlx"
	"context"
	"sync"

	"time"
	_ "github.com/lib/pq"

)


var Q = `
SELECT 1
`

func prep(wg *sync.WaitGroup) {
	ctx, cf := context.WithTimeout(context.Background(), 2 * time.Second)
	defer cf()
	defer wg.Done()

	var err error
	db, err := sqlx.ConnectContext(ctx, "postgres", "host=localhost port=6432 user=user1 dbname=db1 sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}

	defer db.Close()

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		log.Fatal(err)
	}

	stmt, err := tx.Prepare(Q);

	if err != nil {
		log.Printf("Not ok: %v\n", err)
		return;
	}

	for i := 0; i < 10; i ++  {
		_, err = stmt.Exec()
		if err != nil {
			log.Printf("Not ok: %v\n", err)
			return;
		}
	}

	log.Println("OK")

	tx.Commit()
}

func main() {

	for {
		wg := sync.WaitGroup{}

		cnt := 10
		wg.Add(cnt)
		for i := 0; i < cnt; i ++ {
			go prep(&wg)
		}
		wg.Wait()

		log.Println("ITER OK")

		time.Sleep(1 * time.Second)
	}
}
