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

	stmt, err := db.Prepare(Q);

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
}

func main() {

	for {
		wg := sync.WaitGroup{}

		wg.Add(1)
		for i := 0; i < 1; i ++ {
			go prep(&wg)
		}
		wg.Wait()

		log.Println("ITER OK")

		time.Sleep(1 * time.Second)
		return
	}
}
