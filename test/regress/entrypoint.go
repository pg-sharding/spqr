package main

import (
	"fmt"
	"os"
	"os/exec"
	"time"
)

func main() {
	help, err := exec.Command("pg_regress", "-h").Output()
	if err != nil {
		fmt.Println(err)
		fmt.Println("pg_regress not found")
		os.Exit(1)
	}
	fmt.Println(string(help))

	testdata := [][]string{
		{"router", "regress_router_1", "6432"},
		// {"console", "regress_router_1", "7432"},
		// {"coordinator", "regress_coordinator_1", "7002"},
	}

	time.Sleep(10 * time.Second)
	status := 0
	for _, line := range testdata {
		cmd, err := exec.Command("/bin/sh", "/regress/run_tests.sh", line[0], line[1], line[2]).Output()
		if err != nil {
			status = 2
		}
		fmt.Println(string(cmd))
	}
	os.Exit(status)
}
