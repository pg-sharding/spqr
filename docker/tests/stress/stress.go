
package main 

import (
	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
	"fmt"
	"time"
	"sync"
	"math/rand"
)

var r = rand.New(rand.NewSource(31337))

func simple(wg *sync.WaitGroup) {
	defer wg.Done()


	for {

		time.Sleep(time.Duration(1 + r.Intn(10)) * time.Second)
		
		fmt.Println("SELECT * FROM x WHERE i = ")
	}
}

var par int

var cmd = &cobra.Command{
	Use:   "stress -p `parallel`",
	Short: "SPQR stress test tool",
	CompletionOptions: cobra.CompletionOptions{
		DisableDefaultCmd: true,
	},
	RunE: func(cmd *cobra.Command, args []string) error {
		fmt.Println("loh")

		tracelog.InfoLogger.Printf("loh2")

		wg := &sync.WaitGroup{}

		for i := 0; i < par; i++ {
			wg.Add(1)
			go simple(wg)
		}

		wg.Wait()

		return nil
	},
	SilenceUsage:  true,
	SilenceErrors: true,
}

func init() {
	cmd.PersistentFlags().IntVarP(&par, "parallel", "p", 10, "# of workers")
}

func main() {
	cmd.Execute()
}
