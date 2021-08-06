package shgo

import (
	"fmt"
	"os"
	"sync"

	"github.com/shgo/src/internal/core"
	"github.com/shgo/src/internal/r"
	"github.com/shgo/src/shgo"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v2"
)

var cnfPath string

var runCmd = &cobra.Command{
	Use:   "run",
	Short: "Try and possibly fail at something",
	RunE: func(cmd *cobra.Command, args []string) error {
		fmt.Println("running!")
		f, err := os.Open(cnfPath)
		if err != nil {
			panic(err)
		}
		defer f.Close()

		fmt.Println("i open file", cnfPath)

		var cfg shgo.GlobConfig
		decoder := yaml.NewDecoder(f)
		err = decoder.Decode(&cfg)
		if err != nil {
			panic(err)
		}

		fmt.Println("PARSED:", cfg.Addr)

		sh := shgo.Shgo{
			Cfg:    cfg,
			Router: core.NewRouter(cfg.RouterCfg),
			R:      r.NewR(),
		}

		wg := &sync.WaitGroup{}

		wg.Add(1)
		go func(wg *sync.WaitGroup) {
			err := sh.ProcPG()

			if err != nil {
				panic(err)
			}

			wg.Done()
		}(wg)

		wg.Add(1)
		go func(wg *sync.WaitGroup) {
			err := sh.ServHttp()

			if err != nil {
				panic(err)
			}

			wg.Wait()
		}(wg)

		wg.Wait()

		return nil
	},
}

func init() {

	runCmd.Flags().StringVarP(&cnfPath, "cfg", "c", "", "lolkek")

	RootCmd.AddCommand(runCmd)
}
