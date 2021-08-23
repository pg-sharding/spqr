package main

import (
	"fmt"
	"os"
	"sync"

	"github.com/spf13/cobra"
	"github.com/pg-sharding/spqr/app"
	"github.com/pg-sharding/spqr/internal/core"
	"github.com/pg-sharding/spqr/internal/r"
	"github.com/pg-sharding/spqr/internal/spqr"
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

		var cfg spqr.GlobConfig
		decoder := yaml.NewDecoder(f)
		err = decoder.Decode(&cfg)
		if err != nil {
			panic(err)
		}

		fmt.Println("PARSED:", cfg.Addr)

		rt, err := core.NewRouter(cfg.RouterCfg)

		spqr, err := spqr.NewSpqr(
			cfg,
			rt,
			r.NewR(),
		)
		if err != nil {
			return err
		}

		app := app.NewApp(spqr)
		wg := &sync.WaitGroup{}

		wg.Add(1)
		go func(wg *sync.WaitGroup) {
			err := app.ProcPG()

			if err != nil {
				panic(err)
			}

			wg.Done()
		}(wg)

		wg.Add(1)
		go func(wg *sync.WaitGroup) {
			err := app.ServHttp()

			if err != nil {
				panic(err)
			}

			wg.Done()
		}(wg)

		wg.Add(1)
		go func(wg *sync.WaitGroup) {
			err := app.ProcADM()
			if err != nil {
				panic(err)
			}

			wg.Done()
		}(wg)

		wg.Wait()

		return nil
	},
}

func init() {

	runCmd.Flags().StringVarP(&cnfPath, "cfg", "c", "", "lolkek")

	RootCmd.AddCommand(runCmd)
}
