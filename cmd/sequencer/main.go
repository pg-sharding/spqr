package sequencer

import (
	"github.com/pg-sharding/spqr/pkg"
	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pg-sharding/spqr/qdb"
	"github.com/pg-sharding/spqr/sequencer/seqproto"
	"github.com/spf13/cobra"
)

var (
	rcfgPath string

	rootCmd = &cobra.Command{
		Use:   "spqr-sequencer run --config `path-to-config-folder`",
		Short: "spqr-sequencer",
		Long:  "spqr-sequencer ",
		CompletionOptions: cobra.CompletionOptions{
			DisableDefaultCmd: true,
		},
		Version:       pkg.SpqrVersionRevision,
		SilenceUsage:  false,
		SilenceErrors: false,
	}
)

func init() {
	rootCmd.PersistentFlags().StringVarP(&rcfgPath, "config", "c", "/etc/spqr/router.yaml", "path to router/sequencer config file")

	rootCmd.AddCommand(runCmd)
}

var runCmd = &cobra.Command{
	Use:   "run",
	Short: "run sequencer",
	RunE: func(cmd *cobra.Command, args []string) error {

		err := config.LoadSequencerCfg(rcfgPath)
		if err != nil {
			return err
		}
		scfg := config.SequencerConfig()

		spqrlog.ReloadLogger(scfg.LogFileName)

		pi, err := seqproto.NewNetProtoInteractor(scfg)
		if err != nil {
			return err
		}

		qdbConn := qdb.NewXQDB(scfg)

		for {
			cl, err := pi.AcceptClient()
			if err != nil {
				return err
			}

			go func() {

				tp, body, err := pi.DecodeMessage()
				if err != nil {
					spqrlog.Zero.Error().Err(err)
					return
				}

				switch tp {
				case seqproto.Create:
				case seqproto.NextVal:
				case seqproto.CurrentVal:

				}

			}()
		}

	},
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		spqrlog.Zero.Fatal().Err(err).Msg("")
	}
}
