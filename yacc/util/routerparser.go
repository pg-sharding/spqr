package main

import (
	"bufio"
	"encoding/json"
	"os"

	"github.com/pg-sharding/lyx/lyx"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
)

/*

select * from xx where i = 1 and j = 7;
*/

func main() {
	reader := bufio.NewReader(os.Stdin)

	for {
		line, err := reader.ReadString('\n')

		if err != nil {
			return
		}

		spqrlog.Zero.Info().
			Str("line", line).
			Msg("got query")
		tmp, err := lyx.Parse(line)
		if err != nil {
			spqrlog.Zero.Err(err)
		}

		_, _ = json.Marshal(tmp)
		spqrlog.Zero.Err(err)
	}
}
