package parser

import (
	"strings"
	"unicode"

	"golang.org/x/xerrors"
)

/*
key: value[, key1: value1...]
*/
func ParseComment(comm string) (map[string]string, error) {
	opts := make(map[string]string)

	for i := 0; i < len(comm); {
		if unicode.IsSpace(rune(comm[i])) {
			// skip initial spaces
			i++
			continue
		}
		// opts are in form opt: val, reject all other format

		// now we are looking at *probably* first char of opt name
		j := i
		for ; j < len(comm) && comm[j] != ':' && !unicode.IsSpace(rune(comm[j])); j++ {
		}
		optarg_end := j - 1

		// colon symbol not found
		if j == len(comm) {
			return nil, xerrors.New("invalid comment format")
		}
		optarg_len := optarg_end - i + 1

		if optarg_len == 0 {
			// empty opt name
			return nil, xerrors.New("invalid comment format: empty option name")
		}

		// skip spaces after colon
		for ; j < len(comm) && unicode.IsSpace(rune(comm[j])); j++ {
		}

		if j == len(comm) || comm[j] != ':' {
			return nil, xerrors.New("invalid comment format: expected colon after option name")
		}
		// skip colon symbol
		j++

		//skip spaces after colon
		for ; j < len(comm) && unicode.IsSpace(rune(comm[j])); j++ {
		}

		if j == len(comm) {
			// empty opt name
			return nil, xerrors.New("invalid comment format: empty option values")
		}

		// now we are looking at first char of opt value
		optval_pos := j
		for j+1 < len(comm) && !unicode.IsSpace(rune(comm[j+1])) && comm[j+1] != ',' {
			j++
		}

		optval_end := j

		opt_name := comm[i : optarg_end+1]
		if after, ok := strings.CutPrefix(opt_name, "__spqr__."); ok {
			opt_name = "__spqr__" + after
		}

		opts[opt_name] = comm[optval_pos : optval_end+1]

		j++
		// skip spaces after value
		for ; j < len(comm) && unicode.IsSpace(rune(comm[j])); j++ {
		}
		if j < len(comm) && comm[j] != ',' {
			// empty opt name
			return nil, xerrors.New("invalid comment format: expected comma after not-last key-value pair")
		}
		// skip comma
		j++
		i = j
	}

	return opts, nil
}
