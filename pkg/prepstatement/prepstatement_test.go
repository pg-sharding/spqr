package prepstatement_test

import (
	"testing"

	"github.com/pg-sharding/spqr/pkg/models/spqrerror"
	"github.com/pg-sharding/spqr/pkg/prepstatement"
	"github.com/pg-sharding/spqr/router/xproto"
	"github.com/stretchr/testify/assert"
)

func TestGetParamsDefaultFormatCodes(t *testing.T) {
	t.Parallel()

	codes, err := prepstatement.GetParams(nil, [][]byte{[]byte("a"), []byte("b")})
	assert.NoError(t, err)
	assert.Equal(t, []int16{xproto.FormatCodeText, xproto.FormatCodeText}, codes)
}

func TestGetParamsSingleFormatCode(t *testing.T) {
	t.Parallel()

	codes, err := prepstatement.GetParams([]int16{xproto.FormatCodeBinary}, [][]byte{[]byte("a"), []byte("b")})
	assert.NoError(t, err)
	assert.Equal(t, []int16{xproto.FormatCodeBinary, xproto.FormatCodeBinary}, codes)
}

func TestGetParamsMultiFormatCodesMatchingBindParams(t *testing.T) {
	t.Parallel()

	codes, err := prepstatement.GetParams([]int16{0, 1}, [][]byte{[]byte("a"), []byte("b")})
	assert.NoError(t, err)
	assert.Equal(t, []int16{0, 1}, codes)
}

func TestGetParamsMultiFormatCodesMismatchedBindParams(t *testing.T) {
	t.Parallel()

	codes, err := prepstatement.GetParams([]int16{0, 1}, nil)
	assert.Nil(t, codes)

	var spqrErr *spqrerror.SpqrError
	assert.ErrorAs(t, err, &spqrErr)
	assert.Equal(t, spqrerror.PG_ERRCODE_PROTOCOL_VIOLATION, spqrErr.ErrorCode)
}
