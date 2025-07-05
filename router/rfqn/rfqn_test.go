package rfqn_test

import (
	"testing"

	"github.com/pg-sharding/spqr/router/rfqn"
	"github.com/stretchr/testify/assert"
)

func TestParseQualifiedNameSuccess(t *testing.T) {
	assert := assert.New(t)

	qName1, err1 := rfqn.ParseFQN("schema1.table1")
	assert.NoError(err1)
	assert.Equal(*qName1, rfqn.RelationFQN{SchemaName: "schema1", RelationName: "table1"})

	qName2, err2 := rfqn.ParseFQN("table1")
	assert.NoError(err2)
	assert.Equal(*qName2, rfqn.RelationFQN{RelationName: "table1"})
}

func TestParseQualifiedNameFail(t *testing.T) {
	assert := assert.New(t)
	_, err1 := rfqn.ParseFQN(".table1")
	assert.Error(err1)
	_, err2 := rfqn.ParseFQN("table1.")
	assert.Error(err2)
	_, err3 := rfqn.ParseFQN(".")
	assert.Error(err3)
	_, err4 := rfqn.ParseFQN("")
	assert.Error(err4)
	_, err5 := rfqn.ParseFQN(". ")
	assert.Error(err5)
	_, err6 := rfqn.ParseFQN(" .")
	assert.Error(err6)
}
