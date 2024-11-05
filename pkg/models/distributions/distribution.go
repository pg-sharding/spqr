package distributions

import (
	"fmt"

	"github.com/pg-sharding/spqr/pkg/models/spqrerror"
	proto "github.com/pg-sharding/spqr/pkg/protos"
	"github.com/pg-sharding/spqr/qdb"
	spqrparser "github.com/pg-sharding/spqr/yacc/console"
)

type DistributionKeyEntry struct {
	Column       string
	HashFunction string
}

type DistributedRelation struct {
	Name               string
	DistributionKey    []DistributionKeyEntry
	ReplicatedRelation bool
}

// DistributedRelationFromDB creates a DistributedRelation object from a qdb.DistributedRelation object.
//
// Parameters:
//   - rel: The qdb.DistributedRelation object to convert.
//
// Returns:
//   - *DistributedRelation: The created DistributedRelation object.
func DistributedRelationFromDB(rel *qdb.DistributedRelation) *DistributedRelation {
	rdistr := &DistributedRelation{
		Name: rel.Name,
	}

	for _, e := range rel.DistributionKey {
		rdistr.DistributionKey = append(rdistr.DistributionKey, DistributionKeyEntry{
			Column:       e.Column,
			HashFunction: e.HashFunction,
		})
	}

	rdistr.ReplicatedRelation = rel.ReplicatedRelation

	return rdistr
}

// DistributedRelationToDB converts a DistributedRelation object to a qdb.DistributedRelation object.
// It copies the name and distribution key from the input object to the output object.
//
// Parameters:
//   - rel: The DistributedRelation object to convert.
//
// Returns:
//   - *qdb.DistributedRelation: The converted qdb.DistributedRelation object.
func DistributedRelationToDB(rel *DistributedRelation) *qdb.DistributedRelation {
	rdistr := &qdb.DistributedRelation{
		Name: rel.Name,
	}

	for _, e := range rel.DistributionKey {
		rdistr.DistributionKey = append(rdistr.DistributionKey, qdb.DistributionKeyEntry{
			Column:       e.Column,
			HashFunction: e.HashFunction,
		})
	}

	rdistr.ReplicatedRelation = rel.ReplicatedRelation

	return rdistr
}

// DistributedRelatitonToProto converts a DistributedRelation object to a proto.DistributedRelation object.
// It takes a pointer to a DistributedRelation object as input and returns a pointer to a proto.DistributedRelation object.
//
// Parameters:
//   - rel: The DistributedRelation object to convert.
//
// Returns:
//   - *proto.DistributedRelation: The converted proto.DistributedRelation object.
func DistributedRelatitonToProto(rel *DistributedRelation) *proto.DistributedRelation {
	rdistr := &proto.DistributedRelation{
		Name: rel.Name,
	}

	for _, e := range rel.DistributionKey {
		rdistr.DistributionKey = append(rdistr.DistributionKey, &proto.DistributionKeyEntry{
			Column:       e.Column,
			HashFunction: e.HashFunction,
		})
	}

	rdistr.ReplicatedRelation = rel.ReplicatedRelation

	return rdistr
}

// DistributedRelationFromProto converts a proto.DistributedRelation object to a DistributedRelation object.
//
// Parameters:
//   - rel: The proto.DistributedRelation object to convert.
//
// Returns:
//   - *DistributedRelation: The created DistributedRelation object.
func DistributedRelationFromProto(rel *proto.DistributedRelation) *DistributedRelation {
	rdistr := &DistributedRelation{
		Name: rel.Name,
	}

	for _, e := range rel.DistributionKey {
		rdistr.DistributionKey = append(rdistr.DistributionKey, DistributionKeyEntry{
			Column:       e.Column,
			HashFunction: e.HashFunction,
		})
	}

	rdistr.ReplicatedRelation = rel.ReplicatedRelation

	return rdistr
}

// DistributedRelationFromSQL converts a spqrparser.DistributedRelation object to a DistributedRelation object.
//
// Parameters:
//   - rel: The spqrparser.DistributedRelation object to convert.
//
// Returns:
//   - *DistributedRelation: The created DistributedRelation object.
func DistributedRelationFromSQL(rel *spqrparser.DistributedRelation) *DistributedRelation {
	rdistr := &DistributedRelation{
		Name: rel.Name,
	}

	for _, e := range rel.DistributionKey {
		rdistr.DistributionKey = append(rdistr.DistributionKey, DistributionKeyEntry{
			Column:       e.Column,
			HashFunction: e.HashFunction,
		})
	}

	rdistr.ReplicatedRelation = rel.ReplicatedRelation

	return rdistr
}

type Distribution struct {
	Id string
	// column types to be used
	ColTypes  []string
	Relations map[string]*DistributedRelation
}

// local table sharding distr -> route to world

// NewDistribution creates a new Distribution with the specified ID and column types.
//
// Parameters:
//   - id: The ID of the distribution.
//   - coltypes: The column types to be used.
//
// Returns:
//   - *Distribution: The created Distribution object.
func NewDistribution(id string, coltypes []string) *Distribution {
	return &Distribution{
		Id:        id,
		ColTypes:  coltypes,
		Relations: map[string]*DistributedRelation{},
	}
}

// ID returns the ID of the distribution.
func (s *Distribution) ID() string {
	return s.Id
}

// DistributionFromDB creates a new Distribution object from a qdb.Distribution object.
// It initializes the new Distribution with the provided ID and column types, and
// populates its relations by converting the relations from the qdb.Distribution object.
//
// Parameters:
//   - distr: The qdb.Distribution object to convert.
//
// Returns:
//   - *Distribution: The created Distribution object.
func DistributionFromDB(distr *qdb.Distribution) *Distribution {
	ret := NewDistribution(distr.ID, distr.ColTypes)
	for name, val := range distr.Relations {
		ret.Relations[name] = DistributedRelationFromDB(val)
	}

	return ret
}

// DistributionFromProto creates a Distribution object from a proto.Distribution object.
//
// Parameters:
//   - ds: The proto.Distribution object to convert.
//
// Returns:
//   - *Distribution: The created Distribution object.
func DistributionFromProto(ds *proto.Distribution) *Distribution {
	return &Distribution{
		Id:       ds.Id,
		ColTypes: ds.ColumnTypes,
		Relations: func() map[string]*DistributedRelation {
			res := make(map[string]*DistributedRelation)
			for _, rel := range ds.Relations {
				res[rel.Name] = DistributedRelationFromProto(rel)
			}
			return res
		}(),
	}
}

// DistributionToProto converts a Distribution object to its corresponding proto.Distribution representation.
//
// Parameters:
//   - ds: The Distribution object to convert.
//
// Returns:
//   - *proto.Distribution: The converted proto.Distribution object.
func DistributionToProto(ds *Distribution) *proto.Distribution {
	drels := make([]*proto.DistributedRelation, 0)
	for _, r := range ds.Relations {
		drels = append(drels, DistributedRelatitonToProto(r))
	}
	return &proto.Distribution{
		Id:          ds.Id,
		ColumnTypes: ds.ColTypes,
		Relations:   drels,
	}
}

// DistributionToDB converts a Distribution struct to a qdb.Distribution struct.
// It takes a pointer to a Distribution struct (ds) as input and returns a pointer to a qdb.Distribution struct.
//
// Parameters:
//   - ds: The Distribution struct to convert.
//
// Returns:
//   - *qdb.Distribution: The converted qdb.Distribution struct.
func DistributionToDB(ds *Distribution) *qdb.Distribution {
	d := &qdb.Distribution{
		ID:        ds.Id,
		ColTypes:  ds.ColTypes,
		Relations: map[string]*qdb.DistributedRelation{},
	}

	for _, r := range ds.Relations {
		d.Relations[r.Name] = DistributedRelationToDB(r)
	}

	return d
}

// GetDistributionKeyColumns returns array of a DistributedRelation column names.
// Hash functions are added to column names if necessary.
//
// Returns:
//   - []string: Columns with optional hash function.
//   - error: An error if any occured
func (rel *DistributedRelation) GetDistributionKeyColumns() ([]string, error) {
	res := make([]string, len(rel.DistributionKey))
	for i, col := range rel.DistributionKey {
		hashedCol, err := GetHashedColumn(col.Column, col.HashFunction)
		if err != nil {
			return nil, err
		}
		res[i] = hashedCol
	}
	return res, nil
}

// GetHashedColumn returns column name with optional hash function application
//
// Parameters:
//   - col: The column name
//   - hash: THe name of the hash function
//
// Returns:
//   - string: The resulting value
//   - error: An error if hash function's name is invalid
func GetHashedColumn(col string, hash string) (string, error) {
	switch hash {
	case "identity", "ident", "":
		return col, nil
	case "city":
		// TODO: change internal city hashing to 64 bits (32 bit hashing is not supported in postgresql)
		// return fmt.Sprintf("hash_string(%s, 'city64')", col), nil
		return "", spqrerror.New(spqrerror.SPQR_NOT_IMPLEMENTED, "city hashing is not supported in coordinator operations")
	case "murmur":
		return fmt.Sprintf("hash_string(%s, 'murmur3')", col), nil
	default:
		return "", spqrerror.Newf(spqrerror.SPQR_KEYRANGE_ERROR, "invalid hash function \"%s\"", hash)
	}
}
