package acl

import (
	proto "github.com/pg-sharding/spqr/pkg/protos"
	"github.com/pg-sharding/spqr/qdb"
)

/* See also ACLItem in postgres
* https://github.com/postgres/postgres/tree/dd1398f1378799acc60c3ed85d82439b2ff69141/src/include/utils/acl.h#L54 */
type ACLItem struct {
	AIGrantee string /* ID that this item grants privs to */
	AIGrantor string /* grantor of privs */
	AIPrivs   uint64 /* privilege bits */
}

func ACLFromDB(acl []qdb.ACLItem) []ACLItem {
	ret := []ACLItem{}

	for _, el := range acl {
		ret = append(ret, ACLItem{
			AIGrantee: el.AIGrantee,
			AIGrantor: el.AIGrantor,
			AIPrivs:   el.AIPrivs,
		})
	}
	return ret
}

func ACLTODB(acl []ACLItem) []qdb.ACLItem {
	ret := []qdb.ACLItem{}

	for _, el := range acl {
		ret = append(ret, qdb.ACLItem{
			AIGrantee: el.AIGrantee,
			AIGrantor: el.AIGrantor,
			AIPrivs:   el.AIPrivs,
		})
	}
	return ret
}

func ACLFromProto(acl []*proto.ACL) []ACLItem {
	ret := []ACLItem{}

	for _, el := range acl {
		ret = append(ret, ACLItem{
			AIGrantee: el.AIGrantee,
			AIGrantor: el.AIGrantor,
			AIPrivs:   el.AIPrivs,
		})
	}
	return ret
}

func ACLTOProto(acl []ACLItem) []*proto.ACL {
	ret := []*proto.ACL{}

	for _, el := range acl {
		ret = append(ret, &proto.ACL{
			AIGrantee: el.AIGrantee,
			AIGrantor: el.AIGrantor,
			AIPrivs:   el.AIPrivs,
		})
	}
	return ret
}
