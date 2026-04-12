package acl

type AclMode uint32 /* a bitmask of privilege bits */

/* See also AclItem in postgres */
type AclItem struct {
	AI_grantee int     /* ID that this item grants privs to */
	AI_grantor int     /* grantor of privs */
	AI_privs   AclMode /* privilege bits */
}
