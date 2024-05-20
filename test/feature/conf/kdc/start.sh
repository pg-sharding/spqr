#!/bin/sh

set -e

# The /keytab directory is volume mounted on both kdc and cockroach. kdc
# can create the keytab with kadmin.local here and it is then useable
# by cockroach.
kadmin.local -q "ktadd -k /keytab/tester.keytab postgres/gss_spqr_1.gss_spqr@MY.EX"

krb5kdc -n