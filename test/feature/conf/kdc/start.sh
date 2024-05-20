#!/bin/sh

set -e

# The /keytab directory is volume mounted on both kdc and spqr
kadmin.local -q "ktadd -k /keytab/tester.keytab postgres/gss_spqr_1.gss_spqr@MY.EX"

krb5kdc -n