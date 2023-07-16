# Authentication

SPQR currently supports these auth methods for backend auth:
- `trust`, see [Trust Authentication](https://www.postgresql.org/docs/15/auth-trust.html). 
- `password`, `md5` and `scram-sha-256`, see [Password Authentcation](https://www.postgresql.org/docs/15/auth-password.html).

Methods supported for frontend auth, the way they`re specified in config:
- `ok`, same as `trust`
- `notok`, opposite to `ok`
- `clear_text`, same as `password`
- `md5`
- `scram`, same as `scram-sha-256`

For more information about authentication config, see [pkg/config/auth.go](../pkg/config/auth.go)
