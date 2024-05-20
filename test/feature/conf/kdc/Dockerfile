FROM alpine:3.14

RUN apk add --no-cache \
  krb5-server \
  && rm -rf /var/cache/apk/*

COPY krb5.conf /etc/krb5.conf

RUN kdb5_util create -s -P kpass \
  && kadmin.local -q "addprinc -pw psql tester@MY.EX" \
  && kadmin.local -q "addprinc -randkey postgres/gss_spqr_1.gss_spqr@MY.EX"

CMD ["sh", "/start.sh"]