#!/bin/bash
set -ex
FQDN=$1

if [[ "${FQDN}" == "" ]];
then
    FQDN=$(hostname)
fi

ls /etc/zk-ssl/server.crt && exit 0 || true
mkdir /etc/zk-ssl

echo "-----BEGIN CERTIFICATE-----
MIIE/TCCAuWgAwIBAgIUU9e6chP84r3iZk3JtvnWb1V2N1YwDQYJKoZIhvcNAQEL
BQAwDTELMAkGA1UEBhMCUlUwIBcNMjMwMzEwMDgzNTUzWhgPMzAyMjA3MTEwODM1
NTNaMA0xCzAJBgNVBAYTAlJVMIICIjANBgkqhkiG9w0BAQEFAAOCAg8AMIICCgKC
AgEAwJuy394cK127yT8nGHVPKF6TG6xL0WpxahyaKwIYp5lbv9wDvzjMPE7KmONU
8GhCFUdEJTRqBkaRdZNYxnOUxufU3+jIf1hq1Csg8q1NXICVWVwfFL2F5mKHgeHQ
n3FaJM2pZQ5iIWFY1c18MgV8qqNWbtyLeppcyZOL9duLM9A8XpYb0JOZis82d+lh
kcxzE1XM+MZEgZfHImh0zod9OMtSAOwQzVXpiA3JO/eHkLQGYcy6KNTm42mubVlX
kBcu/BplnP7gXGOYDt/JyRhGSLAfn762+jRbAlAvbPzOy67hc4pW7aloU5zPBhYf
BaTxM9UPqPtyp7Lxkp9HL68QXtm5MobDuDtZ6ePQtHgHrl7P7PXvEUPwK7BZzgZy
MerVhxIssutA2yBCuu5T7dMSwIsUdvXtgdHRdHDwn1D/V1CxnujDv9l6/T3sCmRv
tWPwTOCUf5BLLw6N6TnSsVR5I9NALKCLYE8LsfCuLdyi363JZqubkdJr1Ro8yI5J
m0GX5pypwZJPV2Ivt6kKVTQiN2hoWNe+3TNPS+7ysqit37s71YRDajZaZ55DopmF
+oIYdA3MqUZEVZyKFifWvo/l2gYarlEtcEJl++OwydirWLAjCPHh9UvDhjKS43bQ
zSlRC+d4CfRqXftmETHVAxMokai3WvAdUpJrW2RrjiuR0MkCAwEAAaNTMFEwHQYD
VR0OBBYEFJGDr6xmoKJFU6cgS90aFg6lUGbhMB8GA1UdIwQYMBaAFJGDr6xmoKJF
U6cgS90aFg6lUGbhMA8GA1UdEwEB/wQFMAMBAf8wDQYJKoZIhvcNAQELBQADggIB
ACj87ymjBlgY9UZTUbudHREPPXfqMi2TgWt5hygQSiTrNeQOodnq+Swp86qX/y8w
xtnvc+iILfFnh9ZevHKmLx+JziN4kD4ywEpHW7zS7c3+2QEjIZUwj5qlIg0ByOBd
0M/kpimmuTwlDylBaY12GcFlZcsbuezzm4hU+0qoCV/zi2DvSdAPKXMAeZ3lOkde
PUYJUpRz/QkkxEhSdM3BQYI51mUiltCHMhe6COoN4MHV7tix0Pj9vPjhAVN/4sot
2PgUiCwY8eNQugZhpTosMTSBLZvg/EKG+4slY75/voNTIxWHAHmnPMOAzVgNTya0
/eP6NB3MCjFuY2E+fGox9YTomjI5oxBr+1LlwVy7wbwXTrgBz9Z4izScAsVbPrk6
jSrqNeNWK1f+JVnYZkjgPGgPaQVCJ22vdLmkW7U/ATdeedQS3RCApMnb9VCRTUaO
eY4ccuEvj0huhdcUguw6fBjrhPjoPxKMn6S93ginW8Wz9vo8qLkEg2NtQDFu1Omb
cJM5F8uLRr8NotPV5QPg1koHeBv/N2WTRZiUoavAogR9XdyOtrB8+MBu1nsp4Goi
7/suv9XzMJ7IpgXiQfCM++1x7oooyWWdeFTCzqNDJ1IbQDeOCc9cQgeOAPWcIqWO
nAWt08+eToI1YUvjl6UT0bpVaJEACv+/HfBr1T26u4Jh
-----END CERTIFICATE-----" > /etc/zk-ssl/ca.cert.pem

echo "-----BEGIN RSA PRIVATE KEY-----
MIIJKAIBAAKCAgEAwJuy394cK127yT8nGHVPKF6TG6xL0WpxahyaKwIYp5lbv9wD
vzjMPE7KmONU8GhCFUdEJTRqBkaRdZNYxnOUxufU3+jIf1hq1Csg8q1NXICVWVwf
FL2F5mKHgeHQn3FaJM2pZQ5iIWFY1c18MgV8qqNWbtyLeppcyZOL9duLM9A8XpYb
0JOZis82d+lhkcxzE1XM+MZEgZfHImh0zod9OMtSAOwQzVXpiA3JO/eHkLQGYcy6
KNTm42mubVlXkBcu/BplnP7gXGOYDt/JyRhGSLAfn762+jRbAlAvbPzOy67hc4pW
7aloU5zPBhYfBaTxM9UPqPtyp7Lxkp9HL68QXtm5MobDuDtZ6ePQtHgHrl7P7PXv
EUPwK7BZzgZyMerVhxIssutA2yBCuu5T7dMSwIsUdvXtgdHRdHDwn1D/V1CxnujD
v9l6/T3sCmRvtWPwTOCUf5BLLw6N6TnSsVR5I9NALKCLYE8LsfCuLdyi363JZqub
kdJr1Ro8yI5Jm0GX5pypwZJPV2Ivt6kKVTQiN2hoWNe+3TNPS+7ysqit37s71YRD
ajZaZ55DopmF+oIYdA3MqUZEVZyKFifWvo/l2gYarlEtcEJl++OwydirWLAjCPHh
9UvDhjKS43bQzSlRC+d4CfRqXftmETHVAxMokai3WvAdUpJrW2RrjiuR0MkCAwEA
AQKCAgAgemC4RTDE00J2FfMWublGWmQ991i1kFhdh0Mr22ei40ZIXOY42W/+/15E
V5kcDMiP4/uGtobmVgHzLIx8skK1I6SOuScN6i/hZQBiS3zPC1OjxNfs3GR2y8iD
yzstl6SWriNRShKcBFlBfCvkF27FK1PIz+GpI9xflUS1iXa4nvV/EZrRGgJ7GKPb
pnvwZORGr2In1O76V0iZ8bk4ljo0WHyUcToIFeOSMJjtRrkSWnj1BtuhRP1F/a0O
/VC5mF8w3Zai2YulqJmccHoLMc+wNBqxCiy6lhd+lVzZ6OtKB0w2+m3cF4PjDX8P
TK2gewa9McE5QmU8B/2aNsd/L+r3eGEvWAF/1vRq6NcrFwigq8uCTtgw9edRlDnm
RvICkfAbrwhNaixWwqBVQHoy53H29TohxGNNKa6TTKeJvYEdYKgHx55TxkB9X9jc
iSisqb3fgEl4Yh1Izpu+6nULOqdlldfkKPgKJqVB1AT/avR8J09zmMvW5fPa6fFx
alZ1iVahR5bIFEu1lXygsrBP6N+K/ogyztg7ZKLTIN/FguwMKnXMaUbN/Y/ZZXV1
oGil9vHKnDrRnUGfcm9tyH2Ddcy6RDoDz+O4cYgMGxDhHran2cicVY1q+Yi08q5h
Napk1phNra5HIHnNHwMxQ75ZKZZ3TOGJL+HMF4yRDj19C/6sAQKCAQEA8a9ZQhWw
0vhZENmSYZgGZLa7RZLSbBzQOX/cetdI6/kvmZVcMvNz4q0/UI9XLkqokL1wJiku
O0zXkaVrBVAsgozp4I3oFqwtcAAGw0KwF4FDAS36k4gkE4SmIUl2eI0XMZCPQIKp
3TB81+XdBITtwfPl5yG+IZDkXNu16qUHEhnhvs/kKhMr8flhFC1J4gdrrQhfuRHY
Jv8e1RLJzMhu/ErRjh82LkzB6m3jp0YxBeIA+9Kkw+OX6SlzRbJPirKxJTaZnB8o
wQmzOy1kTRG4qjKswjdTbzf6549721i8QHwSpwPI3NZQhlSkfsvZ5QL4qPW0nRta
m76YeLlS12yQSQKCAQEAzAQz6OcE6yS2q5UfTZluGaU54Zkm0YSnS394pitJpHoh
JSZlvkL1DzpacquDxa3uQLDikai5TqpNnkuufeMJf7I2ygg4n/v4OFaE+/qj5uNA
3QnL3BVT9DCJ0JvQ1qA5Q/6P5WpUHYB7JHBM9BpaE8e4xocJyWSdcSJDaEXns4Hx
WzhpBdVpPSamqB0VHYg1bv6OGFPfwUaRafWhNzljtxbY8RYcz7IfPmnLImFePTtZ
AjzIoAwUIRFzvmoduda0kQKogRVoEeaW1q6ebPUjYjIZvohnpe27EvgCiTNkcaSf
C96uIxHrSvI8114z9CBXer60xQ0Kz+ds18LtY6w8gQKCAQEAkP/JxlsrHje/f9t4
9jJ2S4BSNLiUpCZZStYKWmzFJEX5J+SzTyI+uZWFcfi9rlk+brApE8wLH6rHfmtH
HQXv3ldajc21m7yq+hIZ/JYK/d8gaxnBxzebpVYlMb1YZZUIgEUhnOuHq9vGWuVe
x7JUztNccGIPJyY9y/RJXUCrUFHU3Vzun8umxuL+OlO9iu02zbZDb85j52mSfvVp
uwHZjGX6+ZCCOh71DIfnWFlFWikwu+Sx05C9eDbVINCM5kK1AwWR/Ve4ZLBEJtHh
5lcmen4ypcb5uLVWRA0SmxPOxcVqj2c24D94Sk+H7UayMLKqqvvW45cgsmYUJgHR
0MsieQKCAQB9goBk4erWtmliuYTeemuPf2RSc6O79b3t5mfU4oCVnUTS1AJ3wD1+
tsl6DiYs8MnIJoncTk5iJMdHgQvCCnCHjJ3EQLaFRb/4+NErK5C1tEztLt+pb72M
VmgSXCloQH26ZNslqfpBhA895ZCSA7wyuwXjrKPKsAlj1k5d0dOvTVusYNHLcvUh
V6vjdLDO0EL/G79THBZlkwJWi3Q4wyejNX0VJCNpaw1pmjAL4JbXWLFzfO13+LZR
eakZFbNf5sSDCX2cnAzAJnnZbOet5El2WZgY7VXGcLBMBSOaQHGksD/gT4gVrypv
mwLvA9c2cscejkArkdB7AsalHhho30cBAoIBAFJBO0RU7o0S+F6KHIP5aFbItcUd
NfUgoJTAFUD3EnBirvDv0pu8T8zkgKf7PRFkZQIOXocvpX0Zy6N7fiPbvzTA/vH3
mFqias89pTUAgv43R8ZsAC/qlozUuByegigEz2zeVd34w7MdkgGo1jnqmijAIXZE
INBo0swkxAbix+W1Pur/yvGUpC6xu3ISmdrn0p20B7QhyuoqC3ea/az7ePwx+Pu9
Jl8tzMujbHNHhw+OQAQOPHi6EUPs/H37euj3G7oBaVUwXJq3Tbwg95W5Jih+CgTB
Sbe6eYpR/j/SYGwbS6/DbHi3IjvblN+2pSPI05JvXMhLC/lAeqcdVJAgTvw=
-----END RSA PRIVATE KEY-----" > /etc/zk-ssl/ca.key

openssl genrsa -out /etc/zk-ssl/server.key -passout pass:testpassword123 4096
openssl req -new -key /etc/zk-ssl/server.key -out /etc/zk-ssl/server.csr -passin pass:testpassword123 -subj "/C=RU/ST=Test/L=Test/O=Test/OU=Test/CN=${FQDN}"
echo "[SAN]
subjectAltName = @alt_names
[alt_names]
DNS.1 = ${FQDN}" > /etc/zk-ssl/openssl.cnf
openssl x509 -req -days 365 -in /etc/zk-ssl/server.csr -CA /etc/zk-ssl/ca.cert.pem -CAkey /etc/zk-ssl/ca.key -CAcreateserial -out /etc/zk-ssl/server.crt -passin pass:testpassword123 -extensions SAN -extfile /etc/zk-ssl/openssl.cnf

if [[ "${FQDN}" == *"zoo"* ]];
then
    keytool -import -trustcacerts -alias yandex -file /etc/zk-ssl/ca.cert.pem -keystore /etc/zk-ssl/truststore.jks -storepass testpassword123 -noprompt && \
    openssl pkcs12 -export -in /etc/zk-ssl/server.crt -inkey /etc/zk-ssl/server.key -out /etc/zk-ssl/server.p12 -passout pass:testpassword321 -name ${FQDN} && \
    keytool -importkeystore -destkeystore /etc/zk-ssl/server.jks -srckeystore /etc/zk-ssl/server.p12 -deststorepass testpassword321 -srcstoretype PKCS12 -srcstorepass testpassword321 -alias ${FQDN} && \
    rm -f /etc/zk-ssl/server.p12
fi

chmod 755 /etc/zk-ssl/*
