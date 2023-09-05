# Logproxy

Logproxy is a tool for recording and replaying test queries.

## Main features

- Write log of incoming queries with timestamp
- Replay queries from log file 

## Usage

To start querry collecting session:
```
logproxy run -H bdHost -p bdPort -l LogFile
```
To connect use:
```
psql -H proxyHost -p proxyPort *creds for db*
```
After closing connection, queries will be saved to the log file. To replay it execute:
```
logproxy replay -H bdHost -p bdPort -U userName -d dbName -l LogFile
```