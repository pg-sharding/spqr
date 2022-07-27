# Coordinator

                                                                               |
                                                                               |
                     Control plane                                             |                Data plane
                                                                               |                ----------------
                                        ----------------                       |                |              |
                                        |              |                       |                | Data shard 2 |
                                        |    Router1   |                       |                |              |
                                        |              |                       |                ----------------
                                        ----------------                       |
                                   /                                           |
                                  /                                            |
                                                                               |                ----------------
        ------------------------        ----------------                       |                |              |
        |                      |        |              |                       |                | Data shard 1 |
        |      Psql Client     |   ---> |   Router2    |                       |                |              |
        |                      |        |              |                       |                ----------------
        ------------------------        ----------------                       |
                                                                               |                ----------------
                                                                               |                |              |
                                                                               |                | Data shard 3 |
                                                                               |                |              |
                                                                               |                ----------------
--------------------------------------------------------------------------------


                   Coordinator + Resharder

      -------------------           ---------------------
      |                 |    gRPC   |                   |
      |  Coordinator    |    <---   |   Resharder       |
      |                 |           |                   |
      -------------------           ---------------------


```
➜ grpcurl -plaintext 'localhost:7002' describe yandex.spqr.KeyRangeService
yandex.spqr.KeyRangeService is a service:
service KeyRangeService {
  rpc ListKeyRange ( .yandex.spqr.ListKeyRangeRequest ) returns ( .yandex.spqr.KeyRangeReply );
  rpc LockKeyRange ( .yandex.spqr.LockKeyRangeRequest ) returns ( .yandex.spqr.KeyRangeReply );
  rpc SplitKeyRange ( .yandex.spqr.SplitKeyRangeRequest ) returns ( .yandex.spqr.KeyRangeReply );
  rpc UnlockKeyRange ( .yandex.spqr.UnlockKeyRangeRequest ) returns ( .yandex.spqr.KeyRangeReply );
}

~
➜ grpcurl -plaintext 'localhost:7002' yandex.spqr.KeyRangeService/ListKeyRange
{
  "keyRanges": [
    {
      "krid": "1",
      "shardId": "2"
    }
  ]
}
```