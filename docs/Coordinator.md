# Coordinator

The coordinator manages the routers and saves the installation state in QDB([etcd](https://etcd.io/)). It blocks key ranges on a shard for modification, and moves key ranges consistency from one shard to another.

We considered different options for moving data. The most popular way is to make a copy via logical replication, then delete half of the data on one node and delete another half of the data on the other node. We decided that logical replication does not work well enough yet. Instead, the coordinator makes ε-split - cut off a small part of the data. Since it is small, it all works very quickly.

![ε-split](e-split.png "ε-split")

The load from the moving key ranges measured with [pg_comment_stats](https://github.com/munakoiso/pg_comment_stats) extension.

```
> /* a: 1 c: hmm*/ select 1;
> select comment_keys, query_count, user_time from pgcs_get_stats() limit 1;
-[ RECORD 1 ]+----------------------
comment_keys | {"a": "1"}
query_count  | 1
user_time    | 6.000000000000363e-06
```

## Configuration

WIP

## KeyRangeService

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