# SPQR Admin prototypes

Open the prototype directory:

```bash
cd /Users/denchick/Code/spqr/docs/design/spqradmin/prototype
python3 serve.py
```

Open the printed URL without adding punctuation:

http://127.0.0.1:8766/

The start page groups prototypes by page. Direct comparison links:

- Cluster: http://127.0.0.1:8766/cluster/
- Distribution: http://127.0.0.1:8766/distribution/
- Range: http://127.0.0.1:8766/range/
- Shard: http://127.0.0.1:8766/shard/
- Move: http://127.0.0.1:8766/move/

Each gallery compares three visual variants using the same demo data. Use the controls to switch between 1/8/32 distributions, 2/8/32/128 shards, and 12/240/1,842 key ranges per distribution.

Stop the server with `Ctrl+C`.

If port `8766` is already in use, either stop the previous server or choose another port:

```bash
python3 serve.py 8767
```
