# SPQR Admin prototypes

Open the prototype directory:

```bash
cd /Users/denchick/Code/spqr/docs/design/spqradmin/prototype
python3 serve.py
```

Open the printed URL without adding punctuation:

http://127.0.0.1:8766/

The start page links to all three concepts. Direct links:

- Atlas: http://127.0.0.1:8766/atlas/
- Transfer Desk: http://127.0.0.1:8766/transfer-desk/
- Control Map: http://127.0.0.1:8766/control-map/

Each concept now includes cluster, distribution, shard, key-range, and move pages. Use the controls in the prototype to switch between 1/8/32 distributions, 2/8/32/128 shards, and 12/240/1,842 key ranges per distribution.

Stop the server with `Ctrl+C`.

If port `8766` is already in use, either stop the previous server or choose another port:

```bash
python3 serve.py 8767
```
