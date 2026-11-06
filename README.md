# validator-snapshots

Restarting a Solana validator / RPC node from a fresh snapshot is much faster when you download it from a peer that
is both **close to you** and **recent**. `find_snapshot_peer.py` finds that peer:

1. `getClusterNodes` + `getSlot` → every node with a public RPC port, and the cluster slot;
2. TCP connect time to each RPC port (in parallel) → the closest `--top-rtt` nodes;
3. `HEAD /snapshot.tar.bz2` and `/incremental-snapshot.tar.bz2` → full snapshot slot, incremental (base, slot);
   an incremental is only usable on top of the **same** full snapshot;
4. a short download speed test, one node at a time, on the freshest and on the closest nodes;
5. prints the newest snapshots, the fastest nodes, and a recommendation (the newest among the fresh nodes within
   70 % of the best speed) with the `wget` commands to fetch it.

## Usage
```bash
python3 find_snapshot_peer.py                       # defaults
python3 find_snapshot_peer.py --top-rtt 300 --speed-n 10 --secs 4 --max-age 3000
```
| Flag | Default | |
|---|---|---|
| `--top-rtt` | 300 | closest nodes to check for snapshots |
| `--speed-n` | 10 | nodes to speed-test |
| `--secs` | 4 | seconds of download per speed test |
| `--max-age` | 3000 | max slots behind the tip for a "fresh" snapshot |

Optional `RPC_URL` (environment or `.env`, see `.env.example`); default is the public mainnet endpoint.
Read-only: a few seconds of download per speed test, nothing is written.

## Credits & third-party licenses
MIT-licensed (see `LICENSE`). Python standard library only. Queries public Solana RPC endpoints and the snapshot
endpoints of other nodes — use responsibly.
