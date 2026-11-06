#!/usr/bin/env python3
"""Find mainnet RPC nodes that serve snapshots, rank them by FRESHNESS and SPEED.
1) getClusterNodes + getSlot (api.mainnet-beta) -> nodes with a public RPC port, current slot
2) TCP connect time to the RPC port (parallel) -> the closest --top-rtt nodes
3) HEAD /snapshot.tar.bz2 and /incremental-snapshot.tar.bz2 -> full slot, incremental (base, slot)
   An incremental is only usable on top of the SAME full snapshot (its base slot).
4) speed test (one node at a time) on the freshest nodes and on the closest ones
5) prints: newest snapshots, fastest nodes, and the recommendation = the fastest among the fresh ones
Read-only: only reads from the network (a few seconds of download per speed test), writes nothing.
usage: python3 find_snapshot_peer.py [--top-rtt N] [--speed-n N] [--secs S] [--max-age SLOTS]
env:   RPC_URL (default https://api.mainnet-beta.solana.com) — getClusterNodes / getSlot"""
import argparse, http.client, json, os, re, socket, sys, time, urllib.request
from concurrent.futures import ThreadPoolExecutor

ap = argparse.ArgumentParser()
ap.add_argument("--top-rtt", type=int, default=300, help="closest nodes to check for snapshots")
ap.add_argument("--speed-n", type=int, default=10, help="nodes to speed-test")
ap.add_argument("--secs", type=float, default=4.0, help="seconds of download per speed test")
ap.add_argument("--max-age", type=int, default=3000, help="max slots behind the tip for a 'fresh' snapshot")
a = ap.parse_args()

_f = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
if os.path.exists(_f):  # optional .env next to the script; real environment variables win
    for _l in open(_f):
        _k, _s, _v = _l.strip().partition("=")
        if _s and not _k.startswith("#"): os.environ.setdefault(_k.strip(), _v.strip().strip('"').strip("'"))
RPC = os.environ.get("RPC_URL", "https://api.mainnet-beta.solana.com")

def rpc(method):
    req = urllib.request.Request(RPC,
        json.dumps({"jsonrpc": "2.0", "id": 1, "method": method}).encode(), {"Content-Type": "application/json"})
    return json.loads(urllib.request.urlopen(req, timeout=30).read())["result"]

nodes = rpc("getClusterNodes")
tip = rpc("getSlot")
rpcs = [n["rpc"] for n in nodes if n.get("rpc")]
print(f"{len(nodes)} nodes in gossip, {len(rpcs)} with a public RPC, cluster slot {tip}")

def rtt(addr):
    host, port = addr.rsplit(":", 1)
    t = time.time()
    try:
        socket.create_connection((host, int(port)), timeout=1.0).close()
        return (time.time() - t) * 1000, addr
    except Exception:
        return None

with ThreadPoolExecutor(128) as ex:
    near = sorted(r for r in ex.map(rtt, rpcs) if r)
print(f"{len(near)} respond on TCP; closest: " + ", ".join(f"{ad} {ms:.1f}ms" for ms, ad in near[:5]))

FULL_RE = re.compile(r"snapshot-(\d+)-(\w+)\.tar\.\w+$")
INC_RE = re.compile(r"incremental-snapshot-(\d+)-(\d+)-(\w+)\.tar\.\w+$")

def redirect(host, port, path):
    try:
        c = http.client.HTTPConnection(host, int(port), timeout=3)
        c.request("HEAD", path)
        r = c.getresponse()
        loc = r.getheader("Location") or ""
        c.close()
        if r.status in (301, 302, 303, 307):
            return loc
    except Exception:
        pass
    return ""

def probe(item):
    ms, addr = item
    host, port = addr.rsplit(":", 1)
    floc = redirect(host, port, "/snapshot.tar.bz2")
    fm = FULL_RE.search(floc)
    if not fm or "incremental" in floc:
        return None
    full = int(fm.group(1))
    iloc = redirect(host, port, "/incremental-snapshot.tar.bz2")
    im = INC_RE.search(iloc)
    inc = None
    if im and int(im.group(1)) == full:  # incremental on top of THIS node's full snapshot
        inc = (int(im.group(2)), iloc)
    return {"ms": ms, "addr": addr, "full": full, "floc": floc, "inc": inc,
            "newest": inc[0] if inc else full}

with ThreadPoolExecutor(64) as ex:
    have = [r for r in ex.map(probe, near[:a.top_rtt]) if r]
print(f"{len(have)} of the closest {min(a.top_rtt, len(near))} serve a snapshot")
if not have:
    sys.exit("no node serves a snapshot - increase --top-rtt")

def speed(n):
    host, port = n["addr"].rsplit(":", 1)
    try:
        c = http.client.HTTPConnection(host, int(port), timeout=5)
        c.request("GET", n["floc"])
        r = c.getresponse()
        got, t0 = 0, time.time()
        while time.time() - t0 < a.secs:
            b = r.read(1 << 20)
            if not b:
                break
            got += len(b)
        c.close()
        return got / (time.time() - t0) / 1e6
    except Exception:
        return 0.0

# speed-test the CLOSEST of the fresh nodes (within --max-age of the tip), then the closest overall
fresh_first = sorted(have, key=lambda n: (-n["newest"], n["ms"]))
close_first = sorted(have, key=lambda n: n["ms"])
fresh_close = [n for n in close_first if tip - n["newest"] <= a.max_age]
cand = []
for n in fresh_close[: max(1, a.speed_n * 2 // 3)] + close_first:
    if n not in cand:
        cand.append(n)
    if len(cand) >= a.speed_n:
        break
for n in cand:  # one at a time: parallel tests would share our own bandwidth
    n["mbs"] = speed(n)

def row(n):
    inc = f"inc {n['inc'][0]} (age {tip - n['inc'][0]})" if n["inc"] else "no matching incremental"
    mbs = f"{n['mbs']:6.1f}" if "mbs" in n else "     -"
    return f"{mbs} {n['ms']:6.1f}ms  {n['addr']:22s} full {n['full']} (age {tip - n['full']}), {inc}"

print("\n== NEWEST snapshots (newest usable slot first)\n    MB/s    RTT    node")
for n in fresh_first[:10]:
    print("  " + row(n))
tested = sorted((n for n in have if "mbs" in n), key=lambda n: -n["mbs"])
print("\n== FASTEST (speed-tested)\n    MB/s    RTT    node")
for n in tested:
    print("  " + row(n))

# recommendation: fresh (newest usable slot within --max-age of the tip), fastest among those
fresh = [n for n in tested if tip - n["newest"] <= a.max_age and n["mbs"] > 0]
if fresh:
    # among the fresh nodes within 70% of the best fresh speed, the newest wins (less replay at boot)
    top = max(n["mbs"] for n in fresh)
    best = max((n for n in fresh if n["mbs"] >= 0.7 * top), key=lambda n: (n["newest"], n["mbs"]))
    why = f"fresh, >=70% of the best fresh speed ({top:.0f} MB/s), newest"
else:
    best = fresh_first[0]
    why = f"no speed-tested node within {a.max_age} slots - newest instead"
print(f"\nRecommended ({why}): {best['addr']}  full {best['full']}"
      + (f" + incremental {best['inc'][0]}" if best["inc"] else "")
      + f", {tip - best['newest']} slots behind the tip" + (f", {best['mbs']:.0f} MB/s" if best.get("mbs") else ""))
print("Download (from your snapshots directory; the incremental belongs to this exact full snapshot):")
print(f"  wget --trust-server-names http://{best['addr']}/snapshot.tar.bz2")
if best["inc"]:
    print(f"  wget --trust-server-names http://{best['addr']}/incremental-snapshot.tar.bz2")
