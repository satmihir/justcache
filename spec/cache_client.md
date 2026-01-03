# Cache Client

In `justcache`, servers implement a small, dumb HTTP interface while clients implement the “smart” caching semantics: rendezvous hashing, primary/replicas selection, retries, thundering herd prevention, and best-effort write-back.

## Choosing cache hosts

For a given key, the client selects the top **N** hosts (configurable) using rendezvous hashing.

- The top-ranked host is the **primary** for that key (client-defined; servers are unaware).
- The remaining `N-1` hosts are **replicas**.

Default: `N = 2`.

- `N = 1` is faster and effectively doubles usable cache capacity, but reduces availability (no replica fallback).
- `N > 2` increases resilience, but uses more memory and can add tail latency.

### Note on replication and coherence

JustCache optimizes for scalability and simplicity. As a result, it does **not** provide cross-replica coherence guarantees.

Because clients operate independently, insertion and expiration races may cause different replicas to temporarily hold different values for the same key (e.g., two clients fetch from origin and upload to different replicas).

**The contract is TTL-based validity:** a cached value is considered valid as long as its TTL is respected. If multiple values exist with overlapping TTLs, we assume they are all valid.

This is no worse than purely host-local caching (which can yield up to `fleet-size` distinct values). With small `N`, JustCache typically produces *far fewer* variants.

---

## Getting a key

1. **Rendezvous lookup:** compute the `N` candidate hosts (primary + replicas).

2. **Read path (serial):** issue `GET /cache/{key}` serially in rendezvous order until:
   - A host returns `200` (hit) → return the value.
   - Continue on `404` (miss) or transient host failures.

3. **Best-effort write-back (optional):**  
   If the hit came from a replica (not the primary), the client may **best-effort** upload the value to the primary (and optionally other replicas). This improves future hit rate but is not required for correctness.

4. **Miss / herd-control path:** if all `GET`s missed, issue parallel `POST /cache/{key}` to the `N` hosts to coordinate population:
   - If any host responds `200`, the key appeared during the race → immediately `GET` it (prefer that host first).
   - If one or more hosts respond `202`, those hosts are requesting an upload (promise granted).
   - If hosts respond `409`, another client is already uploading → wait (using `Retry-After` / promise TTL hints) and retry `GET`.

5. **Origin fetch + upload:** if no host already has the value, fetch from origin and `PUT` the value to each host that previously responded with `202`.

> Note: Clients may use `x-jc-dryrun: true` on `POST` to query server intent without creating promises. This is useful for probing, but normal population flows use real promises.

---

## Putting a key

To write a key to a set of target hosts:

1. Issue parallel `POST /cache/{key}` to all target hosts.
2. Collect the subset that respond with `202 Accepted`.
3. Issue parallel `PUT /cache/{key}` to those hosts (with `Content-Length` and optional TTL).

Hosts that respond with `200` already have the value; hosts that respond with `409` are already being populated by another client; hosts that respond with `507` cannot accept the key due to capacity constraints.
