# Communication Protocol

Each JustCache server exposes a small HTTP API that allows clients to **read**, **publish**, and **coordinate population** (“promise”) of cache entries. The server is intentionally dumb; clients implement all routing / replication / retries / herd control.

- Base path: `/cache/{key}`
- Values are transported as **raw bytes** in request/response bodies
- Protocol metadata is carried in `x-jc-*` headers

---

## Common headers

- `x-jc-size`: value size in bytes (integer)
- `x-jc-ttl`: remaining TTL in milliseconds (integer, ≥ 0)
- `x-jc-superhot`: `true|false` (server hint; clients may choose to locally cache)

---

## GET

**PATH:** `/cache/{key}`

### Response headers (on hit)

- `x-jc-size: <bytes>`
- `x-jc-ttl: <ms>`
- `x-jc-superhot: true|false`

### Response body (on hit)

- Raw value bytes

### Response codes

- `200 OK` — key found; body contains value
- `404 Not Found` — key not present on this server

---

## POST (intent / promise)

**PATH:** `/cache/{key}`

Coordinates cache population to prevent thundering herds.

### Request headers

- `x-jc-size: <bytes>` *(optional but recommended; enables early reject and size validation on PUT)*
- `x-jc-promise-ttl: <ms>` *(optional; default 30000 = 30 seconds)* — how long the promise should remain valid
- `x-jc-dryrun: true|false` *(optional; default `false`)*

If `x-jc-dryrun: true`, the server returns the decision it *would* make, but **does not create a promise**.

### Response codes

- `200 OK` — key already exists; client should `GET` it
- `202 Accepted` — server requests an upload; client should `PUT /cache/{key}`
- `409 Conflict` — another client is already uploading (promise exists); client should back off and retry `GET` later
- `507 Insufficient Storage` — server cannot accept this key/value (e.g., capacity constraints)

### Optional response headers

- `x-jc-promise-ttl: <ms>` *(on `202`/`409`)* — how long the promise remains valid
- `Retry-After: <seconds>` *(on `409`)* — suggested backoff

---

## PUT (upload value)

**PATH:** `/cache/{key}`

Uploads the value bytes. This should generally be preceded by a successful `POST` that returned `202`.

### Request headers

- `Content-Length: <bytes>` *(required)*
- `x-jc-ttl: <ms>` *(optional; default 1800000 = 30 minutes)* — TTL for the cached value in milliseconds

### Request body

- Raw value bytes

### Response codes

- `200 OK` — value stored successfully
- `400 Bad Request` — invalid `x-jc-ttl` header (non-numeric, zero, or negative)
- `409 Conflict` — upload rejected (e.g., no active promise, promise owned by someone else, or size mismatch vs promised size)
- `411 Length Required` — missing `Content-Length`
- `413 Payload Too Large` — exceeds server limits
- `507 Insufficient Storage` — cannot accept due to capacity


---

## Notes

- `x-jc-ttl` in **response headers** is interpreted as **remaining TTL** for an existing stored value.
- `x-jc-ttl` in **PUT request headers** sets the TTL for the new value (defaults to 30 minutes if not provided).
- If the client provided `x-jc-size` on `POST` and received `202`, the server **requires** `PUT Content-Length` to match the promised size.
- Promises are automatically cleaned up by the server every 5 minutes, and on access if expired.
- PUT requests **require** an active promise created by a prior POST (returns 409 Conflict otherwise).
