# API reference and design notes

## API behavior

| Operation | Result |
| --- | --- |
| `create_queue(name, inactivity_timeout_seconds)` | `True` when created, `False` for identical configuration; conflict raises `QueueConfigurationConflict` |
| `join(queue, user_id, attributes=None)` | `Membership`; duplicate preserves token, join time, attributes and FIFO order, and renews inactivity |
| `claim_next(queue)` | Atomically return and remove the first active membership; `None` when empty |
| `leave(queue, user_id)` | Whether a stored membership was physically removed, including expired membership; absent is `False` |
| `heartbeat(queue, user_id, membership_token)` | Renewed `Membership`; absent or expired returns `None`; wrong token on active membership raises `StaleMembershipToken` |
| `get_user(queue, user_id)` | Active `Membership` or `None` (also serves as the waiting check) |
| `get_position(queue, user_id)` | Active 1-based position or `None` |
| `get_queue_size(queue)` | Active count |
| `list_users(queue, offset=0, limit=50)` | Active memberships in FIFO order; nonnegative offset, limit 1–1000 |
| `cleanup_expired(queue, batch_size=100)` | Number physically removed, at most batch_size (1–1000) |

All queue operations raise `QueueNotFound` for a missing queue. Validation errors
raise `ValueError` before Redis access. Membership contains `user_id`,
`membership_token`, UTC `joined_at`, `last_heartbeat_at`, `expires_at`, and
`attributes`. Visibility results also contain `position`; mutation results have
`position=None` because no position scan is performed in mutation scripts.
Results are snapshots, not promises that membership remains active afterward.

Timeouts are rounded to microseconds, positive, and at most one year. Queue
configuration is persistent and immutable; this release has no queue deletion
API. A new join after leave or expiry gets a new token and sequence at the back.
Duplicate join input must still be valid JSON, but never replaces stored attributes.
Treat tokens as opaque identifiers, not authentication credentials.

Pagination uses active offsets in an individual atomic snapshot. Between calls,
deletions/expiry can shift offsets and cause skipped users; rejoins and inserts
can change later pages. There is no cross-request snapshot or cursor guarantee.

## Periodic cleanup

Call cleanup from your scheduler, worker, cron entrypoint, or framework task:

```python
def scheduled_cleanup(room: WaitingRoom, queue: str) -> None:
    # Bound the work per scheduler invocation even under a sustained backlog.
    for _ in range(10):
        if room.cleanup_expired(queue, batch_size=100) < 100:
            break
```

Pass an open `WaitingRoom` instance and an existing queue name from your
application, for example `scheduled_cleanup(room, 'tickets')`.

Schedule this at an interval appropriate to storage needs. Correct visibility
does not depend on scheduling: every read excludes deadlines <= Redis server
time. An expired heartbeat removes that old stored membership and returns None;
it never renews it. Cleanup retries are safe and remove all membership data.
Logging uses the module logger with structured `queue_name` and `removed_count`
fields; the package does not configure handlers or application logging.

## Design and guarantees

Keys are `<prefix>:{<sha256(queue UTF-8)>}:<suffix>`:

| Suffix | Redis type | Purpose |
| --- | --- | --- |
| `config` | string | Timeout in microseconds |
| `seq` | string counter | Redis-generated monotonically increasing sequence |
| `order` | sorted set | User ID scored by sequence |
| `expiry` | sorted set | User ID scored by absolute deadline in microseconds |
| `members` | hash | User ID to JSON record (token, timestamps, encoded attributes) |

Each mutation is one Lua invocation using Redis `TIME`. Concurrent FIFO means
the order Redis executes enqueue scripts, not client start or response order.
Duplicate joins do not increment sequence. Exact score integers are guarded at
2^53-1; exhaustion raises `SequenceExhausted` instead of corrupting ordering.
Application hosts' clocks do not affect expiry; Redis clock jumps still do.

Cleanup selects and removes expired entries inside the same bounded Lua script;
there are no client-side candidate lists that could target a later rejoin.
If a renewal executes first while active, cleanup sees its updated deadline.
If cleanup or expiry wins, heartbeat cannot revive the removed membership.
A stale token cannot renew a later membership. Leave is deliberately by user ID:
it removes whichever membership exists at its atomic execution point. Callers
requiring conditional leave must coordinate that policy themselves. Concurrent
heartbeat/leave/rejoin outcomes follow Redis script execution order.

Visibility reads configuration, server time, FIFO members, and records in one
`MULTI/EXEC` snapshot, then filters expiry and computes positions in Python.
This avoids unbounded Lua scans and keeps active positions exact even with a
cleanup backlog. The tradeoff is O(n) network, memory, and server read work per
visibility call, including paginated calls. It is intended for moderate queues;
pagination bounds the returned page, not the snapshot cost. Cleanup is
O(batch_size log n), with no `KEYS` or full-queue Lua scans. Attribute payloads
and user IDs are application-controlled: impose suitable size limits upstream.

Use a dedicated namespace and do not externally alter keys, expire individual
keys, or use an eviction policy that can evict them (`noeviction` recommended).
Lua atomicity isolates concurrent operations; Redis does not roll back script
writes after runtime failures such as wrong key types or resource failures.
Redis transport/server exceptions propagate. A lost response can mean an
operation committed: joins and leave are safely retryable for membership
semantics, but retrying join renews its deadline. Creation is idempotent;
cleanup is retryable. Durability and failover loss depend on Redis persistence
and replication settings; this package offers no exactly-once delivery or
durability beyond Redis. Read and write through the same primary.

Cluster hash tags colocate queue keys for future compatibility, but **Redis
Cluster clients are not supported or tested** in this release: the transactional
snapshot implementation targets `redis.Redis`. Read replicas, active-active
Redis, async clients, admission control, and processing leases are outside
this version's scope. Tests cover independent instances and concurrent threads
using shared Redis, plus independent Python processes for joins and claims.

## Atomic claims

`claim_next` is a destructive dequeue, not a lease. Each Lua invocation inspects
at most 100 FIFO heads, removes expired entries, and returns and removes the
first active membership atomically. If a full batch was expired, the Python
method retries with another bounded invocation and fresh Redis server time.
An expiry backlog therefore increases round trips, not the Lua batch size.
Concurrent operations follow Redis execution order; another worker may claim
between batches. Returned memberships have `position=None`.

A claimed user is no longer waiting; heartbeat returns `None` unless they have
rejoined, in which case the old token is rejected. A rejoin gets a new token and
a new FIFO sequence. No acknowledgement, reservation lease or automatic recovery
is provided. A worker crash or lost claim response can lose the removed membership.
Do not configure automatic transport retries for claims when losing additional
memberships is unacceptable: retrying an uncertain claim may remove another user.
