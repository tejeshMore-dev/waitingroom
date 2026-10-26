# WaitingRoom

A synchronous, framework-independent Python package for FIFO waiting queues on Redis.
Python 3.10+; Redis 6.2+ standalone primary (including a primary obtained through
redis-py Sentinel). No background threads or web framework dependencies.

## Install and run

```sh
docker run --rm -p 6379:6379 redis:7-alpine
pip install -e '.[test]'
pytest -q
python examples/basic.py
```

Run these commands from this directory. Tests require **real Redis** and use a
unique prefix per test; they never flush a database. Set `REDIS_URL` to override
the default test URL `redis://localhost:6379/15` (example defaults to database 0).

```python
from redis import Redis
from waitingroom import WaitingRoom

room = WaitingRoom(Redis.from_url('redis://localhost:6379/0'), key_prefix='myapp')
room.create_queue('tickets', inactivity_timeout_seconds=60)
member = room.join('tickets', 'user-123', {'display_name': 'Alice'})
room.heartbeat('tickets', 'user-123', member.membership_token)
print(room.get_user('tickets', 'user-123'))
print(room.get_position('tickets', 'user-123'))  # 1-based
room.leave('tickets', 'user-123')
```

The injected client may return bytes or decoded strings (UTF-8). Queue names and
user IDs must be nonempty strings. Attributes accept JSON-serializable values;
NaN and infinity are rejected. Attributes round-trip through JSON (e.g. tuples
become arrays). Large integers in attributes retain Python JSON precision.

## API behavior

| Operation | Result |
| --- | --- |
| `create_queue(name, inactivity_timeout_seconds)` | `True` when created, `False` for identical configuration; conflict raises `QueueConfigurationConflict` |
| `join(queue, user_id, attributes=None)` | `Membership`; duplicate preserves token, join time, attributes and FIFO order, and renews inactivity |
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
def scheduled_cleanup():
    # Bound the work per scheduler invocation even under a sustained backlog.
    for _ in range(10):
        if room.cleanup_expired('tickets', batch_size=100) < 100:
            break
```

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
Redis, async clients, admission control, and dequeue/serve policies are outside
this version's scope. Tests cover independent instances and concurrent threads
using shared Redis, exercising the same server serialization as multiprocess use.
