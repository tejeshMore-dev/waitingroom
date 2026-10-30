# WaitingRoom

A synchronous, framework-independent Python package for FIFO waiting queues on Redis.
Python 3.10+; Redis 6.2+ standalone primary (including a primary obtained through
redis-py Sentinel). No background threads or web framework dependencies.

**Current scope:** you can add users, keep them active with heartbeats, inspect
their FIFO order, remove a specified user, and clean up abandoned memberships.
**Claiming the first available user is not implemented.** There is no
`claim_next()`, atomic dequeue, reservation, or acknowledgement API in this release.
Reading the first user does not claim them.

## Queue lifecycle at a glance

| Step | Application calls | What happens |
| --- | --- | --- |
| 1. Create a queue | `create_queue(...)` | Persist its inactivity timeout |
| 2. Add a user | `join(...)` | Append an active membership to FIFO order and return its token |
| 3. Keep the user active | `heartbeat(..., membership_token)` | Extend inactivity without changing order |
| 4. Inspect the queue | `get_user(...)`, `get_position(...)`, `get_queue_size(...)`, `list_users(...)` | Return active memberships and their current positions |
| 5. Read the first available user | `list_users(..., limit=1)` | Peek only; no removal or exclusive ownership |
| 6. Claim the first available user | **Not available** | Requires a new atomic operation; see [Claiming versus reading](#claiming-versus-reading) |
| 7. Remove a known user | `leave(...)` | Atomically remove that user's membership and associated data |
| 8. Drop abandoned users | `cleanup_expired(...)` | Physically remove a bounded batch; reads already exclude expired users |

```mermaid
sequenceDiagram
    participant App as Application
    participant Room as WaitingRoom package
    participant Redis as Redis primary
    participant Scheduler as Application scheduler

    App->>Room: create_queue("tickets", 60)
    Room->>Redis: Atomic create or validate configuration
    Redis-->>Room: Created or same configuration
    Room-->>App: True or False
    App->>Room: join("tickets", "alice", attributes)
    Room->>Redis: Atomic enqueue using sequence and server time
    Redis-->>Room: Membership with token and deadline
    Room-->>App: alice membership
    App->>Room: join("tickets", "bob")
    Room->>Redis: Atomic enqueue after Alice
    Redis-->>Room: Bob membership
    Room-->>App: bob membership
    loop Before Alice's inactivity deadline
        App->>Room: heartbeat("tickets", "alice", token)
        Room->>Redis: Validate active membership and token; renew deadline
        Redis-->>Room: Updated membership; FIFO unchanged
        Room-->>App: Renewed membership
    end
    App->>Room: list_users("tickets")
    Room->>Redis: Atomic snapshot with server time
    Redis-->>Room: Order and membership records
    Room-->>App: Active users: Alice at 1, Bob at 2
    App->>Room: list_users("tickets", limit=1)
    Room->>Redis: Read snapshot
    Redis-->>Room: Queue records
    Room-->>App: Alice (peek only)
    Note over App,Room: claim_next is not implemented. A read does not reserve Alice.
    alt Application removes a known user
        App->>Room: leave("tickets", "alice")
        Room->>Redis: Atomically remove Alice's stored membership
        Redis-->>Room: Removed or already absent
        Room-->>App: True or False
    else Alice stops heartbeats and reaches her deadline
        Note over Room,Redis: Alice is immediately excluded from reads, even before cleanup
        Scheduler->>Room: cleanup_expired("tickets", batch_size=100)
        Room->>Redis: Atomically select and remove expired memberships
        Redis-->>Room: Removed count
        Room-->>Scheduler: Removed count
    end
```

The application decides when to send heartbeats and call `leave`. The scheduler
is supplied by your application; WaitingRoom starts no background work. Redis
defines enqueue order and inactivity time across all application instances.

## Install and run

```sh
docker run -d --rm --name waitingroom-redis -p 127.0.0.1:6379:6379 redis:7-alpine
python -m pip install -e ".[test]"
python -m pytest -q
python examples/basic.py
```

Run these commands from this directory. Tests require **real Redis** and use a
unique prefix per test; they never flush a database. Set `REDIS_URL` to override
the default test URL `redis://localhost:6379/15` (example defaults to database 0).
Docker Desktop must be running if you use the Docker command above. When done,
stop this Redis instance with `docker stop waitingroom-redis`.

## Walk through the working flow

Run the following in a Python session after installation. It uses a fresh queue
name so rerunning it does not reuse previous memberships. The expected positions
assume it runs before the 60-second inactivity deadlines; heartbeat again if you
pause while following along.

```python
import os
from uuid import uuid4
from redis import Redis
from waitingroom import WaitingRoom

client = Redis.from_url(os.getenv('REDIS_URL', 'redis://localhost:6379/0'))
room = WaitingRoom(client, key_prefix='myapp')
queue = 'walkthrough-' + uuid4().hex

# 1. Create an independent queue.
assert room.create_queue(queue, inactivity_timeout_seconds=60)
assert not room.create_queue(queue, inactivity_timeout_seconds=60)

# 2. Add Alice, then Bob. Keep Alice's membership token for heartbeats.
alice = room.join(queue, 'alice', {'display_name': 'Alice'})
bob = room.join(queue, 'bob', {'display_name': 'Bob'})
duplicate = room.join(queue, 'alice', {'display_name': 'Ignored'})
assert duplicate.membership_token == alice.membership_token
assert duplicate.attributes == {'display_name': 'Alice'}
# Duplicate join renews inactivity but preserves join time, token and FIFO order.

# 3. Renew while active, for example every 20 seconds for this 60-second timeout.
renewed = room.heartbeat(queue, 'alice', alice.membership_token)
assert renewed is not None
assert renewed.joined_at == alice.joined_at
# None means absent/expired: join again and keep the NEW membership token.
# A different token on an active membership raises StaleMembershipToken.

# 4. Check waiting status, metadata, position and size.
current = room.get_user(queue, 'alice')
assert current is not None
print(current.joined_at, current.last_heartbeat_at, current.attributes)
assert room.get_position(queue, 'alice') == 1
assert room.get_queue_size(queue) == 2

# 5. List active users in FIFO order, including their absolute 1-based positions.
users = room.list_users(queue, offset=0, limit=50)
print([(user.position, user.user_id) for user in users])
# [(1, 'alice'), (2, 'bob')]
second_page = room.list_users(queue, offset=1, limit=1)
assert [(user.position, user.user_id) for user in second_page] == [(2, 'bob')]

# 6. Peek at the first active user. This DOES NOT claim or remove them.
first_page = room.list_users(queue, limit=1)
first_user = first_page[0] if first_page else None
assert first_user is not None and first_user.user_id == 'alice'
assert room.get_queue_size(queue) == 2

# 7. Remove a known user when your application decides they should leave.
# This is not a concurrent worker claim operation.
assert room.leave(queue, 'alice')
assert not room.leave(queue, 'alice')  # Idempotent.
assert room.get_user(queue, 'alice') is None
assert room.get_position(queue, 'bob') == 1

# 8. A later rejoin goes behind Bob and gets a new token.
alice_again = room.join(queue, 'alice')
assert alice_again.membership_token != alice.membership_token
assert room.get_position(queue, 'alice') == 2

room.leave(queue, 'bob')
room.leave(queue, 'alice')
assert room.list_users(queue) == []
client.close()
```

### Claiming versus reading

**There is currently no supported way to atomically claim the first available
user.** `list_users(queue, limit=1)` returns the first active membership at the
time of its read, or `[]` if no active users are waiting. It leaves that user
available to every other reader.

Do not build a concurrent worker claim by reading the first user and then calling
`leave`: two workers can read the same user before either removes them, and both
can start processing. `leave`'s atomic removal alone does not make that two-call
workflow atomic. It also operates by user ID and may remove a later rejoin.

A future claim API needs an explicit delivery policy:

| Policy (not implemented) | Behavior | If a worker crashes |
| --- | --- | --- |
| Atomic dequeue | Return and remove the first active membership in one Redis operation | The membership is already gone; recovery belongs to the application |
| Claim with a lease | Atomically reserve the first active membership, then acknowledge completion | An expired reservation can make it available again; requires ownership tokens and retry rules |

The supported lifecycle currently ends at explicit removal or inactivity expiry.
Membership heartbeat tokens protect waiting membership renewal; they are not
worker lease tokens. Neither policy above is provided by the current package.

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
Redis, async clients, admission control, and dequeue/serve policies are outside
this version's scope. Tests cover independent instances and concurrent threads
using shared Redis, exercising the same server serialization as multiprocess use.
