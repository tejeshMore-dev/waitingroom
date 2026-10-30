# WaitingRoom

A Redis-backed Python package for serving waiting users in FIFO order.
Use it from Django, Flask, FastAPI, or workers without a framework dependency.
Python 3.10+ and Redis 6.2+ are required.

## How it works

1. **Create** a named queue with an inactivity timeout.
2. **Join** users; each receives a membership token and a place at the back.
3. **Heartbeat** with that token to keep the membership active without changing order.
4. **Inspect** active users, their positions, or the first user without removing them.
5. **Claim next** to atomically return and remove the first active user for processing.
6. **Leave** to remove a specific user, or periodically **clean up** abandoned users.

`join → heartbeat → list users → claim_next → process user`

Claiming is an immediate removal. Concurrent workers cannot claim the same
membership, but a worker crash after claiming does not put it back in the queue.
There are no processing leases or acknowledgements.

## Install and test

Run from this repository with Docker Desktop running:

```sh
docker run -d --rm --name waitingroom-redis -p 127.0.0.1:6379:6379 redis:7-alpine
python -m pip install -e ".[test]"
python -m pytest -q
python examples/basic.py
```

Set `REDIS_URL` to use another server (PowerShell:
`$env:REDIS_URL = "redis://localhost:6379/15"`). Tests require real Redis and use
isolated prefixes; they never flush a database. Stop the container afterward
with `docker stop waitingroom-redis`.

## Quick walkthrough

```python
import os
from uuid import uuid4
from redis import Redis
from waitingroom import WaitingRoom

client = Redis.from_url(os.getenv('REDIS_URL', 'redis://localhost:6379/0'))
room = WaitingRoom(client, key_prefix='myapp')
queue = 'tickets-' + uuid4().hex  # Fresh queue for this demo.
room.create_queue(queue, inactivity_timeout_seconds=60)

alice = room.join(queue, 'alice', {'display_name': 'Alice'})
room.join(queue, 'bob')
room.heartbeat(queue, 'alice', alice.membership_token)

print([(m.position, m.user_id) for m in room.list_users(queue)])
# [(1, 'alice'), (2, 'bob')]
print(room.get_position(queue, 'alice'))  # 1
print(room.get_queue_size(queue))         # 2

claimed = room.claim_next(queue)          # Atomically takes Alice.
print(claimed.user_id if claimed else None)  # alice
print(room.get_user(queue, 'alice'))      # None: no longer waiting.
print(room.get_position(queue, 'bob'))    # 1

room.leave(queue, 'bob')                  # Remove a specified user.
print(room.claim_next(queue))             # None: empty queue.
client.close()
```

## API at a glance

| Method | Result |
| --- | --- |
| `create_queue(name, inactivity_timeout_seconds)` | `True` if created; `False` for the same existing configuration |
| `join(queue, user_id, attributes=None)` | Membership with token, timestamps and JSON attributes |
| `heartbeat(queue, user_id, membership_token)` | Renewed membership, or `None` if absent/expired |
| `get_user(queue, user_id)` | Active membership, or `None` |
| `get_position(queue, user_id)` | Active position starting at 1, or `None` |
| `get_queue_size(queue)` | Active user count |
| `list_users(queue, offset=0, limit=50)` | Active users in FIFO order, each with their position |
| `claim_next(queue)` | First active membership, atomically removed; `None` if empty |
| `leave(queue, user_id)` | `True` if a stored membership was removed; otherwise `False` |
| `cleanup_expired(queue, batch_size=100)` | Number physically removed, up to the batch size |

Duplicate joins preserve ordering, attributes and token, and renew inactivity.
Rejoining after removal or expiry creates a new membership at the back.
A nonexistent queue raises `QueueNotFound`; conflicting configuration raises
`QueueConfigurationConflict`. A wrong token on an active membership raises
`StaleMembershipToken`. An expired heartbeat cannot revive a membership.

## Periodic cleanup

Have your scheduler call `room.cleanup_expired(queue, batch_size=100)` periodically.
Each call is bounded; the package starts no background threads. Send heartbeats
before the timeout (for example every 20 seconds for a 60-second timeout).
Expired users disappear from counts, positions, listings and claims immediately,
even before physical cleanup.

## Important details

- FIFO uses Redis-generated sequence numbers; mutation scripts use Redis server time.
- Reads use an O(n) atomic snapshot. Pagination offsets may shift between requests.
- Claims skip expired users in batches of 100. An expiry backlog adds round trips.
- Use a standalone Redis primary or Sentinel primary; Cluster clients are unsupported.
- A lost claim response may mean the user was already removed; retrying can take
  another user. Redis persistence settings determine durability.

See [API reference and design notes](docs/design.md) for key layout, validation,
concurrency rules, and failure guarantees.
