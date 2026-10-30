"""Run with: python examples/basic.py (Redis on localhost, or REDIS_URL)."""
import os
from uuid import uuid4

from redis import Redis
from waitingroom import WaitingRoom

client = Redis.from_url(os.getenv('REDIS_URL', 'redis://localhost:6379/0'))
room = WaitingRoom(client, key_prefix='waitingroom-example')
queue = 'demo-' + uuid4().hex
room.create_queue(queue, inactivity_timeout_seconds=30)
alice = room.join(queue, 'alice', {'display_name': 'Alice'})
room.join(queue, 'bob')
print('Alice position:', room.get_position(queue, 'alice'))
print('Heartbeat renewed:', room.heartbeat(queue, 'alice', alice.membership_token) is not None)
print('Queue:', [(m.position, m.user_id) for m in room.list_users(queue)])
claimed = room.claim_next(queue)
print('Claimed first user:', claimed.user_id if claimed else None)
print('Queue after claim:', [(m.position, m.user_id) for m in room.list_users(queue)])
print('Removed abandoned memberships:', room.cleanup_expired(queue))
room.leave(queue, 'bob')
print('Empty queue:', room.claim_next(queue))
client.close()
