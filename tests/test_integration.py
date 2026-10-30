import os
import time
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from uuid import uuid4

import pytest
from redis import Redis

from waitingroom import WaitingRoom, QueueNotFound, QueueConfigurationConflict, StaleMembershipToken


def process_join(args):
    url, prefix, uid = args
    client = Redis.from_url(url)
    try:
        return WaitingRoom(client, prefix).join('q', uid).membership_token
    finally:
        client.close()


@pytest.fixture(params=[False, True], ids=['bytes', 'decoded'])
def room(request):
    client = Redis.from_url(os.getenv('REDIS_URL', 'redis://localhost:6379/15'), decode_responses=request.param)
    client.ping()  # Real Redis is required; never silently substitute a fake.
    instance = WaitingRoom(client, 'test-waitingroom-' + uuid4().hex)
    yield instance
    keys = list(client.scan_iter(instance.key_prefix + ':*'))
    if keys:
        client.delete(*keys)
    client.close()


def test_missing_and_config(room):
    for operation in [lambda: room.join('q', 'a'), lambda: room.leave('q', 'a'),
                      lambda: room.heartbeat('q', 'a', 'token'), lambda: room.get_user('q', 'a'),
                      lambda: room.get_position('q', 'a'), lambda: room.get_queue_size('q'),
                      lambda: room.list_users('q'), lambda: room.cleanup_expired('q')]:
        with pytest.raises(QueueNotFound):
            operation()
    assert room.create_queue('q', 60)
    assert not room.create_queue('q', 60.0)
    with pytest.raises(QueueConfigurationConflict):
        room.create_queue('q', 61)
    with pytest.raises(QueueNotFound):
        room.claim_next('missing')


def process_claim(args):
    url, prefix = args
    client = Redis.from_url(url)
    try:
        member = WaitingRoom(client, prefix).claim_next('q')
        return member.membership_token if member else None
    finally:
        client.close()


def test_claim_fifo_data_removal_and_rejoin(room):
    room.create_queue('q', 60)
    assert room.claim_next('q') is None
    a = room.join('q', 'a', {'name': 'Alice', 'number': 2**60})
    room.join('q', 'b')
    claimed = room.claim_next('q')
    assert claimed == a
    assert room.get_user('q', 'a') is None
    assert room.get_position('q', 'b') == 1
    assert room.heartbeat('q', 'a', a.membership_token) is None
    for key in room._keys('q')[2:4]:
        assert room.redis.zscore(key, 'a') is None
    assert room.redis.hget(room._keys('q')[4], 'a') is None
    rejoined = room.join('q', 'a')
    assert rejoined.membership_token != a.membership_token
    with pytest.raises(StaleMembershipToken):
        room.heartbeat('q', 'a', a.membership_token)
    assert room.claim_next('q').user_id == 'b'
    assert room.claim_next('q').membership_token == rejoined.membership_token
    assert room.claim_next('q') is None


def test_claim_skips_expired_backlog(room):
    room.create_queue('q', .3)
    for i in range(205):
        room.join('q', str(i))
    time.sleep(.35)
    active = room.join('q', 'active')
    assert room.claim_next('q') == active
    assert room.cleanup_expired('q') == 0
    assert room.get_queue_size('q') == 0
    for i in range(105):
        room.join('q', str(i))
    time.sleep(.35)
    assert room.claim_next('q') is None
    assert room.redis.hlen(room._keys('q')[4]) == 0


def test_concurrent_claims(room):
    room.create_queue('q', 60)
    expected = {room.join('q', str(i)).membership_token for i in range(40)}
    with ThreadPoolExecutor(max_workers=16) as pool:
        results = list(pool.map(lambda _: WaitingRoom(room.redis, room.key_prefix).claim_next('q'), range(60)))
    tokens = [m.membership_token for m in results if m]
    assert len(tokens) == len(set(tokens)) == 40
    assert set(tokens) == expected
    assert room.get_queue_size('q') == 0


def test_multiprocess_claims(room):
    room.create_queue('q', 60)
    expected = {room.join('q', str(i)).membership_token for i in range(12)}
    args = [(os.getenv('REDIS_URL', 'redis://localhost:6379/15'), room.key_prefix)] * 20
    with ProcessPoolExecutor(max_workers=4) as pool:
        results = list(pool.map(process_claim, args))
    tokens = [token for token in results if token]
    assert len(tokens) == len(set(tokens)) == 12
    assert set(tokens) == expected


def test_claim_races_with_other_mutations(room):
    room.create_queue('q', 60)
    for _ in range(10):
        old = room.join('q', 'a')
        def heartbeat():
            try:
                return room.heartbeat('q', 'a', old.membership_token)
            except StaleMembershipToken:
                return None
        def rejoin():
            room.leave('q', 'a')
            return room.join('q', 'a')
        with ThreadPoolExecutor(max_workers=4) as pool:
            claims = [pool.submit(room.claim_next, 'q'), pool.submit(heartbeat),
                      pool.submit(rejoin), pool.submit(room.cleanup_expired, 'q')]
            claimed, _, new, _ = [f.result() for f in claims]
        remaining = room.get_user('q', 'a')
        if claimed and claimed.membership_token == new.membership_token:
            assert remaining is None
        else:
            assert remaining.membership_token == new.membership_token
        room.leave('q', 'a')


def test_fifo_duplicate_delete_and_attributes(room):
    room.create_queue('q', 60)
    room.create_queue('other', 60)
    attrs = {'nested': [True, None, '☃'], 'value': 2**60}
    a = room.join('q', 'a', attrs)
    room.join('q', 'b')
    duplicate = room.join('q', 'a', {'ignored': True})
    assert duplicate.membership_token == a.membership_token
    assert duplicate.joined_at == a.joined_at
    assert duplicate.attributes == attrs
    assert room.join('other', 'a').membership_token != a.membership_token
    assert room.get_position('q', 'a') == 1
    assert [m.user_id for m in room.list_users('q', 1, 1)] == ['b']
    assert room.leave('q', 'a')
    assert not room.leave('q', 'a')
    assert room.get_user('q', 'a') is None
    assert room.join('q', 'a').membership_token != a.membership_token
    assert [m.user_id for m in room.list_users('q')] == ['b', 'a']


def test_expiry_visibility_cleanup_and_stale_tokens(room):
    room.create_queue('q', .25)
    old = room.join('q', 'old')
    time.sleep(.30)
    fresh = room.join('q', 'fresh')
    assert room.get_queue_size('q') == 1
    assert room.get_position('q', 'fresh') == 1
    assert room.get_user('q', 'old') is None
    assert room.heartbeat('q', 'old', old.membership_token) is None
    new = room.join('q', 'old')
    assert new.membership_token != old.membership_token
    with pytest.raises(StaleMembershipToken):
        room.heartbeat('q', 'old', old.membership_token)
    assert room.cleanup_expired('q') == 0
    assert room.get_position('q', 'old') == 2
    assert room.heartbeat('q', 'fresh', fresh.membership_token)


def test_renewal_and_bounded_cleanup(room):
    room.create_queue('q', .4)
    a = room.join('q', 'a')
    room.join('q', 'b')
    room.join('q', 'c')
    time.sleep(.25)
    updated = room.heartbeat('q', 'a', a.membership_token)
    assert updated.last_heartbeat_at > a.last_heartbeat_at
    time.sleep(.20)
    assert room.cleanup_expired('q', 1) == 1
    assert room.cleanup_expired('q', 1) == 1
    assert room.cleanup_expired('q', 1) == 0
    assert room.get_queue_size('q') == 1
    assert room.get_position('q', 'a') == 1


def test_concurrent_join_clients(room):
    room.create_queue('q', 60)
    def join(i):
        # Independent instances share only Redis, not a Python lock.
        return WaitingRoom(room.redis, room.key_prefix).join('q', str(i % 12))
    with ThreadPoolExecutor(max_workers=16) as pool:
        results = list(pool.map(join, range(120)))
    assert room.get_queue_size('q') == 12
    for uid in range(12):
        assert len({m.membership_token for m in results if m.user_id == str(uid)}) == 1
    keys = room._keys('q')
    accepted = room.redis.zrange(keys[2], 0, -1)
    assert [m.user_id for m in room.list_users('q')] == [x.decode() if isinstance(x, bytes) else x for x in accepted]


def test_concurrent_cleanup_heartbeat_leave_rejoin(room):
    room.create_queue('q', 60)
    for _ in range(20):
        old = room.join('q', 'a')
        def beat():
            try:
                return room.heartbeat('q', 'a', old.membership_token)
            except StaleMembershipToken:
                return None
        def rejoin():
            room.leave('q', 'a')
            return room.join('q', 'a')
        with ThreadPoolExecutor(max_workers=3) as pool:
            futures = [pool.submit(beat), pool.submit(rejoin), pool.submit(room.cleanup_expired, 'q')]
            results = [f.result() for f in futures]
        assert room.get_user('q', 'a').membership_token == results[1].membership_token
        assert room.get_queue_size('q') == 1
        room.leave('q', 'a')


def test_duplicate_renews_and_cleanup_removes_all_data(room):
    room.create_queue('q', .3)
    a = room.join('q', 'a')
    time.sleep(.2)
    assert room.join('q', 'a').membership_token == a.membership_token
    time.sleep(.15)
    assert room.cleanup_expired('q') == 0
    time.sleep(.2)
    assert room.cleanup_expired('q') == 1
    assert room.cleanup_expired('q') == 0
    keys = room._keys('q')
    assert room.redis.zcard(keys[2]) == room.redis.zcard(keys[3]) == room.redis.hlen(keys[4]) == 0


def test_validation(room):
    for value in [0, -1, float('nan'), float('inf'), True, .0000001]:
        with pytest.raises(ValueError):
            room.create_queue('q', value)
    room.create_queue('q', 60)
    with pytest.raises(ValueError):
        room.join('q', 'a', {'bad': float('nan')})
    with pytest.raises(ValueError):
        room.cleanup_expired('q', 1001)


def test_multiple_processes(room):
    room.create_queue('q', 60)
    url = os.getenv('REDIS_URL', 'redis://localhost:6379/15')
    args = [(url, room.key_prefix, str(i % 4)) for i in range(24)]
    with ProcessPoolExecutor(max_workers=4) as pool:
        tokens = list(pool.map(process_join, args))
    assert room.get_queue_size('q') == 4
    for i in range(4):
        assert len(set(tokens[i::4])) == 1


def test_cleanup_races_with_expired_rejoin(room):
    room.create_queue('q', .08)
    for _ in range(8):
        old = room.join('q', 'a')
        time.sleep(.1)
        with ThreadPoolExecutor(max_workers=2) as pool:
            cleanup = pool.submit(room.cleanup_expired, 'q')
            rejoin = pool.submit(room.join, 'q', 'a')
            cleanup.result()
            new = rejoin.result()
        assert new.membership_token != old.membership_token
        assert room.get_user('q', 'a').membership_token == new.membership_token
        room.leave('q', 'a')
