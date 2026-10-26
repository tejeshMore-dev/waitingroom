"""Framework-independent synchronous Redis waiting queues."""
from __future__ import annotations

import hashlib
import json
import logging
import math
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from redis import Redis

from .scripts import CREATE, MUTATE

__all__ = ['WaitingRoom', 'Membership', 'QueueNotFound', 'QueueConfigurationConflict',
           'StaleMembershipToken', 'SequenceExhausted']
logger = logging.getLogger(__name__)


class QueueNotFound(LookupError):
    """The named queue has not been created."""


class QueueConfigurationConflict(ValueError):
    """An existing queue has a different timeout."""


class StaleMembershipToken(ValueError):
    """The token does not identify the current active membership."""


class SequenceExhausted(OverflowError):
    """Queue sequence has exhausted exact Redis score integers."""


@dataclass(frozen=True)
class Membership:
    user_id: str
    membership_token: str
    joined_at: datetime
    last_heartbeat_at: datetime
    expires_at: datetime
    attributes: Any
    position: int | None = None


def _text(value: Any) -> str:
    return value.decode('utf-8') if isinstance(value, bytes) else str(value)


def _member(raw: str | bytes, position: int | None = None) -> Membership:
    data = json.loads(raw)
    def dt(key: str) -> datetime:
        return datetime.fromtimestamp(int(data[key]) / 1_000_000, timezone.utc)
    return Membership(data['user_id'], data['token'], dt('joined_us'),
                      dt('heartbeat_us'), dt('deadline_us'),
                      json.loads(data['attributes_json']), position)


class WaitingRoom:
    def __init__(self, redis: Redis, key_prefix: str = 'waitingroom') -> None:
        if not isinstance(key_prefix, str) or '{' in key_prefix or '}' in key_prefix:
            raise ValueError('key_prefix must be a string without braces')
        self.redis = redis
        self.key_prefix = key_prefix
        self._create = redis.register_script(CREATE)
        self._mutate = redis.register_script(MUTATE)

    def _keys(self, queue: str) -> list[str]:
        if not isinstance(queue, str) or not queue:
            raise ValueError('queue must be a nonempty string')
        digest = hashlib.sha256(queue.encode('utf-8')).hexdigest()
        base = f'{self.key_prefix}:{{{digest}}}'
        return [f'{base}:{suffix}' for suffix in ('config', 'seq', 'order', 'expiry', 'members')]

    @staticmethod
    def _user(user_id: str) -> None:
        if not isinstance(user_id, str) or not user_id:
            raise ValueError('user_id must be a nonempty string')

    def create_queue(self, name: str, inactivity_timeout_seconds: float) -> bool:
        value = inactivity_timeout_seconds
        if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value <= 0 or value > 31_536_000:
            raise ValueError('timeout must be positive and at most one year')
        micros = round(value * 1_000_000)
        if micros < 1:
            raise ValueError('timeout must be at least one microsecond')
        result = self._create(keys=[self._keys(name)[0]], args=[micros])
        if result == -1:
            raise QueueConfigurationConflict(name)
        return bool(result)

    def _run(self, queue: str, action: str, user: str = '', token: str | int = '', attributes: str = 'null') -> Any:
        result = self._mutate(keys=self._keys(queue), args=[action, user, token, attributes])
        status = _text(result[0])
        if status == 'missing':
            raise QueueNotFound(queue)
        if status == 'stale':
            raise StaleMembershipToken(user)
        if status == 'exhausted':
            raise SequenceExhausted(queue)
        if status == 'member':
            return _member(result[1])
        if status == 'absent':
            return None
        return int(result[1])

    def join(self, queue: str, user_id: str, attributes: Any = None) -> Membership:
        self._user(user_id)
        encoded = json.dumps(attributes, allow_nan=False, ensure_ascii=False)
        return self._run(queue, 'join', user_id, uuid4().hex, encoded)

    def leave(self, queue: str, user_id: str) -> bool:
        self._user(user_id)
        return bool(self._run(queue, 'leave', user_id))

    def heartbeat(self, queue: str, user_id: str, membership_token: str) -> Membership | None:
        self._user(user_id)
        if not isinstance(membership_token, str) or not membership_token:
            raise ValueError('membership_token must be a nonempty string')
        return self._run(queue, 'heartbeat', user_id, membership_token)

    def _snapshot(self, queue: str) -> list[Membership]:
        keys = self._keys(queue)
        with self.redis.pipeline(transaction=True) as pipe:
            pipe.get(keys[0])
            pipe.time()
            pipe.zrange(keys[2], 0, -1)
            pipe.hgetall(keys[4])
            config, time, order, records = pipe.execute()
        if config is None:
            raise QueueNotFound(queue)
        now = time[0] * 1_000_000 + time[1]
        active = []
        for uid in order:
            raw = records[uid]
            if int(json.loads(raw)['deadline_us']) > now:
                active.append(_member(raw, len(active) + 1))
        return active

    def get_user(self, queue: str, user_id: str) -> Membership | None:
        self._user(user_id)
        return next((m for m in self._snapshot(queue) if m.user_id == user_id), None)

    def get_position(self, queue: str, user_id: str) -> int | None:
        member = self.get_user(queue, user_id)
        return member.position if member else None

    def get_queue_size(self, queue: str) -> int:
        return len(self._snapshot(queue))

    def list_users(self, queue: str, offset: int = 0, limit: int = 50) -> list[Membership]:
        if isinstance(offset, bool) or not isinstance(offset, int) or offset < 0:
            raise ValueError('offset must be a nonnegative integer')
        if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 1000:
            raise ValueError('limit must be an integer from 1 to 1000')
        return self._snapshot(queue)[offset:offset + limit]

    def cleanup_expired(self, queue: str, batch_size: int = 100) -> int:
        if isinstance(batch_size, bool) or not isinstance(batch_size, int) or not 1 <= batch_size <= 1000:
            raise ValueError('batch_size must be an integer from 1 to 1000')
        removed = self._run(queue, 'cleanup', token=batch_size)
        logger.info('waitingroom.cleanup', extra={'queue_name': queue, 'removed_count': removed})
        return removed
