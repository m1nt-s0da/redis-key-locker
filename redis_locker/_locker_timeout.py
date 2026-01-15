from contextlib import contextmanager
from uuid import uuid4
from typing import Generator
from redis import Redis
from time import perf_counter, sleep
from ._locker import RedisLock, RedisLocker, _RedisLock


class RedisLockerWithTimeout:
    def __init__(self, redis: Redis, default_timeout: float):
        self._locker = RedisLocker(redis)
        self.default_timeout = default_timeout

    def _acquire(
        self,
        key: str,
        ttl_ms: int,
        *,
        uuid: str | None = None,
        timeout: float | None = None,
    ) -> str:
        timeout = timeout if timeout is not None else self.default_timeout
        end_time = perf_counter() + timeout
        uuid = uuid or str(uuid4())
        while perf_counter() < end_time:
            if self._locker._acquire(key, ttl_ms, uuid=uuid):
                return uuid
            sleep(0.01)  # Sleep briefly to avoid busy-waiting
        raise TimeoutError(
            f"Could not acquire lock for key '{key}' within {timeout} seconds."
        )

    @contextmanager
    def lock(
        self,
        key: str,
        ttl_ms: int,
        *,
        timeout: float | None = None,
    ) -> Generator[RedisLock, None, None]:
        uuid = self._acquire(key, ttl_ms, timeout=timeout)
        lock = _RedisLock(self._locker, key, uuid)
        try:
            yield lock
        finally:
            self._locker._release(key, uuid)

    @contextmanager
    def __call__(
        self,
        key: str,
        ttl_ms: int,
        *,
        timeout: float | None = None,
    ) -> Generator[RedisLock, None, None]:
        with self.lock(key, ttl_ms, timeout=timeout) as lock:
            yield lock
