from contextlib import contextmanager
from uuid import uuid4
from typing import Generator, Protocol
from redis import Redis


__all__ = ["RedisLock", "RedisLocker", "RedisLockerKey"]


class RedisLock(Protocol):
    def touch(self, ttl_ms: int) -> bool:
        """
        Extend the lock's time-to-live (TTL) by the specified milliseconds.
        Returns True if the TTL was successfully extended, False otherwise.
        """
        ...

    @property
    def key(self) -> str: ...

    @property
    def uuid(self) -> str: ...


class _RedisLock(RedisLock):
    def __init__(self, locker: "RedisLocker", key: str, uuid: str):
        self._locker = locker
        self._key = key
        self._uuid = uuid

    def touch(self, ttl_ms: int) -> bool:
        return self.locker._touch(self.key, self.uuid, ttl_ms)

    @property
    def key(self) -> str:
        return self._key

    @property
    def uuid(self) -> str:
        return self._uuid

    @property
    def locker(self) -> "RedisLocker":
        return self._locker


class RedisLocker:
    def __init__(self, redis: Redis):
        self.redis = redis
        release_script = """
        if redis.call("get", KEYS[1]) == ARGV[1] then
            return redis.call("del", KEYS[1])
        else
            return 0
        end
        """
        self.__release_script = self.redis.register_script(release_script)

        touch_script = """
        if redis.call("get", KEYS[1]) == ARGV[1] then
            return redis.call("pexpire", KEYS[1], ARGV[2])
        else
            return 0
        end
        """
        self.__touch_script = self.redis.register_script(touch_script)

    def _acquire(self, key: str, ttl_ms: int, *, uuid: str | None = None) -> str | None:
        uuid = uuid or str(uuid4())
        if self.redis.set(name=key, value=uuid, nx=True, px=ttl_ms):
            return uuid
        return None

    def _release(self, key: str, uuid: str) -> bool:
        result = self.__release_script(keys=[key], args=[uuid])
        return result == 1

    def _touch(self, key: str, uuid: str, ttl_ms: int) -> bool:
        result = self.__touch_script(keys=[key], args=[uuid, ttl_ms])
        return result == 1

    @contextmanager
    def lock(self, key: str, ttl_ms: int) -> Generator[RedisLock]:
        uuid = self._acquire(key, ttl_ms)
        if uuid is None:
            raise RuntimeError("Failed to acquire lock")

        try:
            yield _RedisLock(self, key, uuid)
        finally:
            self._release(key, uuid)

    @contextmanager
    def __call__(self, key: str, ttl_ms: int) -> Generator[RedisLock]:
        with self.lock(key, ttl_ms) as lock:
            yield lock

    def __getitem__(self, key: str) -> "RedisLockerKey":
        return RedisLockerKey(self.redis, key)


class RedisLockerKey:
    def __init__(self, redis, key: str):
        self._locker = RedisLocker(redis)
        self._key = key

    @contextmanager
    def lock(self, ttl_ms: int) -> Generator[RedisLock]:
        with self._locker.lock(self._key, ttl_ms) as lock:
            yield lock

    @contextmanager
    def __call__(self, ttl_ms: int) -> Generator[RedisLock]:
        with self.lock(ttl_ms) as lock:
            yield lock
