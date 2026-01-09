from typing import Protocol


class RedisScript(Protocol):
    def __call__(self, keys: list[str], args: list[str | int]) -> int: ...


class RedisClient(Protocol):
    def set(
        self, name: str, value: str, *, px: int | None = None, nx: bool = False
    ) -> bool: ...

    def register_script(self, script: str) -> RedisScript: ...
