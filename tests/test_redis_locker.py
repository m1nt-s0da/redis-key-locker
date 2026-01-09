import os
import pytest
import time
from redis import Redis
from redis_locker import RedisLocker, RedisLockerKey
from redis_locker._locker import _RedisLock


@pytest.fixture(scope="session")
def redis_client():
    """実際のRedisクライアントを提供するフィクスチャ"""
    redis_url = os.getenv("TEST_REDIS", "redis://localhost:6379/0")
    client = Redis.from_url(redis_url, decode_responses=True)

    # 接続を確認
    try:
        client.ping()
    except Exception as e:
        pytest.skip(f"Redisに接続できません: {e}")

    yield client

    # テスト後にクリーンアップ
    client.flushdb()
    client.close()


@pytest.fixture
def locker(redis_client):
    """RedisLockerインスタンスを提供するフィクスチャ"""
    return RedisLocker(redis_client)


@pytest.fixture(autouse=True)
def cleanup_test_keys(redis_client):
    """各テスト後にtest_で始まるキーをクリーンアップ"""
    yield
    keys = redis_client.keys("test_*")
    if keys:
        redis_client.delete(*keys)


class TestRedisLocker:
    """RedisLockerクラスのテスト"""

    def test_init(self, redis_client):
        """RedisLockerの初期化テスト"""
        locker = RedisLocker(redis_client)
        assert locker.redis == redis_client

    def test_acquire_success(self, locker):
        """ロック取得成功のテスト"""
        uuid = locker._acquire("test_lock_key", 5000)

        assert uuid is not None
        # Redisに実際にキーが設定されていることを確認
        stored_value = locker.redis.get("test_lock_key")
        assert stored_value == uuid

    def test_acquire_failure(self, locker):
        """ロック取得失敗のテスト（すでにロックされている）"""
        # 最初のロックを取得
        uuid1 = locker._acquire("test_lock_key", 5000)
        assert uuid1 is not None

        # 同じキーで2回目のロックを試みる（失敗するはず）
        uuid2 = locker._acquire("test_lock_key", 5000)
        assert uuid2 is None

    def test_acquire_with_custom_uuid(self, locker):
        """カスタムUUIDでのロック取得テスト"""
        custom_uuid = "custom-uuid-12345"
        uuid = locker._acquire("test_lock_key", 5000, uuid=custom_uuid)

        assert uuid == custom_uuid
        stored_value = locker.redis.get("test_lock_key")
        assert stored_value == custom_uuid

    def test_acquire_ttl(self, locker, redis_client):
        """ロックのTTLが正しく設定されることをテスト"""
        uuid = locker._acquire("test_lock_key", 1000)  # 1秒
        assert uuid is not None

        # TTLを確認（ミリ秒単位）
        ttl = redis_client.pttl("test_lock_key")
        assert 900 <= ttl <= 1000  # 多少の誤差を許容

    def test_release_success(self, locker):
        """ロック解放成功のテスト"""
        uuid = locker._acquire("test_lock_key", 5000)
        assert uuid is not None

        result = locker._release("test_lock_key", uuid)
        assert result is True

        # キーが削除されていることを確認
        assert locker.redis.get("test_lock_key") is None

    def test_release_failure_wrong_uuid(self, locker):
        """ロック解放失敗のテスト（UUIDが一致しない）"""
        uuid = locker._acquire("test_lock_key", 5000)
        assert uuid is not None

        # 間違ったUUIDで解放を試みる
        result = locker._release("test_lock_key", "wrong-uuid")
        assert result is False

        # キーがまだ存在することを確認
        assert locker.redis.get("test_lock_key") == uuid

    def test_release_nonexistent_lock(self, locker):
        """存在しないロックの解放テスト"""
        result = locker._release("nonexistent_key", "some-uuid")
        assert result is False

    def test_touch_success(self, locker, redis_client):
        """ロックのTTL更新成功のテスト"""
        uuid = locker._acquire("test_lock_key", 1000)
        assert uuid is not None

        # TTLを延長
        result = locker._touch("test_lock_key", uuid, 5000)
        assert result is True

        # 新しいTTLを確認
        ttl = redis_client.pttl("test_lock_key")
        assert 4900 <= ttl <= 5000

    def test_touch_failure_wrong_uuid(self, locker, redis_client):
        """ロックのTTL更新失敗のテスト（UUIDが一致しない）"""
        uuid = locker._acquire("test_lock_key", 5000)
        assert uuid is not None

        # 間違ったUUIDでtouchを試みる
        result = locker._touch("test_lock_key", "wrong-uuid", 10000)
        assert result is False

        # TTLが変更されていないことを確認
        ttl = redis_client.pttl("test_lock_key")
        assert ttl <= 5000

    def test_touch_nonexistent_lock(self, locker):
        """存在しないロックのtouch操作のテスト"""
        result = locker._touch("nonexistent_key", "some-uuid", 5000)
        assert result is False

    def test_lock_context_manager_success(self, locker, redis_client):
        """ロックのコンテキストマネージャー（成功）のテスト"""
        with locker.lock("test_lock_key", 5000) as lock:
            assert isinstance(lock, _RedisLock)
            assert lock.key == "test_lock_key"
            assert lock.uuid is not None

            # ロック中はキーが存在する
            assert redis_client.get("test_lock_key") == lock.uuid

        # コンテキストを抜けた後、ロックが解放されている
        assert redis_client.get("test_lock_key") is None

    def test_lock_context_manager_acquire_failure(self, locker):
        """ロック取得失敗時のテスト"""
        # 最初のロックを取得
        uuid = locker._acquire("test_lock_key", 5000)
        assert uuid is not None

        # 同じキーでロックを試みる（失敗するはず）
        with pytest.raises(RuntimeError, match="Failed to acquire lock"):
            with locker.lock("test_lock_key", 5000):
                pass

    def test_lock_context_manager_exception_handling(self, locker, redis_client):
        """コンテキストマネージャー内で例外が発生してもロックが解放されることをテスト"""
        with pytest.raises(ValueError):
            with locker.lock("test_lock_key", 5000) as lock:
                # ロック中はキーが存在する
                assert redis_client.get("test_lock_key") == lock.uuid
                raise ValueError("Test exception")

        # 例外が発生してもロックが解放されていることを確認
        assert redis_client.get("test_lock_key") is None

    def test_call_method(self, locker, redis_client):
        """__call__メソッドのテスト"""
        with locker("test_lock_key", 5000) as lock:
            assert isinstance(lock, _RedisLock)
            assert lock.key == "test_lock_key"
            assert redis_client.get("test_lock_key") == lock.uuid

        # ロックが解放されていることを確認
        assert redis_client.get("test_lock_key") is None

    def test_getitem_returns_locker_key(self, locker, redis_client):
        """__getitem__がRedisLockerKeyを返すことをテスト"""
        locker_key = locker["test_lock_key"]

        assert isinstance(locker_key, RedisLockerKey)
        assert locker_key._key == "test_lock_key"


class TestRedisLock:
    """_RedisLockクラスのテスト"""

    def test_redis_lock_properties(self, locker):
        """_RedisLockのプロパティのテスト"""
        lock = _RedisLock(locker, "test_lock_key", "test-uuid-123")

        assert lock.key == "test_lock_key"
        assert lock.uuid == "test-uuid-123"
        assert lock.locker == locker

    def test_redis_lock_touch(self, locker, redis_client):
        """_RedisLockのtouchメソッドのテスト"""
        uuid = locker._acquire("test_lock_key", 1000)
        lock = _RedisLock(locker, "test_lock_key", uuid)

        result = lock.touch(5000)
        assert result is True

        # TTLが更新されていることを確認
        ttl = redis_client.pttl("test_lock_key")
        assert 4900 <= ttl <= 5000


class TestRedisLockerKey:
    """RedisLockerKeyクラスのテスト"""

    def test_locker_key_init(self, redis_client):
        """RedisLockerKeyの初期化テスト"""
        locker_key = RedisLockerKey(redis_client, "test_lock_key")

        assert locker_key._key == "test_lock_key"
        assert isinstance(locker_key._locker, RedisLocker)

    def test_locker_key_lock_method(self, redis_client):
        """RedisLockerKeyのlockメソッドのテスト"""
        locker_key = RedisLockerKey(redis_client, "test_lock_key")

        with locker_key.lock(5000) as lock:
            assert isinstance(lock, _RedisLock)
            assert lock.key == "test_lock_key"
            assert redis_client.get("test_lock_key") == lock.uuid

        # ロックが解放されていることを確認
        assert redis_client.get("test_lock_key") is None

    def test_locker_key_call_method(self, redis_client):
        """RedisLockerKeyの__call__メソッドのテスト"""
        locker_key = RedisLockerKey(redis_client, "test_lock_key")

        with locker_key(5000) as lock:
            assert isinstance(lock, _RedisLock)
            assert lock.key == "test_lock_key"
            assert redis_client.get("test_lock_key") == lock.uuid

        # ロックが解放されていることを確認
        assert redis_client.get("test_lock_key") is None


class TestConcurrency:
    """並行処理のテスト"""

    def test_lock_prevents_concurrent_access(self, locker, redis_client):
        """ロックが並行アクセスを防ぐことをテスト"""
        acquired_locks = []

        # 最初のロックを取得
        with locker.lock("test_lock_key", 5000) as lock1:
            acquired_locks.append(lock1)

            # 同時に同じキーでロックを取得しようとする（失敗するはず）
            try:
                with locker.lock("test_lock_key", 5000):
                    acquired_locks.append("should_not_reach")
            except RuntimeError:
                pass  # 期待通りの動作

        # 最初のロックのみ取得できたことを確認
        assert len(acquired_locks) == 1
        assert acquired_locks[0].key == "test_lock_key"

        # ロック解放後は新しいロックを取得できる
        with locker.lock("test_lock_key", 5000) as lock2:
            assert lock2.key == "test_lock_key"

    def test_multiple_different_locks(self, locker, redis_client):
        """異なるキーの複数のロックを同時に保持できることをテスト"""
        with locker.lock("test_lock_key_1", 5000) as lock1:
            with locker.lock("test_lock_key_2", 5000) as lock2:
                with locker.lock("test_lock_key_3", 5000) as lock3:
                    assert lock1.key == "test_lock_key_1"
                    assert lock2.key == "test_lock_key_2"
                    assert lock3.key == "test_lock_key_3"

                    # すべてのロックが有効
                    assert redis_client.get("test_lock_key_1") == lock1.uuid
                    assert redis_client.get("test_lock_key_2") == lock2.uuid
                    assert redis_client.get("test_lock_key_3") == lock3.uuid


class TestLockExpiration:
    """ロックの有効期限のテスト"""

    def test_lock_expires_after_ttl(self, locker, redis_client):
        """ロックがTTL後に期限切れになることをテスト"""
        uuid = locker._acquire("test_lock_key", 100)  # 100ms
        assert uuid is not None

        # すぐにはキーが存在する
        assert redis_client.get("test_lock_key") == uuid

        # TTLが過ぎるのを待つ
        time.sleep(0.15)  # 150ms待機

        # キーが期限切れで削除されている
        assert redis_client.get("test_lock_key") is None

    def test_touch_extends_lock_lifetime(self, locker, redis_client):
        """touchがロックの有効期限を延長することをテスト"""
        uuid = locker._acquire("test_lock_key", 200)  # 200ms
        assert uuid is not None

        # 100ms待機
        time.sleep(0.1)

        # TTLを延長
        locker._touch("test_lock_key", uuid, 500)  # さらに500ms

        # 元のTTLを過ぎても有効
        time.sleep(0.15)  # 合計250ms
        assert redis_client.get("test_lock_key") == uuid

        # 延長後のTTLも過ぎれば期限切れ
        time.sleep(0.4)  # さらに400ms（合計650ms）
        assert redis_client.get("test_lock_key") is None


class TestIntegration:
    """統合テスト"""

    def test_realistic_lock_usage(self, locker):
        """現実的なロック使用シナリオのテスト"""
        counter = {"value": 0}

        # ロックを使ってカウンターを安全にインクリメント
        for i in range(5):
            with locker.lock("test_counter_lock", 1000) as lock:
                # クリティカルセクション
                current = counter["value"]
                counter["value"] = current + 1

                # ロック中にTTLを延長
                if i == 2:
                    lock.touch(2000)

        assert counter["value"] == 5

    def test_locker_key_convenience_syntax(self, locker, redis_client):
        """RedisLockerKeyの便利な構文のテスト"""
        # 角括弧構文でロックを取得
        with locker["test_resource"](3000) as lock:
            assert lock.key == "test_resource"
            assert redis_client.get("test_resource") == lock.uuid

        # ロックが解放されている
        assert redis_client.get("test_resource") is None

        # 再度同じリソースをロック
        locker_key = locker["test_resource"]
        with locker_key.lock(3000) as lock:
            assert lock.key == "test_resource"

    def test_multiple_lockers_same_redis(self, redis_client):
        """同じRedisインスタンスを使う複数のLockerのテスト"""
        locker1 = RedisLocker(redis_client)
        locker2 = RedisLocker(redis_client)

        # locker1でロックを取得
        with locker1.lock("test_shared_lock", 5000) as lock1:
            # locker2では同じロックを取得できない
            with pytest.raises(RuntimeError):
                with locker2.lock("test_shared_lock", 5000):
                    pass

        # locker1のロックが解放された後は、locker2でロック可能
        with locker2.lock("test_shared_lock", 5000) as lock2:
            assert lock2.key == "test_shared_lock"
