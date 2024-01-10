#  MIT License
#
#  Copyright (c) 2022 Daniel C. Brotsky
#
#  Permission is hereby granted, free of charge, to any person obtaining a copy
#  of this software and associated documentation files (the "Software"), to deal
#  in the Software without restriction, including without limitation the rights
#  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
#  copies of the Software, and to permit persons to whom the Software is
#  furnished to do so, subject to the following conditions:
#
#  The above copyright notice and this permission notice shall be included in all
#  copies or substantial portions of the Software.
#
#  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
#  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
#  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
#  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
#  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
#  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
#  SOFTWARE.
import os
import uuid


class RedisAsync:
    """
    A singleton dispenser of async redis connections.
    """

    from redis.asyncio import Redis, RedisError

    _pool = None
    Error = RedisError

    @classmethod
    async def connect(cls) -> Redis:
        if cls._pool is None:
            url = os.getenv("REDIS_URL") or "redis://localhost:6379/0"
            cls._pool = cls.Redis.from_url(url, max_connections=5)
        return cls._pool

    @classmethod
    async def close(cls):
        if cls._pool is None:
            return
        await cls._pool.close()
        cls._pool = None


class RedisSync:
    """
    A singleton dispenser of sync redis connections
    """

    from redis import Redis, RedisError

    _pool = None
    Redis = Redis
    Error = RedisError

    @classmethod
    def connect(cls) -> Redis:
        if cls._pool is None:
            url = os.getenv("REDIS_URL") or "redis://localhost:6379/0"
            cls._pool = cls.Redis.from_url(url, max_connections=5)
        return cls._pool

    @classmethod
    def close(cls):
        if cls._pool is None:
            return
        cls._pool.close()
        cls._pool = None


class LockingQueue:
    lock_release_script = """
        if redis.call("get",KEYS[1]) == ARGV[1]
        then
            return redis.call("del",KEYS[1])
        else
            return 0
        end
    """

    class LockedByOther(PermissionError):
        pass

    class AlreadyLocked(PermissionError):
        pass

    class AlreadyUnlocked(PermissionError):
        pass

    class NotLocked(PermissionError):
        pass

    def __init__(self, queue_name: str, secs_to_lock: int = 600):
        if not queue_name:
            raise ValueError("You must supply a non-empty queue name to lock")
        self.queue_name = queue_name
        self.key_name = f"LockingQueue<{queue_name}>"
        self.lock_value = str(uuid.uuid4())
        self.lock_state = "unlocked"
        self.duration = secs_to_lock
        self.db = RedisSync.connect()
        self.script = self.db.script_load(self.lock_release_script)

    def __del__(self):
        self.db.close()
        self.db = None

    def __repr__(self):
        return f"<LockingQueue '{self.key_name}' ({self.lock_state})>"

    def state(self):
        return self.lock_state

    def lock(self):
        """Lock the queue"""
        if self.lock_state == "locked":
            raise self.AlreadyLocked("The queue is already locked")
        result = self.db.set(self.key_name, self.lock_value, nx=True, ex=self.duration)
        if not result:
            raise self.LockedByOther("The queue is locked by another client")
        self.lock_state = "locked"

    def unlock(self):
        """Unlock the queue"""
        if self.lock_state == "unlocked":
            raise self.AlreadyUnlocked("The queue is already unlocked")
        result = self.db.evalsha(self.script, 1, self.key_name, self.lock_value)
        self.lock_state = "unlocked"
        if not result:
            raise self.NotLocked("The queue's lock had already expired")

    def renew_lock(self):
        """Renew an existing queue lock"""
        if self.lock_state == "unlocked":
            raise self.AlreadyUnlocked("You must lock before you renew")
        result = self.db.set(self.key_name, self.lock_value, xx=True, ex=self.duration)
        if not result:
            self.lock_state = "locked"
            raise self.NotLocked("The lock has been lost")
