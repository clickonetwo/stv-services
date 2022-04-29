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


class RedisAsync:
    """
    A singleton dispenser of async redis connections.
    """

    from aioredis import Redis, RedisError

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
