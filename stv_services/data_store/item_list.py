#  MIT License
#
#  Copyright (c) 2020 Daniel C. Brotsky
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
import time
from random import uniform
from threading import Timer
from time import time as now
from typing import ClassVar, Optional, Any

from .redis_db import RedisAsync, RedisSync


class ItemListCore:
    """
    Item lists are stored in a sorted set in Redis,
    with the sort key being the time the item can
    next be processed.

    When an item is actually
    being processed, the item time is reset to a
    special marker time encoding the time its
    processing is expected to complete.  This
    allows discovering items that were left
    over from crashes in prior runs.

    This class is never meant to be instantiated
    directly.  Instead, async clients instantiate
    the async version of it, and sync clients instantiate
    the sync version of it.
    """

    IN_PROCESS: ClassVar[float] = 1.0 * 60 * 60 * 24 * 7
    """
    Item lists that are going to be processed have their score set to
    a week from now, so they are way further out than delayed items.
    """

    TIMEOUT: ClassVar[float] = 1.0 * 60 * 60 * 6
    """
    If an item list has been in processing for six hours, it's believed
    to have been left over from a prior run.
    """

    RETRY_DELAY: ClassVar[float] = 1.0 * 60 * 30
    """
    Retries of failed item lists are delayed 30 minutes to let
    the upstream systems recover from whatever their issue was. 
    """

    CLOCK_DRIFT: ClassVar[float] = 1.0 * 15
    """
    There are multiple participating clients of the Store and
    we allow their clocks to drift by 15 seconds relative to
    ours whenever we check the time on an item.
    """

    set_key_template: ClassVar[str] = "{list_type} Store"
    """
    Key template for a sorted set of typed item lists with next-ready times.
    """

    circle_key_template: ClassVar[str] = "{list_type} Deferrals"
    """
    Key template for a circular list of deferred typed item lists.
    """

    channel_name_template: ClassVar[str] = "{list_type} Ready"
    """
    Redis pub/sub channel where item lists of the given type
    that are ready to process are published
    so workers can pick them up.
    """

    @classmethod
    def _set_key(cls, list_type: str):
        return cls.set_key_template.format(list_type=list_type)

    @classmethod
    def _circle_key(cls, list_type: str):
        return cls.circle_key_template.format(list_type=list_type)

    @classmethod
    def _channel_name(cls, list_type: str):
        return cls.channel_name_template.format(list_type=list_type)


class ItemListAsync(ItemListCore):
    """
    The async version of the item list store.  This is used
    by the async web handlers.

    This class is a singleton.
    """

    _db = None

    @classmethod
    async def initialize(cls):
        if cls._db is None:
            cls._db = await RedisAsync.connect()

    @classmethod
    async def finalize(cls):
        if cls._db is not None:
            await RedisAsync.close()
        cls._db = None

    @classmethod
    async def add_new_list(cls, list_type: str, key: str) -> Any:
        """
        Add a newly posted item list to the set with no delay.
        Notify the item list channel that it's ready.
        """
        set_key = cls._set_key(list_type)
        channel_name = cls._channel_name(list_type)
        result = await cls._db.zadd(set_key, score=now(), member=key)
        cls._db.compute_status_for_type(channel_name, key)
        return result

    @classmethod
    async def get_deferred_count(cls, list_type: str) -> int:
        """
        Return the count of deferred item lists.
        """
        circle_key = cls._circle_key(list_type)
        result = await cls._db.llen(circle_key)
        return result

    @classmethod
    async def select_for_undeferral(cls, list_type: str) -> Optional[str]:
        """
        Find the oldest deferred list and return it.
        The returned list is made the most recently deferred.
        Returns None if there are no more deferred lists.
        """
        circle_key = cls._circle_key(list_type)
        result = await cls._db.rpoplpush(circle_key, circle_key)
        return result


class ItemListSync(ItemListCore):
    """
    The sync version of the item list store.  This is used
    by the synchronous workers.

    This class is a singleton.
    """

    _db: Optional[RedisSync.Redis] = None

    @classmethod
    def initialize(cls):
        if cls._db is None:
            cls._db = RedisSync.connect()

    @classmethod
    def finalize(cls):
        if cls._db is not None:
            RedisSync.close()
        cls._db = None

    @classmethod
    def add_retry_list(cls, list_type: str, key: str) -> Any:
        """
        Add a retry list to the set with an appropriate delay.

        Also arrange to notify the channel when it's ready.
        """
        set_key = cls._set_key(list_type)
        result = cls._db.zadd(set_key, {key: now() + cls.RETRY_DELAY})
        channel_name = cls._channel_name(list_type)
        Timer(cls.RETRY_DELAY, lambda: cls._db.publish(channel_name, key)).start()
        return result

    @classmethod
    def remove_processed_list(cls, list_type: str, key: str) -> Any:
        """
        Remove a processed item list from the set.
        """
        set_key = cls._set_key(list_type)
        result = cls._db.zrem(set_key, key)
        # since we're done with the list, delete it
        cls._db.delete(key)
        return result

    @classmethod
    def add_deferred_list(cls, list_type: str, key: str) -> Any:
        """
        Add a deferred item list to the list of them.
        This is a circular list that adds on the left.
        """
        circle_key = cls._circle_key(list_type)
        result = cls._db.lpush(circle_key, key)
        return result

    @classmethod
    def remove_deferred_list(cls, list_type: str, key: str) -> Any:
        """
        Remove an item list from the deferred list.
        """
        circle_key = cls._circle_key(list_type)
        result = cls._db.lrem(circle_key, 1, key)
        return result

    @classmethod
    def get_deferred_count(cls, list_type: str) -> int:
        """
        Return the count of deferred item lists.
        """
        circle_key = cls._circle_key(list_type)
        result = cls._db.llen(circle_key)
        return result

    @classmethod
    def select_for_undeferral(cls, list_type: str) -> Optional[str]:
        """
        Find the oldest deferred list and return it.
        The returned list is made the most recently deferred.
        Returns None if there are no more deferred lists.
        """
        circle_key = cls._circle_key(list_type)
        result = cls._db.rpoplpush(circle_key, circle_key)
        return result

    @classmethod
    def select_from_channels(
        cls, list_types: list[str], timeout: float
    ) -> Optional[str]:
        """
        Wait until there's an item sent to the channel, then return it.
        """
        channel_names = list(map(cls._channel_name, list_types))
        try:
            pubsub = cls._db.pubsub(ignore_subscribe_messages=True)
            pubsub.subscribe(*channel_names)
            message = pubsub.get_message(
                ignore_subscribe_messages=True, timeout=timeout
            )
            key = message["data"].decode("utf-8")
            pubsub.unsubscribe(*channel_names)
            return key
        except RedisSync.Error:
            # typically this error means that the process is shutting down
            # by closing connections.  There's no way to recover from it,
            # so we exit silently rather than report the error.
            return None

    @classmethod
    def select_for_processing(cls, list_type: str) -> Optional[str]:
        """
        Find the first item list of the given type that's ready for processing,
        mark it as in-process, and return it.  The select
        prioritizes any older item lists that were left from a prior run
        over item lists that haven't been processed before.

        Returns:
            The item list key, if one is ready for processing, None otherwise.
        """
        set_key = cls._set_key(list_type)

        def mark_for_processing(key: str) -> bool:
            """
            Mark an item list key as being in process.
            Returns whether setting the mark was successful.
            """
            new_score = now() + cls.IN_PROCESS
            with cls._db.pipeline(transaction=True) as pipe:
                pipe.zadd(set_key, {key: new_score})
                # return the exceptions rather than raising them because of
                # this issue: https://github.com/aio-libs/aioredis/issues/558
                values = pipe.execute(raise_on_error=False)
            return values[0] == 0

        # because we are using optimistic locking, keep trying if we
        # fail due to interference from other _workers.
        while True:
            try:
                start = now()
                # optimistically lock the item set
                cls._db.watch(set_key)
                # first look for abandoned item lists from a prior run
                item_lists = cls._db.zrangebyscore(
                    name=set_key,
                    min=start + (cls.RETRY_DELAY + cls.CLOCK_DRIFT),
                    max=start + cls.IN_PROCESS - (cls.TIMEOUT + cls.CLOCK_DRIFT),
                    start=0,
                    num=1,
                )
                if not item_lists:
                    # next look for the first item list that's ready now
                    item_lists = cls._db.zrangebyscore(
                        name=set_key,
                        min=start - cls.CLOCK_DRIFT,
                        max=start + cls.CLOCK_DRIFT,
                        start=0,
                        num=1,
                    )
                if not item_lists:
                    # no item lists ready for processing, give up
                    return None
                # found one to process
                item_list = item_lists[0]
                if mark_for_processing(item_list):
                    return item_list
                # if marking fails, it's due to a conflict failure,
                # so loop and try again after taking a beat
                time.sleep(uniform(0.1, 0.8))
            finally:
                # we may have been interrupted and already
                # closed down the connection, so make sure
                # it still exists before we unwatch
                if cls._db:
                    cls._db.unwatch()
