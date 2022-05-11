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
#
import os
from random import random
from typing import Any

from sqlalchemy.future import Connection

from . import airtable, act_blue
from ..act_blue.metadata import ActBlueDonationMetadata
from ..core.logging import get_logger, log_exception
from ..core.utilities import local_timestamp
from ..data_store import Postgres
from ..data_store.redis_db import LockingQueue, RedisSync

logger = get_logger(__name__)
queues = ("airtable", "internal", "act_blue", "action_network")


def main():
    logger.info(f"Starting worker at {local_timestamp()}...")
    locking_queues = {queue: LockingQueue(queue) for queue in queues}
    ActBlueDonationMetadata.initialize_forms()
    db = RedisSync.connect()
    pubsub = db.pubsub(ignore_subscribe_messages=True)
    pubsub.subscribe("webhooks")
    try:
        while True:
            timeout = 50.0 + 20.0 * random()
            logger.info(f"Waiting {timeout} seconds for new publish...")
            message: dict = pubsub.get_message(timeout=timeout)
            if not message:
                continue
            if message.get("type") != "message":
                logger.warning(f"Ignoring non-message: {message}")
                continue
            queue_name = message.get("data", b"stop").decode("utf-8").lower()
            if not queue_name or queue_name == "stop":
                logger.error(f"Received exit signal, stopping: {message}")
                break
            locking_queue = locking_queues.get(queue_name)
            if not locking_queue:
                logger.error(f"Received unknown queue name, ignoring: {queue_name}")
                continue
            processed_name = f"{queue_name}:processed"
            results_name = f"{queue_name}:results"
            try:
                logger.info(f"Locking queue '{queue_name}'...")
                locking_queue.lock()
                logger.info(f"Processing messages on queue '{queue_name}'...")
                while result := db.lmove(queue_name, processed_name, "RIGHT", "LEFT"):
                    process_result = process_queue_item(queue_name, result)
                    db.lpush(results_name, process_result)
                logger.info(f"Waiting for published webhooks...")
            except (KeyError, ValueError, UnicodeDecodeError):
                msg = log_exception(logger, "While processing queue item")
                db.lpush(results_name, msg)
            except LockingQueue.LockedByOther:
                # some other worker got it, move on
                logger.info(f"Failed to lock '{queue_name}'")
                continue
            finally:
                locking_queue.unlock()
    except (UnicodeDecodeError, KeyError) as err:
        logger.info(f"Terminating on message decode error: '{err}'...")
    except KeyboardInterrupt:
        logger.info("Terminating on interrupt...")
    finally:
        pubsub.reset()
    db.close()
    logger.info(f"Stopping worker at {local_timestamp()}.")


def process_queue_item(queue_name: str, result: bytes) -> str:
    body: str = result.decode("utf-8")
    logger.info(f"Processing '{queue_name}' webhook: {body}")
    with Postgres.get_global_engine().connect() as conn:  # type: Connection
        if queue_name == "airtable":
            airtable.process_webhook_notification(conn, body)
            result = "processed"
        elif queue_name == "act_blue":
            if act_blue.process_webhook_notification(body):
                result = "processed and metadata saved"
            else:
                result = "processed and metadata discarded"
        elif queue_name == "action_network":
            result = "not processed"
        elif queue_name == "internal":
            result = "not processed"
        else:
            result = f"no such queue: '{queue_name}'"
            logger.error(result)
        conn.commit()
    logger.info(f"Result for '{queue_name}' webhook: {result}")
    return result
