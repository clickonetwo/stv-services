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
import hashlib
import json
from datetime import datetime, timezone, timedelta
from random import random
from time import sleep

from requests import HTTPError
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.future import Connection

from . import airtable, act_blue
from ..act_blue.metadata import ActBlueDonationMetadata
from ..airtable.bulk import update_all_records, verify_schemas, register_webhooks
from ..core import Configuration
from ..core.logging import get_logger, log_exception
from ..core.utilities import local_timestamp
from ..data_store import Postgres
from ..data_store.redis_db import LockingQueue, RedisSync

logger = get_logger(__name__)
queues = ("control", "act_blue", "action_network", "airtable")
locking_queues = {}
db: RedisSync.Redis


def main():
    startup()
    process_incoming_webhooks()
    shutdown()


def startup():
    global db, locking_queues
    logger.info(f"Starting worker at {local_timestamp()}...")
    # make sure the Airtable schema is as expected
    verify_schemas(verbose=True)
    # make sure the Airtable webhooks are registered
    register_webhooks(verbose=True, sync_first=True)
    # connect to the redis signalling backend
    db = RedisSync.connect()
    locking_queues = {queue: LockingQueue(queue) for queue in queues}
    # make sure the metadata form cache is loaded
    ActBlueDonationMetadata.initialize_forms()


def shutdown():
    db.close()
    logger.info(f"Stopping worker at {local_timestamp()}.")


def do_housekeeping(scheduled: datetime):
    """Do routine housekeeping, such as processing hooks that have
    to be retried, or importing data from external systems that
    don't have webhook capabilities.  The argument is the time
    the houskeeping was scheduled to run.  The return value is
    the time that housekeeping should next be scheduled to run."""
    logger.info("Processing pending webhooks on all queues")
    for queue in queues:
        target, completed = process_queue_items(queue)
        if target == 0:
            logger.info(f"No items on queue '{queue}'")
        elif completed < 0:
            # assume another worker has this queue
            logger.info(f"Skipping locked queue '{queue}'")
        elif completed < target:
            # an item failed; it will get picked
            # up in the housekeeping phase later
            logger.info(f"Failed webhook left on queue '{queue}'")
    return datetime.now(tz=timezone.utc) + timedelta(minutes=5)


def process_incoming_webhooks():
    pubsub = db.pubsub(ignore_subscribe_messages=True)
    pubsub.subscribe("webhooks")
    next_housekeeping = datetime.now(tz=timezone.utc)
    try:
        while True:
            if datetime.now(tz=timezone.utc) >= next_housekeeping:
                next_housekeeping = do_housekeeping(next_housekeeping)
            timeout = 50.0 + 20.0 * random()
            logger.info(f"Waiting {timeout} seconds for new publish...")
            message: dict = pubsub.get_message(timeout=timeout)
            if not message:
                continue
            if message.get("type") != "message":
                logger.warning(f"Ignoring non-message: {message}")
                continue
            queue = message.get("data", b"stop").decode("utf-8").lower()
            if queue not in queues:
                logger.error(f"Received non-webhook: stopping: {message}")
                break
            process_queue_items(queue)
    except (UnicodeDecodeError, KeyError) as err:
        log_exception(logger, f"While receiving message: '{err}'")
        logger.critical(f"Terminating on message receive error.")
        if Configuration.get_env() == "DEV":
            raise
    except KeyboardInterrupt:
        logger.info("Terminating on interrupt...")
    finally:
        pubsub.reset()


def process_queue_items(queue: str) -> (int, int):
    """Try locking and processing all items on the queue.
    Returns the number of items found and the number processed.

    Note that items can be added to the queue while it's being
    processed, so the second number may be higher than the first.

    As a special case, if the second number is negative, that
    means that we failed to lock the queue."""
    if target := db.llen(queue):
        processed = 0
    else:
        return 0, 0
    locking_queue = locking_queues[queue]
    try:
        logger.info(f"Locking queue '{queue}'...")
        locking_queue.lock()
    except LockingQueue.LockedByOther:
        # some other worker got it, move on
        logger.info(f"Failed to lock '{queue}'")
        return target, -1
    try:
        logger.info(f"Starting to process {target} webhooks on queue '{queue}'")
        while result := db.lrange(queue, -1, -1):
            hook_id = hashlib.md5(result[0]).hexdigest()
            hook = json.loads(result[0])
            if process_webhook(queue, hook, hook_id):
                processed += 1
                db.rpop(queue, 1)
                locking_queue.renew_lock()
            else:
                logger.warning(f"Temporary processing failure, will retry item later")
                break
    except (KeyError, ValueError, json.JSONDecodeError, NotImplementedError, HTTPError):
        log_exception(logger, f"While processing webhook on '{queue}'")
        logger.error("Permanent processing failure, putting item in quarantine")
        db.lmove(queue, "quarantine", "RIGHT", "LEFT")
        if Configuration.get_env() == "DEV":
            raise
    finally:
        logger.info(f"Processed {processed} webhook(s) on queue '{queue}'")
        if processed > 0:
            # update all the records touched by the processing
            count = update_all_records(verbose=False, force=False)
            logger.info(f"Updated {count} affected record(s)")
        if locking_queue.state() == "locked":
            try:
                locking_queue.unlock()
            except LockingQueue.NotLocked:
                # lock may have timed out during the operation, that's OK
                pass
    return target, processed


def process_webhook(queue: str, hook: dict, hook_id: str) -> bool:
    try:
        logger.info(f"Processing webhook '{hook_id}' from '{queue}'")
        with Postgres.get_global_engine().connect() as conn:  # type: Connection
            if queue == "airtable":
                airtable.process_webhook_notification(conn, hook["name"])
            elif queue == "act_blue":
                act_blue.process_webhook_notification(conn, hook)
            elif queue == "action_network":
                logger.error("Action Network webhooks are not implemented.")
                raise NotImplementedError
            elif queue == "control":
                logger.error("Control webhooks are not implemented.")
                raise NotImplementedError
            else:
                logger.error(f"Unknown queue: '{queue}'")
                raise NotImplementedError
            conn.commit()
            logger.info(f"Processing complete on webhook '{hook_id}'")
            return True
    except SQLAlchemyError:
        log_exception(logger, f"Database error on webhook '{hook_id}' on '{queue}'")
        return False
    except HTTPError as err:
        if err.response.status_code in [429, 500, 502, 503]:
            log_exception(logger, f"Network error on webhook '{hook_id}' on '{queue}'")
            delay = 30.0 + 10.0 * random()
            logger.info("Backing off {delay} seconds before retry")
            sleep(delay)
        else:
            raise
        return False
