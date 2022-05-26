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
import json

from sqlalchemy.future import Connection

from stv_services.core.logging import get_logger
from stv_services.data_store import RedisSync

logger = get_logger(__name__)


def process_webhook_notification(_conn: Connection, body: dict):
    for key, val in body.items():
        if key == "resubmit-failed":
            resubmit_failed(val)
        else:
            raise NotImplementedError(f"Don't know how to '{key}'")


def resubmit_all_failed(queues: list = None):
    if queues is None:
        queues = ("act_blue", "action_network", "airtable")
    db = RedisSync.connect()
    requests = []
    for queue in queues:
        requests.append(dict(queue=queue))
    hook = {"resubmit-failed": requests}
    db.lpush("control", json.dumps(hook, separators=(",", ":")))
    db.publish("webhooks", "control")


def resubmit_failed(requests: list[dict]):
    db = RedisSync.connect()
    for request in requests:
        queue = request["queue"]
        failed = f"{queue}:process-failure"
        items: list = request.get("items")
        if not items:
            item_map = db.hgetall(failed)
        else:
            item_map = {}
            for item in items:
                if hook := db.hget(failed, item):
                    item_map[item] = hook
                else:
                    logger.warning(f"No item '{item}' on '{failed}'")
        for key, hook in item_map.items():
            db.lpush(queue, hook)
            db.hdel(failed, key)
        if item_map:
            logger.info(f"Resubmitted {len(item_map)} items to '{queue}'")
            db.publish("webhooks", queue)