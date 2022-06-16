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

import sqlalchemy as sa
from sqlalchemy.future import Connection

from stv_services.action_network.person import ActionNetworkPerson
from stv_services.airtable import bulk
from stv_services.airtable.sync import verify_match
from stv_services.core.logging import get_logger
from stv_services.data_store import RedisSync, model, Postgres
from stv_services.worker import mobilize
from stv_services.worker.airtable import update_airtable_records

logger = get_logger(__name__)


def process_webhook_notification(_conn: Connection, body: dict):
    for key, val in body.items():
        if key == "resubmit-failed":
            resubmit_failed_requests(val)
        elif key == "match-and-repair":
            match_and_repair(val)
        elif key == "external-data-change":
            external_data_change(val)
        elif key == "update-from-mobilize":
            update_from_mobilize(val)
        else:
            raise NotImplementedError(f"Don't know how to '{key}'")


def resubmit_successful_request(queue: str, request_id: str):
    db = RedisSync.connect()
    requests = db.lrange(f"{queue}:success", 0, -1)
    for request in requests:  # type: bytes
        hook = json.loads(request)
        for r_id, r_body in hook.items():
            if r_id == request_id:
                logger.info(f"Resubmitting {queue} request id {r_id}")
                db.lpush(queue, json.dumps(r_body, separators=(",", ":")))
                return
    logger.warning(f"Can't find request '{request_id}' on queue '{queue}' to resubmit")


def resubmit_all_failed_requests(queues: list = None):
    if queues is None:
        queues = ("act_blue", "action_network", "airtable")
    db = RedisSync.connect()
    requests = []
    for queue in queues:
        requests.append(dict(queue=queue))
    hook = {"resubmit-failed": requests}
    db.lpush("control", json.dumps(hook, separators=(",", ":")))
    db.publish("webhooks", "control")


def resubmit_failed_requests(requests: list[dict]):
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


def match_and_repair(params: dict):
    types = params.get("types")
    remove_extra = params.get("repair", False)
    verify_match(types=types, remove_extra=remove_extra)
    update_airtable_records(is_retry=True)


def submit_match_request(types: list, do_repair: bool):
    db = RedisSync.connect()
    request = {"match-and-repair": {"types": types, "repair": do_repair}}
    db.lpush("control", json.dumps(request, separators=(",", ":")))


def notice_external_data_change(email_file: str):
    emails = []
    with open(email_file, encoding="utf-8") as in_file:
        while email := in_file.readline():
            if email := email.strip():
                emails.append(email)
    if emails:
        db = RedisSync.connect()
        hook = {"external-data-change": emails}
        db.lpush("control", json.dumps(hook, separators=(",", ":")))
        db.publish("webhooks", "control")


def external_data_change(emails: list[str]):
    query = sa.select(model.person_info).where(model.person_info.c.email.in_(emails))
    with Postgres.get_global_engine().connect() as conn:  # type: Connection
        people = ActionNetworkPerson.from_query(conn, query)
        logger.info(f"Noticing external data change on {len(people)} people")
        for person in people:
            person.notice_external_data()
            person.persist(conn)
        conn.commit()


def update_from_mobilize(params: dict):
    verbose = params.get("verbose", True)
    force = params.get("force", False)
    logger.info(f"Update from mobilize (verbose={verbose}, force={force}) starting")
    execute_update_request("mobilize", verbose, force)
    logger.info(f"Update from mobilize complete")


def submit_update_request(source: str, verbose: bool = True, force: bool = False):
    db = RedisSync.connect()
    if source.lower() == "mobilize":
        hook = {"update-from-mobilize": dict(verbose=verbose, force=force)}
    else:
        raise ValueError(f"Unknown source for update: '{source}'")
    db.lpush("control", json.dumps(hook, separators=(",", ":")))
    db.publish("webhooks", "control")


def execute_update_request(source: str, verbose: bool = True, force: bool = False):
    if source.lower() == "mobilize":
        mobilize.import_and_update_all(verbose, force)
        bulk.update_contact_records(verbose, force)
        bulk.update_event_records(verbose, force)
    else:
        raise ValueError(f"Unknown source for update: '{source}'")
