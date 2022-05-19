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

from ..action_network.donation import ActionNetworkDonation
from ..action_network.person import ActionNetworkPerson
from ..action_network.submission import ActionNetworkSubmission
from ..action_network.utils import validate_hash
from ..core.logging import get_logger
from ..data_store import RedisSync

logger = get_logger(__name__)
db = RedisSync.connect()


def process_webhook_notification(conn: Connection, body: dict):
    for key, val in body.items():
        if key == "osdi:donation":
            process_donation_webhook(conn, val)
        elif key == "osdi:submission":
            process_submission_webhook(conn, val)
        else:
            save_webhook("action_network:other", {key: val})
            raise NotImplementedError("Don't know how to handle")


def process_donation_webhook(conn: Connection, body: dict):
    save_webhook("action_network:donations", {"osdi:donation": body})
    try:
        uuid, created_date, modified_date = validate_hash(body)
        donation = ActionNetworkDonation.from_lookup(conn, uuid)
        # we really shouldn't be getting webhook updates to donations...
        if (amount := body.get("amount")) and amount != donation["amount"]:
            # this is likely a return updating the existing amount
            logger.warning(f"Received amount change to existing donation '{uuid}'")
            donation.notice_amount_change(conn, amount)
        else:
            logger.warning(f"Received unknown update to existing donation '{uuid}'")
        donation.update(dict(modified_date=modified_date))
    except KeyError:
        # no existing one, so build a new one
        donation = ActionNetworkDonation.from_webhook(body)
    # make sure the donation exists in the database
    donation.compute_status(conn)
    donation.persist(conn)
    # now make sure we have the person in the database
    person_id = donation["donor_id"]
    try:
        person = ActionNetworkPerson.from_lookup(conn, uuid=person_id)
        if person_data := body.get("person"):
            person_data["identifiers"] = [person_id]
            person.notice_webhook(person_data)
    except KeyError:
        person = ActionNetworkPerson.from_action_network(conn, person_id)
    # now update the person status given the donation
    person.compute_status(conn)
    person.persist(conn)


def process_submission_webhook(conn: Connection, body: dict):
    save_webhook("action_network:submissions", {"osdi:submission": body})
    try:
        uuid, created_date, modified_date = validate_hash(body)
        submission = ActionNetworkSubmission.from_lookup(conn, uuid)
        # we really shouldn't be getting webhook updates to submissions...
        logger.warning(f"Received unknown update to existing submission '{uuid}'")
        submission.update(dict(modified_date=modified_date))
    except KeyError:
        # no existing one, so build a new one
        submission = ActionNetworkSubmission.from_webhook(body)
    # make sure the donation exists in the database
    submission.persist(conn)
    # now make sure we have the person in the database
    person_id = submission["person_id"]
    try:
        person = ActionNetworkPerson.from_lookup(conn, uuid=person_id)
        if person_data := body.get("person"):
            person_data["identifiers"] = [person_id]
            person.notice_webhook(person_data)
    except KeyError:
        person = ActionNetworkPerson.from_action_network(conn, person_id)
    # now update the person status given the donation
    person.compute_status(conn)
    person.persist(conn)


def save_webhook(list_name: str, body: dict):
    compact = json.dumps(body, separators=(",", ":"))
    length: int = db.lpush(list_name, compact)
    logger.info(f"Saved webhook as #{length} in '{list_name}' list")
