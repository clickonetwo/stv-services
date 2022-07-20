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
from typing import TypeVar, Type, Optional

import requests
from restnavigator.exc import HALNavigatorError
from sqlalchemy.future import Connection

from ..action_network.bulk import import_all, compute_status_all
from ..action_network.donation import ActionNetworkDonation
from ..action_network.person import ActionNetworkPerson
from ..action_network.submission import ActionNetworkSubmission
from ..action_network.utils import validate_hash, ActionNetworkObject
from ..core.logging import get_logger

logger = get_logger(__name__)


def process_webhook_notification(conn: Connection, body: dict):
    for key, val in body.items():
        if key == "osdi:donation":
            process_donation_webhook(conn, val)
        elif key == "osdi:submission":
            process_submission_webhook(conn, val)
        elif key == "action_network:upload":
            process_upload_webhook(conn, val)
        else:
            raise NotImplementedError(f"Don't know how to handle webhook '{key}'")


def process_donation_webhook(conn: Connection, body: dict):
    logger.info(f"Processing incoming donation webhook")
    donation = ensure_new_object(conn, body, ActionNetworkDonation)
    if not donation:
        # we've already processed this webhook
        return
    # first make sure the donation exists in the database
    donation.compute_status(conn)
    donation.persist(conn)
    # next make sure we have the person in the database
    donor = process_webhook_person_data(conn, donation["donor_id"], body)
    # now annotate the person with the donation
    donor.notice_donation(conn, donation)
    donor.persist(conn)
    logger.info(f"Donation webhook processing done")


def process_submission_webhook(conn: Connection, body: dict):
    logger.info(f"Processing incoming form submission webhook")
    submission = ensure_new_object(conn, body, ActionNetworkSubmission)
    if not submission:
        # we've already processed this webhook
        return
    # make sure the submission exists in the database
    submission.persist(conn)
    # make sure we have the person in the database
    submitter = process_webhook_person_data(conn, submission["person_id"], body)
    # make sure the person knows of the submission, even if it's old
    submitter.notice_submission(conn, submission)
    submitter.persist(conn)
    logger.info(f"Form submission webhook processing done")


def process_upload_webhook(conn: Connection, body: dict):
    logger.info(f"Processing incoming upload webhook")
    link = body.get("_links", {}).get("osdi:person", {}).get("href", "")
    if link and (start := link.rfind("/")):
        uuid = "action_network:" + link[start + 1 :]
        person = ActionNetworkPerson.from_action_network(uuid)
        # completely update status, since the fetch may have changed a lot
        person.compute_status(conn, True)
        person.persist(conn)
        logger.info("Upload webhook processing done")
    else:
        logger.warning("No valid person link found in upload webhook body")


def process_webhook_person_data(conn: Connection, person_id: str, body: dict):
    """Ensure the person in a webhook is in the database and is updated with any
    new information from the webhook body."""
    try:
        person = ActionNetworkPerson.from_lookup(conn, uuid=person_id)
        if person_data := body.get("person"):
            person_data["identifiers"] = [person_id]
            _, _, modified_date = validate_hash(person_data)
            if modified_date > person["modified_date"]:
                person.update_from_hash(person_data)
    except KeyError:
        person = ActionNetworkPerson.from_action_network(person_id)
    # update the person status based on new info
    person.compute_status(conn)
    return person


def import_and_update_all(verbose: bool = True, force: bool = False):
    """Get all new/updated records from Action Network"""
    try:
        import_all(verbose, force)
    except (requests.HTTPError, HALNavigatorError):
        logger.info("Import failed, so computing status for what succeeded")
    compute_status_all(verbose, force)


T = TypeVar("T", bound=ActionNetworkObject, covariant=True)


def ensure_new_object(conn: Connection, body: dict, cls: Type[T]) -> Optional[T]:
    try:
        uuid, _, modified_date = validate_hash(body)
        obj = cls.from_lookup(conn, uuid=uuid)
        if modified_date <= obj["modified_date"]:
            # we've already got this object
            return None
        obj.update_from_hash(body)
    except KeyError:
        # no existing one, so build a new one
        obj = cls.from_webhook(body)
    return obj
