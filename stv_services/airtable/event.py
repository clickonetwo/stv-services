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

import sqlalchemy as sa
from sqlalchemy.future import Connection

from .schema import fetch_and_validate_table_schema, FieldInfo
from .utils import (
    upsert_records,
    delete_records,
)
from ..mobilize.event import MobilizeEvent
from ..core import Configuration
from ..core.logging import get_logger
from ..core.utilities import airtable_timestamp
from ..data_store import model

logger = get_logger(__name__)
event_table_name = "Mobilize Events"
event_table_schema = {
    "uuid": FieldInfo("Mobilize Event ID*", "number", "event"),
    "sponsor_id": FieldInfo("Mobilize Partner ID*", "number", "event"),
    "title": FieldInfo("Mobilize Event Name*", "singleLineText", "event"),
    "partner_name": FieldInfo("Mobilize Partner Name*", "singleLineText", "event"),
    "shift_summary": FieldInfo("Shift Signup Summary*", "multilineText", "compute"),
    "contact": FieldInfo("Event Organizer*", "multipleRecordLinks", "compute"),
    "event_url": FieldInfo("Mobilize Event Link*", "url", "event"),
    "is_featured": FieldInfo("Featured on calendar?", "checkbox", "event"),
    "featured_name": FieldInfo("STV Event Name", "singleLineText", "readonly"),
    "featured_description": FieldInfo("Event Description", "multilineText", "readonly"),
    "featured_start_date": FieldInfo("Featured Start Date", "date", "readonly"),
    "featured_end_date": FieldInfo("Featured End Date", "date", "readonly"),
}


def verify_event_schema() -> dict:
    config = Configuration.get_global_config()
    base_name = config["airtable_stv_base_name"]
    access_info = fetch_and_validate_table_schema(
        base_name, event_table_name, event_table_schema
    )
    config["airtable_stv_event_schema"] = access_info
    return access_info


def create_event_record(conn: Connection, event: ActionNetworkDonation) -> dict:
    config = Configuration.get_global_config()
    table = model.person_info
    # find the matching donor record, if there is one
    query = sa.select(table).where(table.c.uuid == event["donor_id"])
    donor: dict = conn.execute(query).mappings().first()
    if not donor:
        raise KeyError("Donation '{event['uuid']}' has no donor")
    if not donor["contact_record_id"]:
        raise KeyError(f"Donor '{donor['uuid']}' is not a contact")
    attribution_record_id = None
    if attribution_id := event["attribution_id"]:
        # find the matching fundraising page record, if there is one
        query = sa.select(table).where(table.c.uuid == attribution_id)
        if attribution := conn.execute(query).mappings().first():
            attribution_record_id = attribution["funder_record_id"]
            if not attribution_record_id:
                logger.warning(f"Attributor {attribution['uuid']} is not a funder")
    column_ids = config["airtable_stv_event_schema"]["column_ids"]
    record = dict()
    for field_name, info in event_table_schema.items():
        if info.source == "event":
            # all fields should have values, but we are cautious
            if value := event.get(field_name):
                record[column_ids[field_name]] = value
    # created date must be an airtable date
    value = airtable_timestamp(event["created_date"])
    record[column_ids["created_date"]] = value
    # recurrence data requires parsing the recurrence object
    value = event["recurrence_data"].get("recurring", False)
    record[column_ids["recurrence_data"]] = value
    # this is a link to the Donor's record ID in the Contacts table
    record[column_ids["donor_id"]] = [donor["contact_record_id"]]
    # set the attribution, if any
    if attribution_record_id:
        record[column_ids["attribution_id"]] = attribution_record_id
        record[column_ids["override_attribution_id"]] = attribution_record_id
    return record


def upsert_events(conn: Connection, events: list[ActionNetworkDonation]) -> (int, int):
    pairs = [(event, create_event_record(conn, event)) for event in events]
    return upsert_records(conn, "event", pairs)


def delete_events(conn: Connection, events: list[ActionNetworkDonation]) -> int:
    return delete_records(conn, "event", events)
