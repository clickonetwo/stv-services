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
from datetime import datetime
from zoneinfo import ZoneInfo

import sqlalchemy as sa

from sqlalchemy.future import Connection

from .schema import fetch_and_validate_table_schema, FieldInfo
from .utils import (
    upsert_records,
    delete_records,
)
from .webhook import register_hook
from ..action_network.person import ActionNetworkPerson
from ..core import Configuration
from ..core.logging import get_logger
from ..data_store import model
from ..mobilize.event import MobilizeEvent

logger = get_logger(__name__)
event_table_name = "Mobilize Events"
event_table_schema = {
    "uuid": FieldInfo("Mobilize Event ID*", "number", "event"),
    "sponsor_id": FieldInfo("Mobilize Partner ID*", "number", "event"),
    "title": FieldInfo("Mobilize Event Name*", "singleLineText", "event"),
    "event_type": FieldInfo("Mobilize Event Type*", "singleSelect", "event"),
    "partner_name": FieldInfo("Mobilize Partner Name*", "singleLineText", "event"),
    "is_coordinated": FieldInfo("Coordinated?*", "checkbox", "event"),
    "shift_summary": FieldInfo("Shift Signup Summary*", "multilineText", "compute"),
    "contact": FieldInfo("Event Organizer*", "multipleRecordLinks", "compute"),
    "event_url": FieldInfo("Mobilize Event Link*", "url", "event"),
    "first_slot": FieldInfo("Earliest Shift Date*", "date", "compute"),
    "last_slot": FieldInfo("Latest Shift Date*", "date", "compute"),
    "is_featured": FieldInfo("Featured on calendar?", "checkbox", "event"),
    "featured_name": FieldInfo("STV Event Name", "singleLineText", "observe"),
    "featured_description": FieldInfo("Event Description", "multilineText", "observe"),
    "feature_start": FieldInfo("Featured Start Date", "date", "observe"),
    "feature_end": FieldInfo("Featured End Date", "date", "observe"),
}


def verify_event_schema() -> dict:
    config = Configuration.get_global_config()
    base_name = config["airtable_stv_base_name"]
    access_info = fetch_and_validate_table_schema(
        base_name, event_table_name, event_table_schema
    )
    config["airtable_stv_event_schema"] = access_info
    return access_info


def create_event_record(conn: Connection, event: MobilizeEvent) -> dict:
    config = Configuration.get_global_config()
    column_ids = config["airtable_stv_event_schema"]["column_ids"]
    record = dict()
    for field_name, info in event_table_schema.items():
        if info.source == "event":
            # all fields should have values, but we are cautious
            if (value := event.get(field_name)) is not None:
                record[column_ids[field_name]] = value
    # compute the shift summary
    record[column_ids["shift_summary"]] = event.create_shift_summary(conn)
    # insert the event organizer, if there is one
    contact_id = event.get("contact_id", "")
    if contact_id == "pending":
        # handling of pending contacts is subtle :).  The contact being
        # pending means that there is a person record for the event organizer,
        # but there was not yet a contact record for the event organizer.  So
        # before we skip filling this field we check to see if the contact
        # record for this person has appeared and, if so, we both notify the
        # person of the event they organized and we put them in this event.
        person = ActionNetworkPerson.from_lookup(conn, email=event["contact_email"])
        contact_id = person["contact_record_id"]
        if contact_id:
            event["contact_id"] = contact_id
            person.notice_event(conn, event)
            person.persist(conn)
    if contact_id:
        record[column_ids["contact"]] = [contact_id]
    # now compute the first and last timeslot dates
    query = (
        sa.select(model.timeslot_info)
        .where(model.timeslot_info.c.event_id == event["uuid"])
        .order_by(model.timeslot_info.c.start_date)
    )
    rows = conn.execute(query).all()
    if rows:
        earliest_utc: datetime = rows[0].start_date
        earliest_pst = earliest_utc.astimezone(tz=ZoneInfo("America/Los_Angeles"))
        latest_utc: datetime = rows[-1].start_date
        latest_pst = latest_utc.astimezone(tz=ZoneInfo("America/Los_Angeles"))
        record[column_ids["first_slot"]] = earliest_pst.date().isoformat()
        record[column_ids["last_slot"]] = latest_pst.date().isoformat()
    return record


def upsert_events(conn: Connection, events: list[MobilizeEvent]) -> (int, int):
    pairs = [(event, create_event_record(conn, event)) for event in events]
    return upsert_records(conn, "event", pairs)


def delete_events(conn: Connection, events: list[MobilizeEvent]) -> int:
    return delete_records(conn, "event", events)


def register_event_hook():
    schema = verify_event_schema()
    base_id = schema["base_id"]
    table_id = schema["table_id"]
    column_ids = schema["column_ids"]
    field_ids = [
        column_ids[name]
        for name in [
            "is_featured",
            "featured_name",
            "featured_description",
            "feature_start",
            "feature_end",
        ]
    ]
    register_hook("event", base_id, table_id, field_ids)
