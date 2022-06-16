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
from sqlalchemy.sql.expression import func

from .assignment import insert_needed_assignments
from .schema import fetch_and_validate_table_schema, FieldInfo
from .utils import (
    upsert_records,
    delete_records,
)
from .webhook import register_hook
from ..action_network.person import ActionNetworkPerson
from ..core import Configuration
from ..data_store import model

contact_table_name = "Contacts"
contact_table_schema = {
    "email": FieldInfo("Email*", "email", "person"),
    "given_name": FieldInfo("First Name*", "singleLineText", "person"),
    "family_name": FieldInfo("Last Name*", "singleLineText", "person"),
    "phone": FieldInfo("Phone*", "phoneNumber", "person"),
    "street_address": FieldInfo("Address*", "multilineText", "person"),
    "locality": FieldInfo("City*", "singleLineText", "person"),
    "region": FieldInfo("State*", "singleLineText", "person"),
    "postal_code": FieldInfo("Zip Code*", "singleLineText", "person"),
    "has_submission": FieldInfo("Filled Out STV Form?*", "checkbox", "person"),
    "total_2020": FieldInfo("2020 Total Donations*", "currency", "person"),
    "summary_2020": FieldInfo("2020 Donations Summary*", "multilineText", "person"),
    "total_2021": FieldInfo("2021 Total Donations*", "currency", "person"),
    "summary_2021": FieldInfo("2021 Donations Summary*", "multilineText", "person"),
    "is_funder": FieldInfo("In Fundraising Table?", "checkbox", "person"),
    "signup_interests": FieldInfo(
        "2022 Signup Interests*", "multipleSelects", "compute"
    ),
    "signup_notes": FieldInfo("2022 Signup Notes*", "multilineText", "compute"),
    "fundraise_interests": FieldInfo(
        "2022 Fundraising Interests*", "multipleSelects", "compute"
    ),
    "fundraise_notes": FieldInfo("2022 Fundraising Notes*", "multilineText", "compute"),
    "team_lead": FieldInfo("Pod Leader", "multipleRecordLinks", "observe"),
    "team": FieldInfo("Pod Members*", "multipleRecordLinks", "compute"),
    "events": FieldInfo("Events Signed Up For*", "multipleRecordLinks", "compute"),
    "event_contacts": FieldInfo("Connections*", "multipleRecordLinks", "compute"),
    "pb_shifts": FieldInfo("Total Phone Bank Shift Signups*", "number", "compute"),
    "tb_shifts": FieldInfo("Total Text Banking Shift Signups*", "number", "compute"),
    "dk_shifts": FieldInfo("Total Door Knocking Shift Signups*", "number", "compute"),
}
signup_interest_map = {
    "2022_calls": "Phone Bank",
    "2022_doors": "Door Knock",
    "2022_fundraise": "Fundraise",
    "2022_recruit": "Recruit friends",
    "2022_podlead": "Start a Pod",
    "2022_branchlead": "Lead a Branch",
    "branch_lead_interest_I want to help build a branch in my region!": "Lead a Branch",
}
fundraise_interest_map = {
    "2022_happyhour": "Host a Happy Hour",
    "2022_fundraisepage": "Create a fundraising page",
    "2022_donate": "Donate",
    "2022_fundraiseidea": "Other Idea",
}


def verify_contact_schema() -> dict:
    config = Configuration.get_global_config()
    base_name = config["airtable_stv_base_name"]
    access_info = fetch_and_validate_table_schema(
        base_name, contact_table_name, contact_table_schema
    )
    config["airtable_stv_contact_schema"] = access_info
    return access_info


def create_contact_record(conn: Connection, person: ActionNetworkPerson) -> dict:
    config = Configuration.get_global_config()
    column_ids = config["airtable_stv_contact_schema"]["column_ids"]
    record = dict()
    for field_name, info in contact_table_schema.items():
        if info.source == "person":
            # not all fields have values, so only assign if there is one
            if value := person.get(field_name):
                record[column_ids[field_name]] = value
    custom_fields = person["custom_fields"]
    signup_interests, fundraise_interests = set(), set()
    for name in custom_fields:
        if interest := signup_interest_map.get(name):
            signup_interests.add(interest)
        if interest := fundraise_interest_map.get(name):
            fundraise_interests.add(interest)
    record[column_ids["signup_interests"]] = list(signup_interests)
    record[column_ids["signup_notes"]] = custom_fields.get("2022_notes", "")
    record[column_ids["fundraise_interests"]] = list(fundraise_interests)
    record[column_ids["fundraise_notes"]] = custom_fields.get("2022_fundraiseidea", "")
    query = sa.select(model.person_info.c.contact_record_id).where(
        sa.and_(
            model.person_info.c.team_lead == person["uuid"],
            model.person_info.c.contact_record_id != "",
        )
    )
    rows: list[dict] = conn.execute(query).mappings().all()
    if rows:
        record[column_ids["team"]] = [r["contact_record_id"] for r in rows]
    else:
        record[column_ids["team"]] = []
    for key, value in gather_events_contacts_shifts(conn, person).items():
        record[column_ids[key]] = value
    return record


def upsert_contacts(conn: Connection, people: list[ActionNetworkPerson]) -> (int, int):
    pairs = [(person, create_contact_record(conn, person)) for person in people]
    (inserted, updated) = upsert_records(conn, "contact", pairs)
    # now insert any needed assignments for these people
    insert_needed_assignments(conn, people)
    return inserted, updated


def delete_contacts(conn: Connection, people: list[ActionNetworkPerson]) -> int:
    return delete_records(conn, "contact", people)


def register_contact_hook():
    schema = verify_contact_schema()
    base_id = schema["base_id"]
    table_id = schema["table_id"]
    column_ids = schema["column_ids"]
    field_ids = [column_ids[name] for name in ["is_funder", "team_lead"]]
    register_hook("contact", base_id, table_id, field_ids)


def gather_events_contacts_shifts(
    conn: Connection, person: ActionNetworkPerson
) -> dict:
    cols = model.attendance_info.columns
    query = (
        sa.select(
            cols.event_id, cols.event_type, func.count(cols.timeslot_id).label("count")
        )
        .where(cols.person_id == person["uuid"])
        .group_by(cols.event_id, cols.event_type)
    )
    event_ids = []
    pb_shifts, tb_shifts, dk_shifts = 0, 0, 0
    for row in conn.execute(query):
        event_ids.append(row.event_id)
        if row.event_type == "PHONE_BANK":
            pb_shifts += row.count
        elif row.event_type == "CANVASS":
            dk_shifts += row.count
        elif row.event_type == "TEXT_BANK":
            tb_shifts += row.count
    cols = model.event_info.columns
    query = sa.select(cols.event_record_id, cols.contact_id).where(
        cols.uuid.in_(event_ids)
    )
    events, contacts = [], []
    for row in conn.execute(query):
        if row.event_record_id != "":
            events.append(row.event_record_id)
        if row.contact_id != "" and row.contact_id != "pending":
            contacts.append(row.contact_id)
    result = {
        "events": events,
        "event_contacts": contacts,
        "pb_shifts": pb_shifts,
        "tb_shifts": tb_shifts,
        "dk_shifts": dk_shifts,
    }
    return result
