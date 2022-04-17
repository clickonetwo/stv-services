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
from datetime import datetime, timezone
from typing import Dict

import sqlalchemy as sa
from pyairtable import Table
from sqlalchemy.engine import Connection

from .schema import fetch_and_validate_table_schema, FieldInfo, fetch_airtable_base_id
from .utils import (
    insert_airtable_records,
    update_airtable_records,
    delete_airtable_records,
)
from ..action_network.person import ActionNetworkPerson
from ..core import Configuration
from ..data_store import Database, model

contact_table_name = "Contacts"
contact_table_schema = {
    "email": FieldInfo("Email*", "email", "person"),
    "given_name": FieldInfo("First Name*", "singleLineText", "person"),
    "family_name": FieldInfo("Last Name*", "singleLineText", "person"),
    "phone": FieldInfo("Phone*", "phoneNumber", "person"),
    "street_address": FieldInfo("Address*", "multilineText", "person"),
    "locality": FieldInfo("City*", "singleLineText", "person"),
    "region": FieldInfo("State*", "singleLineText", "person"),
    "postal_code": FieldInfo("Zip code*", "singleLineText", "person"),
    "total_2020": FieldInfo("2020 Total Donations*", "currency", "person"),
    "summary_2020": FieldInfo("2020 Donations Summary*", "multilineText", "person"),
    "total_2021": FieldInfo("2021 Total Donations*", "currency", "person"),
    "summary_2021": FieldInfo("2021 Donations Summary*", "multilineText", "person"),
    "is_funder": FieldInfo("In Fundraising table?", "checkbox", "person"),
    "custom_fields": FieldInfo("2022 Signup Interests*", "multipleSelects", "compute"),
}
contact_custom_field_map = {
    "2022_calls": "Make calls",
    "2022_doors": "Knock on doors",
    "2022_fundraise": "Fundraise",
    "2022_recruit": "Recruit friends",
    "2022_podlead": "Start a Pod",
    "2022_branchlead": "Lead a Branch",
}


def verify_contact_schema() -> dict:
    config = Configuration.get_global_config()
    base_name = config.get("airtable_stv_base_name")
    access_info = fetch_and_validate_table_schema(
        base_name, contact_table_name, contact_table_schema
    )
    config["airtable_stv_contact_schema"] = access_info
    return access_info


def create_contact_record(person: ActionNetworkPerson) -> dict:
    config = Configuration.get_global_config()
    column_ids = config["airtable_stv_contact_schema"]["column_ids"]
    record = dict()
    for field_name, info in contact_table_schema.items():
        if info.source == "person":
            # not all fields have values, so only assign if there is one
            if value := person.get(field_name):
                record[column_ids[field_name]] = value
    interests = []
    for field_name, selection_text in contact_custom_field_map.items():
        if person["custom_fields"].get(field_name):
            interests.append(selection_text)
    record[column_ids["custom_fields"]] = interests
    return record


def insert_contacts(conn: Connection, people: list[ActionNetworkPerson]) -> int:
    if not people:
        return 0
    schema = Configuration.get_global_config()["airtable_stv_contact_schema"]
    records = list(map(create_contact_record, people))
    record_ids = insert_airtable_records(schema, records)
    for record_id, person in zip(record_ids, people):
        person["contact_record_id"] = record_id
        person["contact_last_updated"] = person["modified_date"]
        person.persist(conn)
    return len(people)


def update_contacts(conn: Connection, people: list[ActionNetworkPerson]) -> int:
    if not people:
        return 0
    schema = Configuration.get_global_config()["airtable_stv_contact_schema"]
    updates = []
    for person in people:
        record_id = person["contact_record_id"]
        record = create_contact_record(person)
        updates.append({"id": record_id, "fields": record})
    update_airtable_records(schema, updates)
    for person in people:
        person["contact_last_updated"] = person["modified_date"]
        person.persist(conn)
    return len(people)


def upsert_contacts(conn: Connection, people: list[ActionNetworkPerson]) -> (int, int):
    inserts, updates = [], []
    for person in people:
        if person.get("contact_record_id"):
            updates.append(person)
        else:
            inserts.append(person)
    i_count = insert_contacts(conn, inserts)
    u_count = update_contacts(conn, updates)
    return i_count, u_count


def delete_contacts(conn: Connection, people: list[ActionNetworkPerson]):
    schema = Configuration.get_global_config()["airtable_stv_contact_schema"]
    deletes = []
    for person in people:
        if record_id := person.get("contact_record_id"):
            deletes.append(record_id)
            person["contact_record_id"] = ""
            person["contact_last_updated"] = model.epoch
    delete_airtable_records(schema, deletes)
    return len(deletes)
