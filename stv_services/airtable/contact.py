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

from sqlalchemy.future import Connection

from .schema import fetch_and_validate_table_schema, FieldInfo
from .utils import (
    insert_records,
    update_records,
    upsert_records,
    delete_records,
)
from ..action_network.person import ActionNetworkPerson
from ..core import Configuration

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
    "total_2020": FieldInfo("2020 Total Donations*", "currency", "person"),
    "summary_2020": FieldInfo("2020 Donations Summary*", "multilineText", "person"),
    "total_2021": FieldInfo("2021 Total Donations*", "currency", "person"),
    "summary_2021": FieldInfo("2021 Donations Summary*", "multilineText", "person"),
    "is_funder": FieldInfo("In Fundraising Table?", "checkbox", "person"),
    "custom1": FieldInfo("2022 Signup Interests*", "multipleSelects", "compute"),
    "custom2": FieldInfo("2022 Signup Notes*", "multilineText", "compute"),
}
custom1_field_map = {
    "2022_calls": "Phone Bank",
    "2022_doors": "Door Knock",
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


def create_contact_record(_: Connection, person: ActionNetworkPerson) -> dict:
    config = Configuration.get_global_config()
    column_ids = config["airtable_stv_contact_schema"]["column_ids"]
    record = dict()
    for field_name, info in contact_table_schema.items():
        if info.source == "person":
            # not all fields have values, so only assign if there is one
            if value := person.get(field_name):
                record[column_ids[field_name]] = value
    custom_fields = person["custom_fields"]
    interests = []
    for field_name, selection_text in custom1_field_map.items():
        if custom_fields.get(field_name):
            interests.append(selection_text)
    record[column_ids["custom1"]] = interests
    record[column_ids["custom2"]] = custom_fields.get("2022_notes", "")
    return record


def insert_contacts(conn: Connection, people: list[ActionNetworkPerson]) -> int:
    pairs = [(person, create_contact_record(conn, person)) for person in people]
    return insert_records(conn, "contact", pairs)


def update_contacts(conn: Connection, people: list[ActionNetworkPerson]) -> int:
    pairs = [(person, create_contact_record(conn, person)) for person in people]
    return update_records(conn, "contact", pairs)


def upsert_contacts(conn: Connection, people: list[ActionNetworkPerson]) -> (int, int):
    pairs = [(person, create_contact_record(conn, person)) for person in people]
    return upsert_records(conn, "contact", pairs)


def delete_contacts(conn: Connection, people: list[ActionNetworkPerson]) -> int:
    return delete_records(conn, "contact", people)


# def sync_contacts(conn: Connection, people: list[ActionNetworkPerson]) -> (dict, dict):
#     emails = {person.get("email"): person for person in people}
#     extras = {}
#     unmatched = {}
#     empties = []
#
#     def page_processor(page: list[dict]):
#         for record in page:
#             record_id = record["id"]
#             record_email = record["fields"].get("email")
#             if not record_email:
#                 # records with no email shouldn't exist in Airtable
#                 empties.append(record_id)
#             elif match := emails.get(record_email):
#                 # we have an existing person record for this email
#                 contact_record_id = match.get("contact_record_id")
#                 if contact_record_id is None:
#                     # adopt this as the record for this email
#                     match["contact_record_id"] = record_id
#                     match["contact_last_updated"] = model.epoch
#                 elif contact_record_id != record_id:
#                     # remember this as an extra email
#                     extras[record_email] = record_id
#             else:
#                 # this is an extra record because we don't have a person with it
#                 unmatched[record_email] = record_id
#
#     process_airtable_records()
