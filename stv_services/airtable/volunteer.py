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
from .webhook import register_hook
from ..action_network.person import ActionNetworkPerson
from ..core import Configuration
from ..data_store import model

volunteer_table_name = "Historical Volunteers"
volunteer_table_schema = {
    "given_name": FieldInfo("First Name*", "singleLineText", "person"),
    "family_name": FieldInfo("Last Name*", "singleLineText", "person"),
    "email": FieldInfo("Email*", "email", "person"),
    "phone": FieldInfo("Phone*", "phoneNumber", "person"),
    "street_address": FieldInfo("Address*", "multilineText", "person"),
    "locality": FieldInfo("City*", "singleLineText", "person"),
    "region": FieldInfo("State*", "singleLineText", "person"),
    "postal_code": FieldInfo("Zip Code*", "singleLineText", "person"),
    "is_contact": FieldInfo("Moved to 2022?", "checkbox", "person"),
    "shifts_2020": FieldInfo("Participated Shift Count 2020*", "number", "external"),
    "events_2020": FieldInfo("Organized Event Count 2020*", "number", "external"),
    "total_2020": FieldInfo("2020 Total Donations*", "currency", "person"),
    "summary_2020": FieldInfo("2020 Donations Summary*", "multilineText", "person"),
    "total_2021": FieldInfo("2021 Total Donations*", "currency", "person"),
    "summary_2021": FieldInfo("2021 Donations Summary*", "multilineText", "person"),
    "connect_2020": FieldInfo("Connected Org*", "multipleSelects", "compute"),
    "assigns_2020": FieldInfo("Assignments 2020*", "multipleSelects", "compute"),
    "notes_2020": FieldInfo("Notes*", "multilineText", "external"),
    "history_2020": FieldInfo("Contact History*", "multilineText", "external"),
    "fundraise_2020": FieldInfo("Fundraise*", "checkbox", "external"),
    "doorknock_2020": FieldInfo("Door Knocking*", "checkbox", "external"),
    "phonebank_2020": FieldInfo("Phone Banking*", "checkbox", "external"),
    "recruit_2020": FieldInfo("Recruit*", "checkbox", "external"),
    "delegate_ga_2020": FieldInfo("GA Delegation*", "checkbox", "external"),
    "delegate_pa_2020": FieldInfo("PA Delegation*", "checkbox", "external"),
    "delegate_az_2020": FieldInfo("AZ Delegation*", "checkbox", "external"),
    "delegate_fl_2020": FieldInfo("FL Delegation*", "checkbox", "external"),
}


def verify_volunteer_schema() -> dict:
    config = Configuration.get_global_config()
    base_name = config.get("airtable_stv_base_name")
    access_info = fetch_and_validate_table_schema(
        base_name, volunteer_table_name, volunteer_table_schema
    )
    config["airtable_stv_volunteer_schema"] = access_info
    return access_info


def create_volunteer_record(conn: Connection, person: ActionNetworkPerson) -> dict:
    config = Configuration.get_global_config()
    # find the matching external record, if there is one
    query = sa.select(model.external_info).where(
        model.external_info.c.email == person["email"]
    )
    match = conn.execute(query).mappings().first()
    column_ids = config["airtable_stv_volunteer_schema"]["column_ids"]
    record = dict()
    for field_name, info in volunteer_table_schema.items():
        if info.source == "person":
            # not all fields have values, so only assign if there is one
            if value := person.get(field_name):
                record[column_ids[field_name]] = value
        elif info.source == "external":
            if match and (value := match.get(field_name)):
                record[column_ids[field_name]] = value
        elif field_name in ["connect_2020", "assigns_2020"]:
            if match and (value := match.get(field_name)):
                # convert comma-separated text to multi-select array
                values = value.split(",")
                record[column_ids[field_name]] = [v.strip() for v in values]
    return record


def upsert_volunteers(
    conn: Connection, people: list[ActionNetworkPerson]
) -> (int, int):
    pairs = [(person, create_volunteer_record(conn, person)) for person in people]
    return upsert_records(conn, "volunteer", pairs)


def delete_volunteers(conn: Connection, people: list[ActionNetworkPerson]) -> int:
    return delete_records(conn, "volunteer", people)


def register_volunteer_hook():
    schema = verify_volunteer_schema()
    base_id = schema["base_id"]
    table_id = schema["table_id"]
    column_ids = schema["column_ids"]
    field_ids = [column_ids[name] for name in ["is_contact"]]
    register_hook("volunteer", base_id, table_id, field_ids)
