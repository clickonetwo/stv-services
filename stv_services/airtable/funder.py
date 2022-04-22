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

funder_table_name = "Fundraising"
funder_table_schema = {
    "contact": FieldInfo("Name (from Contacts)*", "multipleRecordLinks", "compute"),
}


def verify_funder_schema() -> dict:
    config = Configuration.get_global_config()
    base_name = config.get("airtable_stv_base_name")
    access_info = fetch_and_validate_table_schema(
        base_name, funder_table_name, funder_table_schema
    )
    config["airtable_stv_funder_schema"] = access_info
    return access_info


def create_funder_record(person: ActionNetworkPerson) -> dict:
    config = Configuration.get_global_config()
    column_ids = config["airtable_stv_funder_schema"]["column_ids"]
    if record_id := person.get("contact_record_id"):
        record = {column_ids["contact"]: [record_id]}
    else:
        raise ValueError(f"Person '{person['uuid']}' must be a contact to fundraise")
    return record


def insert_funders(conn: Connection, people: list[ActionNetworkPerson]) -> int:
    pairs = [(person, create_funder_record(person)) for person in people]
    return insert_records(conn, "funder", pairs)


def update_funders(conn: Connection, people: list[ActionNetworkPerson]) -> int:
    pairs = [(person, create_funder_record(person)) for person in people]
    return update_records(conn, "funder", pairs)


def upsert_funders(conn: Connection, people: list[ActionNetworkPerson]) -> (int, int):
    pairs = [(person, create_funder_record(person)) for person in people]
    return upsert_records(conn, "funder", pairs)


def delete_funders(conn: Connection, people: list[ActionNetworkPerson]) -> int:
    return delete_records(conn, "funder", people)
