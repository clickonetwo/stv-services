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
from typing import ClassVar, Dict
import sqlalchemy as sa

from .schema import fetch_and_validate_table_schema, FieldInfo
from ..action_network.person import ActionNetworkPerson
from ..core import Configuration
from ..data_store import model, Database

table_name = "Contacts"
table_schema = [
    FieldInfo("email", "Email*", "email", "person_info"),
    FieldInfo("given_name", "First Name*", "singleLineText", "person_info"),
    FieldInfo("family_name", "Last Name*", "singleLineText", "person_info"),
    FieldInfo("phone", "Phone*", "phoneNumber", "person_info"),
    FieldInfo("street_address", "Address*", "multilineText", "person_info"),
    FieldInfo("locality", "City*", "singleLineText", "person_info"),
    FieldInfo("region", "State*", "singleLineText", "person_info"),
    FieldInfo("postal_code", "Zip code*", "singleLineText", "person_info"),
    FieldInfo(
        "2020_donation_total", "Total 2020 Donations*", "currency", "person_info"
    ),
    FieldInfo(
        "2020_donation_summary",
        "2020 Donation Summary*",
        "multilineText",
        "person_info",
    ),
    FieldInfo(
        "2021_donation_total", "Total 2021 Donations*", "currency", "person_info"
    ),
    FieldInfo(
        "2021_donation_summary",
        "2021 Donation Summary*",
        "multilineText",
        "person_info",
    ),
    FieldInfo("is_contact", "Moved to 2022?", "checkbox", "person_map"),
    FieldInfo("is_funder", "In Fundraising table?", "checkbox", "person_map"),
]
column_ids: Dict[str, str] = {}


def verify_schema():
    global column_ids
    config = Configuration.get_global_config()
    base_name = config.get("airtable_stv_base_name")
    prior_schema = config.get("")
    column_ids = fetch_and_validate_table_schema(base_name, table_name, table_schema)


def create_airtable_record(conn: sa.engine.Connection, uuid: str) -> dict:
    record = dict()
    for field_name, _, field_type in table_schema:
        if field_type != "checkbox":
            record[column_ids[field_name]] = person[field_name]
    record[column_ids["is_contact"]] = person["email"]
    record[column_ids["given_name"]] = person["given_name"]
    record[column_ids["family_name"]] = person["family_name"]
    record[column_ids["phone"]] = person["phone"]
    record[column_ids["street_address"]] = person["street_address"]
    record[column_ids["locality"]] = person["locality"]
    record[column_ids["region"]] = person["region"]
    record[column_ids["postal_code"]] = person["postal_code"]
