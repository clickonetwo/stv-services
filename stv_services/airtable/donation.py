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
from ..action_network.donation import ActionNetworkDonation
from ..core import Configuration
from ..core.logging import get_logger
from ..core.utilities import airtable_timestamp
from ..data_store import model

logger = get_logger(__name__)
donation_table_name = "Donations"
donation_table_schema = {
    "uuid": FieldInfo("Donation ID*", "singleLineText", "donation"),
    "fundraising_page_id": FieldInfo(
        "Fundraising Page ID*", "singleLineText", "donation"
    ),
    "amount": FieldInfo("Donation Amount*", "currency", "donation"),
    "created_date": FieldInfo("Donation Date*", "date", "compute"),
    "recurrence_data": FieldInfo(
        "Made as part of a recurring donation?*", "checkbox", "compute"
    ),
    "donor_id": FieldInfo(
        "Donor Name (from Contacts)*", "multipleRecordLinks", "compute"
    ),
    "attribution_id": FieldInfo(
        "Initial Fundraiser Attribution*", "multipleRecordLinks", "compute"
    ),
    "override_attribution_id": FieldInfo(
        "Fundraiser Attribution", "multipleRecordLinks", "compute"
    ),
}


def verify_donation_schema() -> dict:
    config = Configuration.get_global_config()
    base_name = config["airtable_stv_base_name"]
    access_info = fetch_and_validate_table_schema(
        base_name, donation_table_name, donation_table_schema
    )
    config["airtable_stv_donation_schema"] = access_info
    return access_info


def create_donation_record(conn: Connection, donation: ActionNetworkDonation) -> dict:
    config = Configuration.get_global_config()
    table = model.person_info
    # find the matching donor record, if there is one
    query = sa.select(table).where(table.c.uuid == donation["donor_id"])
    donor: dict = conn.execute(query).mappings().first()
    if not donor:
        raise KeyError("Donation '{donation['uuid']}' has no donor")
    if not donor["contact_record_id"]:
        raise KeyError(f"Donor '{donor['uuid']}' is not a contact")
    attribution_record_id = None
    if attribution_id := donation["attribution_id"]:
        # find the matching fundraising page record, if there is one
        query = sa.select(table).where(table.c.uuid == attribution_id)
        if attribution := conn.execute(query).mappings().first():
            attribution_record_id = attribution["funder_record_id"]
            if not attribution_record_id:
                logger.warning(f"Attributor {attribution['uuid']} is not a funder")
    column_ids = config["airtable_stv_donation_schema"]["column_ids"]
    record = dict()
    for field_name, info in donation_table_schema.items():
        if info.source == "donation":
            # all fields should have values, but we are cautious
            if value := donation.get(field_name):
                record[column_ids[field_name]] = value
    # created date must be an airtable date
    value = airtable_timestamp(donation["created_date"])
    record[column_ids["created_date"]] = value
    # recurrence data requires parsing the recurrence object
    value = donation["recurrence_data"].get("recurring", False)
    record[column_ids["recurrence_data"]] = value
    # this is a link to the Donor's record ID in the Contacts table
    record[column_ids["donor_id"]] = [donor["contact_record_id"]]
    # set the attribution, if any
    if attribution_record_id:
        record[column_ids["attribution_id"]] = attribution_record_id
        record[column_ids["override_attribution_id"]] = attribution_record_id
    return record


def upsert_donations(
    conn: Connection, donations: list[ActionNetworkDonation]
) -> (int, int):
    pairs = [
        (donation, create_donation_record(conn, donation)) for donation in donations
    ]
    return upsert_records(conn, "donation", pairs)


def delete_donations(conn: Connection, donations: list[ActionNetworkDonation]) -> int:
    return delete_records(conn, "donation", donations)
