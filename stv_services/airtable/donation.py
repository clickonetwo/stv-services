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

import sqlalchemy as sa
from sqlalchemy.engine import Connection

from .schema import fetch_and_validate_table_schema, FieldInfo
from .utils import (
    insert_airtable_records,
    update_airtable_records,
    delete_airtable_records,
)
from ..action_network.donation import ActionNetworkDonation
from ..core import Configuration
from ..data_store import model

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
}


def verify_donation_schema() -> dict:
    config = Configuration.get_global_config()
    base_name = config.get("airtable_stv_base_name")
    access_info = fetch_and_validate_table_schema(
        base_name, donation_table_name, donation_table_schema
    )
    config["airtable_stv_donation_schema"] = access_info
    return access_info


def create_donation_record(conn: Connection, donation: ActionNetworkDonation) -> dict:
    config = Configuration.get_global_config()
    # find the matching donor record, if there is one
    query = sa.select(model.person_info).where(
        model.person_info.c.uuid == donation["donor_id"]
    )
    match = conn.execute(query).mappings().first()
    if not match:
        raise KeyError("Donation '{donation['uuid']}' has no donor")
    if not match["contact_record_id"]:
        raise KeyError("Donor '{person['uuid']}' is not a contact")
    column_ids = config["airtable_stv_donation_schema"]["column_ids"]
    record = dict()
    for field_name, info in donation_table_schema.items():
        if info.source == "donation":
            # all fields should have values, but we are cautious
            if value := donation.get(field_name):
                record[column_ids[field_name]] = value
        elif field_name == "created_date":
            # Airtable requires a special format: "2014-09-05T12:34:56.000Z"
            value = donation[field_name].strftime("%Y-%m-%dT%H:%M:%SZ")
            record[column_ids[field_name]] = value
        elif field_name == "recurrence_data":
            # this is a boolean from parsing the recurrence data
            value = donation[field_name].get("recurring", False)
            record[column_ids[field_name]] = value
        elif field_name == "donor_id":
            # this is a link to the Donor's record ID in the Contacts table
            record[column_ids[field_name]] = [match["contact_record_id"]]
        else:
            raise KeyError(f"Unknown donation field: {field_name}")
    return record


def insert_donations(conn: Connection, donations: list[ActionNetworkDonation]) -> int:
    if not donations:
        return 0
    records = [create_donation_record(conn, donation) for donation in donations]
    schema = Configuration.get_global_config()["airtable_stv_donation_schema"]
    record_ids = insert_airtable_records(schema, records)
    for record_id, donation in zip(record_ids, donations):
        donation["donation_record_id"] = record_id
        donation["donation_last_modified"] = donation["modified_date"]
        donation.persist(conn)
    return len(record_ids)


def update_donations(conn: Connection, donations: list[ActionNetworkDonation]) -> int:
    if not donations:
        return 0
    schema = Configuration.get_global_config()["airtable_stv_donation_schema"]
    updates = []
    for donation in donations:
        record_id = donation["donation_record_id"]
        record = create_donation_record(conn, donation)
        updates.append({"id": record_id, "fields": record})
    update_airtable_records(schema, updates)
    for donation in donations:
        donation["donation_last_modified"] = donation["modified_date"]
        donation.persist(conn)
    return len(donations)


def upsert_donations(
    conn: Connection, donations: list[ActionNetworkDonation]
) -> (int, int):
    inserts, updates = [], []
    for donation in donations:
        if donation.get("donation_record_id"):
            updates.append(donation)
        else:
            inserts.append(donation)
    i_count = insert_donations(conn, inserts)
    u_count = update_donations(conn, updates)
    return i_count, u_count


def delete_donations(conn: Connection, donations: list[ActionNetworkDonation]) -> int:
    if not donations:
        return 0
    schema = Configuration.get_global_config()["airtable_stv_donation_schema"]
    deletes, deleted_donations = [], []
    for donation in donations:
        if record_id := donation.get("donation_record_id"):
            deletes.append(record_id)
            deleted_donations.append(donation)
    delete_airtable_records(schema, deletes)
    for donation in deleted_donations:
        donation["donation_record_id"] = ""
        donation["donation_last_modified"] = model.epoch
        donation.persist(conn)
    return len(deletes)


def find_donations_to_update(conn: Connection, force: bool = False):
    cutoff = datetime(2022, 1, 1, tzinfo=timezone.utc)
    if force:
        query = sa.select(model.donation_info).where(
            model.donation_info.c.created_date > cutoff
        )
    else:
        query = sa.select(model.donation_info).where(
            sa.and_(
                model.donation_info.c.created_date > cutoff,
                sa.or_(
                    model.donation_info.c.donation_record_id == "",
                    model.donation_info.c.modified_date
                    > model.donation_info.c.donation_last_updated,
                ),
            )
        )
    donations = ActionNetworkDonation.from_query(conn, query)
    return donations
