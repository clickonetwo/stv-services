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
from typing import Callable

import sqlalchemy as sa
from sqlalchemy.future import Connection

from stv_services.action_network.person import ActionNetworkPerson
from stv_services.airtable.assignment import insert_empty_assignments
from stv_services.core import Session, Configuration
from stv_services.data_store import model, Postgres


def sync_contacts(_conn: Connection, people: list[ActionNetworkPerson]) -> (dict, dict):
    emails = {person.get("email"): person for person in people}
    extras = {}
    unmatched = {}
    empties = []

    def page_processor(page: list[dict]):
        for record in page:
            record_id = record["id"]
            record_email = record["fields"].get("email")
            if not record_email:
                # records with no email shouldn't exist in Airtable
                empties.append(record_id)
            elif match := emails.get(record_email):
                # we have an existing person record for this email
                contact_record_id = match.get("contact_record_id")
                if contact_record_id is None:
                    # adopt this as the record for this email
                    match["contact_record_id"] = record_id
                    match["contact_last_updated"] = model.epoch
                elif contact_record_id != record_id:
                    # remember this as an extra email
                    extras[record_email] = record_id
            else:
                # this is an extra record because we don't have a person with it
                unmatched[record_email] = record_id

    _ = page_processor
    pass


def ensure_empty_assignments(verbose=True) -> int:
    config = Configuration.get_global_config()
    schema = config["airtable_stv_assignment_schema"]
    column_ids = schema["column_ids"]
    if verbose:
        print(f"Fetching contact record ids...", end="", flush=True)
    with Postgres.get_global_engine().connect() as conn:
        query = sa.select(model.person_info.c.contact_record_id).where(
            model.person_info.c.contact_record_id != ""
        )
        record_ids = [row[0] for row in conn.execute(query).all()]
    if verbose:
        print(f"({len(record_ids)})")
    all_record_ids = set(record_ids)
    seen_record_ids = set()
    processed = 0

    def process_page(records: list[dict]):
        nonlocal processed, seen_record_ids
        if verbose and processed > 0:
            print(f"(({processed})...", end="", flush=True)
        for record in records:
            links = next(iter(record["fields"].values()))
            if not links or len(links) > 1:
                print(f"Warning: invalid assignment record: {record.get('id')}")
            elif (record_id := links[0]) in all_record_ids:
                seen_record_ids.add(record_id)
            else:
                print(f"Warning: unknown contact record: {record_id}")
        processed += len(records)

    if verbose:
        print(f"Looking for matching assignment records...", end="", flush=True)
    process_airtable_records(
        schema=schema, processor=process_page, fields=[column_ids["contact_record_id"]]
    )
    if verbose:
        print(f"({processed})")
    needed = list(all_record_ids - seen_record_ids)
    total, inserts = len(needed), 0
    if verbose:
        if total == 0:
            print("All contacts have assignments.")
        else:
            print(f"There are {total} contacts who don't have assignments.")
    if total == 0:
        return 0
    if verbose:
        print(f"Inserting {total} empty assignment records...", end="", flush=True)
    for start in range(0, total, 50):
        if verbose and inserts > 0:
            print(f"({inserts})...", end="", flush=True)
        inserts += insert_empty_assignments(needed[start : min(start + 50, total)])
    if verbose:
        print(f"({inserts})")
        print(f"Inserted {inserts} records.")
    return inserts


def remove_empty_assignments(verbose: bool = True, remove_all: bool = False):
    config = Configuration.get_global_config()
    schema = config["airtable_stv_assignment_schema"]
    column_ids = schema["column_ids"]
    processed, empty_ids = 0, []

    def process_page(records: list[dict]):
        nonlocal processed, empty_ids
        if verbose and processed > 0:
            print(f"({processed})...", end="", flush=True)
        for record in records:
            summary: str = next(iter(record["fields"].values()))
            if remove_all or summary == "NONE YET":
                empty_ids.append(record["id"])
        processed += len(records)

    if verbose:
        modifier = "any" if remove_all else "empty"
        print(f"Looking for {modifier} assignment records...", end="", flush=True)
    process_airtable_records(
        schema=schema, processor=process_page, fields=[column_ids["summary"]]
    )
    if verbose:
        print(f"({processed})")
        modifier = "" if remove_all else "empty "
        print(f"Found {len(empty_ids)} {modifier}assignment records.")
    if len(empty_ids) > 0:
        return delete_airtable_records(
            schema=schema, record_ids=empty_ids, verbose=verbose
        )
    else:
        return 0


def process_airtable_records(
    schema: dict,
    processor: Callable[[list[dict]], None],
    fields: list[str] = None,
):
    web = Session.get_airtable_api()
    base_id = schema["base_id"]
    table_id = schema["table_id"]
    if fields:
        iterator = web.iterate(base_id, table_id, fields=fields)
    else:
        iterator = web.iterate(base_id, table_id)
    for page in iterator:
        processor(page)


def delete_airtable_records(
    schema: dict, record_ids: list[str], verbose: bool = False
) -> int:
    web = Session.get_airtable_api()
    base_id = schema["base_id"]
    table_id = schema["table_id"]
    total, deletes = len(record_ids), 0
    if verbose:
        print(f"Deleting {total} records...", end="", flush=True)
    for start in range(0, total, 50):
        if verbose and deletes > 0:
            print(f"({deletes})...", end="", flush=True)
        deletes += len(
            web.batch_delete(
                base_id, table_id, record_ids[start : min(start + 50, total)]
            )
        )
    if verbose:
        print(f"({deletes})")
    return deletes
