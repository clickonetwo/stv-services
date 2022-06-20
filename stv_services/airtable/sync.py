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
from collections import namedtuple
from typing import Callable, Union, Iterable

import sqlalchemy as sa
from dateutil.parser import parse
from sqlalchemy.future import Connection

from stv_services.action_network.person import ActionNetworkPerson
from stv_services.core import Session, Configuration
from stv_services.core.logging import get_logger
from stv_services.data_store import model, Postgres

logger = get_logger(__name__)
SyncReport = namedtuple(
    "SyncReport",
    [
        "matched_records",
        "extra_records",
        "adoptable_records",
        "unmatched_records",
        "empty_records",
    ],
)


def analyze_report(report: SyncReport):
    match = True
    if count := len(report.empty_records):
        match = False
        logger.info(f"There are {count} records with no email.")
    if len(report.unmatched_records):
        match = False
        count = sum([len(val) for val in report.unmatched_records.values()])
        logger.info(f"There are {count} records with emails that don't match people.")
    if len(report.adoptable_records) > 0:
        match = False
        count = sum([len(val) for val in report.adoptable_records.values()])
        logger.info(f"There are {count} records with emails for people with no record.")
    if len(report.extra_records) > 0:
        match = False
        deltas = []
        for email, extras in report.extra_records.items():
            if len(extras) > 1:
                logger.info(f"There are {len(extras)} extra records for '{email}'")
            actual = report.matched_records[email]
            actual_created = parse(actual["createdTime"])
            for extra in extras:
                extra_created = parse(extra["createdTime"])
                deltas.append((actual_created - extra_created).total_seconds() / 3600)
        count = len(deltas)
        average = sum(deltas) / count
        min_delta, max_delta = min(deltas), max(deltas)
        logger.info(f"There are {count} extra records with matching emails.")
        logger.info(
            f"On average, the extra was created {average} hours before the actual."
        )
        logger.info(f"The differences range from {min_delta} to {max_delta} hours.")
    if match:
        logger.info(f"The records match the people completely.")


def sync_report(type_: str) -> SyncReport:
    def page_processor(page: list[dict]):
        for record in page:
            record_id = record["id"]
            if type_ == "funder":
                # looked-up email field is a list of one email
                record_email = record["fields"].get("Email*")[0]
            else:
                record_email = record["fields"].get("Email*")
            if not record_email:
                # records with no email shouldn't exist in Airtable
                empties.append(record)
            elif match := emails.get(record_email):
                # we have an existing person record for this email
                type_record_id = match.get(type_record_id_field)
                if type_record_id is None:
                    # this record can be adopted by this person
                    existing = adoptable.setdefault(record_email, [])
                    existing.append(record)
                elif type_record_id != record_id:
                    # remember this as an extra record for this email
                    existing = extras.setdefault(record_email, [])
                    existing.append(record)
                else:
                    # normal situation: record id and email match!
                    matches[record_email] = record
            else:
                # we don't have a person with this email
                existing = unmatched.setdefault(record_email, [])
                existing.append(record)

    schema = Configuration.get_global_config()[f"airtable_stv_{type_}_schema"]
    type_record_id_field = f"{type_}_record_id"
    matches = {}
    extras = {}
    adoptable = {}
    unmatched = {}
    empties = []
    with Postgres.get_global_engine().connect() as conn:  # type: Connection
        people = ActionNetworkPerson.from_query(conn, sa.select(model.person_info))
    emails = {person["email"]: person for person in people}
    process_airtable_records(schema, page_processor)
    return SyncReport(matches, extras, adoptable, unmatched, empties)


def process_airtable_records(
    schema: dict,
    processor: Callable[[list[dict]], None],
    fields: list[str] = None,
):
    web = Session.get_airtable_api()
    base_id = schema["base_id"]
    table_id = schema["table_id"]
    if fields is not None:
        iterator = web.iterate(base_id, table_id, fields=fields)
    else:
        iterator = web.iterate(base_id, table_id)
    for page in iterator:
        processor(page)


def verify_match(
    types: Union[str, Iterable[str]] = None, remove_extra: bool = False
) -> dict:
    all_types = {"volunteer", "contact", "funder", "donation"}
    if not types or types == "all":
        types = list(all_types)
    elif isinstance(types, str):
        if types in all_types:
            types = [types]
        else:
            raise ValueError(f"Unknown record type: '{types}'")
    elif not all_types.issuperset(set(types)):
        raise ValueError(f"Contains unknown record type: {types}")
    results = {}
    for type_ in types:
        logger.info(f"Verifying match for {type_} records")
        shortfall, extra_records = match_records(type_)
        extra_count = len(extra_records)
        results[type_] = (shortfall, extra_records)
        if shortfall > 0:
            if type_ == "donation":
                msg = f"are {shortfall} donations" if shortfall > 1 else "is 1 donation"
                logger.critical(f"There {msg} with no record")
            else:
                msg = f"are {shortfall} people" if shortfall > 1 else "is 1 person"
                logger.critical(f"There {msg} with no {type_} record")
        if extra_count > 0:
            if extra_count == 1:
                logger.critical(f"There is 1 extra {type_} record")
            else:
                logger.critical(f"There are {extra_count} extra {type_} records")
            if remove_extra:
                msg = "record" if extra_count == 1 else "records"
                logger.info(f"Removing {extra_count} extra {type_} {msg}")
                extra_ids = [record["id"] for record in extra_records]
                delete_airtable_records(type_, extra_ids, verbose=False)
        if shortfall == 0 and extra_count == 0:
            logger.info(f"Verified match for {type_} records")
    return results


def match_records(type_: str) -> (int, list):
    def page_processor(page: list[dict]):
        nonlocal matches
        for record in page:
            record_id = record["id"]
            if record_id not in record_ids:
                extra_records.append(record)
            else:
                matches += 1

    schema = Configuration.get_global_config()[f"airtable_stv_{type_}_schema"]
    matches = 0
    record_ids = set()
    extra_records = []
    with Postgres.get_global_engine().connect() as conn:  # type: Connection
        if type_ == "volunteer":
            is_col = model.person_info.columns.is_volunteer
            id_col = model.person_info.columns.volunteer_record_id
        elif type_ == "contact":
            is_col = model.person_info.columns.is_contact
            id_col = model.person_info.columns.contact_record_id
        elif type_ == "funder":
            is_col = model.person_info.columns.is_funder
            id_col = model.person_info.columns.funder_record_id
        elif type_ == "donation":
            is_col = model.donation_info.columns.is_donation
            id_col = model.donation_info.columns.donation_record_id
        else:
            raise ValueError(f"Unknown record type: {type_}")
        rows = conn.execute(sa.select(id_col).where(is_col)).all()
    record_ids = {row[0] for row in rows}
    process_airtable_records(schema, page_processor, fields=[])
    return matches - len(rows), extra_records


def delete_airtable_records(
    type_: str, record_ids: list[str], verbose: bool = False
) -> int:
    web = Session.get_airtable_api()
    schema = Configuration.get_global_config()[f"airtable_stv_{type_}_schema"]
    base_id = schema["base_id"]
    table_id = schema["table_id"]
    total, deletes = len(record_ids), 0
    if verbose:
        logger.info(f"Deleting {total} {type_} records...")
    for start in range(0, total, 50):
        if verbose and deletes > 0:
            logger.info(f"({deletes})...")
        deletes += len(
            web.batch_delete(
                base_id, table_id, record_ids[start : min(start + 50, total)]
            )
        )
    if verbose:
        logger.info(f"({deletes})")
    return deletes
