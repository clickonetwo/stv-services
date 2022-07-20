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

from stv_services.action_network.donation import ActionNetworkDonation
from stv_services.action_network.person import ActionNetworkPerson
from stv_services.core import Session, Configuration
from stv_services.core.logging import get_logger
from stv_services.data_store import model, Postgres
from stv_services.mobilize.event import MobilizeEvent

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
    logger.info("Processing records from Airtable...")
    process_airtable_records(schema, page_processor)
    return SyncReport(matches, extras, adoptable, unmatched, empties)


def process_airtable_records(
    schema: dict, processor: Callable[[list[dict]], None], fields: list[str] = None
):
    web = Session.get_airtable_api()
    base_id = schema["base_id"]
    table_id = schema["table_id"]
    if fields is not None:
        iterator = web.iterate(base_id, table_id, fields=fields)
    else:
        iterator = web.iterate(base_id, table_id)
    count = 0
    for page in iterator:
        count += len(page)
        if count > 0:
            processor(page)
            logger.info(f"({count})")


def verify_match(types: Union[str, Iterable[str]] = None, repair: bool = False) -> dict:
    all_types = {"volunteer", "contact", "funder", "donation", "event"}
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
        missing, extra = match_records(type_, repair)
        results[type_] = (missing, extra)
        level = logger.info if repair else logger.critical
        if missing > 0:
            verb = "Repaired" if repair else "Found"
            s = "" if missing == 1 else "s"
            level(f"{verb} {missing} {type_}{s} with no record in Airtable")
        if extra > 0:
            verb = "Deleted" if repair else "Found"
            s = "" if extra == 1 else "s"
            level(f"{verb} {extra} extra {type_} record{s} in Airtable")
        if missing == 0 and extra == 0:
            logger.info(f"All {type_} records match in Airtable")
    return results


def match_records(type_: str, repair: bool = False) -> (int, int):
    def page_processor(page: list[dict]):
        for record in page:
            record_id = record["id"]
            if record_id not in record_id_map:
                extra_records.append(record_id)
            else:
                del record_id_map[record_id]

    schema = Configuration.get_global_config()[f"airtable_stv_{type_}_schema"]
    record_id_map = {}
    extra_records = []
    with Postgres.get_global_engine().connect() as conn:  # type: Connection
        cls = ActionNetworkPerson
        table = model.person_info
        if type_ == "volunteer":
            id_col = model.person_info.columns.volunteer_record_id
        elif type_ == "contact":
            id_col = model.person_info.columns.contact_record_id
        elif type_ == "funder":
            id_col = model.person_info.columns.funder_record_id
        elif type_ == "donation":
            cls = ActionNetworkDonation
            table = model.donation_info
            id_col = model.donation_info.columns.donation_record_id
        elif type_ == "event":
            cls = MobilizeEvent
            table = model.event_info
            id_col = model.event_info.columns.event_record_id
        else:
            raise ValueError(f"Unknown record type: {type_}")
        objects = cls.from_query(conn, sa.select(table).where(id_col != ""))
        record_id_map = {obj[f"{type_}_record_id"]: obj for obj in objects}
        logger.info("Matching records from Airtable...")
        process_airtable_records(schema, page_processor, fields=[])
        if repair:
            for obj in record_id_map.values():
                obj[f"{type_}_record_id"] = ""
                obj.persist(conn)
            conn.commit()
            delete_airtable_records(type_, extra_records)
    return len(record_id_map), len(extra_records)


def delete_airtable_records(type_: str, record_ids: list[str]):
    if not record_ids:
        return
    web = Session.get_airtable_api()
    schema = Configuration.get_global_config()[f"airtable_stv_{type_}_schema"]
    base_id = schema["base_id"]
    table_id = schema["table_id"]
    total = len(record_ids)
    s = "" if total == 1 else "s"
    logger.info(f"Deleting {total} Airtable {type_} record{s}...")
    for start in range(0, total, 50):
        end = min(start + 50, total)
        web.batch_delete(base_id, table_id, record_ids[start:end])
        logger.info(f"({end})")
