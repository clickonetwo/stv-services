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

from ..act_blue.metadata import ActBlueDonationMetadata
from ..action_network.person import ActionNetworkPerson
from ..airtable.webhook import fetch_hook_payloads
from ..core import logging, Configuration
from ..data_store import model

logger = logging.get_logger(__name__)


def process_webhook_notification(conn: Connection, name: str):
    if name == "volunteer":
        payloads = fetch_hook_payloads(conn, "volunteer")
        process_promotion_webhook_payloads(conn, "volunteer", payloads)
    elif name == "contact":
        payloads = fetch_hook_payloads(conn, "contact")
        process_promotion_webhook_payloads(conn, "contact", payloads)
        process_team_webhook_payloads(conn, "contact", payloads)
    elif name == "assignment":
        payloads = fetch_hook_payloads(conn, "assignment")
        process_refcode_payloads(conn, "assignment", payloads)
    else:
        raise ValueError(f"Unknown webhook type: '{name}'")


def process_promotion_webhook_payloads(
    conn: Connection, name: str, payloads: list[dict]
):
    logger.info(f"Processing Airtable '{name}' payloads for promotion...")
    config = Configuration.get_session_config(conn)
    schema = config[f"airtable_stv_{name}_schema"]
    table_id = schema["table_id"]
    column_ids = schema["column_ids"]
    field_id = column_ids["is_contact" if name == "volunteer" else "is_funder"]
    record_ids = set()
    for payload in payloads:
        change: dict = payload.get("changedTablesById", {}).get(table_id, {})
        if change and (records := change.get("changedRecordsById")):
            for record_id, change in records.items():
                current = change.get("current", {}).get("cellValuesByFieldId", {})
                if field_id in current:
                    record_ids.add(record_id)
    promote_volunteers_or_contacts(conn, name, list(record_ids))
    logger.info(f"Promotion processing done.")


def promote_volunteers_or_contacts(conn: Connection, name: str, record_ids: list[str]):
    if name == "volunteer":
        query = sa.select(model.person_info).where(
            model.person_info.c.volunteer_record_id.in_(record_ids)
        )
    elif name == "contact":
        query = sa.select(model.person_info).where(
            model.person_info.c.contact_record_id.in_(record_ids)
        )
    else:
        raise ValueError(f"Hook name ({name}) is not volunteer or contact")
    people = ActionNetworkPerson.from_query(conn, query)
    for person in people:
        person.notice_promotion(conn, name)
        person.persist(conn)


def process_team_webhook_payloads(conn: Connection, name: str, payloads: list[dict]):
    logger.info(f"Processing Airtable '{name}' payloads for team changes...")
    config = Configuration.get_session_config(conn)
    schema = config[f"airtable_stv_{name}_schema"]
    table_id = schema["table_id"]
    column_ids = schema["column_ids"]
    field_id = column_ids["team_lead"]
    changed_ids = set()
    lead_map: dict[str, (str, str)] = {}
    for payload in payloads:
        change: dict = payload.get("changedTablesById", {}).get(table_id, {})
        if change and (records := change.get("changedRecordsById")):
            for record_id, change in records.items():
                new_vals = change.get("current", {}).get("cellValuesByFieldId", {})
                old_vals = change.get("previous", {}).get("cellValuesByFieldId", {})
                if field_id in new_vals:
                    changed_ids.add(record_id)
                    if new_leads := new_vals.get(field_id, []):
                        if len(new_leads) > 1:
                            logger.error(f"Two team leads for {record_id}: {new_leads}")
                        new_lead = new_leads[0].get("id")
                        changed_ids.add(new_lead)
                    else:
                        new_lead = ""
                    if old_leads := old_vals.get(field_id, []):
                        if len(old_leads) > 1:
                            logger.error(f"Two team leads for {record_id}: {old_leads}")
                        old_lead = old_leads[0].get("id")
                        changed_ids.add(old_lead)
                    else:
                        old_lead = ""
                    if new_lead != old_lead:
                        lead_map[record_id] = (old_lead, new_lead)
    change_team_leads(conn, list(changed_ids), lead_map)
    logger.info(f"Team change processing done.")


def change_team_leads(
    conn: Connection, changed_ids: list[str], lead_map: dict[str, (str, str)]
):
    query = sa.select(model.person_info).where(
        model.person_info.c.contact_record_id.in_(changed_ids)
    )
    people = ActionNetworkPerson.from_query(conn, query)
    back_map = {person["contact_record_id"]: person for person in people}
    for member, (old, new) in lead_map.items():
        person = back_map[member]
        old_lead = back_map.get(old)
        new_lead = back_map.get(new)
        person.notice_team_lead_change(conn, old_lead, new_lead)
    for person in people:
        person.persist(conn)


def process_refcode_payloads(conn: Connection, name: str, payloads: list[dict]):
    logger.info(f"Processing Airtable '{name}' payloads for refcode assignments...")
    config = Configuration.get_session_config(conn)
    schema = config[f"airtable_stv_{name}_schema"]
    table_id = schema["table_id"]
    column_ids = schema["column_ids"]
    refcode_field_id = column_ids["refcode"]
    contact_field_id = column_ids["contact_record_id"]
    refcode_map = {}
    for payload in payloads:
        change: dict = payload.get("changedTablesById", {}).get(table_id, {})
        if change and (records := change.get("changedRecordsById")):
            for change in records.values():
                changed = change.get("current", {}).get("cellValuesByFieldId", {})
                refcode = changed.get(refcode_field_id)
                unchanged = change.get("unchanged", {}).get("cellValuesByFieldId", {})
                contact_record_list = unchanged.get(contact_field_id)
                if not contact_record_list:
                    logger.warning("Ignoring invalid assignment change: {records}")
                # because this is a record linked to a field with a value, each link
                # is a dict of both a record ID and a value field.
                contact_record_id = contact_record_list[0]["id"]
                refcode_map[contact_record_id] = refcode or ""
    process_refcode_assignments(conn, refcode_map)
    logger.info(f"Refcode assignment processing done.")


def process_refcode_assignments(conn: Connection, refcode_map: dict):
    """When a refcode assignment gets made in Airtable, we are likely to get
    multiple payloads, because Airtable sends incremental updates as users
    type. If all the payloads arrive in the same fetch, then we will only be
    called once with the last value, but if they arrive in different fetches,
    then we will get multiple calls and the last will win."""
    for contact_record_id, refcode in refcode_map.items():
        # find the user
        query = sa.select(model.person_info).where(
            model.person_info.c.contact_record_id == contact_record_id
        )
        row = conn.execute(query).mappings().first()
        if not row:
            logger.error(f"No person with contact record '{contact_record_id}'")
            continue
        # notify the user of their assigned refcode
        person = ActionNetworkPerson.from_lookup(conn, uuid=row["uuid"])
        person.notice_refcode(conn, refcode)
        person.persist(conn)
        if refcode:
            # if this is not an empty refcode, we notify the user that has it
            query = sa.select(model.donation_metadata).where(
                model.donation_metadata.c.refcode == refcode
            )
            for row in conn.execute(query).mappings().all():
                metadata = ActBlueDonationMetadata.from_lookup(conn, row["uuid"])
                metadata.notice_person(conn, person)
                metadata.persist(conn)
