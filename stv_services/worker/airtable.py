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

from ..action_network.person import ActionNetworkPerson
from ..airtable.contact import upsert_contacts
from ..airtable.funder import upsert_funders
from ..airtable.volunteer import upsert_volunteers
from ..airtable.webhook import fetch_hook_payloads
from ..core import logging, Configuration
from ..data_store import Postgres, model

logger = logging.get_logger(__name__)


def process_webhook_notification(conn: Connection, name: str):
    if name == "volunteer":
        payloads = fetch_hook_payloads(conn, "volunteer")
        process_promotion_webhook_payloads(conn, "volunteer", payloads)
    elif name == "contact":
        payloads = fetch_hook_payloads(conn, "contact")
        process_promotion_webhook_payloads(conn, "contact", payloads)
        process_team_webhook_payloads(conn, "contact", payloads)
    else:
        raise ValueError(f"Unknown webhook type: '{name}'")


def process_promotion_webhook_payloads(
    conn: Connection, name: str, payloads: list[dict]
):
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
    volunteers = []
    contacts = []
    funders = []
    for person in people:
        if name == "volunteer":
            # have to refresh the volunteer record to fix the checkmark
            volunteers.append(person)
            if not person["is_contact"]:
                person["is_contact"] = True
                contacts.append(person)
                # new contact may also become a funder
                person.publish(conn)
                if person["is_funder"]:
                    funders.append(person)
        else:
            # have to refresh the contact record to fix the checkmark
            contacts.append(person)
            if not person["is_funder"]:
                person["is_funder"] = True
                funders.append(person)
        person.persist(conn)
    # now refresh all three tables, as needed
    upsert_volunteers(conn, people)
    upsert_contacts(conn, contacts)
    upsert_funders(conn, funders)


def process_team_webhook_payloads(conn: Connection, name: str, payloads: list[dict]):
    config = Configuration.get_session_config(conn)
    schema = config[f"airtable_stv_{name}_schema"]
    table_id = schema["table_id"]
    column_ids = schema["column_ids"]
    field_id = column_ids["team_lead"]
    changed_ids, new_lead_map = set(), {}
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
                        new_lead_map[record_id] = new_lead
                    else:
                        new_lead_map[record_id] = ""
                    if old_leads := old_vals.get(field_id, []):
                        if len(old_leads) > 1:
                            logger.error(f"Two team leads for {record_id}: {old_leads}")
                        old_lead = old_leads[0].get("id")
                        changed_ids.add(old_lead)
    change_team_leads(conn, list(changed_ids), new_lead_map)


def change_team_leads(conn: Connection, changed_ids: list[str], new_lead_map: dict):
    query = sa.select(model.person_info).where(
        model.person_info.c.contact_record_id.in_(changed_ids)
    )
    people = ActionNetworkPerson.from_query(conn, query)
    back_map = {person["contact_record_id"]: person["uuid"] for person in people}
    for person in people:
        if new_lead := new_lead_map.get(person["contact_record_id"]):
            person["team_lead"] = back_map[new_lead]
        else:
            person["team_lead"] = ""
        person.persist(conn)
    upsert_contacts(conn, people)
