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


def process_promotion_webhooks():
    payloads = fetch_hook_payloads("volunteer")
    process_promotion_webhook_payloads("volunteer", payloads)
    payloads = fetch_hook_payloads("contact")
    process_promotion_webhook_payloads("contact", payloads)


def process_promotion_webhook_payloads(name: str, payloads: list[dict]):
    config = Configuration.get_global_config()
    schema = config[f"airtable_stv_{name}_schema"]
    table_id = schema["table_id"]
    column_ids = schema["column_ids"]
    field_id = column_ids["is_contact" if name == "volunteer" else "is_funder"]
    record_ids = set()
    for payload in payloads:
        change: dict = payload.get("changedTablesById", {}).get(table_id, {})
        if change and (records := change.get("changedRecordsById")):
            for record_id, change in records.items():
                cell_vals = change.get("current", {}).get("cellValuesByFieldId", {})
                if field_id in cell_vals:
                    record_ids.add(record_id)
    promote_volunteers_or_contacts(name, list(record_ids))


def promote_volunteers_or_contacts(name: str, record_ids: list[str]):
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
    with Postgres.get_global_engine().connect() as conn:  # type: Connection
        people = ActionNetworkPerson.from_query(conn, query)
        funders = []
        for person in people:
            if name == "volunteer":
                person["is_contact"] = True
                # new contact may also become a funder
                person.classify_for_airtable(conn)
                if person["is_funder"]:
                    funders.append(person)
            else:
                person["is_funder"] = True
                funders.append(person)
            person.persist(conn)
        # now refresh all three tables, as needed
        if name == "volunteer":
            upsert_volunteers(conn, people)
        upsert_contacts(conn, people)
        upsert_funders(conn, funders)
        conn.commit()
