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
from sqlalchemy.engine import Connection

from ..action_network.person import ActionNetworkPerson
from ..core import Configuration, Session
from ..data_store import model

#
# templated person-based calls
#
def insert_people(
    conn: Connection, person_type: str, pairs: list[(ActionNetworkPerson, dict)]
) -> int:
    if not pairs:
        return 0
    schema_name, id_field, date_field = field_names(person_type)
    schema = Configuration.get_global_config()[schema_name]
    record_ids = insert_airtable_records(schema, [pair[1] for pair in pairs])
    for record_id, person in zip(record_ids, [pair[0] for pair in pairs]):
        person[id_field] = record_id
        person[date_field] = person["modified_date"]
        person.persist(conn)
    return len(record_ids)


def update_people(
    conn: Connection, person_type: str, pairs: list[(ActionNetworkPerson, dict)]
) -> int:
    if not pairs:
        return 0
    schema_name, id_field, date_field = field_names(person_type)
    schema = Configuration.get_global_config()[schema_name]
    updates = []
    for person, record in pairs:
        record_id = person[id_field]
        updates.append({"id": record_id, "fields": record})
    update_airtable_records(schema, updates)
    for person, _ in pairs:
        person[date_field] = person["modified_date"]
        person.persist(conn)
    return len(pairs)


def upsert_people(
    conn: Connection, person_type: str, pairs: list[(ActionNetworkPerson, dict)]
) -> (int, int):
    _, id_field, _ = field_names(person_type)
    inserts, updates = [], []
    for person, record in pairs:
        if person.get(id_field):
            updates.append((person, record))
        else:
            inserts.append((person, record))
    i_count = insert_people(conn, person_type, inserts)
    u_count = update_people(conn, person_type, updates)
    return i_count, u_count


def delete_people(
    conn: Connection, person_type: str, people: list[ActionNetworkPerson]
) -> int:
    if not people:
        return 0
    schema_name, id_field, date_field = field_names(person_type)
    schema = Configuration.get_global_config()[schema_name]
    deletes, deleted_people = [], []
    for person in people:
        if record_id := person.get(id_field):
            deletes.append(record_id)
            deleted_people.append(person)
    delete_airtable_records(schema, deletes)
    for person in deleted_people:
        person[id_field] = ""
        person[date_field] = model.epoch
        person.persist(conn)
    return len(deletes)


def field_names(person_type: str) -> (str, str, str):
    if person_type in ["contact", "volunteer", "funder"]:
        return (
            f"airtable_stv_{person_type}_schema",
            f"{person_type}_record_id",
            f"{person_type}_last_updated",
        )
    else:
        raise ValueError(f"Type ({person_type}) must be contact, volunteer, or funder")


#
# underlying airtable calls
#
def insert_airtable_records(schema: dict, records: list[dict]) -> list[str]:
    if not records:
        return []
    api = Session.get_airtable_api()
    base_id = schema["base_id"]
    table_id = schema["table_id"]
    response: list[dict] = api.batch_create(base_id, table_id, records, typecast=True)
    return [row["id"] for row in response]


def update_airtable_records(schema: dict, updates: list[dict]):
    if not updates:
        return
    api = Session.get_airtable_api()
    base_id = schema["base_id"]
    table_id = schema["table_id"]
    response: list[dict] = api.batch_update(
        base_id, table_id, updates, replace=False, typecast=True
    )
    if Configuration.get_env() == "DEV":
        assert set(r["id"] for r in response) == set(u["id"] for u in updates)


def delete_airtable_records(schema: dict, record_ids: list[str]):
    if not record_ids:
        return
    api = Session.get_airtable_api()
    base_id = schema["base_id"]
    table_id = schema["table_id"]
    response: list[dict] = api.batch_delete(base_id, table_id, record_ids)
    if Configuration.get_env() == "DEV":
        assert set(r["id"] for r in response) == set(record_ids)
