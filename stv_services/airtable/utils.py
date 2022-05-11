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
from sqlalchemy.future import Connection

from ..core import Configuration, Session
from ..data_store import model
from ..data_store.persisted_dict import PersistedDict


def find_records_to_update(dict_type: str, force: bool = False):
    table, is_col, id_col, date_col = table_columns(dict_type)
    if force:
        query = sa.select(table).where(is_col)
    else:
        query = sa.select(table).where(
            sa.and_(is_col, sa.or_(id_col == "", table.c.updated_date > date_col))
        )
    return query


#
# generic calls for inserting Action Network records into Airtable
#
def insert_records(
    conn: Connection, dict_type: str, pairs: list[(PersistedDict, dict)]
) -> int:
    if not pairs:
        return 0
    _, schema_name, _, id_field, date_field = table_fields(dict_type)
    schema = Configuration.get_global_config()[schema_name]
    dicts, records = [pair[0] for pair in pairs], [pair[1] for pair in pairs]
    record_ids = _insert_records(schema, records)
    for record_id, p_dict in zip(record_ids, dicts):
        p_dict[id_field] = record_id
        p_dict[date_field] = datetime.now(tz=timezone.utc)
        p_dict.persist(conn)
    return len(record_ids)


def update_records(
    conn: Connection, person_type: str, pairs: list[(PersistedDict, dict)]
) -> int:
    if not pairs:
        return 0
    _, schema_name, _, id_field, date_field = table_fields(person_type)
    schema = Configuration.get_global_config()[schema_name]
    updates = []
    for p_dict, record in pairs:
        record_id = p_dict[id_field]
        updates.append({"id": record_id, "fields": record})
    _update_records(schema, updates)
    for p_dict, _ in pairs:
        p_dict[date_field] = datetime.now(tz=timezone.utc)
        p_dict.persist(conn)
    return len(pairs)


def upsert_records(
    conn: Connection, person_type: str, pairs: list[(PersistedDict, dict)]
) -> (int, int):
    _, _, _, id_field, _ = table_fields(person_type)
    inserts, updates = [], []
    for p_dict, record in pairs:
        if p_dict.get(id_field):
            updates.append((p_dict, record))
        else:
            inserts.append((p_dict, record))
    i_count = insert_records(conn, person_type, inserts)
    u_count = update_records(conn, person_type, updates)
    return i_count, u_count


def delete_records(
    conn: Connection, person_type: str, dicts: list[PersistedDict]
) -> int:
    if not dicts:
        return 0
    _, schema_name, _, id_field, date_field = table_fields(person_type)
    schema = Configuration.get_global_config()[schema_name]
    deletes, deleted_people = [], []
    for p_dict in dicts:
        if record_id := p_dict.get(id_field):
            deletes.append(record_id)
            deleted_people.append(p_dict)
    _delete_records(schema, deletes)
    for p_dict in deleted_people:
        p_dict[id_field] = ""
        p_dict[date_field] = model.epoch
        p_dict.persist(conn)
    return len(deletes)


def table_fields(dict_type: str) -> (sa.Table, str, str, str, str):
    if Configuration.get_env() == "DEV" and dict_type not in {
        "donation",
        "contact",
        "volunteer",
        "funder",
    }:
        raise ValueError(f"Unknown persisted dict type: {dict_type}")
    return (
        model.donation_info if dict_type == "donation" else model.person_info,
        f"airtable_stv_{dict_type}_schema",
        f"is_{dict_type}",
        f"{dict_type}_record_id",
        f"{dict_type}_updated",
    )


def table_columns(dict_type: str) -> (sa.Table, sa.Column, sa.Column, sa.Column):
    if Configuration.get_env() == "DEV" and dict_type not in {
        "donation",
        "contact",
        "volunteer",
        "funder",
    }:
        raise ValueError(f"Unknown persisted dict type: {dict_type}")
    table = model.donation_info if dict_type == "donation" else model.person_info
    is_col, record_col, updated_col = None, None, None
    for column in table.columns:  # type: sa.Column
        if column.key == f"is_{dict_type}":
            is_col = column
        elif column.key == f"{dict_type}_record_id":
            record_col = column
        elif column.key == f"{dict_type}_updated":
            updated_col = column
    else:
        if is_col is None or record_col is None or updated_col is None:
            raise ValueError("Table is missing expected columns")
    return table, is_col, record_col, updated_col


#
# underlying airtable calls
#
def _insert_records(schema: dict, records: list[dict]) -> list[str]:
    if not records:
        return []
    api = Session.get_airtable_api()
    base_id = schema["base_id"]
    table_id = schema["table_id"]
    response: list[dict] = api.batch_create(base_id, table_id, records, typecast=True)
    if Configuration.get_env() == "DEV":
        assert len(response) == len(records)
    return [row["id"] for row in response]


def _update_records(schema: dict, updates: list[dict]):
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


def _delete_records(schema: dict, record_ids: list[str]):
    from requests import HTTPError

    if not record_ids:
        return
    api = Session.get_airtable_api()
    base_id = schema["base_id"]
    table_id = schema["table_id"]
    try:
        api.batch_delete(base_id, table_id, record_ids)
    except HTTPError as err:
        if err.response.status_code != 404:
            raise
