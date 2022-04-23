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

from ..action_network.utils import ActionNetworkPersistedDict
from ..core import Configuration, Session
from ..data_store import model


def find_records_to_update(dict_type: str, force: bool = False):
    table, _, is_field, id_field, date_field = table_fields(dict_type)
    if force:
        query = sa.select(table).where(table.c[is_field])
    else:
        query = sa.select(table).where(
            sa.and_(
                table.c[is_field],
                sa.or_(
                    table.c[id_field] == "",
                    table.c.modified_date > table.c[date_field],
                ),
            )
        )
    return query


#
# generic calls for inserting Action Network records into Airtable
#
def insert_records(
    conn: Connection, dict_type: str, pairs: list[(ActionNetworkPersistedDict, dict)]
) -> int:
    if not pairs:
        return 0
    _, schema_name, _, id_field, date_field = table_fields(dict_type)
    schema = Configuration.get_global_config()[schema_name]
    record_ids = _insert_records(schema, [pair[1] for pair in pairs])
    for record_id, p_dict in zip(record_ids, [pair[0] for pair in pairs]):
        p_dict[id_field] = record_id
        p_dict[date_field] = p_dict["modified_date"]
        p_dict.persist(conn)
    return len(record_ids)


def update_records(
    conn: Connection, person_type: str, pairs: list[(ActionNetworkPersistedDict, dict)]
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
        p_dict[date_field] = p_dict["modified_date"]
        p_dict.persist(conn)
    return len(pairs)


def upsert_records(
    conn: Connection, person_type: str, pairs: list[(ActionNetworkPersistedDict, dict)]
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
    conn: Connection, person_type: str, dicts: list[ActionNetworkPersistedDict]
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
        f"{dict_type}_last_updated",
    )


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


#
# bulk processing of existing airtable records
#
# def process_airtable_records(
#     schema: dict, processor: Callable[[list[dict]], None], fields: list[str] = None
# ):
#     api = Session.get_airtable_api()
#     base_id = schema["base_id"]
#     table_id = schema["table_id"]
#     if fields:
#         iterator = api.iterate(base_id, table_id, fields=fields)
#     else:
#         iterator = api.iterate(base_id, table_id)
#     for page in iterator:
#         processor(page)
#
#
# def sync_airtable_records(
#     hash_type: str,
#     page_processor: Callable[[list[dict]], None],
#     query: str = None,
#     verbose: bool = True,
#     skip_pages: int = 0,
#     max_pages: int = 0,
# ) -> int:
#     config = Configuration.get_global_config()
#     url = config["action_network_api_base_url"] + f"/{hash_type}"
#     query_args = {}
#     if query:
#         query_args["filter"] = query
#         if verbose:
#             print(f"Fetching {hash_type} matching filter={query}...")
#     else:
#         if verbose:
#             print(f"Fetching all {hash_type}...")
#     if skip_pages:
#         query_args["page"] = skip_pages + 1
#         if verbose:
#             print(f"(Starting import on page {skip_pages + 1})")
#     if query_args:
#         url += "?" + urlencode(query_args)
#     return fetch_hash_pages(
#         hash_type=hash_type,
#         url=url,
#         page_processor=page_processor,
#         verbose=verbose,
#         skip_pages=skip_pages,
#         max_pages=max_pages,
#     )
