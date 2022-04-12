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
from time import process_time
from typing import Optional, Callable
from urllib.parse import urlencode

import requests
import sqlalchemy as sa
from dateutil.parser import parse
from restnavigator import Navigator
from sqlalchemy.dialects import postgresql as psql

from ..core import Configuration, Session
from ..data_store import Database, model


def validate_hash(data: dict) -> (str, datetime, datetime):
    if not isinstance(data, dict) or len(data) == 0:
        raise ValueError(f"Not a valid Action Network hash: {data}")
    for candidate in data.get("identifiers", []):  # type: str
        if candidate.startswith("action_network:"):
            hash_id = candidate
            break
    else:
        hash_id = None
    created_date: datetime = parse(data.get("created_date"))
    modified_date: datetime = parse(data.get("modified_date"))
    if not hash_id or not created_date or not modified_date:
        raise ValueError(f"Action Network hash is missing required items: {data}")
    return hash_id, created_date, modified_date


class ActionNetworkPersistedDict(dict):
    def __init__(self, table: sa.Table, **fields):
        self.table = table
        if (
            not fields.get("uuid")
            or not fields.get("created_date")
            or not fields.get("modified_date")
        ):
            raise ValueError(
                f"uuid, created_date, and modified_date must be present: {fields}"
            )
        fields = {key: value for key, value in fields.items() if value is not None}
        super().__init__(**fields)

    def persist(self, conn: Optional[sa.engine.Connection] = None):
        """
        Persist the current object to the datastore.

        If a connection is supplied, then the caller is responsible for the commit.
        Otherwise, a connection is opened and a commit is done before returning.
        """
        insert_fields = {key: value for key, value in self.items() if value is not None}
        update_fields = {
            key: value for key, value in insert_fields.items() if key != "uuid"
        }
        insert_query = psql.insert(self.table).values(insert_fields)
        upsert_query = insert_query.on_conflict_do_update(
            index_elements=["uuid"], set_=update_fields
        )
        if conn is not None:
            conn.execute(upsert_query)
        else:
            with Database.get_global_engine().connect() as conn:
                conn.execute(upsert_query)
                conn.commit()
        pass

    def reload(self):
        query = sa.select(self.table).where(self.table.c.uuid == self["uuid"])
        with Database.get_global_engine().connect() as conn:
            result = conn.execute(query).first()
            if result is None:
                raise KeyError(f"Can't reload person identified by '{self['uuid']}'")
            fields = {
                key: value
                for key, value in result._asdict().items()
                if value is not None
            }
        self.clear()
        self.update(fields)
        pass

    def remove(self):
        query = sa.delete(self.table).where(self.table.c.uuid == self["uuid"])
        with Database.get_global_engine().connect() as conn:
            conn.execute(query)
            conn.commit()


def lookup_hash(
    table: sa.Table, key_value: str, key_field: Optional[str] = None
) -> Optional[dict]:
    if not key_value:
        raise ValueError("Key value must be supplied for lookup")
    if not key_field:
        query = sa.select(table).where(table.c.uuid == key_value)
    else:
        query = sa.select(table).where(table.c[key_field] == key_value)
    with Database.get_global_engine().connect() as conn:
        result = conn.execute(query).first()
    if result is None:
        return None
    fields = {
        key: value for key, value in result._asdict().items() if value is not None
    }
    return fields


def fetch_hash(hash_type: str, hash_id: str) -> dict:
    if not hash_id.startswith("action_network:"):
        raise ValueError(f"Not an action network identifier: '{hash_id}'")
    else:
        uuid = hash_id[len("action_network:") :]
    config = Configuration.get_global_config()
    session = Session.get_global_session("action_network")
    url = config["action_network_api_base_url"] + f"/{hash_type}/{uuid}"
    nav = Navigator.hal(url, session=session)
    data: dict = nav(raise_exc=False)
    response: requests.Response = nav.response
    if response.status_code == 404:
        raise KeyError(f"No hash of type '{hash_type}' identified by id '{hash_id}'")
    if response.status_code != 200:
        response.raise_for_status()
    return data


def fetch_hashes(
    hash_type: str,
    page_processor: Callable[[[dict]], None],
    query: Optional[str] = None,
    verbose: bool = True,
    skip_pages: int = 0,
    max_pages: int = 0,
) -> int:
    config = Configuration.get_global_config()
    session = Session.get_global_session("action_network")
    url = config["action_network_api_base_url"] + f"/{hash_type}"
    query_args = {}
    if query:
        query_args["filter"] = query
        if verbose:
            print(
                f"Fetching {hash_type} from Action Network matching filter={query}..."
            )
    else:
        if verbose:
            print(f"Fetching all {hash_type} from Action Network...")
    if skip_pages:
        query_args["page"] = skip_pages + 1
        if verbose:
            print(f"(Starting import on page {skip_pages + 1})")
    if query_args:
        url += "?" + urlencode(query_args)
    start_time = datetime.now()
    start_process_time = process_time()
    pages = Navigator.hal(url, session=session)
    page_number, total_count, last_page = skip_pages, 0, None
    for page in pages:
        navigators = page.embedded()[f"osdi:{hash_type}"]
        if (page_count := len(navigators)) == 0:
            break
        page_number += 1
        if verbose:
            if last_page := page.state.get("total_pages", last_page):
                print(
                    f"Processing {page_count} {hash_type} on page {page_number}/{last_page}...",
                    end="",
                )
            else:
                print(
                    f"Processing {page_count} {hash_type} on page {page_number}...",
                    end="",
                )
        hash_list = []
        for navigator in navigators:
            hash_list.append(navigator.state)
        page_processor(hash_list)
        total_count += page_count
        if verbose:
            print(f"({total_count})")
        if max_pages and page_number >= (skip_pages + max_pages):
            if verbose:
                print(f"(Stopped after importing {max_pages} pages)")
            break
    elapsed_process_time = process_time() - start_process_time
    elapsed_time = datetime.now() - start_time
    if verbose:
        print(f"Fetched {total_count} {hash_type} from Action Network.")
        print(
            f"Fetch time was {elapsed_time} (processor time: {elapsed_process_time} seconds)."
        )
    return total_count
