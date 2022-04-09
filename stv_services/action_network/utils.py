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
from datetime import datetime
from time import process_time
from typing import Optional, Callable
from urllib.parse import urlencode

import requests
import sqlalchemy as sa
from dateutil.parser import parse
from restnavigator import Navigator
from sqlalchemy.dialects import postgresql as psql

from ..core import Configuration, Session
from ..data_store import Database


def validate_hash(data: dict) -> (str, datetime, datetime):
    if not isinstance(data, dict) or len(data) == 0:
        raise ValueError(f"Not a valid Action Network hash: {data}")
    hash_id: str = ""
    for candidate in data.get("identifiers", []):  # type: str
        if candidate.startswith("action_network:"):
            hash_id = candidate
            break
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
            raise ValueError("uuid, created_date, and modified_date must be present")
        fields = {key: value for key, value in fields.items() if value is not None}
        super().__init__(**fields)

    def persist(self):
        insert_fields = {key: value for key, value in self.items() if value is not None}
        update_fields = {
            key: value for key, value in insert_fields.items() if key != "uuid"
        }
        insert_query = psql.insert(self.table).values(insert_fields)
        upsert_query = insert_query.on_conflict_do_update(
            index_elements=["uuid"], set_=update_fields
        )
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


def load_hashes(
    hash_type: str,
    hash_processor: Callable[[dict], None],
    query: Optional[str] = None,
    verbose: bool = True,
) -> int:
    config = Configuration.get_global_config()
    session = Session.get_global_session("action_network")
    url = config["action_network_api_base_url"] + f"/{hash_type}"
    if query:
        query_string = urlencode({"filter": query})
        url += f"?{query_string}"
        if verbose:
            print(f"Loading {hash_type} from Action Network matching filter={query}...")
    else:
        if verbose:
            print("Loading all {hash_type} from Action Network...")
    start_time = datetime.now()
    start_process_time = process_time()
    pages = Navigator.hal(url, session=session)
    total_count, total_pages = 0, None
    for i, page in enumerate(pages):
        navigators = page.embedded()[f"osdi:{hash_type}"]
        if (page_count := len(navigators)) == 0:
            break
        total_count += page_count
        if verbose:
            if total_pages := page.state.get("total_pages", total_pages):
                print(
                    f"Processing {page_count} {hash_type} on page {i + 1}/{total_pages}"
                )
            else:
                print(f"Processing {page_count} {hash_type} on page {i + 1}...")
        for navigator in navigators:
            hash_processor(navigator.state)
    elapsed_process_time = process_time() - start_process_time
    elapsed_time = datetime.now() - start_time
    if verbose:
        print(f"Loaded {total_count} {hash_type} from Action Network.")
        print(
            f"Load time was {elapsed_time} (processor time: {elapsed_process_time} seconds)."
        )
    return total_count
