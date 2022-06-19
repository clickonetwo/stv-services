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
from typing import ClassVar, Type
from urllib.parse import urlencode

import requests
import sqlalchemy as sa
from dateutil.parser import parse
from restnavigator import Navigator
from sqlalchemy.future import Connection

from ..core import Configuration, Session
from ..data_store import Postgres
from ..data_store.persisted_dict import PersistedDict


class ActionNetworkObject(PersistedDict):
    table: ClassVar[sa.Table]
    # we keep a cache of objects by key to save database queries during import
    cache: ClassVar[dict]

    @classmethod
    def initialize_cache(cls):
        cls.cache.clear()
        with Postgres.get_global_engine().connect() as conn:  # type: Connection
            for row in conn.execute(sa.select(cls.table)):
                cls.cache[row.uuid] = cls(**row)

    def __init__(self, **fields):
        super().__init__(self.table, **fields)

    def persist(self, conn: Connection):
        super().persist(conn)
        self.cache[self["uuid"]] = self

    def remove(self, conn: Connection):
        del self.cache[self["uuid"]]
        super().remove(conn)

    def update_from_hash(self, _data: dict):
        # meant to be overridden by subclasses
        raise NotImplementedError("You must implement update_from_hash")

    @classmethod
    def from_hash(cls, _data: dict) -> "ActionNetworkObject":
        # meant to be overridden by subclasses
        raise NotImplementedError("You must implement from_hash")


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


def fetch_hash(hash_type: str, hash_id: str) -> (dict, dict):
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
    return data, nav.links()


def fetch_all_hashes(
    hash_type: str,
    cls: Type[ActionNetworkObject],
    query: str = None,
    verbose: bool = True,
    skip_pages: int = 0,
    max_pages: int = 0,
) -> int:
    config = Configuration.get_global_config()
    url = config["action_network_api_base_url"] + f"/{hash_type}"
    query_args = {}
    if query:
        query_args["filter"] = query
        if verbose:
            print(f"Fetching {hash_type} matching filter={query}...")
    else:
        if verbose:
            print(f"Fetching all {hash_type}...")
    if skip_pages:
        query_args["page"] = skip_pages + 1
        if verbose:
            print(f"(Starting import on page {skip_pages + 1})")
    if query_args:
        url += "?" + urlencode(query_args)
    return fetch_hash_pages(
        hash_type=hash_type,
        url=url,
        cls=cls,
        verbose=verbose,
        skip_pages=skip_pages,
        max_pages=max_pages,
    )


def fetch_all_child_hashes(
    parent_hash_type: str,
    child_hash_type: str,
    cls: Type[ActionNetworkObject],
    query: str = None,
    verbose: bool = True,
) -> int:
    config = Configuration.get_global_config()
    url = config["action_network_api_base_url"] + f"/{parent_hash_type}"
    if query:
        url += "?" + urlencode({"filter": query})
        if verbose:
            print(
                f"Fetching {child_hash_type} from {parent_hash_type} matching filter={query}..."
            )
    else:
        if verbose:
            print(f"Fetching {child_hash_type} from all {parent_hash_type}...")
    session = Session.get_global_session("action_network")
    pages = Navigator.hal(url, session=session)
    total_count = 0
    for page in pages:
        navigators = page.links()[f"osdi:{parent_hash_type}"]
        if len(navigators) == 0:
            break
        for nav in navigators:
            if verbose:
                parent_id = nav().get("identifiers", ["unknown"])[0]
                print(f"Processing {parent_hash_type} {parent_id}...")
            total_count += fetch_related_hashes(
                url=nav.uri + f"/{child_hash_type}",
                hash_type=child_hash_type,
                cls=cls,
                verbose=verbose,
            )
    return total_count


def fetch_related_hashes(
    hash_type: str,
    url: str,
    cls: Type[ActionNetworkObject],
    verbose: bool = False,
) -> int:
    if verbose:
        print(f"Fetching related {hash_type}...")
    return fetch_hash_pages(hash_type=hash_type, url=url, cls=cls, verbose=verbose)


def fetch_hash_pages(
    hash_type: str,
    url: str,
    cls: Type[ActionNetworkObject],
    verbose: bool = True,
    skip_pages: int = 0,
    max_pages: int = 0,
) -> int:
    start_time = datetime.now()
    start_process_time = process_time()
    session = Session.get_global_session("action_network")
    pages = Navigator.hal(url, session=session)
    page_number, total_count, last_page = skip_pages, 0, None
    total_created, total_updated, total_ignored = 0, 0, 0
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
                    flush=True,
                )
            else:
                print(
                    f"Processing {page_count} {hash_type} on page {page_number}...",
                    end="",
                    flush=True,
                )
        hash_list = [navigator.state for navigator in navigators]
        created, updated, ignored = import_or_update_objects(cls, hash_list)
        total_created += created
        total_updated += updated
        total_ignored += ignored
        total_count += page_count
        if verbose:
            print(
                f"(+{created} created, +{updated} updated, +{ignored} ignored "
                f"= {total_count})"
            )
        if max_pages and page_number >= (skip_pages + max_pages):
            if verbose:
                print(f"(Stopped after importing {max_pages} pages)")
            break
    elapsed_process_time = process_time() - start_process_time
    elapsed_time = datetime.now() - start_time
    if verbose:
        print(f"Fetched {total_count} {hash_type}.")
        print(
            f"Created {total_created}, updated {total_updated}, ignored {total_ignored}"
        )
        print(
            f"Fetch time was {elapsed_time} (processor time: {elapsed_process_time} seconds)."
        )
    return total_count


def import_or_update_objects(
    cls: Type[ActionNetworkObject], hashes: [dict]
) -> (int, int, int):
    created, updated, ignored = 0, 0, 0
    with Postgres.get_global_engine().connect() as conn:  # type: Connection
        for data in hashes:
            try:
                uuid, created_date, modified_date = validate_hash(data)
                if obj := cls.cache.get(uuid):
                    # we already have this object, see if this hash is newer
                    if modified_date > obj["modified_date"]:
                        updated += 1
                        obj["modified_date"] = modified_date
                        obj.update_from_hash(data)
                        obj.persist(conn)
                    else:
                        ignored += 1
                    continue
                created += 1
                obj = cls.from_hash(data)
                obj.persist(conn)
            except ValueError as err:
                print(f"Skipping import of invalid hash: {err}")
        conn.commit()
    return created, updated, ignored
