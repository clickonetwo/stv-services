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
from typing import Optional, Any

import sqlalchemy as sa
from sqlalchemy.future import Connection

from .utils import (
    validate_hash,
    fetch_hash,
    fetch_all_hashes,
)
from ..data_store.persisted_dict import PersistedDict, lookup_objects
from ..data_store import model, Postgres


class ActionNetworkFundraisingPage(PersistedDict):
    def __init__(self, **fields):
        for key in ["title"]:
            if not fields.get(key):
                raise ValueError(f"Fundraising page must have field '{key}': {fields}")
        super().__init__(model.fundraising_page_info, **fields)

    @classmethod
    def from_hash(cls, data: dict) -> "ActionNetworkFundraisingPage":
        uuid, created_date, modified_date = validate_hash(data)
        origin_system = data.get("origin_system")
        title = data.get("title")
        return cls(
            uuid=uuid,
            created_date=created_date,
            modified_date=modified_date,
            origin_system=origin_system,
            title=title,
        )

    @classmethod
    def from_lookup(cls, conn: Connection, uuid: str) -> "ActionNetworkFundraisingPage":
        query = sa.select(model.fundraising_page_info).where(
            model.fundraising_page_info.c.uuid == uuid
        )
        result = lookup_objects(conn, query, lambda d: cls(**d))
        if not result:
            raise KeyError(f"No fundraising page identified by '{uuid}'")
        return result[0]

    @classmethod
    def from_query(
        cls, conn: Connection, query: Any
    ) -> list["ActionNetworkFundraisingPage"]:
        """
        See `.utils.lookup_hashes` for details.
        """
        return lookup_objects(conn, query, lambda d: cls(**d))

    @classmethod
    def from_action_network(
        cls,
        conn: Connection,
        hash_id: str,
    ) -> "ActionNetworkFundraisingPage":
        data, _ = fetch_hash("fundraising_pages", hash_id)
        fundraising_page = ActionNetworkFundraisingPage.from_hash(data)
        fundraising_page.persist(conn)
        return fundraising_page


def import_fundraising_pages(
    query: Optional[str] = None,
    verbose: bool = True,
    skip_pages: int = 0,
    max_pages: int = 0,
) -> int:
    return fetch_all_hashes(
        hash_type="fundraising_pages",
        page_processor=insert_fundraising_pages_from_hashes,
        query=query,
        verbose=verbose,
        skip_pages=skip_pages,
        max_pages=max_pages,
    )


def insert_fundraising_pages_from_hashes(hashes: [dict]):
    with Postgres.get_global_engine().connect() as conn:  # type: Connection
        for data in hashes:
            try:
                fundraising_page = ActionNetworkFundraisingPage.from_hash(data)
                fundraising_page.persist(conn)
            except ValueError as err:
                print(f"Skipping invalid fundraising page: {err}")
        conn.commit()
