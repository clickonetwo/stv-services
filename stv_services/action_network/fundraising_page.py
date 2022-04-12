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
from typing import Optional

from .utils import (
    validate_hash,
    ActionNetworkPersistedDict,
    fetch_hash,
    fetch_hashes,
    lookup_hash,
)
from ..data_store import model, Database


class ActionNetworkFundraisingPage(ActionNetworkPersistedDict):
    def __init__(self, **fields):
        for key in ["title"]:
            if not fields.get(key):
                raise ValueError(f"Fundraising page must have field '{key}': {fields}")
        super().__init__(model.fundraising_page_info, **fields)

    @classmethod
    def from_action_network(cls, data: dict) -> "ActionNetworkFundraisingPage":
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
    def lookup(cls, uuid: str) -> "ActionNetworkFundraisingPage":
        result = lookup_hash(model.fundraising_page_info, uuid)
        if result is None:
            raise KeyError("No fundraising page identified by '{uuid}'")
        fields = {key: value for key, value in result.items() if value is not None}
        return cls(**fields)


def load_fundraising_page(hash_id: str) -> ActionNetworkFundraisingPage:
    data = fetch_hash("fundraising_pages", hash_id)
    fundraising_page = ActionNetworkFundraisingPage.from_action_network(data)
    fundraising_page.persist()
    return fundraising_page


def load_fundraising_pages(
    query: Optional[str] = None,
    verbose: bool = True,
    skip_pages: int = 0,
    max_pages: int = 0,
) -> int:
    def insert_from_hashes(hashes: [dict]):
        with Database.get_global_engine().connect() as conn:
            for data in hashes:
                try:
                    fundraising_page = ActionNetworkFundraisingPage.from_action_network(
                        data
                    )
                    fundraising_page.persist(conn)
                except ValueError as err:
                    if verbose:
                        print(f"Skipping invalid fundraising page: {err}")
            conn.commit()

    return fetch_hashes(
        "fundraising_pages", insert_from_hashes, query, verbose, skip_pages
    )
