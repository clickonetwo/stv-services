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
from typing import Optional, Any

import sqlalchemy as sa
from sqlalchemy.future import Connection

from .donation import ActionNetworkDonation
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
        initial_values = dict(
            updated_date=model.epoch,
            origin_system="",
            attribution_id="",
        )
        value_fields = {k: v for k, v in fields.items() if v is not None}
        initial_values.update(value_fields)
        super().__init__(model.fundraising_page_info, **initial_values)

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

    def compute_status(self, conn: Connection, force: bool = False):
        """Try to attribute this fundraising page based on latest metadata."""
        if self["origin_system"] != "ActBlue":
            return
        if not self.get("title", "").startswith("actblue_146845_"):
            return
        if not force and self["attribution_id"]:
            return
        # get attribution from metadata, if any
        form_name = self["title"][len("actblue_146845_") :]
        query = sa.select(model.donation_metadata).where(
            model.donation_metadata.c.form_name == form_name
        )
        if metadata := conn.execute(query).mappings().first():
            self.notice_attribution(conn, metadata["attribution_id"])
        self["updated_date"] = datetime.now(tz=timezone.utc)

    def notice_attribution(self, conn: Connection, attribution_id: str):
        if attribution_id:
            self["attribution_id"] = attribution_id
            self.notify_donations(conn, attribution_id)
        self["updated_date"] = datetime.now(tz=timezone.utc)

    def notify_donations(self, conn: Connection, attribution_id: str):
        query = sa.select(model.donation_info).where(
            model.donation_info.c.fundraising_page_id == self["uuid"]
        )
        for donation in ActionNetworkDonation.from_query(conn, query):
            donation.notice_attribution(conn, attribution_id)
            donation.persist(conn)

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
        See `.utils.lookup_objects` for details.
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
