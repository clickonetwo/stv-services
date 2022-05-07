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

from .utils import (
    validate_hash,
    fetch_hash,
    fetch_all_hashes,
    lookup_objects,
)
from ..data_store.persisted_dict import PersistedDict
from ..data_store import model, Postgres


class ActionNetworkDonation(PersistedDict):
    def __init__(self, **fields):
        for key in ["amount", "recurrence_data", "donor_id", "fundraising_page_id"]:
            if not fields.get(key):
                raise ValueError(f"Donation must have field '{key}': {fields}")
        super().__init__(model.donation_info, **fields)

    @classmethod
    def from_hash(cls, data: dict) -> "ActionNetworkDonation":
        uuid, created_date, modified_date = validate_hash(data)
        # only donations made in 2022 go to Airtable
        is_donation = created_date >= datetime(2022, 1, 1, tzinfo=timezone.utc)
        # donations sometimes get later updates in which the amount is removed,
        # so we treat missing (or explicitly null) amounts as 0.00 in order to
        # update a prior fetch with the new value
        amount = data.get("amount") or "0.00"
        recurrence_data = data.get("action_network:recurrence")
        if donor_id := data.get("action_network:person_id"):
            donor_id = "action_network:" + donor_id
        if fundraising_page_id := data.get("action_network:fundraising_page_id"):
            fundraising_page_id = "action_network:" + fundraising_page_id
        return cls(
            uuid=uuid,
            created_date=created_date,
            modified_date=modified_date,
            is_donation=is_donation,
            amount=amount,
            recurrence_data=recurrence_data,
            donor_id=donor_id,
            fundraising_page_id=fundraising_page_id,
        )

    @classmethod
    def from_lookup(cls, conn: Connection, uuid: str) -> "ActionNetworkDonation":
        query = sa.select(model.donation_info).where(model.donation_info.c.uuid == uuid)
        result = lookup_objects(conn, query, lambda d: cls(**d))
        if not result:
            raise KeyError(f"No donation identified by '{uuid}'")
        return result[0]

    @classmethod
    def from_query(cls, conn: Connection, query: Any) -> list["ActionNetworkDonation"]:
        """
        See `.utils.lookup_hashes` for details.
        """
        return lookup_objects(conn, query, lambda d: cls(**d))

    @classmethod
    def from_action_network(
        cls,
        conn: Connection,
        hash_id: str,
    ) -> "ActionNetworkDonation":
        data, _ = fetch_hash("donations", hash_id)
        donation = ActionNetworkDonation.from_hash(data)
        donation.persist(conn)
        return donation


def import_donations(
    query: Optional[str] = None,
    verbose: bool = True,
    skip_pages: int = 0,
    max_pages: int = 0,
) -> int:
    return fetch_all_hashes(
        hash_type="donations",
        page_processor=import_donations_from_hashes,
        query=query,
        verbose=verbose,
        skip_pages=skip_pages,
        max_pages=max_pages,
    )


def import_donations_from_hashes(hashes: [dict]):
    with Postgres.get_global_engine().connect() as conn:  # type: Connection
        for data in hashes:
            try:
                donation = ActionNetworkDonation.from_hash(data)
                donation.persist(conn)
            except ValueError as err:
                print(f"Skipping invalid donation: {err}")
        conn.commit()
