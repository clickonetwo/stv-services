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


class ActionNetworkDonation(ActionNetworkPersistedDict):
    def __init__(self, **fields):
        for key in ["amount", "recurrence_data", "donor_id", "fundraising_page_id"]:
            if not fields.get(key):
                raise ValueError(f"Donation must have field '{key}': {fields}")
        super().__init__(model.donation_info, **fields)

    @classmethod
    def from_action_network(cls, data: dict) -> "ActionNetworkDonation":
        uuid, created_date, modified_date = validate_hash(data)
        # donations sometimes get later updates in which the amount is removed
        # so we treat missing amounts as 0.00 in order to update a prior fetch
        amount = data.get("amount", "0.00")
        recurrence_data = data.get("action_network:recurrence")
        if donor_id := data.get("action_network:person_id"):
            donor_id = "action_network:" + donor_id
        if fundraising_page_id := data.get("action_network:fundraising_page_id"):
            fundraising_page_id = "action_network:" + fundraising_page_id
        return cls(
            uuid=uuid,
            created_date=created_date,
            modified_date=modified_date,
            amount=amount,
            recurrence_data=recurrence_data,
            donor_id=donor_id,
            fundraising_page_id=fundraising_page_id,
        )

    @classmethod
    def lookup(cls, uuid: str) -> "ActionNetworkDonation":
        result = lookup_hash(model.donation_info, uuid)
        if result is None:
            raise KeyError("No fundraising page identified by '{uuid}'")
        fields = {key: value for key, value in result.items() if value is not None}
        return cls(**fields)


def load_donation(hash_id: str) -> ActionNetworkDonation:
    data = fetch_hash("donations", hash_id)
    donation = ActionNetworkDonation.from_action_network(data)
    donation.persist()
    return donation


def load_donations(
    query: Optional[str] = None,
    verbose: bool = True,
    skip_pages: int = 0,
    max_pages: int = 0,
) -> int:
    def insert_from_hashes(hashes: [dict]):
        with Database.get_global_engine().connect() as conn:
            for data in hashes:
                try:
                    donation = ActionNetworkDonation.from_action_network(data)
                    donation.persist(conn)
                except ValueError as err:
                    if verbose:
                        print(f"Skipping invalid donation: {err}")
            conn.commit()

    return fetch_hashes(
        "donations", insert_from_hashes, query, verbose, skip_pages, max_pages
    )
