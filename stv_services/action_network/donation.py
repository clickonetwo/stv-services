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
from typing import Optional, Any, ClassVar

import sqlalchemy as sa
from sqlalchemy.future import Connection

from .utils import validate_hash, fetch_hash, fetch_all_hashes, ActionNetworkObject
from ..core.logging import get_logger
from ..data_store import model
from ..data_store.persisted_dict import lookup_objects

logger = get_logger(__name__)


class ActionNetworkDonation(ActionNetworkObject):
    # the database table for this class
    table: ClassVar[sa.Table] = model.donation_info
    # the cache for this class
    cache: ClassVar[dict] = {}

    donation_cutoff_lo: ClassVar[datetime] = datetime(2021, 11, 1, tzinfo=timezone.utc)

    def __init__(self, **fields):
        for key in ["amount", "recurrence_data", "donor_id", "fundraising_page_id"]:
            if not fields.get(key):
                raise ValueError(f"Donation must have field '{key}': {fields}")
        super().__init__(**fields)

    def compute_status(self, conn: Connection, force: bool = False):
        """Try to attribute this donation based on latest data."""
        if self["created_date"] > self.donation_cutoff_lo:
            self["is_donation"] = True
        else:
            # attribution status not needed for non-Airtable donations
            return
        if not force and self.get("attribution_id"):
            return
        # get attribution from fundraising page, if any
        query = sa.select(model.fundraising_page_info.c.attribution_id).where(
            model.fundraising_page_info.c.uuid == self["fundraising_page_id"],
        )
        if page := conn.execute(query).mappings().first():
            self.notice_attribution(conn, page["attribution_id"])
        # if we still need an attribution, look for a refcode
        if force or not self.get("attribution_id"):
            if metadata_id := self.get("metadata_id", ""):
                query = sa.select(model.donation_metadata).where(
                    model.donation_metadata.c.uuid == metadata_id
                )
                if metadata := conn.execute(query).mappings().first():
                    self.notice_attribution(conn, metadata["attribution_id"])
        self["updated_date"] = datetime.now(tz=timezone.utc)

    def notice_attribution(self, _conn: Connection, attribution_id: str):
        if attribution_id:
            self["attribution_id"] = attribution_id
        self["updated_date"] = datetime.now(tz=timezone.utc)

    def notice_amount_change(self, _conn: Connection, amount: str):
        self["amount"] = amount
        self["updated_date"] = datetime.now(tz=timezone.utc)

    def update_from_hash(self, data: dict):
        # The only thing that can change about a donation is the amount
        # If the amount hasn't changed, we issue a warning about the update
        uuid, _, mod_date = validate_hash(data)
        self["modified_date"] = mod_date
        if (amount := data.get("amount")) and amount != self["amount"]:
            self["amount"] = amount
        else:
            logger.warning(f"Ignoring update of donation '{uuid}' dated {mod_date}")

    @staticmethod
    def _get_metadata_id(data: dict):
        """Return the ActBlue metadata ID for this donation, if any"""
        for candidate in data.get("identifiers", []):  # type: str
            if candidate.startswith("act_blue:"):
                return candidate
        else:
            return None

    @classmethod
    def from_webhook(cls, data: dict) -> "ActionNetworkDonation":
        uuid, created_date, modified_date = validate_hash(data)
        # we are in 2022, so this is a donation
        is_donation = created_date >= cls.donation_cutoff_lo
        # amount is typically specified, but not if this is a return
        amount = data.get("amount") or "0.00"
        # recurrence data is in the embedded data
        recurrence_data = data.get("action_network:recurrence")
        # donor_id and fundraising_page_id are in links, not embedded data
        links = data.get("_links", {})
        if donor_id := links.get("osdi:person", {}).get("href"):
            id_part = donor_id[donor_id.rfind("/") + 1 :]
            donor_id = "action_network:" + id_part
        else:
            raise KeyError(f"Donation webhook does not have donor link")
        if fundraising_page_id := links.get("osdi:fundraising_page", {}).get("href"):
            id_part = fundraising_page_id[fundraising_page_id.rfind("/") + 1 :]
            fundraising_page_id = "action_network:" + id_part
        else:
            raise KeyError(f"Donation webhook does not have fundraising page link")
        return cls(
            uuid=uuid,
            created_date=created_date,
            modified_date=modified_date,
            is_donation=is_donation,
            amount=amount,
            recurrence_data=recurrence_data,
            donor_id=donor_id,
            fundraising_page_id=fundraising_page_id,
            metadata_id=cls._get_metadata_id(data),
        )

    @classmethod
    def from_hash(cls, data: dict) -> "ActionNetworkDonation":
        uuid, created_date, modified_date = validate_hash(data)
        # only donations newer than the cutoff go to Airtable
        is_donation = created_date >= cls.donation_cutoff_lo
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
            metadata_id=cls._get_metadata_id(data),
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
        See `.utils.lookup_objects` for details.
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
    ActionNetworkDonation.initialize_cache()
    return fetch_all_hashes(
        hash_type="donations",
        cls=ActionNetworkDonation,
        query=query,
        verbose=verbose,
        skip_pages=skip_pages,
        max_pages=max_pages,
    )
