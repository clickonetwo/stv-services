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
from dateutil.parser import parse
from sqlalchemy.future import Connection

from .utils import (
    validate_hash,
    fetch_all_hashes,
    fetch_hash,
)
from ..act_blue.metadata import ActBlueDonationMetadata
from ..core.utilities import action_network_timestamp
from ..data_store import model, Postgres
from ..data_store.persisted_dict import PersistedDict, lookup_objects

interest_table_map = {
    "2022_calls": "contact",
    "2022_doors": "contact",
    "2022_fundraise": "funder",
    "2022_recruit": "contact",
    "2022_podlead": "contact",
    "2022_branchlead": "contact",
    "branch_lead_interest_I want to help build a branch in my region!": "contact",
    "2022_notes": "contact",
    "2022_happyhour": "funder",
    "2022_fundraisepage": "funder",
    "2022_donate": "funder",
    "2022_fundraiseidea": "funder",
    "2022_fundraisingnotes": "funder",
}


class ActionNetworkPerson(PersistedDict):
    def __init__(self, **fields):
        if not fields.get("email") and not fields.get("phone"):
            raise ValueError(f"Person record must have either email or phone: {fields}")
        super().__init__(model.person_info, **fields)

    def publish(self, conn: Connection, force: bool = False):
        """
        Classify an existing person based on current data. The classification
        determines what tables they belong in, what their donor status is,
        and so on.

        We are careful never to remove a person from a table. We don't
        republish people unless there is new data about them since their
        last annotation or unless the `force` flag is specified.
        """
        if not force and self["classification_date"] > self["modified_date"]:
            return
        # if they existed before 2022, they are a historical volunteer, else a contact
        if self["created_date"] < datetime(2022, 1, 1, tzinfo=timezone.utc):
            self["is_volunteer"] = True
        else:
            self["is_contact"] = True
        # update with the latest submission status
        self.compute_submission_status(conn, force)
        # update with the latest donor status
        self.compute_donor_status(conn, force)
        self["published_date"] = datetime.now(timezone.utc)
        self.persist(conn)

    def compute_submission_status(self, conn: Connection, force: bool = False):
        # if they have checked any of the 2022 form fields, they are contacts
        # and possibly funders (if it's a form field on the fundraising form)
        if custom_fields := self.get("custom_fields"):
            for key in custom_fields:
                if table := interest_table_map.get(key):
                    self["has_submission"] = True
                    self["is_contact"] = True
                    if table == "funder":
                        self["is_funder"] = True
        # if they didn't check any of the 2022 form fields, they may still have
        # signed up with no interests, so look for any submissions of either the
        # 2022 signup form or any forms in 2022
        if force or not self["has_submission"]:
            signup_form_2022 = "action_network:b399bd2b-b9a9-4916-9550-5a8a47e045fb"
            table = model.submission_info
            query = sa.select(table).where(
                sa.and_(
                    table.c.person_id == self["uuid"],
                    sa.or_(
                        table.c.form_id == signup_form_2022,
                        table.c.created_date
                        > datetime(2022, 1, 1, tzinfo=timezone.utc),
                    ),
                )
            )
            signup = conn.execute(query).first()
            if signup:
                self["has_submission"] = True
                self["is_contact"] = True

    def compute_donor_status(self, conn: Connection, force: bool = False):
        compute_historic = force or self["published_date"] == model.epoch
        table = model.donation_info
        # first compute status based on new orders
        cutoff_lo = model.epoch if compute_historic else self["published_date"]
        query = (
            sa.select(table)
            .where(
                sa.and_(
                    table.c.donor_id == self["uuid"], table.c.created_date >= cutoff_lo
                )
            )
            .order_by(table.c.created_date.desc())
        )
        donations: list[dict] = conn.execute(query).mappings().all()
        if not donations:
            if compute_historic:
                self["total_2020"], self["total_2021"] = 0, 0
            return
        if donations[0]["created_date"] > datetime(2021, 11, 1, tzinfo=timezone.utc):
            # if they have donated since 11/1/2021, they are a contact and a funder
            self["is_contact"] = True
            self["is_funder"] = True
        elif self.get("is_contact"):
            # if they are a contact and have any donations, they are a funder
            self["is_funder"] = True
        # if their most recent donation is recurring, they have a recurring start
        if donations[0].get("recurrence_data", {}).get("recurring"):
            self["recur_start"] = donations[0]["created_date"]
        # if they have a cancellation, they have a recurring end
        query = (
            sa.select(model.donation_metadata)
            .where(
                sa.and_(
                    model.donation_metadata.c.item_type == "cancellation",
                    model.donation_metadata.c.donor_email == self["email"],
                )
            )
            .order_by(model.donation_metadata.c.created_date.desc())
        )
        cancels = ActBlueDonationMetadata.from_query(conn, query)
        if cancels:
            self["recur_end"] = cancels[0]["created_date"]
        if compute_historic:
            max_2021 = datetime(2022, 1, 1)
            max_2020 = datetime(2021, 1, 1)
            total_2021, entries_2021 = 0, []
            total_2020, entries_2020 = 0, []
            is_recurring = self["recur_start"] > model.epoch
            was_recurring = self["recur_end"] > model.epoch
            for donation in donations[1:]:
                donation_date = donation["created_date"]
                if not is_recurring and not was_recurring:
                    # if we aren't recurring, but we were, mark us that way
                    if donation.get("recurrence_data", {}).get("recurring"):
                        was_recurring = True
                        self["recur_end"] = donation["created_date"]
                if donation_date >= max_2021:
                    continue
                amount = float(donation["amount"])
                day = donation_date.strftime("%m/%d/%y")
                entry = f"${int(round(amount, 0))} ({day})"
                if donation_date >= max_2020:
                    total_2021 += amount
                    entries_2021.append(entry)
                else:
                    total_2020 += amount
                    entries_2020.append(entry)
            self["total_2021"] = total_2021
            self["summary_2021"] = ", ".join(entries_2021)
            self["total_2020"] = total_2020
            self["summary_2020"] = ", ".join(entries_2020)

    @classmethod
    def from_hash(cls, data: dict) -> "ActionNetworkPerson":
        uuid, created_date, modified_date = validate_hash(data)
        is_contact, is_volunteer = None, None
        if created_date.year >= 2022:
            is_contact = True
        else:
            is_volunteer = True
        given_name: str = data.get("given_name")
        family_name: str = data.get("family_name", "")
        email: Optional[str] = None
        email_status: Optional[str] = None
        for entry in data.get("email_addresses", []):  # type: dict
            if entry.get("primary"):
                if address := entry.get("address"):
                    email = address.lower()
                email_status = entry.get("status")
                break
        phone: Optional[str] = None
        phone_type: Optional[str] = None
        phone_status: Optional[str] = None
        for entry in data.get("phone_numbers", []):  # type: dict
            if entry.get("primary"):
                phone = entry.get("number")
                phone_type = entry.get("number_type")
                phone_status = entry.get("status")
                break
        street_address: Optional[str] = None
        locality: Optional[str] = None
        region: Optional[str] = None
        postal_code: Optional[str] = None
        country: Optional[str] = None
        for entry in data.get("postal_addresses", []):  # type: dict
            if entry.get("primary"):
                if lines := entry.get("address_lines"):
                    street_address = "\n".join(lines)
                locality = entry.get("locality")
                region = entry.get("region")
                postal_code = entry.get("postal_code")
                country = entry.get("country")
                break
        custom_fields: dict = data.get("custom_fields", {})
        return cls(
            uuid=uuid,
            created_date=created_date,
            email=email,
            modified_date=modified_date,
            email_status=email_status,
            phone=phone,
            phone_type=phone_type,
            phone_status=phone_status,
            given_name=given_name,
            family_name=family_name,
            street_address=street_address,
            locality=locality,
            region=region,
            postal_code=postal_code,
            country=country,
            custom_fields=custom_fields,
            is_contact=is_contact,
            is_volunteer=is_volunteer,
        )

    @classmethod
    def from_lookup(
        cls, conn: Connection, uuid: Optional[str] = None, email: Optional[str] = None
    ) -> "ActionNetworkPerson":
        if uuid:
            query = sa.select(model.person_info).where(model.person_info.c.uuid == uuid)
        elif email:
            query = sa.select(model.person_info).where(
                model.person_info.c.email == email
            )
        else:
            raise ValueError("One of uuid or email must be specified for lookup")
        result = lookup_objects(conn, query, lambda d: cls(**d))
        if not result:
            raise KeyError(f"No person identified by '{uuid or email}'")
        return result[0]

    @classmethod
    def from_query(cls, conn: Connection, query: Any) -> list["ActionNetworkPerson"]:
        """
        See `.utils.lookup_hashes` for details.
        """
        return lookup_objects(conn, query, lambda d: cls(**d))

    @classmethod
    def from_action_network(
        cls,
        conn: Connection,
        hash_id: str,
    ) -> "ActionNetworkPerson":
        data, _ = fetch_hash("people", hash_id)
        person = ActionNetworkPerson.from_hash(data)
        person.persist(conn)
        return person


def import_people(
    query: Optional[str] = None,
    verbose: bool = True,
    skip_pages: int = 0,
    max_pages: int = 0,
) -> int:
    return fetch_all_hashes(
        "people", import_people_from_hashes, query, verbose, skip_pages, max_pages
    )


def import_people_from_hashes(hashes: [dict]):
    with Postgres.get_global_engine().connect() as conn:  # type: Connection
        for data in hashes:
            try:
                person = ActionNetworkPerson.from_hash(data)
                person.persist(conn)
            except ValueError as err:
                print(f"Skipping invalid person: {err}")
        conn.commit()
