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
    ActionNetworkPersistedDict,
    fetch_all_hashes,
    fetch_hash,
    lookup_objects,
)
from ..data_store import model, Database


class ActionNetworkPerson(ActionNetworkPersistedDict):
    def __init__(self, **fields):
        if not fields.get("email") and not fields.get("phone"):
            raise ValueError(f"Person record must have either email or phone: {fields}")
        super().__init__(model.person_info, **fields)

    def classify_for_airtable(self, conn: sa.future.Connection):
        """
        Reclassify an existing person based on current data.
        We are careful never to remove a person from a table.
        """
        # if they existed before 2022, they are a historical volunteer, else a contact
        if self["created_date"] < datetime(2022, 1, 1, tzinfo=timezone.utc):
            self["is_volunteer"] = True
        else:
            self["is_contact"] = True
        if not self["is_contact"]:
            # see if they have submitted the 2022 signup form or any forms in 2022
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
                self["is_contact"] = True
        if not self["is_funder"]:
            # find their most recent donation
            table = model.donation_info
            query = (
                sa.select(table)
                .where(table.c.donor_id == self["uuid"])
                .order_by(table.c.created_date.desc())
            )
            newest = conn.execute(query).mappings().first()
            if newest is not None:
                # if they have donated since 11/1/2021, they are a contact and a funder
                if newest["created_date"] > datetime(2021, 11, 1, tzinfo=timezone.utc):
                    self["is_contact"] = True
                    self["is_funder"] = True
                # if they are a contact and have any donations, they are a funder
                elif self["is_contact"]:
                    self["is_funder"] = True
        self.persist(conn)

    def update_donation_summaries(self, conn: sa.future.Connection):
        self._update_donation_summary(conn, 2020)
        self._update_donation_summary(conn, 2021)
        self.persist(conn)

    def _update_donation_summary(self, conn: sa.future.Connection, year: int):
        total_field, summary_field = f"total_{year}", f"summary_{year}"
        cutoff_hi = datetime(year + 1, 1, 1, tzinfo=timezone.utc)
        cutoff_lo = datetime(year, 1, 1, tzinfo=timezone.utc)
        table = model.donation_info
        query = (
            sa.select(table)
            .where(
                sa.and_(
                    table.c.donor_id == self["uuid"],
                    table.c.created_date < cutoff_hi,
                    table.c.created_date >= cutoff_lo,
                )
            )
            .order_by(table.c.created_date)
        )
        entries, total = [], 0.0
        rows = conn.execute(query).mappings().all()
        for row in rows:
            amount = float(row["amount"])
            day = row["created_date"].strftime("%m/%d/%y")
            total += amount
            entries.append(f"${int(round(amount, 0))} ({day})")
        self[total_field] = int(round(total, 0))
        self[summary_field] = ", ".join(entries)

    @classmethod
    def from_hash(cls, data: dict) -> "ActionNetworkPerson":
        uuid, created_date, modified_date = validate_hash(data)
        is_contact, is_volunteer = None, None
        if created_date.year >= 2022:
            is_contact = True
            # no need to create donation summaries
            total_2020, total_2021 = 0, 0
        else:
            is_volunteer = True
            total_2020, total_2021 = None, None
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
        custom_fields: dict = data.get("custom_fields")
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
            total_2020=total_2020,
            total_2021=total_2021,
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
    with Database.get_global_engine().connect() as conn:  # type: Connection
        for data in hashes:
            try:
                person = ActionNetworkPerson.from_hash(data)
                person.persist(conn)
            except ValueError as err:
                print(f"Skipping invalid person: {err}")
        conn.commit()
