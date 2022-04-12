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
from typing import Optional

import sqlalchemy as sa

from .utils import (
    validate_hash,
    ActionNetworkPersistedDict,
    fetch_hashes,
    fetch_hash,
    lookup_hash,
)
from ..data_store import model, Database


class ActionNetworkPerson(ActionNetworkPersistedDict):
    def __init__(self, **fields):
        if not fields.get("email") and not fields.get("phone"):
            raise ValueError(f"Person record must have either email or phone: {fields}")
        super().__init__(model.person_info, **fields)

    @classmethod
    def from_action_network(cls, data: dict) -> "ActionNetworkPerson":
        uuid, created_date, modified_date = validate_hash(data)
        if created_date.year >= 2022:
            # no need to create donation summaries
            total_2020, total_2021 = 0, 0
        else:
            # sentinel value indicating donation summaries needed
            total_2020, total_2021 = -1, -1
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
        )

    @classmethod
    def lookup(
        cls, uuid: Optional[str] = None, email: Optional[str] = None
    ) -> "ActionNetworkPerson":
        if uuid:
            result = lookup_hash(model.person_info, uuid)
        else:
            result = lookup_hash(model.person_info, email.lower(), "email")
        if result is None:
            raise KeyError(f"No person identified by '{uuid or email}'")
        fields = {key: value for key, value in result.items() if value is not None}
        return cls(**fields)


def load_person(hash_id: str) -> ActionNetworkPerson:
    data = fetch_hash("people", hash_id)
    person = ActionNetworkPerson.from_action_network(data)
    person.persist()
    return person


def load_people(
    query: Optional[str] = None,
    verbose: bool = True,
    skip_pages: int = 0,
    max_pages: int = 0,
) -> int:
    def insert_from_hashes(hashes: [dict]):
        with Database.get_global_engine().connect() as conn:
            for data in hashes:
                try:
                    person = ActionNetworkPerson.from_action_network(data)
                    person.persist(conn)
                except ValueError as err:
                    if verbose:
                        print(f"Skipping invalid person: {err}")
            conn.commit()

    return fetch_hashes(
        "people", insert_from_hashes, query, verbose, skip_pages, max_pages
    )


def compute_donation_summary(
    conn: sa.engine.Connection, person_id: str, year: int
) -> (int, str):
    cutoff_hi = datetime(year + 1, 1, 1, tzinfo=timezone.utc)
    cutoff_lo = datetime(year, 1, 1, tzinfo=timezone.utc)
    table = model.donation_info
    query = (
        sa.select(table)
        .where(
            sa.and_(
                table.c.donor_id == person_id,
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
    return int(round(total, 0)), ", ".join(entries)
