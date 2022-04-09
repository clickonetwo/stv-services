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
import time
from datetime import datetime
from typing import Optional
from urllib.parse import urlencode

import requests
import sqlalchemy as sa
from dateutil.parser import parse
from restnavigator import Navigator
from sqlalchemy.dialects import postgresql as psql

from ..core import Configuration
from ..core import Session
from ..data_store import model, Database


class ActionNetworkPerson(dict):
    def __init__(self, **fields):
        if not fields.get("uuid"):
            raise ValueError("uuid field cannot be empty")
        if not fields.get("email") and not fields.get("phone"):
            raise ValueError("email and phone fields cannot both be empty")
        fields = {key: value for key, value in fields.items() if value is not None}
        super().__init__(**fields)

    @classmethod
    def from_action_network(cls, data: dict) -> "ActionNetworkPerson":
        if not isinstance(data, dict) or len(data) == 0:
            raise ValueError(f"Not a valid Action Network response: {data}")
        uuid: Optional[str] = None
        for id in data.get("identifiers", []):  # type: str
            if id.startswith("action_network:"):
                uuid = id
                break
        created_date: datetime = parse(data.get("created_date"))
        modified_date: datetime = parse(data.get("modified_date"))
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
        )

    def persist(self):
        insert_fields = {key: value for key, value in self.items() if value is not None}
        update_fields = {
            key: value for key, value in insert_fields.items() if key != "uuid"
        }
        insert_query = psql.insert(model.person_info).values(insert_fields)
        upsert_query = insert_query.on_conflict_do_update(
            index_elements=["uuid"], set_=update_fields
        )
        with Database.get_global_engine().connect() as conn:
            conn.execute(upsert_query)
            conn.commit()
        pass

    def reload(self):
        query = sa.select(model.person_info).where(
            model.person_info.c.uuid == self["uuid"]
        )
        with Database.get_global_engine().connect() as conn:
            result = conn.execute(query).first()
            if result is None:
                raise KeyError(f"Can't reload person identified by '{self['uuid']}'")
            fields = {
                key: value
                for key, value in result._asdict().items()
                if value is not None
            }
        self.clear()
        self.update(fields)
        pass

    def remove(self):
        query = sa.delete(model.person_info).where(
            model.person_info.c.uuid == self["uuid"]
        )
        with Database.get_global_engine().connect() as conn:
            conn.execute(query)
            conn.commit()

    @classmethod
    def lookup(
        cls, uuid: Optional[str] = None, email: Optional[str] = None
    ) -> "ActionNetworkPerson":
        if uuid:
            query = sa.select(model.person_info).where(model.person_info.c.uuid == uuid)
        elif email:
            query = sa.select(model.person_info).where(
                model.person_info.c.email == email.lower()
            )
        else:
            raise ValueError("One of uuid or email must be supplied for lookup")
        with Database.get_global_engine().connect() as conn:
            result = conn.execute(query).first()
        if result is None:
            raise KeyError("No person identified by '{uuid or email}'")
        fields = {
            key: value for key, value in result._asdict().items() if value is not None
        }
        return cls(**fields)


def load_person(an_id: str) -> ActionNetworkPerson:
    if not an_id.startswith("action_network:"):
        raise ValueError(f"Not an action network identifier: '{an_id}'")
    else:
        uuid = an_id[len("action_network:") :]
    config = Configuration.get_global_config()
    session = Session.get_global_session("action_network")
    url = config["action_network_api_base_url"] + f"/people/{uuid}"
    nav = Navigator.hal(url, session=session)
    data: dict = nav(raise_exc=False)
    response: requests.Response = nav.response
    if response.status_code == 404:
        raise KeyError("No person identified by '{an_id}'")
    if response.status_code != 200:
        response.raise_for_status()
    person = ActionNetworkPerson.from_action_network(data)
    person.persist()
    return person


def load_people(query: Optional[str] = None, verbose: bool = True) -> int:
    config = Configuration.get_global_config()
    session = Session.get_global_session("action_network")
    url = config["action_network_api_base_url"] + "/people"
    if query:
        query_string = urlencode({"filter": query})
        url += f"?{query_string}"
        if verbose:
            print(f"Loading people from Action Network matching filter={query}...")
    else:
        if verbose:
            print("Loading all people from Action Network...")
    start_time = datetime.now()
    start_process_time = time.process_time()
    pages = Navigator.hal(url, session=session)
    total_count, total_pages = 0, None
    for i, page in enumerate(pages):
        people = page.embedded()["osdi:people"]
        if (page_count := len(people)) == 0:
            break
        total_count += page_count
        if verbose:
            if total_pages := page.state.get("total_pages", total_pages):
                print(f"Processing {page_count} people on page {i+1}/{total_pages}")
            else:
                print(f"Processing {page_count} people on page {i+1}...")
        for person in people:
            hash: dict = person.state
            person = ActionNetworkPerson.from_action_network(hash)
            person.persist()
    elapsed_process_time = time.process_time() - start_process_time
    elapsed_time = datetime.now() - start_time
    if verbose:
        print(f"Loaded {total_count} people from Action Network.")
        print(
            f"Load time was {elapsed_time} (processor time: {elapsed_process_time} seconds)."
        )
    return total_count
