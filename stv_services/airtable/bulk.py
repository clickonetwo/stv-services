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

from stv_services.airtable.contact import (
    upsert_contacts,
    verify_contact_schema,
    delete_contacts,
)
from stv_services.airtable.funder import (
    upsert_funders,
    verify_funder_schema,
    delete_funders,
)
from stv_services.airtable.utils import find_people_to_update
from stv_services.airtable.volunteer import (
    upsert_volunteers,
    verify_volunteer_schema,
    delete_volunteers,
)
from stv_services.core import Configuration
from stv_services.data_store import Database


def verify_schemas(verbose: bool = True):
    config = Configuration.get_global_config()
    if verbose:
        print("Verifying contact schema...")
    verify_contact_schema()
    if verbose:
        print("Verifying volunteer schema...")
    verify_volunteer_schema()
    if verbose:
        print("Verifying funder schema...")
    verify_funder_schema()
    if verbose:
        print("Saving verified schemas...")
    config.save_to_data_store()


def update_contacts(verbose: bool = True, force: bool = False):
    with Database.get_global_engine().connect() as conn:
        people = find_people_to_update(conn, "contact", force)
        if verbose:
            print(f"Updating {len(people)} contacts...")
        inserts, updates = upsert_contacts(conn, people)
        if verbose:
            print(f"Inserted {inserts} new and updated {updates} existing contacts.")
        conn.commit()


def update_volunteers(verbose: bool = True, force: bool = False):
    with Database.get_global_engine().connect() as conn:
        people = find_people_to_update(conn, "volunteer", force)
        if verbose:
            print(f"Updating {len(people)} volunteers...")
        count = upsert_volunteers(conn, people)
        if verbose:
            print(f"Updated {count} volunteers.")
        conn.commit()


def update_funders(verbose: bool = True, force: bool = False):
    with Database.get_global_engine().connect() as conn:
        people = find_people_to_update(conn, "funder", force)
        if verbose:
            print(f"Updating {len(people)} funders...")
        count = upsert_funders(conn, people)
        if verbose:
            print(f"Updated {count} funders.")
        conn.commit()


def remove_contacts(verbose: bool = True):
    with Database.get_global_engine().connect() as conn:
        people = find_people_to_update(conn, "contact", True)
        if verbose:
            print(f"Deleting {len(people)} contacts...")
        count = delete_contacts(conn, people)
        if verbose:
            print(f"Deleted {count} contacts.")
        conn.commit()


def remove_volunteers(verbose: bool = True):
    with Database.get_global_engine().connect() as conn:
        people = find_people_to_update(conn, "volunteer", True)
        if verbose:
            print(f"Deleting {len(people)} volunteers...")
        count = delete_volunteers(conn, people)
        if verbose:
            print(f"Deleted {count} volunteers.")
        conn.commit()


def remove_funders(verbose: bool = True):
    with Database.get_global_engine().connect() as conn:
        people = find_people_to_update(conn, "funder", True)
        if verbose:
            print(f"Deleting {len(people)} funders...")
        count = delete_funders(conn, people)
        if verbose:
            print(f"Deleted {count} funders.")
        conn.commit()
