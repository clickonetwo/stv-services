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
from typing import Callable

from sqlalchemy.future import Connection

from stv_services.action_network.donation import ActionNetworkDonation
from stv_services.action_network.person import ActionNetworkPerson
from stv_services.airtable import webhook
from stv_services.airtable.assignment import (
    verify_assignment_schema,
    insert_needed_assignments,
)
from stv_services.airtable.contact import (
    verify_contact_schema,
    create_contact_record,
    register_contact_hook,
)
from stv_services.airtable.donation import (
    create_donation_record,
    verify_donation_schema,
)
from stv_services.airtable.funder import verify_funder_schema, create_funder_record
from stv_services.airtable.utils import (
    find_records_to_update,
    delete_records,
    upsert_records,
)
from stv_services.airtable.volunteer import (
    verify_volunteer_schema,
    create_volunteer_record,
    register_volunteer_hook,
)
from stv_services.core import Configuration
from stv_services.data_store import Postgres
from stv_services.data_store.persisted_dict import PersistedDict
from stv_services.worker.airtable import process_webhook_notification


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
        print("Verifying donation schema...")
    verify_donation_schema()
    if verbose:
        print("Verifying assignment schema...")
    verify_assignment_schema()
    if verbose:
        print("Saving verified schemas...")
    config.save_to_data_store()


def update_contacts(verbose: bool = True, force: bool = False):
    with Postgres.get_global_engine().connect() as conn:  # type: Connection
        if verbose:
            print(f"Loading person data for contacts...")
        people = ActionNetworkPerson.from_query(
            conn, find_records_to_update("contact", force)
        )
    bulk_upsert_records("contact", create_contact_record, people, verbose)


def update_volunteers(verbose: bool = True, force: bool = False):
    with Postgres.get_global_engine().connect() as conn:  # type: Connection
        if verbose:
            print(f"Loading person data for historical volunteers...")
        people = ActionNetworkPerson.from_query(
            conn, find_records_to_update("volunteer", force)
        )
    bulk_upsert_records("volunteer", create_volunteer_record, people, verbose)


def update_funders(verbose: bool = True, force: bool = False):
    with Postgres.get_global_engine().connect() as conn:  # type: Connection
        if verbose:
            print(f"Loading person data for funders...")
        people = ActionNetworkPerson.from_query(
            conn, find_records_to_update("funder", force)
        )
    bulk_upsert_records("funder", create_funder_record, people, verbose)


def update_donation_records(verbose: bool = True, force: bool = False):
    with Postgres.get_global_engine().connect() as conn:  # type: Connection
        if verbose:
            print(f"Loading donation data...")
        donations = ActionNetworkDonation.from_query(
            conn, find_records_to_update("donation", force)
        )
    bulk_upsert_records("donation", create_donation_record, donations, verbose)


def bulk_upsert_records(
    record_type: str,
    record_maker: Callable,
    dicts: list[PersistedDict],
    verbose: bool = True,
):
    total, inserts, updates = len(dicts), 0, 0
    if verbose:
        print(f"Updating {total} {record_type} records...", flush=True)
    for start in range(0, total, 100):
        if verbose and inserts + updates > 0:
            print(f"({inserts+updates})...", flush=True)
        with Postgres.get_global_engine().connect() as conn:  # type: Connection
            pairs = [(p_dict, record_maker(conn, p_dict)) for p_dict in dicts]
            i, u = upsert_records(conn, record_type, pairs[start : start + 100])
            # now insert any needed assignments for contacts
            if record_type == "contact":
                insert_needed_assignments(conn, dicts)
            conn.commit()
        inserts += i
        updates += u
    if verbose:
        print(f"({inserts+updates})")
        print(f"Updated {inserts+updates} records ({inserts} new, {updates} existing).")


def remove_contacts(verbose: bool = True):
    with Postgres.get_global_engine().connect() as conn:  # type: Connection
        people = ActionNetworkPerson.from_query(
            conn, find_records_to_update("contact", True)
        )
    bulk_remove_records("person", people, verbose)


def remove_volunteers(verbose: bool = True):
    with Postgres.get_global_engine().connect() as conn:  # type: Connection
        people = ActionNetworkPerson.from_query(
            conn, find_records_to_update("volunteer", True)
        )
    bulk_remove_records("volunteer", people, verbose)


def remove_funders(verbose: bool = True):
    with Postgres.get_global_engine().connect() as conn:  # type: Connection
        people = ActionNetworkPerson.from_query(
            conn, find_records_to_update("funder", True)
        )
    bulk_remove_records("funder", people, verbose)


def remove_donation_records(verbose: bool = True):
    with Postgres.get_global_engine().connect() as conn:  # type: Connection
        donations = ActionNetworkDonation.from_query(
            conn, find_records_to_update("donation", True)
        )
    bulk_remove_records("donation", donations, verbose)


def bulk_remove_records(
    record_type: str,
    dicts: list[PersistedDict],
    verbose: bool = True,
):
    total, deletes = len(dicts), 0
    if verbose:
        print(f"Deleting {total} {record_type} records...", flush=True)
    for start in range(0, total, 100):
        if verbose and deletes > 0:
            print(f"({deletes})...", flush=True)
        with Postgres.get_global_engine().connect() as conn:  # type: Connection
            deletes += delete_records(conn, record_type, dicts[start : start + 100])
            conn.commit()
    if verbose:
        print(f"({deletes})")
        print(f"Deleted {deletes} records.")


def register_webhooks(verbose: bool = True, sync_first: bool = False):
    Configuration.get_global_config(reload=True)
    if sync_first:
        sync_webhooks(verbose)
    if verbose:
        print(f"Registering contact webhook...")
    register_contact_hook()
    if verbose:
        print(f"Registering volunteer webhook...")
    register_volunteer_hook()
    if verbose:
        print(f"Done.")


def sync_webhooks(verbose: bool = True, force_remove: bool = False):
    if verbose:
        print(f"Syncing webhooks against Airtable...")
    webhook.sync_hooks(verbose, force_remove)
    if verbose:
        print(f"Done.")


def fetch_and_process_all_webhooks(verbose: bool = True):
    with Postgres.get_global_engine().connect() as conn:  # type: Connection
        if verbose:
            print(f"Fetching and processing 'volunteer' Airtable payloads...")
        process_webhook_notification(conn, "volunteer")
        if verbose:
            print(f"Fetching and processing 'contact' Airtable payloads...")
        process_webhook_notification(conn, "contact")
        if verbose:
            print(f"Done.")
        conn.commit()
