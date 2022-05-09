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

from .donation import (
    import_donations,
    import_donations_from_hashes,
    ActionNetworkDonation,
)
from .fundraising_page import import_fundraising_pages, ActionNetworkFundraisingPage
from .person import import_people, ActionNetworkPerson
from .submission import import_submissions, insert_submissions_from_hashes
from .utils import fetch_related_hashes, fetch_hash
from ..act_blue.attribution import calculate_attribution
from ..core import Configuration
from ..core.utilities import action_network_timestamp
from ..data_store import model, Postgres
from ..data_store.persisted_dict import PersistedDict


def import_person_cluster(person_id: str, verbose: bool = False):
    if verbose:
        print(f"Fetching person '{person_id}' and their donations and submissions...")
    data, links = fetch_hash("people", person_id)
    person = ActionNetworkPerson.from_hash(data)
    with Postgres.get_global_engine().connect() as conn:  # type: Connection
        person.persist(conn)
        conn.commit()
    for curie, nav in links.items():
        if curie == "osdi:submissions":
            fetch_related_hashes(
                url=nav.uri,
                hash_type="submissions",
                page_processor=insert_submissions_from_hashes,
                verbose=verbose,
            )
        elif curie == "osdi:donations":
            fetch_related_hashes(
                url=nav.uri,
                hash_type="donations",
                page_processor=import_donations_from_hashes,
                verbose=verbose,
            )
    return person


def update_people(
    verbose: bool = True, force: bool = False, skip_pages: int = 0, max_pages: int = 0
):
    """Import Action Network people created or updated since the last import."""
    config = Configuration.get_global_config()
    start_timestamp = datetime.now(timezone.utc)
    import_people(
        query=get_update_filter(config.get("people_last_update_timestamp"), force),
        verbose=verbose,
        skip_pages=skip_pages,
        max_pages=max_pages,
    )
    if not max_pages:
        config["people_last_update_timestamp"] = start_timestamp.timestamp()
        config.save_to_data_store()


def update_donations(
    verbose: bool = True, force: bool = False, skip_pages: int = 0, max_pages: int = 0
):
    """Import Action Network donations created or updated since the last import."""
    config = Configuration.get_global_config()
    start_timestamp = datetime.now(timezone.utc)
    import_donations(
        query=get_update_filter(config.get("donations_last_update_timestamp"), force),
        verbose=verbose,
        skip_pages=skip_pages,
        max_pages=max_pages,
    )
    if not max_pages:
        config["donations_last_update_timestamp"] = start_timestamp.timestamp()
        config.save_to_data_store()


def update_fundraising_pages(
    verbose: bool = True, force: bool = False, skip_pages: int = 0, max_pages: int = 0
):
    """Import Action Network fundraising pages created or updated since the last import."""
    config = Configuration.get_global_config()
    start_timestamp = datetime.now(timezone.utc)
    import_fundraising_pages(
        query=get_update_filter(
            config.get("fundraising_pages_last_update_timestamp"), force
        ),
        verbose=verbose,
        skip_pages=skip_pages,
        max_pages=max_pages,
    )
    if not max_pages:
        config["fundraising_pages_last_update_timestamp"] = start_timestamp.timestamp()
        config.save_to_data_store()


def update_submissions(verbose: bool = True, force: bool = False):
    """Import Action Network submissions created or updated since the last report"""
    config = Configuration.get_global_config()
    start_timestamp = datetime.now(timezone.utc)
    import_submissions(
        query=get_update_filter(config.get("submissions_last_update_timestamp"), force),
        verbose=verbose,
    )
    config["submissions_last_update_timestamp"] = start_timestamp.timestamp()
    config.save_to_data_store()


def get_update_filter(timestamp: Optional[float], force: bool = False) -> Optional[str]:
    if timestamp and not force:
        last_update_time = datetime.fromtimestamp(timestamp, tz=timezone.utc)
        last_update_string = action_network_timestamp(last_update_time)
        return f"modified_date gt '{last_update_string}'"


def update_all_classifications(verbose: bool = True, force: bool = False):
    """
    Update the classifications for Action Network objects that have been
    updated since they were last classified.  This has to be done in a
    particular order.

    Args:
        verbose: print progress reports
        force: recompute status for all objects whether updated or not
    """
    update_classifications("fundraising_pages", verbose, force)
    update_classifications("donations", verbose, force)
    update_classifications("people", verbose, force)


def update_classifications(plural: str, verbose: bool = True, force: bool = False):
    """
    Update the classifications for a specific type of object
    """
    table, cls = get_classification_parameters(plural)
    if force:
        query = sa.select(table)
    else:
        query = sa.select(table).where(table.c.modified_date >= table.c.published_date)
    with Postgres.get_global_engine().connect() as conn:  # type: Connection
        objects = cls.from_query(conn, query)
        total, count, start_time = len(objects), 0, datetime.now()
        if verbose:
            print(
                f"Updating classifications for {total} {plural}...", end="", flush=True
            )
            progress_time = start_time
        for person in objects:
            count += 1
            person.publish(conn)
            if verbose and (datetime.now() - progress_time).seconds > 5:
                print(f"({count})...", end="", flush=True)
                progress_time = datetime.now()
        conn.commit()
    if verbose:
        print(
            f"({count}) done (in {(datetime.now() - start_time).total_seconds()} secs)."
        )


def get_classification_parameters(plural: str):
    if str == "people":
        return model.person_info, ActionNetworkPerson
    elif str == "donations":
        return model.donation_info, ActionNetworkDonation
    elif str == "fundraising_pages":
        return model.fundraising_page_info, ActionNetworkFundraisingPage
    else:
        raise ValueError(f"Don't know how to publish {plural}")
