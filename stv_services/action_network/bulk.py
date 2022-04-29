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
from sqlalchemy.future import Connection

from .donation import import_donations, import_donations_from_hashes
from .fundraising_page import import_fundraising_pages
from .person import import_people, ActionNetworkPerson
from .submission import import_submissions, insert_submissions_from_hashes
from .utils import fetch_related_hashes, fetch_hash
from ..core import Configuration
from ..data_store import model, Postgres


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


def update_donation_summaries(verbose: bool = True, force: bool = False):
    """
    Compute and save fundraising summaries for all people who don't have them.

    If `force` is `True`, then summaries are updated for all people.
    """
    count, start_time = 0, datetime.now()
    if verbose:
        print(f"Updating donation summaries for people...", end="", flush=True)
        progress_time = start_time
    table, sentinel = model.person_info, model.not_computed
    if force:
        query = sa.select(table)
    else:
        query = sa.select(table).where(table.c.total_2020 == sentinel)
    with Postgres.get_global_engine().connect() as conn:  # type: Connection
        people = ActionNetworkPerson.from_query(conn, query)
        for person in people:
            count += 1
            person.update_donation_summaries(conn)
            if verbose and (datetime.now() - progress_time).seconds > 5:
                print(f"({count})...", end="", flush=True)
                progress_time = datetime.now()
        conn.commit()
    if verbose:
        print(
            f"({count}) done (in {(datetime.now() - start_time).total_seconds()} secs)."
        )


def update_airtable_classifications(verbose: bool = True):
    """
    Make sure every person is marked with the correct table(s) for Airtable injection.

    Args:
        verbose: print progress reports
    """
    count, start_time = 0, datetime.now()
    if verbose:
        print(f"Updating Airtable classifications for people...", end="", flush=True)
        progress_time = start_time
    table = model.person_info
    query = sa.select(table)
    with Postgres.get_global_engine().connect() as conn:  # type: Connection
        people = ActionNetworkPerson.from_query(conn, query)
        for person in people:
            count += 1
            person.classify_for_airtable(conn)
            if verbose and (datetime.now() - progress_time).seconds > 5:
                print(f"({count})...", end="", flush=True)
                progress_time = datetime.now()
        conn.commit()
    if verbose:
        print(
            f"({count}) done (in {(datetime.now() - start_time).total_seconds()} secs)."
        )


def get_update_filter(timestamp: Optional[float], force: bool = False) -> Optional[str]:
    if timestamp and not force:
        last_update_time = datetime.fromtimestamp(timestamp, tz=timezone.utc)
        last_update_string = last_update_time.strftime("%Y-%m-%dT%H:%M:%SZ")
        return f"modified_date gt '{last_update_string}'"
