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

from .donation import import_donations
from .fundraising_page import import_fundraising_pages
from .person import import_people, ActionNetworkPerson
from .submission import import_submissions
from ..core import Configuration
from ..data_store import model, Database


def update_people(verbose: bool = True, skip_pages: int = 0, max_pages: int = 0):
    """Import Action Network people created or updated since the last import."""
    config = Configuration.get_global_config()
    start_timestamp = datetime.now(timezone.utc)
    import_people(
        query=get_update_filter(config.get("people_last_update_timestamp")),
        verbose=verbose,
        skip_pages=skip_pages,
        max_pages=max_pages,
    )
    if not max_pages:
        config["people_last_update_timestamp"] = start_timestamp.timestamp()
        config.save_to_data_store()


def update_donations(verbose: bool = True, skip_pages: int = 0, max_pages: int = 0):
    """Import Action Network donations created or updated since the last import."""
    config = Configuration.get_global_config()
    start_timestamp = datetime.now(timezone.utc)
    import_donations(
        query=get_update_filter(config.get("donations_last_update_timestamp")),
        verbose=verbose,
        skip_pages=skip_pages,
        max_pages=max_pages,
    )
    if not max_pages:
        config["donations_last_update_timestamp"] = start_timestamp.timestamp()
        config.save_to_data_store()


def update_fundraising_pages(
    verbose: bool = True, skip_pages: int = 0, max_pages: int = 0
):
    """Import Action Network fundraising pages created or updated since the last import."""
    config = Configuration.get_global_config()
    start_timestamp = datetime.now(timezone.utc)
    import_fundraising_pages(
        query=get_update_filter(config.get("fundraising_pages_last_update_timestamp")),
        verbose=verbose,
        skip_pages=skip_pages,
        max_pages=max_pages,
    )
    if not max_pages:
        config["fundraising_pages_last_update_timestamp"] = start_timestamp.timestamp()
        config.save_to_data_store()


def update_submissions(verbose: bool = True):
    """Import Action Network submissions created or updated since the last report"""
    config = Configuration.get_global_config()
    start_timestamp = datetime.now(timezone.utc)
    import_submissions(
        query=get_update_filter(config.get("submissions_last_update_timestamp")),
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
        print(f"Updating donation summaries for people...", end="")
        progress_time = start_time
    table, sentinel = model.person_info, model.not_computed
    if force:
        query = sa.select(table)
    else:
        query = sa.select(table).where(table.c.total_2020 == sentinel)
    with Database.get_global_engine().connect() as conn:
        people = ActionNetworkPerson.from_query(conn, query)
        for person in people:
            count += 1
            person.update_donation_summaries(conn)
            if verbose and (datetime.now() - progress_time).seconds > 5:
                print(f"({count})...", end="")
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
        print(f"Updating Airtable classifications for people...", end="")
        progress_time = start_time
    table = model.person_info
    query = sa.select(table)
    with Database.get_global_engine().connect() as conn:
        people = ActionNetworkPerson.from_query(conn, query)
        for person in people:
            count += 1
            person.classify_for_airtable(conn)
            if verbose and (datetime.now() - progress_time).seconds > 5:
                print(f"({count})...", end="")
                progress_time = datetime.now()
        conn.commit()
    if verbose:
        print(
            f"({count}) done (in {(datetime.now() - start_time).total_seconds()} secs)."
        )


def get_update_filter(timestamp: Optional[float]) -> Optional[str]:
    if timestamp:
        last_update_time = datetime.fromtimestamp(timestamp, tz=timezone.utc)
        last_update_string = last_update_time.strftime("%Y-%m-%dT%H:%M:%SZ")
        return f"modified_date gt '{last_update_string}'"
