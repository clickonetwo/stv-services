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
from time import process_time
from typing import Callable
from urllib.parse import urlencode

from sqlalchemy.future import Connection

from ..core import Configuration, Session
from ..core.logging import get_logger

logger = get_logger(__name__)


def fetch_all_hashes(
    hash_type: str,
    page_processor: Callable[[list[dict]], int],
    query: list[tuple] = None,
    verbose: bool = True,
) -> int:
    config = Configuration.get_global_config()
    url = config["mobilize_api_base_url"] + f"/{hash_type}"
    if verbose:
        if query:
            logger.info(f"Fetching {hash_type} matching {query}...")
        else:
            logger.info(f"Fetching all {hash_type}...")
    if query:
        url += "?" + urlencode(query)
    return fetch_hash_pages(
        hash_type=hash_type,
        url=url,
        page_processor=page_processor,
        verbose=verbose,
    )


def fetch_hash_pages(
    hash_type: str,
    url: str,
    page_processor: Callable[[list[dict]], int],
    verbose: bool = True,
) -> int:
    start_time = datetime.now()
    start_process_time = process_time()
    session = Session.get_global_session("mobilize")
    page_number, total_count, import_count = 0, 0, 0
    while url:
        response = session.get(url)
        response.raise_for_status()
        body = response.json()
        page_number += 1
        url = body.get("next")
        data = body.get("data", [])
        page_count = len(data)
        if page_count == 0:
            break
        if verbose:
            logger.info(f"Processing {page_count} {hash_type} on page {page_number}...")
        import_count += page_processor(data)
        total_count += page_count
        if verbose:
            logger.info(f"({import_count}/{total_count})")
    elapsed_process_time = process_time() - start_process_time
    elapsed_time = datetime.now() - start_time
    if verbose:
        logger.info(
            f"Imported {import_count} "
            f"of {total_count} {hash_type} "
            f"fetched on {page_number} page(s)."
        )
        logger.info(
            f"Fetch time was {elapsed_time} (processor time: {elapsed_process_time} seconds)."
        )
    return total_count


def compute_status(conn: Connection, objects: list, verbose: bool, force: bool):
    total, count, start_time = len(objects), 0, datetime.now(tz=timezone.utc)
    progress_time = start_time
    for obj in objects:
        count += 1
        obj.compute_status(conn, force)
        now = datetime.now(tz=timezone.utc)
        obj.persist(conn)
        if verbose and (now - progress_time).seconds > 5:
            logger.info(f"({count})...")
            progress_time = now
    if verbose:
        now = datetime.now(tz=timezone.utc)
        logger.info(f"({count}) done (in {(now - start_time).total_seconds()} secs).")
