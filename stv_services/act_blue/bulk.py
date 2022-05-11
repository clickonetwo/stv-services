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
import json
import os
import sqlalchemy as sa

from sqlalchemy.future import Connection

from .metadata import import_metadata_from_webhooks
from ..core.utilities import action_network_timestamp
from ..data_store import Postgres, model
from ..worker.act_blue import process_webhook_notification


def import_donation_metadata(filepath: str, verbose: bool = True):
    if verbose:
        print(f"Importing ActBlue webhooks from '{filepath}'...")
    batch: list[str] = []
    with open(filepath) as file:
        while line := file.readline():
            batch.append(line.strip())
    with Postgres.get_global_engine().connect() as conn:  # type: Connection
        # out with the old
        conn.execute(sa.delete(model.external_info))
        conn.commit()
    # in with the new
    total, imported = 0, 0
    for line in batch:
        total += 1
        imported += 1 if process_webhook_notification(line) else 0
        if verbose and total % 100 == 0:
            print(f"Processed {total}, kept {imported}...")
    if verbose:
        print(f"Imported {imported} metadata records from {total} webhooks.")
