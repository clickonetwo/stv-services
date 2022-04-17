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
from pyairtable import Table

from ..core import Configuration, Session


def insert_airtable_records(schema: dict, records: list[dict]) -> list[str]:
    if not records:
        return []
    api = Session.get_airtable_api()
    base_id = schema["base_id"]
    table_id = schema["table_id"]
    response: list[dict] = api.batch_create(base_id, table_id, records, typecast=True)
    return [row["id"] for row in response]


def update_airtable_records(schema: dict, updates: list[dict]):
    if not updates:
        return
    api = Session.get_airtable_api()
    base_id = schema["base_id"]
    table_id = schema["table_id"]
    response: list[dict] = api.batch_update(
        base_id, table_id, updates, replace=False, typecast=True
    )
    if Configuration.get_env() == "DEV":
        assert set(r["id"] for r in response) == set(u["id"] for u in updates)


def delete_airtable_records(schema: dict, record_ids: list[str]):
    if not record_ids:
        return
    api = Session.get_airtable_api()
    base_id = schema["base_id"]
    table_id = schema["table_id"]
    response: list[dict] = api.batch_delete(base_id, table_id, record_ids)
    if Configuration.get_env() == "DEV":
        assert set(r["id"] for r in response) == set(record_ids)
