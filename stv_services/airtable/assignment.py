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

from .schema import fetch_and_validate_table_schema, FieldInfo
from ..core import Configuration, Session

assignment_table_name = "Assignments"
assignment_table_schema = {
    "contact_record_id": FieldInfo("Name*", "multipleRecordLinks", "provided"),
    "summary": FieldInfo("Assignment Summary*", "formula", "immutable"),
}


def verify_assignment_schema() -> dict:
    config = Configuration.get_global_config()
    base_name = config.get("airtable_stv_base_name")
    access_info = fetch_and_validate_table_schema(
        base_name, assignment_table_name, assignment_table_schema
    )
    config["airtable_stv_assignment_schema"] = access_info
    return access_info


def create_assignment_record(column_ids: dict, contact_record_id: str) -> dict:
    return {column_ids["contact_record_id"]: [contact_record_id]}


def insert_empty_assignments(contact_record_ids: list[str]) -> int:
    config = Configuration.get_global_config()
    schema = config["airtable_stv_assignment_schema"]
    column_ids = schema["column_ids"]
    records = [
        create_assignment_record(column_ids, record_id)
        for record_id in contact_record_ids
    ]
    base_id = schema["base_id"]
    table_id = schema["table_id"]
    api = Session.get_airtable_api()
    response: list[dict] = api.batch_create(base_id, table_id, records, typecast=True)
    if Configuration.get_env() == "DEV":
        assert len(response) == len(records)
    return len(response)
