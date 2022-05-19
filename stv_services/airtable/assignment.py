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
from sqlalchemy.future import Connection

from .schema import fetch_and_validate_table_schema, FieldInfo
from .webhook import register_hook
from ..core import Configuration, Session
from ..data_store.persisted_dict import PersistedDict

assignment_table_name = "Assignments"
assignment_table_schema = {
    "contact_record_id": FieldInfo("Name*", "multipleRecordLinks", "provided"),
    "assignment_name": FieldInfo("Assignment", "singleSelect", "provided"),
    "assignment_status": FieldInfo("Status", "singleSelect", "provided"),
    "summary": FieldInfo("Assignment Summary*", "formula", "immutable"),
    "refcode": FieldInfo("Referrer Code in ActBlue", "singleLineText", "immutable"),
}
field_assignment_map = {
    "2022_calls": "Phone Banker",
    "2022_doors": "Canvasser",
    "2022_recruit": "Recruiter",
    "2022_happyhour": "Happy Hour Host",
    "2022_fundraisepage": "Fundraising Page",
}


def verify_assignment_schema() -> dict:
    config = Configuration.get_global_config()
    base_name = config["airtable_stv_base_name"]
    access_info = fetch_and_validate_table_schema(
        base_name, assignment_table_name, assignment_table_schema
    )
    config["airtable_stv_assignment_schema"] = access_info
    return access_info


def create_assignment_record(
    column_ids: dict, contact_record_id: str, assignment_name: str
) -> dict:
    return {
        column_ids["contact_record_id"]: [contact_record_id],
        column_ids["assignment_name"]: assignment_name,
        column_ids["assignment_status"]: "Signed up for",
    }


def insert_assignments(mapping: dict[str, list[str]]) -> int:
    config = Configuration.get_global_config()
    schema = config["airtable_stv_assignment_schema"]
    column_ids = schema["column_ids"]
    records = []
    for record_id, assignment_names in mapping.items():
        for name in assignment_names:
            records.append(create_assignment_record(column_ids, record_id, name))
    base_id = schema["base_id"]
    table_id = schema["table_id"]
    api = Session.get_airtable_api()
    response: list[dict] = api.batch_create(base_id, table_id, records, typecast=True)
    if Configuration.get_env() == "DEV":
        assert len(response) == len(records)
    return len(response)


def insert_needed_assignments(conn: Connection, people: list[PersistedDict]) -> int:
    """Insert assignments needed for contacts"""
    assignment_map = {}
    for person in people:
        record_id = person["contact_record_id"]
        if not record_id:
            continue
        existing_map = person["contact_assignments"]
        added = {}
        assignments = assignment_map.setdefault(record_id, [])
        for name in person["custom_fields"]:
            if existing_map.get(name):
                continue
            if assignment_name := field_assignment_map.get(name):
                assignments.append(assignment_name)
                added[name] = assignment_name
        if added:
            existing_map.update(added)
            person.persist(conn)
    return insert_assignments(assignment_map)


def register_assignment_hook():
    schema = verify_assignment_schema()
    base = schema["base_id"]
    table = schema["table_id"]
    column_ids = schema["column_ids"]
    targets = [column_ids["refcode"]]
    watches = [column_ids["contact_record_id"]]
    register_hook("assignment", base, table, targets, watches)
