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
import csv
from collections import namedtuple

import sqlalchemy as sa

from stv_services.data_store import Database, model

FieldInfo = namedtuple("FieldInfo", ["db_field", "db_type"])

field_map = {
    "Email*": FieldInfo("email", "Text"),
    "Participated Shift Count 2020": FieldInfo("shifts_2020", "Integer"),
    "Organized Event Count 2020": FieldInfo("events_2020", "Integer"),
    "Connected Org": FieldInfo("connect_2020", "Text"),
    "Assignments 2020": FieldInfo("assigns_2020", "Text"),
    "Notes": FieldInfo("notes_2020", "Text"),
    "Contact History": FieldInfo("history_2020", "Text"),
    "Fundraise*": FieldInfo("fundraise_2020", "Boolean"),
    "Door Knocking*": FieldInfo("doorknock_2020", "Boolean"),
    "Phone banking*": FieldInfo("phonebank_2020", "Boolean"),
    "Recruit*": FieldInfo("recruit_2020", "Boolean"),
    "GA Delegation": FieldInfo("delegate_ga_2020", "Boolean"),
    "PA Delegation": FieldInfo("delegate_pa_2020", "Boolean"),
    "AZ Delegation": FieldInfo("delegate_az_2020", "Boolean"),
    "FL Delegation": FieldInfo("delegate_fl_2020", "Boolean"),
}


def import_spreadsheet(file_path: str, verbose: bool = False) -> (int, int):
    with open(file_path, newline="") as csv_file:
        reader = csv.DictReader(csv_file)
        vals = {}
        for i, row in enumerate(reader, start=2):  # type: int, dict
            row_vals = {}
            for key, val in row.items():
                if info := field_map.get(key):
                    if info.db_type == "Text":
                        row_vals[info.db_field] = val
                    elif info.db_type == "Integer":
                        row_vals[info.db_field] = int(val) if val else 0
                    elif info.db_type == "Boolean":
                        row_vals[info.db_field] = not (not val)
                    else:
                        raise ValueError(
                            f"Unknown database type '{info.db_type}' for field '{key}'"
                        )
            email = row_vals.get("email")
            if not email:
                if verbose:
                    print(f"Skipping row {i} because it has no email.")
                continue
            row_vals["input row"] = i
            if prior := vals.get(email):
                if verbose:
                    print(
                        f"Discarding row {prior['input row']} because '{email}' is also on row {i}."
                    )
            vals[email] = row_vals
    with Database.get_global_engine().connect() as conn:
        # out with the old
        conn.execute(sa.delete(model.external_info))
        # in with the new
        conn.execute(sa.insert(model.external_info), list(vals.values()))
        conn.commit()
    return len(vals), i - 1
