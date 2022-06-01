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
from sqlalchemy.future import Connection

from stv_services.data_store import Postgres, model

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
    with open(file_path, newline="", encoding="utf-8") as csv_file:
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
            email = row_vals.get("email", "").strip().lower()
            if not email:
                if verbose:
                    print(f"Skipping row {i} because it has no email.")
                continue
            row_vals["input row"] = i
            if prior := vals.get(email):
                if verbose:
                    print(
                        f"Discarding row {prior['input row']} "
                        f"because '{email}' is also on row {i}."
                    )
            vals[email] = row_vals
    with Postgres.get_global_engine().connect() as conn:  # type: Connection
        # out with the old
        conn.execute(sa.delete(model.external_info))
        # in with the new
        conn.execute(sa.insert(model.external_info), list(vals.values()))
        conn.commit()
    return len(vals), i - 1


def update_spreadsheet(
    update_file_path: str = "local/external_data_update.csv",
    existing_file_path: str = "local/external_data.csv",
    new_file_path: str = "local/updated_external_data.csv",
    updated_emails_path: str = "local/updated_emails.txt",
    verbose: bool = True,
) -> (int, int):
    reverse_field_map = {v.db_field: k for k, v in field_map.items()}
    with open(update_file_path, newline="", encoding="utf-8") as update_file:
        reader = csv.DictReader(update_file)
        updates = {}
        for i, row in enumerate(reader, start=2):  # type: int, dict
            row["input row"] = i
            email: str = row.get("Email*", "").strip().lower()
            if not email:
                if verbose:
                    print(f"Skipping update row {i} because it has no email")
                continue
            if updates.get(email):
                if verbose:
                    print(
                        f"Discarding update row {updates[email]['input row']} "
                        f"because '{email}' is also on row {i}."
                    )
            updates[email] = row
    if verbose:
        print(f"Found {len(updates)} email(s) with updated data.")
    with open(existing_file_path, newline="", encoding="utf-8") as in_file:
        reader = csv.DictReader(in_file)
        with open(new_file_path, mode="w", newline="", encoding="utf-8") as out_file:
            writer = csv.DictWriter(out_file, reader.fieldnames)
            writer.writeheader()
            updated = []
            for i, row in enumerate(reader, start=2):
                email = row.get("Email*", "").strip().lower()
                if vals := updates.get(email):
                    updated.append(email)
                    if verbose:
                        print(f"Updating row #{i+2} for '{email}'.")
                    del vals["Email*"]  # don't update the match key
                    vals = {k: v for k, v in vals.items() if k in reader.fieldnames}
                    row.update(vals)
                    del updates[email]
                writer.writerow(row)
        with open(updated_emails_path, "w", encoding="utf-8") as email_file:
            for email in updated:
                print(email, file=email_file)
    if verbose:
        print(f"Updated {len(updated)} row(s).")
    if verbose and len(updates) > 0:
        print(f"The following emails were not found to update: {list(updates.keys())}")
    return len(updated), len(updated) + len(updates)


def parse_row_vals(row: dict, preserve_input=False) -> dict:
    row_vals = {}
    for key, val in row.items():
        if info := field_map.get(key):
            if preserve_input or info.db_type == "Text":
                row_vals[info.db_field] = val
            elif info.db_type == "Integer":
                row_vals[info.db_field] = int(val) if val else 0
            elif info.db_type == "Boolean":
                row_vals[info.db_field] = not (not val)
            else:
                raise ValueError(
                    f"Unknown database type '{info.db_type}' for field '{key}'"
                )
        elif preserve_input:
            row_vals[key] = val
    return row_vals
