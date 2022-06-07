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

from sqlalchemy.future import Connection

from stv_services.core import Configuration
from stv_services.data_store import model, Postgres
from stv_services.data_store.persisted_dict import PersistedDict
from stv_services.mobilize.utilities import fetch_all_hashes


class MobilizeAttendance(PersistedDict):
    our_org_id = 3073

    def __init__(self, **fields):
        for field in ("event_id", "timeslot_id", "attendee_id"):
            if not fields.get(field):
                raise ValueError(f"Attendances must have field '{field}'")
        super().__init__(model.attendance_info, **fields)

    @classmethod
    def from_hash(cls, body: dict) -> "MobilizeAttendance":
        uuid = body["id"]
        created_date = datetime.fromtimestamp(body["created_date"], tz=timezone.utc)
        modified_date = datetime.fromtimestamp(body["modified_date"], tz=timezone.utc)
        event_id = body["event"]["id"]
        timeslot_id = body["timeslot"]["id"]
        attendee_id = body["person"]["user_id"]
        status = body["status"]
        return cls(
            uuid=uuid,
            created_date=created_date,
            modified_date=modified_date,
            event_id=event_id,
            timeslot_id=timeslot_id,
            attendee_id=attendee_id,
            status=status,
        )


class MobilizeAttendee(PersistedDict):
    def __init__(self, **fields):
        super().__init__(model.attendee_info, **fields)

    @classmethod
    def from_hash(cls, body: dict) -> "MobilizeAttendee":
        # an attendee is a mobilize Person object
        emails: list[dict] = body["email_addresses"]
        if len(emails) != 1:
            raise ValueError("No email in attendee")
        return cls(
            uuid=body["user_id"],
            created_date=datetime.fromtimestamp(body["created_date"], tz=timezone.utc),
            modified_date=datetime.fromtimestamp(
                body["modified_date"], tz=timezone.utc
            ),
            email=emails[0]["address"],
        )


def import_attendances(
    verbose: bool = True, force: bool = False, skip_pages: int = 0, max_pages: int = 0
):
    config = Configuration.get_global_config()
    start_timestamp = datetime.now(tz=timezone.utc)
    query = attendance_query(config.get("attendances_last_update_timestamp"), force)
    fetch_all_hashes(
        "attendances", import_attendance_data, query, verbose, skip_pages, max_pages
    )
    if not max_pages:
        config["attendances_last_update_timestamp"] = start_timestamp.timestamp()
        config.save_to_data_store()


def attendance_query(timestamp: float = None, force: bool = False) -> dict:
    # we never return attendances created/modified before 6/1/2022
    if timestamp and not force:
        return dict(updated_since=int(timestamp))
    else:
        cutoff_lo = datetime(2022, 6, 1, tzinfo=timezone.utc)
        return dict(updated_since=int(cutoff_lo.timestamp()))


def import_attendance_data(data: list[dict]):
    with Postgres.get_global_engine().connect() as conn:  # type: Connection
        for attendance_dict in data:
            person = attendance_dict["person"]
            try:
                attendance = MobilizeAttendance.from_hash(attendance_dict)
            except ValueError:
                # data hiding prevents using this attendance
                continue
            attendance.persist(conn)
            attendee = MobilizeAttendee.from_hash(person)
            attendee.persist(conn)
        conn.commit()
