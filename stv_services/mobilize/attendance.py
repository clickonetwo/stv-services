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
from typing import Any, ClassVar

import sqlalchemy as sa

from sqlalchemy.future import Connection

from stv_services.action_network.person import ActionNetworkPerson
from stv_services.core import Configuration
from stv_services.data_store import model, Postgres
from stv_services.data_store.persisted_dict import PersistedDict, lookup_objects
from stv_services.mobilize.event import MobilizeEvent
from stv_services.mobilize.utilities import fetch_all_hashes


class MobilizeAttendance(PersistedDict):
    _sentinel: ClassVar[dict] = dict(none=None)
    attendees: ClassVar[dict[str, ActionNetworkPerson]] = _sentinel
    attendee_counts: ClassVar[list[int]] = [0, 0, 0]  # hit, miss, unknown
    events: ClassVar[dict[str, MobilizeEvent]] = _sentinel
    our_org_id = 3073

    def __init__(self, **fields):
        for field in ("event_id", "timeslot_id", "attendee_id"):
            if not fields.get(field):
                raise ValueError(f"Attendances must have field '{field}'")
        super().__init__(model.attendance_info, **fields)

    def compute_status(self, conn: Connection, force: bool = False):
        if force or not self.get("person_id"):
            if person := self.attendees.get(self["email"]):
                self.attendee_counts[0] += 1
                self.notice_person(conn, person)
            else:
                try:
                    person = ActionNetworkPerson.from_lookup(conn, email=self["email"])
                    self.attendee_counts[1] += 1
                    self.notice_person(conn, person)
                except KeyError:
                    # no such person
                    self["updated_date"] = datetime.now(tz=timezone.utc)
                    self.attendee_counts[2] += 1
        if force or self.get("updated_date", model.epoch) == model.epoch:
            event_id = self["event_id"]
            event = self.events[event_id]
            if not event:
                event = MobilizeEvent.from_lookup(conn, event_id)
                self.events[event_id] = event
            self.notice_event(conn, event)

    def notice_person(self, conn: Connection, person: ActionNetworkPerson):
        person_id = person["uuid"]
        self["person_id"] = person_id
        self["updated_date"] = datetime.now(tz=timezone.utc)
        person.notice_attendance(conn, self)
        person.persist(conn)
        event = MobilizeEvent.from_lookup(conn, self["event_id"])
        event.notice_attendance(conn, self)
        event.persist(conn)

    def notice_event(self, conn, event):
        self["updated_date"] = datetime.now(tz=timezone.utc)
        event.notice_attendance(conn, self)
        event.persist(conn)

    @classmethod
    def initialize_caches(cls):
        cls.attendee_counts = [0, 0, 0]
        if cls.attendees is not cls._sentinel and cls.events is not cls._sentinel:
            return
        with Postgres.get_global_engine().connect() as conn:  # type: Connection
            cls.attendees = {}
            for row in conn.execute(sa.select(model.attendance_info)):
                if not cls.attendees.get(row.email):
                    if uuid := row.person_id:
                        person = ActionNetworkPerson.from_lookup(conn, uuid=uuid)
                        cls.attendees[row.email] = person
            cls.events = {}
            for row in conn.execute(sa.select(model.event_info)):
                if not cls.events.get(row.uuid):
                    event = MobilizeEvent.from_lookup(conn, uuid=row.uuid)
                    cls.events[row.uuid] = event
        pass

    @classmethod
    def from_hash(cls, body: dict) -> "MobilizeAttendance":
        uuid = body["id"]
        created_date = datetime.fromtimestamp(body["created_date"], tz=timezone.utc)
        modified_date = datetime.fromtimestamp(body["modified_date"], tz=timezone.utc)
        event_id = body["event"]["id"]
        timeslot_id = body["timeslot"]["id"]
        emails: list[dict] = body["person"]["email_addresses"]
        if len(emails) != 1:
            raise ValueError("No email in attendee")
        status = body["status"]
        return cls(
            uuid=uuid,
            created_date=created_date,
            modified_date=modified_date,
            event_id=event_id,
            timeslot_id=timeslot_id,
            email=emails[0]["address"],
            status=status,
        )

    @classmethod
    def from_query(cls, conn: Connection, query: Any) -> list["MobilizeAttendance"]:
        """
        See `PersistedDict.lookup_objects` for details.
        """
        return lookup_objects(conn, query, lambda d: cls(**d))


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
        cutoff_lo = datetime(2022, 1, 1, tzinfo=timezone.utc)
        return dict(updated_since=int(cutoff_lo.timestamp()))


def import_attendance_data(data: list[dict]) -> int:
    """Import a page of attendance data, returning the number imported"""
    import_count = 0
    with Postgres.get_global_engine().connect() as conn:  # type: Connection
        for attendance_dict in data:
            try:
                attendance = MobilizeAttendance.from_hash(attendance_dict)
            except ValueError:
                # data hiding prevents using this attendance
                continue
            import_count += 1
            attendance.persist(conn)
        conn.commit()
    return import_count


def compute_attendance_status(verbose: bool = True, force: bool = False):
    """Update the status for Mobilize attendances modified since last update"""
    MobilizeAttendance.initialize_caches()
    table = model.attendance_info
    if force:
        query = sa.select(table)
    else:
        query = sa.select(table).where(table.c.modified_date >= table.c.updated_date)
    with Postgres.get_global_engine().connect() as conn:  # type: Connection
        attendances = MobilizeAttendance.from_query(conn, query)
        total, count, start_time = len(attendances), 0, datetime.now(tz=timezone.utc)
        if verbose:
            print(f"Updating status for {total} attendances...")
            progress_time = start_time
        for attendance in attendances:
            count += 1
            attendance.compute_status(conn, force)
            now = datetime.now(tz=timezone.utc)
            attendance.persist(conn)
            if verbose and (now - progress_time).seconds > 5:
                print(f"({count})...", flush=True)
                progress_time = now
        conn.commit()
    if verbose:
        now = datetime.now(tz=timezone.utc)
        print(f"({count}) done (in {(now - start_time).total_seconds()} secs).")
        counts = MobilizeAttendance.attendee_counts
        print(f"Attendee cache lookups [hit/miss/no-person]: {counts}")
