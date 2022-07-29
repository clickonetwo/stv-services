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
from typing import Any, ClassVar, Union

import requests
import sqlalchemy as sa
from sqlalchemy.future import Connection

from stv_services.action_network.person import ActionNetworkPerson
from stv_services.core import Configuration
from stv_services.core.logging import get_logger, log_exception
from stv_services.data_store import model, Postgres
from stv_services.data_store.persisted_dict import PersistedDict, lookup_objects
from stv_services.mobilize.event import MobilizeEvent
from stv_services.mobilize.utilities import fetch_all_hashes, compute_status

logger = get_logger(__name__)


class MobilizeAttendance(PersistedDict):
    _sentinel: ClassVar[dict] = dict(none=None)
    attendees: ClassVar[dict[str, ActionNetworkPerson]] = _sentinel
    attendee_counts: ClassVar[list[int]] = [0, 0, 0]  # hit, miss, unknown
    events: ClassVar[dict[str, MobilizeEvent]] = _sentinel
    our_org_id = 3073

    def __init__(self, **fields):
        for field in ("uuid", "event_id", "timeslot_id", "email"):
            if not fields.get(field):
                raise ValueError(f"Attendances must have field '{field}'")
        super().__init__(model.attendance_info, **fields)

    def compute_status(self, conn: Connection, force: bool = False):
        if force or not self.get("person_id"):
            email = self["email"].lower()  # emails in action network are lowercase
            if person := self.attendees.get(email):
                self.attendee_counts[0] += 1
                self.notice_person(conn, person)
            else:
                # no such person
                self.attendee_counts[2] += 1
                self["updated_date"] = datetime.now(tz=timezone.utc)
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
                email = row.email.lower()
                if not cls.attendees.get(email):
                    if uuid := row.person_id:
                        person = ActionNetworkPerson.from_lookup(conn, uuid=uuid)
                        cls.attendees[email] = person
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
        event = body["event"]
        event_id = event["id"]
        if event_id not in MobilizeEvent.event_ids:
            raise ValueError(f"Attendance is for unknown event {event_id}")
        event_type = event["event_type"]
        timeslot_id = body["timeslot"]["id"]
        email = None
        if emails := body["person"]["email_addresses"]:
            email = emails[0].get("address")
        status = body["status"]
        return cls(
            uuid=uuid,
            created_date=created_date,
            modified_date=modified_date,
            event_id=event_id,
            event_type=event_type,
            timeslot_id=timeslot_id,
            email=email,
            status=status,
        )

    @classmethod
    def from_query(cls, conn: Connection, query: Any) -> list["MobilizeAttendance"]:
        """
        See `PersistedDict.lookup_objects` for details.
        """
        return lookup_objects(conn, query, lambda d: cls(**d))

    @classmethod
    def register_attendee(cls, conn: Connection, body: dict):
        if addresses := body.get("email_addresses", []):
            if email := addresses[0].get("address"):
                email: str = email.lower()
                if cls.attendees.get(email):
                    cls.attendee_counts[0] += 1
                    return
                try:
                    person = ActionNetworkPerson.from_lookup(conn, email=email)
                    cls.attendee_counts[1] += 1
                    cls.attendees[email] = person
                except KeyError:
                    # no such person, so create one
                    try:
                        person = ActionNetworkPerson.import_mobilize_person(body)
                        person.compute_status(conn)
                        person.persist(conn)
                        cls.attendees[email] = person
                        cls.attendee_counts[2] += 1
                    except (KeyError, requests.HTTPError):
                        log_exception(logger, "While importing Mobilize person")
                        logger.info("Ignoring Mobilize person import failure")


def import_attendances(
    verbose: bool = True, force: bool = False, skip_pages: int = 0, max_pages: int = 0
):
    # first make sure the events are cached, so attendance import can find them
    MobilizeEvent.initialize_caches()
    # now make sure prior attendances are cached, so attendance import doesn't
    # have to look people up over and over
    MobilizeAttendance.initialize_caches()
    # now do the import
    config = Configuration.get_global_config()
    start_timestamp = datetime.now(tz=timezone.utc)
    query = attendance_query(config.get("attendances_last_update_timestamp"), force)
    fetch_all_hashes(
        "attendances", import_attendance_data, query, verbose, skip_pages, max_pages
    )
    if not max_pages:
        config["attendances_last_update_timestamp"] = start_timestamp.timestamp()
        config.save_to_data_store()
    if verbose:
        counts = MobilizeAttendance.attendee_counts
        logger.info(f"Attendee cache lookups [repeat/first-time/imported]: {counts}")


def attendance_query(timestamp: float = None, force: bool = False) -> dict:
    if timestamp and not force:
        return dict(updated_since=int(timestamp))
    else:
        # we never return attendances created/modified before 1/1/2022
        # EXCEPT in dev we test back to 2021
        if Configuration.get_env() == "DEV":
            cutoff_lo = datetime(2021, 1, 1, tzinfo=timezone.utc)
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
                MobilizeAttendance.register_attendee(conn, attendance_dict["person"])
            except ValueError:
                # data hiding prevents using this attendance
                continue
            import_count += 1
            attendance.persist(conn)
        conn.commit()
    return import_count


def compute_attendance_status(verbose: bool = True, force: Union[bool, str] = False):
    """Update the status for Mobilize attendances modified since last update"""
    # Cache the existing attendees, so people can be looked up quickly
    MobilizeAttendance.initialize_caches()
    table = model.attendance_info
    if force:
        if isinstance(force, str):
            # query had better return attendances!
            query = sa.text(force)
        else:
            query = sa.select(table)
    else:
        query = sa.select(table).where(table.c.modified_date >= table.c.updated_date)
    with Postgres.get_global_engine().connect() as conn:  # type: Connection
        attendances = MobilizeAttendance.from_query(conn, query)
        if verbose:
            logger.info(f"Updating status for {len(attendances)} attendances...")
        compute_status(conn, attendances, verbose, force)
        conn.commit()
    if verbose:
        hit, lookup, miss = MobilizeAttendance.attendee_counts
        logger.info(f"Attendee cache lookups [hit/miss]: {hit}/{miss}")
