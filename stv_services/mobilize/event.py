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
from zoneinfo import ZoneInfo

import sqlalchemy as sa
from sqlalchemy.future import Connection

from stv_services.action_network.person import ActionNetworkPerson
from stv_services.core import Configuration
from stv_services.data_store import model, Postgres
from stv_services.data_store.persisted_dict import PersistedDict, lookup_objects
from stv_services.mobilize.utilities import fetch_all_hashes, compute_status


class MobilizeEvent(PersistedDict):
    event_ids: ClassVar[set[int]] = set()
    _sentinel: ClassVar[dict] = dict(none=None)
    contacts: ClassVar[dict[str, ActionNetworkPerson]] = _sentinel
    contact_counts: ClassVar[list[int]] = [0, 0, 0, 0]  # hit, miss, unknown, suppressed
    our_org_id: ClassVar = 3073
    suppressed_domains: ClassVar = (
        "@clickonetwo.io",
        "@seedthevote.org",
        "@everydaypeoplepac.org",
    )
    suppressed_users: ClassVar = set()

    def __init__(self, **fields):
        super().__init__(model.event_info, **fields)

    def compute_status(self, conn: Connection, force: bool = False):
        contact_id = self.get("contact_id", "")
        if force or not contact_id or contact_id == "pending":
            email = self.get("contact_email", "")
            if not email or self.suppress_contact(email):
                # contact info is suppressed
                self["updated_date"] = datetime.now(tz=timezone.utc)
                self.contact_counts[3] += 1
            elif person := self.contacts.get(email):
                self.contact_counts[0] += 1
                self.notice_contact(conn, person)
            else:
                try:
                    person = ActionNetworkPerson.from_lookup(conn, email=email)
                    self.contact_counts[1] += 1
                    self.contacts[email] = person
                    self.notice_contact(conn, person)
                except KeyError:
                    # no such person
                    self["updated_date"] = datetime.now(tz=timezone.utc)
                    self.contact_counts[2] += 1

    def create_shift_summary(self, conn: Connection) -> str:
        """Summarize signups by STV folks by timeslot for this event."""
        # first find all the attendances for this event that are for contacts
        query = sa.select(model.attendance_info).where(
            sa.and_(
                model.attendance_info.c.event_id == self["uuid"],
                model.attendance_info.c.person_id != "",
            )
        )
        attendance_list = conn.execute(query).mappings().all()
        attendances_by_timeslot = {}
        for attendance in attendance_list:
            timeslot_id = attendance["timeslot_id"]
            count = attendances_by_timeslot.setdefault(timeslot_id, [0])
            if attendance["status"] != "CANCELLED":
                count[0] += 1
        query = (
            sa.select(model.timeslot_info)
            .where(model.timeslot_info.c.event_id == self["uuid"])
            .order_by(model.timeslot_info.c.start_date.desc())
        )
        timeslots = conn.execute(query).mappings().all()
        entries = []
        for timeslot in timeslots:
            utc_start: datetime = timeslot["start_date"]
            pt_start = utc_start.astimezone(tz=ZoneInfo("America/Los_Angeles"))
            date_string = pt_start.strftime("%m/%d/%y %I:%M%p")
            count = attendances_by_timeslot.get(timeslot["uuid"], [0])
            entries.append(f"{date_string} Signups: {count[0]}")
        return "\n".join(entries)

    def notice_contact(self, conn: Connection, contact: ActionNetworkPerson):
        if contact:
            if record_id := contact["contact_record_id"]:
                self["contact_id"] = record_id
            else:
                self["contact_id"] = "pending"
            self["updated_date"] = datetime.now(tz=timezone.utc)
            contact.notice_event(conn, self)
            contact.persist(conn)

    def notice_attendance(self, _conn: Connection, _attendance: dict):
        """Update due to new attendance"""
        self["updated_date"] = datetime.now(tz=timezone.utc)

    def suppress_contact(self, email: str) -> bool:
        for domain in self.suppressed_domains:
            if email.endswith(domain):
                return True
        if email in self.suppressed_users:
            return True
        else:
            return False

    @classmethod
    def initialize_caches(cls):
        if not cls.event_ids:
            with Postgres.get_global_engine().connect() as conn:  # type: Connection
                cls.event_ids = {
                    row.uuid for row in conn.execute(sa.select(model.event_info.c.uuid))
                }
        cls.contact_counts = [0, 0, 0, 0]
        if cls.contacts is not cls._sentinel:
            return
        with Postgres.get_global_engine().connect() as conn:  # type: Connection
            cls.contacts = {}
            for row in conn.execute(sa.select(model.event_info)):
                email = row.contact_email
                if not cls.contacts.get(email):
                    if row.contact_id:
                        person = ActionNetworkPerson.from_lookup(conn, email=email)
                        cls.contacts[row.contact_email] = person
        pass

    @classmethod
    def from_hash(cls, body: dict) -> "MobilizeEvent":
        uuid = body["id"]
        created_date = datetime.fromtimestamp(body["created_date"], tz=timezone.utc)
        modified_date = datetime.fromtimestamp(body["modified_date"], tz=timezone.utc)
        title = body["title"]
        description = body["description"]
        sponsor_id, partner_name = cls.org_info(body["sponsor"])
        event_type = body["event_type"]
        event_url = body["browser_url"]
        contact_email = cls.contact_info(body["contact"])
        return cls(
            uuid=uuid,
            created_date=created_date,
            modified_date=modified_date,
            title=title,
            description=description,
            sponsor_id=sponsor_id,
            partner_name=partner_name,
            event_type=event_type,
            event_url=event_url,
            contact_email=contact_email,
            is_event=True,
        )

    @classmethod
    def org_info(cls, body: dict) -> (int, str):
        org_id = body["id"]
        org_name = body["name"]
        if body.get("is_coordinated"):
            # we are an independent org, we can't get data from coordinated events
            raise ValueError(f"Event organization '{org_name}' is coordinated")
        if org_id == cls.our_org_id:
            org_name = ""
        return org_id, org_name

    @classmethod
    def contact_info(cls, body: dict) -> str:
        if not body:
            return ""
        return body["email_address"].lower()

    @classmethod
    def from_lookup(cls, conn: Connection, uuid: int) -> "MobilizeEvent":
        query = sa.select(model.event_info).where(model.event_info.c.uuid == uuid)
        result = lookup_objects(conn, query, lambda d: cls(**d))
        if not result:
            raise KeyError(f"No event identified by '{uuid}'")
        return result[0]

    @classmethod
    def from_query(cls, conn: Connection, query: Any) -> list["MobilizeEvent"]:
        """
        See `PersistedDict.lookup_objects` for details.
        """
        return lookup_objects(conn, query, lambda d: cls(**d))


class MobilizeTimeslot(PersistedDict):
    def __init__(self, **fields):
        super().__init__(model.timeslot_info, **fields)

    @classmethod
    def from_hash(cls, event_id: int, body: dict) -> "MobilizeTimeslot":
        return cls(
            uuid=body["id"],
            start_date=datetime.fromtimestamp(body["start_date"], tz=timezone.utc),
            end_date=datetime.fromtimestamp(body["end_date"], tz=timezone.utc),
            event_id=event_id,
        )

    @classmethod
    def from_lookup(cls, conn: Connection, uuid: str) -> "MobilizeTimeslot":
        query = sa.select(model.event_info).where(model.event_info.c.uuid == uuid)
        result = lookup_objects(conn, query, lambda d: cls(**d))
        if not result:
            raise KeyError(f"No donation identified by '{uuid}'")
        return result[0]

    @classmethod
    def from_query(cls, conn: Connection, query: Any) -> list["MobilizeTimeslot"]:
        """
        See `PersistedDict.lookup_objects` for details.
        """
        return lookup_objects(conn, query, lambda d: cls(**d))


def import_events(
    verbose: bool = True, force: bool = False, skip_pages: int = 0, max_pages: int = 0
):
    config = Configuration.get_global_config()
    start_timestamp = datetime.now(tz=timezone.utc)
    query = event_query(config.get("events_last_update_timestamp"), force)
    fetch_all_hashes("events", import_event_data, query, verbose, skip_pages, max_pages)
    if not max_pages:
        config["events_last_update_timestamp"] = start_timestamp.timestamp()
        config.save_to_data_store()


def event_query(timestamp: float = None, force: bool = False) -> dict:
    query = {}
    if timestamp and not force:
        query["updated_since"] = int(timestamp)
    if force:
        cutoff_lo = datetime(2021, 1, 1, tzinfo=timezone.utc)
        query["timeslot_start"] = f"gte_{int(cutoff_lo.timestamp())}"
    else:
        query["timeslot_start"] = "gte_now"
    return query


def import_event_data(data: list[dict]) -> int:
    """Import a page of event data, returning the number of events imported."""
    count = 0
    with Postgres.get_global_engine().connect() as conn:  # type: Connection
        for event_dict in data:
            timeslot_dicts = event_dict.get("timeslots", [])
            if not timeslot_dicts:
                # no timeslots are relevant, so no point to import the event
                continue
            try:
                event = MobilizeEvent.from_hash(event_dict)
            except ValueError:
                # a coordinated event, skip it
                continue
            event.persist(conn)
            count += 1
            event_id = event["uuid"]
            MobilizeEvent.event_ids.add(event_id)
            for timeslot_dict in timeslot_dicts:
                timeslot = MobilizeTimeslot.from_hash(event_id, timeslot_dict)
                timeslot.persist(conn)
        conn.commit()
    return count


def compute_event_status(verbose: bool = True, force: bool = False):
    """Update the status for Mobilize events modified since last update"""
    MobilizeEvent.initialize_caches()
    if force:
        query = sa.select(model.event_info)
    else:
        cols = model.event_info.columns
        query = sa.select(model.event_info).where(
            sa.or_(
                cols.modified_date >= cols.updated_date, cols.contact_id == "pending"
            )
        )
    with Postgres.get_global_engine().connect() as conn:  # type: Connection
        events = MobilizeEvent.from_query(conn, query)
        if verbose:
            print(f"Updating status for {len(events)} events...")
        compute_status(conn, events, verbose, force)
        conn.commit()
    if verbose:
        counts = MobilizeEvent.contact_counts
        print(f"Contact cache lookups [hit/miss/no-person/suppressed]: {counts}")
