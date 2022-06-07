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


class MobilizeEvent(PersistedDict):
    our_org_id = 3073

    def __init__(self, **fields):
        super().__init__(model.event_info, **fields)

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
        )

    @classmethod
    def org_info(cls, body: dict) -> (int, str):
        org_id = body["id"]
        org_name = body["name"]
        if org_id == cls.our_org_id:
            org_name = ""
        return org_id, org_name

    @classmethod
    def contact_info(cls, body: dict) -> str:
        if not body:
            return ""
        return body["email_address"]


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
    query["timeslot_start"] = "gte_now"
    return query


def import_event_data(data: list[dict]):
    with Postgres.get_global_engine().connect() as conn:  # type: Connection
        for event_dict in data:
            timeslot_dicts = event_dict.get("timeslots", [])
            if not timeslot_dicts:
                # no timeslots are relevant, so no point to import the event
                continue
            event = MobilizeEvent.from_hash(event_dict)
            event_id = event["uuid"]
            event.persist(conn)
            for timeslot_dict in timeslot_dicts:
                timeslot = MobilizeTimeslot.from_hash(event_id, timeslot_dict)
                timeslot.persist(conn)
        conn.commit()
