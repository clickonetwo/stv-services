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
import sqlalchemy as sa
from sqlalchemy.future import Connection

from stv_services.action_network.person import ActionNetworkPerson
from stv_services.airtable import volunteer, contact, funder
from stv_services.data_store import Postgres, model
from stv_services.worker.airtable import promote_volunteers_or_contacts


def test_promote_volunteers_or_contacts(reload_db, ensure_schemas):
    with Postgres.get_global_engine().connect() as conn:  # type: Connection
        query = sa.select(model.person_info)
        people = ActionNetworkPerson.from_query(conn, query)
        volunteer.upsert_volunteers(conn, people)
        conn.commit()
    record_ids = [person["volunteer_record_id"] for person in people]
    promote_volunteers_or_contacts("volunteer", record_ids)
    with Postgres.get_global_engine().connect() as conn:  # type: Connection
        people = ActionNetworkPerson.from_query(conn, query)
        for person in people:
            assert person["contact_record_id"] != ""
        contact.upsert_contacts(conn, people)
    record_ids = [person["contact_record_id"] for person in people]
    promote_volunteers_or_contacts("contact", record_ids)
    with Postgres.get_global_engine().connect() as conn:  # type: Connection
        people = ActionNetworkPerson.from_query(conn, query)
        for person in people:
            assert person["funder_record_id"] != ""
        funder.upsert_funders(conn, people)
