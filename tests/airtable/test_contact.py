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
from stv_services.airtable import contact
from stv_services.core import Configuration
from stv_services.data_store import Postgres, model


def test_validate_contact_schema():
    contact.verify_contact_schema()
    cs = Configuration.get_global_config().get("airtable_stv_contact_schema")
    assert cs and cs.get("base_id") and cs.get("table_id") and cs.get("column_ids")


def test_create_contact_record(reload_db, ensure_schemas):
    with Postgres.get_global_engine().connect() as conn:  # type: Connection
        person = ActionNetworkPerson.from_lookup(
            conn, uuid=reload_db["current_signup_non_donor"]
        )
        record = contact.create_contact_record(conn, person)
    column_ids = ensure_schemas["contact_schema"]["column_ids"]
    reverse_column_ids = {v: k for k, v in column_ids.items()}
    for column_id, val in record.items():
        field_name = reverse_column_ids[column_id]
        if contact.contact_table_schema[field_name].source == "person":
            assert val == person.get(field_name)


def test_insert_then_update_then_delete_contact_records(reload_db, ensure_schemas):
    with Postgres.get_global_engine().connect() as conn:  # type: Connection
        query = sa.select(model.person_info)
        people = ActionNetworkPerson.from_query(conn, query)
        inserts, updates = contact.upsert_contacts(conn, people)
        assert inserts == len(people)
        assert updates == 0
        conn.commit()
    with Postgres.get_global_engine().connect() as conn:  # type: Connection
        query = sa.select(model.person_info)
        people = ActionNetworkPerson.from_query(conn, query)
        inserts, updates = contact.upsert_contacts(conn, people)
        assert inserts == 0
        assert updates == len(people)
        conn.commit()
    with Postgres.get_global_engine().connect() as conn:  # type: Connection
        query = sa.select(model.person_info)
        people = ActionNetworkPerson.from_query(conn, query)
        deletes = contact.delete_contacts(conn, people)
        assert deletes == len(people)
        conn.commit()
    pass
