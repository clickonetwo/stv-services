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

from stv_services.action_network.person import ActionNetworkPerson
from stv_services.airtable import funder, contact
from stv_services.core import Configuration
from stv_services.data_store import Database, model


def test_validate_funder_schema():
    funder.verify_funder_schema()
    cs = Configuration.get_global_config().get("airtable_stv_funder_schema")
    assert cs and cs.get("base_id") and cs.get("table_id") and cs.get("column_ids")


def test_create_funder_record(reload_db, ensure_schemas):
    with Database.get_global_engine().connect() as conn:
        person = ActionNetworkPerson.from_lookup(
            conn, uuid=reload_db["current_signup_non_donor"]
        )
        person["contact_record_id"] = "test_id"
    record = funder.create_funder_record(person)
    column_ids = ensure_schemas["funder_schema"]["column_ids"]
    reverse_column_ids = {v: k for k, v in column_ids.items()}
    for column_id, val in record.items():
        field_name = reverse_column_ids[column_id]
        if field_name == "contact":
            assert val == ["test_id"]


def test_insert_then_update_then_delete_funder_records(reload_db, ensure_schemas):
    # make them contacts first, because funders must be contacts
    with Database.get_global_engine().connect() as conn:
        query = sa.select(model.person_info)
        people = ActionNetworkPerson.from_query(conn, query)
        contact.upsert_contacts(conn, people)
        conn.commit()
    with Database.get_global_engine().connect() as conn:
        query = sa.select(model.person_info)
        people = ActionNetworkPerson.from_query(conn, query)
        inserts, updates = funder.upsert_funders(conn, people)
        assert inserts == len(people)
        assert updates == 0
        conn.commit()
    with Database.get_global_engine().connect() as conn:
        query = sa.select(model.person_info)
        people = ActionNetworkPerson.from_query(conn, query)
        inserts, updates = funder.upsert_funders(conn, people)
        assert inserts == 0
        assert updates == len(people)
        conn.commit()
    with Database.get_global_engine().connect() as conn:
        query = sa.select(model.person_info)
        people = ActionNetworkPerson.from_query(conn, query)
        deletes = funder.delete_funders(conn, people)
        assert deletes == len(people)
        conn.commit()
    # remove them from contacts
    with Database.get_global_engine().connect() as conn:
        query = sa.select(model.person_info)
        people = ActionNetworkPerson.from_query(conn, query)
        contact.delete_contacts(conn, people)
        conn.commit()
    pass
