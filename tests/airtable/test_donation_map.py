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
import pytest
from requests import HTTPError

from stv_services.action_network.donation import ActionNetworkDonation
from stv_services.action_network.person import ActionNetworkPerson
from stv_services.airtable import donation
from stv_services.core import Configuration
from stv_services.data_store import Database


def test_validate_donation_schema():
    donation.verify_donation_schema()
    cs = Configuration.get_global_config().get("airtable_stv_donation_schema")
    assert cs and cs.get("base_id") and cs.get("table_id") and cs.get("column_ids")


def test_create_donation_record(reload_db, ensure_schemas):
    with Database.get_global_engine().connect() as conn:
        donor = ActionNetworkPerson.from_lookup(
            conn, uuid=reload_db["historical_donor"]
        )
        # temporarily set contact_record_id so the donation can use it
        donor["contact_record_id"] = "fake-record-id"
        donor.persist(conn)
        donation0 = ActionNetworkDonation.from_lookup(
            conn, uuid=reload_db["historical_donor_donations"][0]
        )
        record = donation.create_donation_record(conn, donation0)
        column_ids = ensure_schemas["donation_schema"]["column_ids"]
        assert record[column_ids["donor_id"]] == ["fake-record-id"]
        # make sure we don't mess up the donor record!
        conn.rollback()


def test_insert_then_update_then_delete_donation_record(reload_db, ensure_schemas):
    with Database.get_global_engine().connect() as conn:
        donor = ActionNetworkPerson.from_lookup(
            conn, uuid=reload_db["historical_donor"]
        )
        # temporarily set contact_record_id so the donation can use it
        donor["contact_record_id"] = "fake-record-id"
        donor.persist(conn)
        donations = [
            ActionNetworkDonation.from_lookup(
                conn, uuid=reload_db["historical_donor_donations"][0]
            ),
        ]
        with pytest.raises(HTTPError) as err:
            # can't insert a record with a fake ID
            donation.upsert_donations(conn, donations)
            assert err.response.status_code == 422
        # don't leave the contact messed up
        conn.rollback()
