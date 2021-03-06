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

#  MIT License
#
#
#  Permission is hereby granted, free of charge, to any person obtaining a copy
#  of this software and associated documentation files (the "Software"), to deal
#  in the Software without restriction, including without limitation the rights
#  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
#  copies of the Software, and to permit persons to whom the Software is
#  furnished to do so, subject to the following conditions:
#
#
#
import json

import pytest
from restnavigator.exc import HALNavigatorError

from stv_services.action_network.donation import (
    import_donations,
    ActionNetworkDonation,
)
from stv_services.data_store import Postgres

fake_an_id = "action_network:fake-donation-identifier"


def test_action_network_donation(clean_db):
    body = """
    {
      "identifiers": [
        "action_network:fake-donation-identifier"
      ],
      "created_date": "2022-04-06T21:38:01Z",
      "modified_date": "2022-04-06T21:38:01Z",
      "currency": "usd",
      "recipients": [
        {
          "display_name": "Everyday People PAC",
          "amount": "40.00"
        }
      ],
      "payment": {
        "method": "Credit Card",
        "reference_number": "f624b75d-bfa7-4c7e-a4fe-e232810a6d5a",
        "authorization_stored": false
      },
      "action_network:recurrence": {
        "recurring": false
      },
      "action_network:person_id": "39297a45-034a-4fb9-aecb-061043b5e824",
      "action_network:fundraising_page_id": "34c0efd5-4dce-449e-afff-ffd211074250",
      "action_network:referrer_data": {
        "source": "widget",
        "referrer": "group-everyday-people-pac",
        "website": "www.everyday-people-pac.org"
      },
      "amount": "40.00"
    }
    """
    data = json.loads(body)
    donation = ActionNetworkDonation.from_hash(data)
    assert donation["uuid"] == fake_an_id
    assert donation["amount"] == "40.00"
    assert donation["recurrence_data"].get("recurring") is False
    with Postgres.get_global_engine().connect() as conn:
        donation.persist(conn)
        donation["amount"] = "100.00"
        del donation["recurrence_data"]
        donation.reload(conn)
        assert donation["amount"] == "40.00"
        assert donation["recurrence_data"].get("recurring") is False
        found_donation = ActionNetworkDonation.from_lookup(conn, fake_an_id)
        assert found_donation == donation
        donation.remove(conn)
        with pytest.raises(KeyError):
            ActionNetworkDonation.from_lookup(conn, fake_an_id)
        with pytest.raises(KeyError):
            found_donation.reload(conn)


def test_import_donation(clean_db):
    with Postgres.get_global_engine().connect() as conn:
        an_id = "action_network:c3b8160a-59d4-4f22-83c6-7ce09b873c4a"
        donation = ActionNetworkDonation.from_action_network(conn, an_id)
        assert donation["uuid"] == an_id
        # there's a bug in Action Network - this should be a KeyError,
        # but they are returning an HTML 404 response rather than JSON.
        # TODO: change this back to KeyError when they fix their bug.
        with pytest.raises(HALNavigatorError):
            ActionNetworkDonation.from_action_network(conn, fake_an_id)


@pytest.mark.slow
def test_import_donations(clean_db):
    count = import_donations(
        f"identifier eq 'action_network:c3b8160a-59d4-4f22-83c6-7ce09b873c4a'"
    )
    assert count == 1
