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
import json

import pytest

from stv_services.action_network.donation import ActionNetworkDonation
from stv_services.action_network.person import (
    import_people,
    ActionNetworkPerson,
    import_person,
)
from stv_services.action_network.submission import ActionNetworkSubmission
from stv_services.data_store import Database

fake_an_id = "action_network:fake-person-identifier"


def test_action_network_person(clean_db):
    body = """
    {
      "given_name": "John",
      "family_name": "Random",
      "identifiers": [
        "action_network:fake-person-identifier",
        "mobilize:18515363958"
      ],
      "email_addresses": [
        {
          "primary": false,
          "address": "johnqrandom@worker.com",
          "status": "bouncing"
        },
        {
          "primary": true,
          "address": "johnqrandom@example.com",
          "status": "bouncing"
        },
        {
          "primary": false,
          "address": "johnqrandom@worker.com",
          "status": "bouncing"
        }
      ],
      "phone_numbers": [
        {
          "primary": false,
          "number": "15555555522",
          "number_type": "Mobile",
          "status": "unsubscribed"
        },
        {
          "primary": true,
          "number": "15555552222",
          "number_type": "Mobile",
          "status": "unsubscribed"
        },
        {
          "primary": false,
          "number": "15555552255",
          "number_type": "Mobile",
          "status": "unsubscribed"
        }
      ],
      "postal_addresses": [
        {
          "address_lines": [
            "3568 Divider St"
          ],
          "locality": "San Francisco",
          "region": "CA",
          "postal_code": "95706",
          "country": "US",
          "location": {
            "latitude": 37.8363,
            "longitude": -123.285,
            "accuracy": "Approximate"
          }
        },
        {
          "primary": true,
          "address_lines": [
            "3568 Divider St"
          ],
          "locality": "San Francisco",
          "region": "CA",
          "postal_code": "94608",
          "location": {
            "latitude": 37.8363,
            "longitude": -123.285,
            "accuracy": "Approximate"
          }
        },
        {
          "address_lines": [
            "3568 Divider St"
          ],
          "locality": "San Francisco",
          "region": "CA",
          "postal_code": "95706",
          "country": "US",
          "location": {
            "latitude": 37.8363,
            "longitude": -123.285,
            "accuracy": "Approximate"
          }
        }
      ],
      "custom_fields": {
        "phone_number": "5555551212",
        "occupation": "Programmer",
        "employer": "Venture Software",
        "gift in honor": "Maya Angelou",
        "checkbox value": 1,
        "logical value": true
      },
      "created_date": "2016-03-30T02:07:04Z",
      "modified_date": "2022-04-06T14:20:49Z",
      "languages_spoken": [
        "en"
      ]
    }
    """
    data = json.loads(body)
    with Database.get_global_engine().connect() as conn:
        person = ActionNetworkPerson.from_hash(data)
        assert person["uuid"] == fake_an_id
        assert person["email"] == "johnqrandom@example.com"
        assert person["phone"] == "15555552222"
        assert person["postal_code"] == "94608"
        assert person["custom_fields"].get("gift in honor") == "Maya Angelou"
        assert person["custom_fields"].get("checkbox value") == 1
        assert person["custom_fields"].get("logical value") is True
        person.persist(conn)
        person["postal_code"] = "94707"
        del person["custom_fields"]
        person.reload(conn)
        assert person["postal_code"] == "94608"
        assert person["custom_fields"].get("gift in honor") == "Maya Angelou"
        assert person["custom_fields"].get("checkbox value") == 1
        assert person["custom_fields"].get("logical value") is True
        found_person1 = ActionNetworkPerson.from_lookup(
            conn, email="johnqrandom@example.com"
        )
        assert found_person1 == person
        found_person2 = ActionNetworkPerson.from_lookup(conn, uuid=fake_an_id)
        assert found_person2 == found_person1
        person.remove(conn)
        with pytest.raises(KeyError):
            ActionNetworkPerson.from_lookup(conn, fake_an_id)
        with pytest.raises(KeyError):
            found_person1.reload(conn)


def test_import_person(clean_db):
    an_id = "action_network:dec233c3-bdee-457c-95ca-055b4647b907"
    with Database.get_global_engine().connect() as conn:
        person = ActionNetworkPerson.from_action_network(conn, an_id)
        assert person["uuid"] == an_id
        with pytest.raises(KeyError):
            ActionNetworkPerson.from_action_network(conn, fake_an_id)


def test_import_person_related(clean_db):
    historical_donor = "action_network:04bfa631-fa5f-404e-8c63-a9e51a753bdc"
    historical_donor_donations = [
        "action_network:f008946f-37d5-4d66-9f46-9336df9e5b9c",
        "action_network:5e14edb5-5bd4-4f85-83db-74e14af15efb",
        "action_network:146a1343-b2af-4cdb-9f31-b5fcdf5c94ce",
        "action_network:e22be3a8-7229-4b0f-9267-bd6b9fa755ef",
        "action_network:105a358b-cbba-4389-9d01-7c6933ced370",
        "action_network:347bf8de-71e7-491d-a73e-4d1b79bae7d9",
        "action_network:687b4b15-9e92-4c48-a7f4-5edaec5a0831",
        "action_network:a0deb800-d94b-4568-ae26-d12fc6a5e363",
        "action_network:f2a2e08d-0562-49e9-a5c6-bdc14d7dfa9d",
        "action_network:63bd0d69-d6ff-4d93-93ee-c0f948b07988",
        "action_network:6d5b56e7-aeb5-48ec-ae05-8b68a7e1f538",
        "action_network:2701a5ca-1e34-46d4-bb29-4829f999f2f3",
        "action_network:c02b9ee0-bb1a-48ec-88cb-3c0ca12ee63d",
    ]
    import_person(historical_donor, verbose=True)
    with Database.get_global_engine().connect() as conn:
        for donation_id in historical_donor_donations:
            donation = ActionNetworkDonation.from_lookup(conn, donation_id)
            assert donation["donor_id"] == historical_donor
    current_signup_non_donor = "action_network:986ac371-7e7d-4607-b0fa-b68a8a29add6"
    current_signup_non_donor_submissions = [
        "action_network:26042188-c143-4211-863f-0d9a2b0919c7"
    ]
    import_person(current_signup_non_donor, verbose=True)
    with Database.get_global_engine().connect() as conn:
        for submission_id in current_signup_non_donor_submissions:
            submission = ActionNetworkSubmission.from_lookup(conn, submission_id)
            assert submission["person_id"] == current_signup_non_donor


@pytest.mark.slow
def test_import_people(clean_db):
    count = import_people(f"family_name eq 'Brotsky'")
    assert count >= 4
