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


def test_import_person_related(clean_db, known_db):
    import_person(known_db["historical_donor"], verbose=True)
    with Database.get_global_engine().connect() as conn:
        for donation_id in known_db["historical_donor_donations"]:
            donation = ActionNetworkDonation.from_lookup(conn, donation_id)
            assert donation["donor_id"] == known_db["historical_donor"]
    current_signup_non_donor = "action_network:986ac371-7e7d-4607-b0fa-b68a8a29add6"
    current_signup_non_donor_submissions = [
        "action_network:26042188-c143-4211-863f-0d9a2b0919c7"
    ]
    import_person(known_db["current_signup_non_donor"], verbose=True)
    with Database.get_global_engine().connect() as conn:
        for submission_id in known_db["current_signup_non_donor_submissions"]:
            submission = ActionNetworkSubmission.from_lookup(conn, submission_id)
            assert submission["person_id"] == known_db["current_signup_non_donor"]


def test_compute_donation_summaries(known_db):
    with Database.get_global_engine().connect() as conn:
        person = ActionNetworkPerson.from_lookup(
            conn, uuid=known_db["historical_donor"]
        )
        person.update_donation_summaries(conn)
        assert person["total_2020"] == 2750
        assert person["total_2021"] == 250


def test_classify_for_airtable(known_db):
    with Database.get_global_engine().connect() as conn:
        # historical donors are not contacts, but if
        # they are made contacts they are also funders
        person = ActionNetworkPerson.from_lookup(
            conn, uuid=known_db["historical_donor"]
        )
        person.classify_for_airtable(conn)
        assert person["is_volunteer"] is True
        assert person["is_contact"] is False
        assert person["is_funder"] is False
        person["is_contact"] = True
        person.classify_for_airtable(conn)
        assert person["is_funder"] is True
        # new sign-ups are not volunteers or funders,
        # but they are contacts.
        person = ActionNetworkPerson.from_lookup(
            conn, uuid=known_db["current_signup_non_donor"]
        )
        person.classify_for_airtable(conn)
        assert person["is_volunteer"] is False
        assert person["is_contact"] is True
        assert person["is_funder"] is False
        # new donors are contacts and funders,
        # but they are not volunteers.
        person = ActionNetworkPerson.from_lookup(
            conn, uuid=known_db["current_donor_non_signup"]
        )
        person.classify_for_airtable(conn)
        assert person["is_volunteer"] is False
        assert person["is_contact"] is True
        assert person["is_funder"] is True
        # historical sign-ups are only volunteers,
        # and not funders even if they are made contacts
        person = ActionNetworkPerson.from_lookup(
            conn, uuid=known_db["historical_signup_non_donor"]
        )
        person.classify_for_airtable(conn)
        assert person["is_volunteer"] is True
        assert person["is_contact"] is False
        assert person["is_funder"] is False
        person["is_contact"] = True
        person.classify_for_airtable(conn)
        assert person["is_funder"] is False


@pytest.mark.slow
def test_import_people(clean_db):
    count = import_people(f"family_name eq 'Brotsky'")
    assert count >= 4
