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

from stv_services.action_network.person import (
    load_people,
    ActionNetworkPerson,
    load_person,
)


def test_action_network_person():
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
    person = ActionNetworkPerson.from_action_network(data)
    assert person["uuid"] == "action_network:fake-person-identifier"
    assert person["email"] == "johnqrandom@example.com"
    assert person["phone"] == "15555552222"
    assert person["postal_code"] == "94608"
    assert person["custom_fields"].get("gift in honor") == "Maya Angelou"
    assert person["custom_fields"].get("checkbox value") == 1
    assert person["custom_fields"].get("logical value") is True
    person.persist()
    person["postal_code"] = "94707"
    del person["custom_fields"]
    person.reload()
    assert person["postal_code"] == "94608"
    assert person["custom_fields"].get("gift in honor") == "Maya Angelou"
    assert person["custom_fields"].get("checkbox value") == 1
    assert person["custom_fields"].get("logical value") is True
    found_person1 = ActionNetworkPerson.lookup(email="johnqrandom@example.com")
    assert found_person1 == person
    found_person2 = ActionNetworkPerson.lookup(
        uuid="action_network:fake-person-identifier"
    )
    assert found_person2 == found_person1
    person.remove()
    with pytest.raises(KeyError):
        ActionNetworkPerson.lookup("action_network:fake-person-identifier")
    with pytest.raises(KeyError):
        found_person1.reload()


def test_load_person():
    an_id = "action_network:dec233c3-bdee-457c-95ca-055b4647b907"
    person = load_person(an_id)
    assert person["uuid"] == an_id
    fake_an_id = "action_network:fake-person-identifier"
    with pytest.raises(KeyError):
        load_person(fake_an_id)


@pytest.mark.slow
def test_load_people():
    count = load_people(f"family_name eq 'Brotsky'")
    assert count >= 4
