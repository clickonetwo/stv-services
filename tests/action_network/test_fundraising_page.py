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

from stv_services.action_network.fundraising_page import (
    load_fundraising_pages,
    ActionNetworkFundraisingPage,
    load_fundraising_page,
)


def test_action_network_fundraising_page():
    body = """
    {
      "identifiers": [
        "action_network:fake-fundraising-page-identifier"
      ],
      "created_date": "2022-04-02T18:02:08Z",
      "total_amount": "1.00",
      "currency": "USD",
      "action_network:sponsor": {
        "title": "Everyday People PAC",
        "browser_url": "https://actionnetwork.org/api/v2/groups/everyday-people-pac"
      },
      "modified_date": "2022-04-02T18:02:08Z",
      "origin_system": "ActBlue",
      "title": "act-blue_146845_stv-test-form-3",
      "action_network:hidden": false
    }
    """
    data = json.loads(body)
    fundraising_page = ActionNetworkFundraisingPage.from_action_network(data)
    assert fundraising_page["uuid"] == "action_network:fake-fundraising-page-identifier"
    assert fundraising_page["origin_system"] == "ActBlue"
    assert fundraising_page["title"] == "act-blue_146845_stv-test-form-3"
    fundraising_page.persist()
    fundraising_page["title"] = "wrong title"
    del fundraising_page["origin_system"]
    fundraising_page.reload()
    assert fundraising_page["origin_system"] == "ActBlue"
    assert fundraising_page["title"] == "act-blue_146845_stv-test-form-3"
    found_fundraising_page = ActionNetworkFundraisingPage.lookup(
        uuid="action_network:fake-fundraising-page-identifier"
    )
    assert found_fundraising_page == fundraising_page
    fundraising_page.remove()
    with pytest.raises(KeyError):
        ActionNetworkFundraisingPage.lookup(
            "action_network:fake-fundraising-page-identifier"
        )
    with pytest.raises(KeyError):
        found_fundraising_page.reload()


def test_load_fundraising_page():
    an_id = "action_network:7f2decaf-4eee-4a1e-bf5c-9c7d0d4e6726"
    person = load_fundraising_page(an_id)
    assert person["uuid"] == an_id
    fake_an_id = "action_network:fake-fundraising-page-identifier"
    with pytest.raises(KeyError):
        load_fundraising_page(fake_an_id)


@pytest.mark.slow
def test_load_fundraising_pages():
    count = load_fundraising_pages(
        f"identifier eq 'action_network:7f2decaf-4eee-4a1e-bf5c-9c7d0d4e6726'"
    )
    assert count == 1
