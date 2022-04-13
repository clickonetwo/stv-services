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

from stv_services.action_network.submission import (
    import_submissions,
    ActionNetworkSubmission,
)
from stv_services.data_store import Database

fake_an_id = "action_network:fake-submission-identifier"


def test_action_network_submission(clean_db):
    body = """
    {
      "identifiers": [
        "action_network:fake-submission-identifier"
      ],
      "modified_date": "2022-04-02T18:21:08Z",
      "action_network:person_id": "986ac371-7e7d-4607-b0fa-b68a8a29add6",
      "action_network:form_id": "b399bd2b-b9a9-4916-9550-5a8a47e045fb",
      "action_network:referrer_data": {
        "source": "none",
        "referrer": "group-everyday-people-pac",
        "website": "none"
      },
      "created_date": "2022-04-02T18:21:08Z"
    }
    """
    data = json.loads(body)
    with Database.get_global_engine().connect() as conn:
        submission = ActionNetworkSubmission.from_hash(data)
        assert submission["uuid"] == fake_an_id
        assert (
            submission["person_id"]
            == "action_network:986ac371-7e7d-4607-b0fa-b68a8a29add6"
        )
        assert (
            submission["form_id"]
            == "action_network:b399bd2b-b9a9-4916-9550-5a8a47e045fb"
        )
        submission.persist(conn)
        submission["person_id"] = "action_network:fake-person-identifier"
        submission.reload(conn)
        assert (
            submission["person_id"]
            == "action_network:986ac371-7e7d-4607-b0fa-b68a8a29add6"
        )
        submission.remove(conn)
        with pytest.raises(KeyError):
            submission.reload(conn)


@pytest.mark.slow
def test_import_submissions(clean_db):
    count = import_submissions(
        # this is the 2022 signup form
        query=f"identifier eq 'action_network:b399bd2b-b9a9-4916-9550-5a8a47e045fb'"
    )
    assert count >= 35
