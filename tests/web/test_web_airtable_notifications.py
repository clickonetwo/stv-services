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
import base64
import hmac
import json

import pytest

from stv_services.airtable.webhook import validate_notification
from stv_services.core.utilities import airtable_timestamp


def test_airtable_notification_validation(ensure_webhooks):
    hook_info = ensure_webhooks["hook_info"]
    vol_info = hook_info["volunteer"]
    notification = {
        "base": {"id": vol_info["base_id"]},
        "webhook": {"id": vol_info["hook_id"]},
        "timestamp": airtable_timestamp(),
    }
    body = json.dumps(notification)
    message = body.encode("ascii")
    secret = base64.b64decode(vol_info["secret"])
    digest = hmac.digest(secret, message, "sha256")
    digest64 = base64.b64encode(digest)
    name = validate_notification(notification, body.encode("utf-8"), digest64)
    assert name == "volunteer"
    with pytest.raises(ValueError):
        validate_notification(notification, body, "crapcrapcrapcrapcrapcrap")
