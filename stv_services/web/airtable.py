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

from fastapi import APIRouter, Request

from .utilities import request_error
from ..airtable.webhook import validate_notification
from ..core.logging import get_logger
from ..data_store import RedisAsync

logger = get_logger(__name__)
airtable = APIRouter()


@airtable.post(
    "/notifications",
    status_code=204,
    summary="Receiver for Airtable webhook notifications.",
)
async def receive_notification(request: Request):
    """
    Receive a notification from an Airtable webhook.

    See https://airtable.com/api/webhooks for details.

    NOTE: We handle this request specially so that we can
    do HMAC verification on the body of the request.
    """
    logger.info(f"Received Airtable webhook")
    body: bytes = await request.body()
    signature: str = request.headers.get("x-airtable-content-mac")
    try:
        payload = json.loads(body)
    except json.JSONDecodeError:
        raise request_error(logger, f"while decoding webhook")
    try:
        hook_name = validate_notification(payload, body, signature)
    except ValueError:
        raise request_error(logger, f"while validating webhook")
    db = await RedisAsync.connect()
    length = await db.lpush("airtable", hook_name)
    logger.info(f"Saved webhook '{hook_name}' as #{length} in 'airtable' queue")
    return
