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

from fastapi import APIRouter

from ..core.logging import get_logger
from ..data_store import RedisAsync

logger = get_logger(__name__)
action_network = APIRouter()


@action_network.post(
    "/notifications",
    status_code=200,
    summary="Receiver for Action Network webhook notifications.",
)
async def receive_notifications(body: list[dict]):
    logger.info("Received Action Network webhook")
    db = await RedisAsync.connect()
    for hash_dict in body:
        for key, val in hash_dict.items():
            if key == "action_network:sponsor":
                logger.info(f"Ignoring '{key}' content")
            else:
                compact = json.dumps({key: val}, separators=(",", ":"))
                length: int = await db.lpush("action_network", compact)
                logger.info(
                    f"Saved '{key}' content as #{length} in 'action_network' queue"
                )
    await db.publish("webhooks", "action_network")
    return "Accepted"
