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
import secrets

from fastapi import Depends, APIRouter, HTTPException, status
from fastapi.security import HTTPBasic, HTTPBasicCredentials

from ..core import Configuration
from ..core.logging import get_logger
from ..data_store import RedisAsync

logger = get_logger(__name__)
act_blue = APIRouter()
basic = HTTPBasic()


def is_authenticated(credentials: HTTPBasicCredentials = Depends(basic)):
    expected: dict = Configuration.get_global_config()["act_blue_webhook_credentials"]
    username, password = expected["username"], expected["password"]
    correct_username = secrets.compare_digest(credentials.username, username)
    correct_password = secrets.compare_digest(credentials.password, password)
    if not (correct_username and correct_password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect credentials",
            headers={"WWW-Authenticate": "Basic"},
        )


@act_blue.post(
    "/notifications",
    status_code=204,
    summary="Receiver for ActBlue webhook notifications.",
)
async def receive_notifications(body: dict):
    logger.info("Received ActBlue webhook")
    db = await RedisAsync.connect()
    compact = json.dumps(body, separators=(",", ":"))
    length: int = await db.lpush("act_blue", compact)
    logger.info(f"Saved webhook content as #1/{length} in 'act_blue' queue")
    await db.publish("webhooks", "act_blue")
    return
