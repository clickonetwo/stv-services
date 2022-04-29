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
import aioredis as redis
from fastapi import APIRouter, Header
from pydantic import BaseModel
from starlette.responses import JSONResponse

from ..airtable.webhook import validate_notification
from ..core.logging import get_logger, log_exception
from ..core.utilities import timestamp
from ..data_store import RedisAsync, ItemListAsync

airtable = APIRouter()
logger = get_logger(__name__)


class ErrorResponse(BaseModel):
    detail: str


def request_error(context: str) -> JSONResponse:
    message = log_exception(logger, f"Request error: {context}")
    return JSONResponse(status_code=400, content={"detail": message})


def database_error(context: str) -> JSONResponse:
    message = log_exception(logger, f"Database error: {context}")
    return JSONResponse(status_code=502, content={"detail": message})


def runtime_error(context: str) -> JSONResponse:
    message = log_exception(logger, f"Unexpected error: {context}")
    return JSONResponse(status_code=500, content={"detail": message})


@airtable.post(
    "/notification",
    status_code=204,
    responses={
        400: {"model": ErrorResponse, "description": "Notification payload is invalid"},
        500: {
            "model": ErrorResponse,
            "description": "Unexpected error during processing",
        },
        502: {
            "model": ErrorResponse,
            "description": "Database error during processing",
        },
    },
    summary="Receiver for Airtable webhook notifications.",
)
async def receive_notification(body: str, x_airtable_content_mac: str = Header("")):
    """
    Receive a notification from an Airtable webhook.

    See https://airtable.com/api/webhooks for details.
    """
    db = await RedisAsync.connect()
    logger.info(f"Received Airtable webhook notification")
    try:
        hook_name = validate_notification(body, x_airtable_content_mac)
    except ValueError:
        return request_error(f"while validating notification")
    values = [hook_name, body]
    list_key = f"airtable-notification|{timestamp()}"
    try:
        await db.rpush(list_key, *values)
        await ItemListAsync.add_new_list("airtable", list_key)
    except redis.RedisError:
        return database_error(f"while saving notification")
    except:
        return runtime_error(f"while saving notification")
    logger.info(f"Saved notification in 'airtable' queue as '{list_key}'")
    return
