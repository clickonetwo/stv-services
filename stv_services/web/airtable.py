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

import aioredis as redis
from fastapi import APIRouter, Header, BackgroundTasks, Request
from pydantic import BaseModel
from starlette.responses import JSONResponse

from ..airtable.bulk import register_webhooks
from ..airtable.webhook import validate_notification
from ..core.logging import get_logger, log_exception
from ..core.utilities import local_timestamp
from ..data_store import RedisAsync, ItemListAsync
from ..worker.airtable import process_webhook_notification

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
    "/notifications",
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
async def receive_notification(
    request: Request,
    worker: BackgroundTasks,
):
    """
    Receive a notification from an Airtable webhook.

    See https://airtable.com/api/webhooks for details.

    NOTE: We handle this request specially so that we can
    do HMAC verification on the body of the request.
    """
    logger.info(f"Received Airtable webhook notification")
    body: bytes = await request.body()
    signature: str = request.headers.get("x-airtable-content-mac")
    try:
        payload = json.loads(body)
    except json.JSONDecodeError:
        message = log_exception(logger, f"while decoding notification")
        return request_error(message)
    try:
        hook_name = validate_notification(payload, body, signature)
    except ValueError:
        # Either we don't recognize the notification ID or the
        # HMAC validation on the notification has failed.  Either
        # way, we need to clean up and re-register our webhooks.
        message = log_exception(logger, f"while validating notification")
        logger.info("Re-registering webhooks due to failed validation")
        worker.add_task(register_webhooks, False, True)
        return request_error(message)
    logger.info(f"Processing '{hook_name}' notification in background")
    # TODO: replace background task with notification of worker process
    worker.add_task(process_webhook_notification, hook_name, body)
    return
