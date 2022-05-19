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

from fastapi import FastAPI
from fastapi.responses import RedirectResponse

from .act_blue import act_blue
from .action_network import action_network
from .airtable import airtable
from ..act_blue.metadata import ActBlueDonationMetadata
from ..airtable.bulk import register_webhooks, verify_schemas
from ..core import Configuration
from ..core.logging import get_logger
from ..core.utilities import local_timestamp

logger = get_logger(__name__)

if Configuration.get_env() == "PRD":
    app = FastAPI(openapi_url=None, docs_url=None, redoc_url=None)
else:
    app = FastAPI()

# mounts an "independent" app on the /static path that handles static files
# app.mount("/static", StaticFiles(directory="static"), name="static")
# add the sub-APIs
app.include_router(airtable, prefix="/airtable", tags=["airtable"])
app.include_router(act_blue, prefix="/actblue", tags=["actblue"])
app.include_router(action_network, prefix="/action_network", tags=["action_network"])


@app.get("/", response_class=RedirectResponse, status_code=303)
async def redirect_to_status():
    return "/status"


@app.get("/status")
async def status():
    return {"message": "stv-services are running"}


@app.on_event("startup")
async def startup():
    logger.info(f"Web server startup at {local_timestamp()}")
    if Configuration.get_env() != "DEV":
        # make sure the Airtable schema is as expected
        verify_schemas(verbose=True)
        # make sure the Airtable webhooks are registered
        register_webhooks(verbose=True, sync_first=True)
    # make sure the metadata form cache is loaded
    ActBlueDonationMetadata.initialize_forms()


@app.on_event("shutdown")
async def shutdown():
    logger.info(f"Web server shutdown at {local_timestamp()}")
