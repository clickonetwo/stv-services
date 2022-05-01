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
from fastapi.staticfiles import StaticFiles
from fastapi.responses import RedirectResponse

from .airtable import airtable
from ..core import Configuration
from ..core.logging import get_logger
from ..data_store import ItemListAsync

logger = get_logger(__name__)

if Configuration.get_env() == "PRD":
    app = FastAPI(openapi_url=None, docs_url=None, redoc_url=None)
else:
    app = FastAPI()

# mounts an "independent" app on the /static path that handles static files
# app.mount("/static", StaticFiles(directory="static"), name="static")
# add the sub-APIs
app.include_router(airtable, prefix="/airtable", tags=["airtable"])


@app.get("/", response_class=RedirectResponse, status_code=303)
async def redirect_to_status():
    return "/status"


@app.get("/status")
async def status():
    return {"message": "stv-services are running"}


@app.on_event("startup")
async def startup():
    await ItemListAsync.initialize()


@app.on_event("shutdown")
async def shutdown():
    await ItemListAsync.finalize()
