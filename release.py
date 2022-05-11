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
import os
import sqlalchemy as sa

import alembic.config
from sqlalchemy.future import Connection

import stv_services.airtable.bulk as at_bulk

from stv_services.core import Configuration
from stv_services.core.logging import init_logging, get_logger
from stv_services.data_store import Postgres, model

logger = get_logger("release")


def run_release_process():
    # start logging
    init_logging()

    # initialize the database
    alembic_args = ["--raiseerr", "upgrade", "head"]
    alembic.config.main(argv=alembic_args)

    # exit if no configuration data is loaded
    config = Configuration.get_global_config()
    if not config.get("airtable_stv_base_name"):
        logger.critical(
            "No configuration data: be sure to load configuration before services start"
        )
        return

    # make sure our endpoint matches our configuration
    url: str = config.get("stv_api_base_url", "")
    env = Configuration.get_env()
    print(f"Environment is '{env}'")
    if not url or url.find("stv-services") < 0:
        assert env == "DEV", "DEV webserver but not in DEV"
        assert not os.getenv("DATABASE_URL"), "DEV builds must use the local database"
    elif url.find("-stage") >= 0:
        assert env == "STG", "STG webserver but not in STG"
        assert os.getenv("DATABASE_URL"), "STG builds require a DATABASE_URL"
        assert os.getenv("REDIS_URL"), "STG builds require a REDIS_URL"
    else:
        assert env == "PRD", "PRD webserver but not in PRD"
        assert os.getenv("DATABASE_URL"), "PRD builds require a DATABASE_URL"
        assert os.getenv("REDIS_URL"), "PRD builds require a REDIS_URL"

    # make sure the Airtable schema validates
    at_bulk.verify_schemas(verbose=True)

    # final check on configuration
    with Postgres.get_global_engine().connect() as conn:  # type: Connection
        rows = conn.execute(sa.select(model.external_info)).first()
        if not rows:
            logger.critical("There is no external spreadsheet data loaded!!")
            logger.critical("heroku local:run ./stv.py import-from-local")


if __name__ == "__main__":
    run_release_process()
