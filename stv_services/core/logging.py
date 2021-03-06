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
import logging
import os
import sys

from stv_services.core import Configuration


def init_logging():
    is_heroku = "HEROKU" == (os.getenv("STV_RUNTIME") or "LOCAL")
    if is_heroku:
        log_format = "%(levelname)s:%(process)d:%(module)s: %(message)s"
    else:
        log_format = "%(asctime)s:%(levelname)s:%(process)d:%(module)s: %(message)s"
    level = os.getenv("STV_LOG_LEVEL")
    if level:
        level = logging.getLevelName(level)
        if not isinstance(level, int):
            level = None
    if not level:
        env = Configuration.get_env()
        level = logging.DEBUG if env == "DEV" else logging.INFO
    logging.basicConfig(
        format=log_format,
        level=level,
        stream=sys.stdout,
        force=True,
    )


def get_logger(name):
    return logging.getLogger(name)


def log_exception(local: logging.Logger, context: str) -> str:
    """Log a message about an exception, and return the message"""
    exc_type, exc_obj, exc_tb = sys.exc_info()
    f_name = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
    message = f"{context}: {f_name}, {exc_tb.tb_lineno}: {repr(exc_obj)}"
    local.critical(message)
    return message


init_logging()
logger = get_logger(__name__)
logger.info(
    f"Running in {Configuration.get_env()} environment "
    f"(log level {logging.getLevelName(logger.getEffectiveLevel())})"
)
