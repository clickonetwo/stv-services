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
from typing import ClassVar

import sqlalchemy as sa
from sqlalchemy.future import Engine, Connection

from . import model


def get_engine_url(default_url: str = None) -> str:
    url = os.getenv("DATABASE_URL", default_url or "postgresql://localhost:5432/stv")
    if not url:
        raise ValueError("Database URL must not be empty")
    # sqlalchemy doesn't understand that the 'postgres:' prefix (used by Heroku)
    # is an alias of 'postgresql:' and so won't connect; compensate for that.
    if url.startswith("postgres:"):
        url = "postgresql:" + url[len("postgres:") :]
    return url


class Postgres:
    _singleton: ClassVar[Engine] = None

    @staticmethod
    def get_new_engine() -> Engine:
        return sa.create_engine(get_engine_url(), future=True)

    @classmethod
    def get_global_engine(cls) -> Engine:
        if cls._singleton is None:
            cls._singleton = cls.get_new_engine()
        return cls._singleton

    @classmethod
    def clear_importable_data(cls):
        with cls.get_global_engine().connect() as conn:  # type: Connection
            conn.execute(sa.delete(model.person_info))
            conn.execute(sa.delete(model.donation_info))
            conn.execute(sa.delete(model.submission_info))
            conn.execute(sa.delete(model.fundraising_page_info))
            # conn.execute(sa.delete(model.donation_metadata))
            conn.execute(sa.delete(model.event_info))
            conn.execute(sa.delete(model.timeslot_info))
            conn.execute(sa.delete(model.attendance_info))
            conn.commit()
