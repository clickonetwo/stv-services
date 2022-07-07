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
import sys
from copy import deepcopy
from os import getenv
from typing import ClassVar

import sqlalchemy as sa
from sqlalchemy.future import Connection

from ..data_store import model, Postgres


class Configuration(dict):
    """A dictionary with persistence in the database.

    Because we often have multiple processes updating this dictionary,
    and each process reads and write the entire dictionary at one time,
    we keep track of the values loaded from the database and only save
    changes to the database (to avoid unnecessary conflicts)."""

    _singleton: ClassVar["Configuration"] = None
    _env: ClassVar[str] = "DEV"

    def __init__(self, *args, **kwargs):
        self.original = {}
        super().__init__(*args, **kwargs)

    @classmethod
    def get_env(cls) -> str:
        return cls._env

    @classmethod
    def set_env(cls, new: str):
        if new.upper() not in ["DEV", "STG", "PRD"]:
            raise ValueError(f"Environment ('{new.upper()}') must be DEV, STG, or PRD")
        cls._env = new.upper()

    @classmethod
    def get_global_config(cls, reload: bool = False) -> "Configuration":
        if cls._singleton is None:
            cls._singleton = cls()
            cls._singleton.load_from_data_store()
        elif reload:
            cls._singleton.load_from_data_store()
        return cls._singleton

    @classmethod
    def get_session_config(cls, conn: Connection) -> "Configuration":
        config = cls()
        config.load_from_connection(conn)
        return config

    def load_from_connection(self, conn: Connection):
        """Load from database, replacing values for existing keys"""
        self.original = {
            key: val for key, val in conn.execute(sa.select(model.configuration))
        }
        self.update(deepcopy(self.original))

    def save_to_connection(self, conn: Connection):
        """Save current state of keys and values"""
        # find all the deleted keys
        deleted = []
        for key in self.original:
            if key not in self:
                deleted.append(key)
        # find all the updated keys
        updated = []
        new = {}
        for key, val in self.items():
            if key not in self.original:
                new[key] = val
            elif val != self.original[key]:
                new[key] = val
                updated.append(key)
        # out with the old
        to_remove = updated + deleted
        if to_remove:
            conn.execute(
                sa.delete(model.configuration).where(
                    model.configuration.c.key.in_(to_remove)
                )
            )
        # in with the new
        if new:
            values = [dict(key=key, value=val) for key, val in new.items()]
            conn.execute(sa.insert(model.configuration), values)
        self.original = deepcopy(self)

    def load_from_data_store(self):
        db = Postgres.get_global_engine()
        with db.connect() as conn:  # type: Connection
            self.load_from_connection(conn)

    def save_to_data_store(self):
        db = Postgres.get_global_engine()
        with db.connect() as conn:  # type: Connection
            self.save_to_connection(conn)
            conn.commit()

    def load_from_file(self, path: str = None) -> int:
        if not path:
            json_data = json.load(sys.stdin)
        else:
            with open(path) as f:
                json_data = json.load(f)
        if not isinstance(json_data, dict):
            raise ValueError(f"No configuration dictionary in input: {repr(json_data)}")
        self.update(json_data)
        return len(json_data)

    def save_to_file(self, path: str = None):
        simple_dict = dict(self)
        if not path:
            json.dump(simple_dict, sys.stdout, indent=2)
            print("")  # add trailing newline
        else:
            with open(path, "w") as f:
                json.dump(simple_dict, f, indent=2)
                print("", file=f)  # add trailing newline


if env := getenv("STV_ENV"):
    Configuration.set_env(env)
