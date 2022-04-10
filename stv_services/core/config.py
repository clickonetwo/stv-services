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
from typing import ClassVar

import sqlalchemy as sa

from ..data_store import model, Database


class Configuration(dict):
    _singleton: ClassVar["Configuration"] = None

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @classmethod
    def get_global_config(cls, reload: bool = False) -> "Configuration":
        if cls._singleton is None:
            cls._singleton = Configuration()
            cls._singleton.load_from_data_store()
        elif reload:
            cls._singleton.load_from_data_store()
        return cls._singleton

    def load_from_data_store(self, db: sa.engine.Engine = Database.get_global_engine()):
        # out with the old
        self.clear()
        # in with the new
        with db.connect() as conn:
            for key, val in conn.execute(sa.select(model.configuration)):
                self[key] = val

    def save_to_data_store(self, db: sa.engine.Engine = Database.get_global_engine()):
        with db.connect() as conn:
            # out with the old
            conn.execute(sa.delete(model.configuration))
            # in with the new
            if self:
                new = [dict(key=key, value=val) for key, val in self.items()]
                conn.execute(sa.insert(model.configuration), new)
            conn.commit()
