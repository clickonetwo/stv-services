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
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql as psql
from sqlalchemy.future import Connection


class PersistedDict(dict):
    """
    A `PersistedDict` is a standard dictionary with an associated table
    that knows how to save and read its field values from the database.
    """

    def __init__(self, table: sa.Table, **fields):
        """
        Since this object is only used for Action Network object data,
        it is smart about requiring the id and created/modified date fields.

        Args:
            table: the associated table in the database
            **fields: standard dict key/value pairs for the fields
        """
        self.table = table
        if (
            not fields.get("uuid")
            or not fields.get("created_date")
            or not fields.get("modified_date")
        ):
            raise ValueError(
                f"uuid, created_date, and modified_date must be present: {fields}"
            )
        fields = {key: value for key, value in fields.items() if value is not None}
        super().__init__(**fields)

    def persist(self, conn: Connection):
        """
        Persist the current object to the datastore using the given connection.

        Caller is responsible for the commit.
        """
        insert_fields = {key: value for key, value in self.items() if value is not None}
        update_fields = {
            key: value for key, value in insert_fields.items() if key != "uuid"
        }
        insert_query = psql.insert(self.table).values(insert_fields)
        upsert_query = insert_query.on_conflict_do_update(
            index_elements=["uuid"], set_=update_fields
        )
        conn.execute(upsert_query)

    def reload(self, conn: Connection):
        """
        Reload the object from the database on the given connection.
        """
        query = sa.select(self.table).where(self.table.c.uuid == self["uuid"])
        result = conn.execute(query).first()
        if result is None:
            raise KeyError(f"Can't find object with uuid '{self['uuid']}'")
        fields = {
            key: value for key, value in result._asdict().items() if value is not None
        }
        self.clear()
        self.update(fields)
        pass

    def remove(self, conn: Connection):
        """
        Remove the object from the database on the given connection.

        Caller is responsible for the commit.
        """
        query = sa.delete(self.table).where(self.table.c.uuid == self["uuid"])
        conn.execute(query)
        conn.commit()
