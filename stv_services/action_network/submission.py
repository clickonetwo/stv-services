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
from typing import Optional, Any

import sqlalchemy as sa
from sqlalchemy.future import Connection

from .utils import (
    validate_hash,
    fetch_all_child_hashes,
)
from ..data_store.persisted_dict import PersistedDict, lookup_objects
from ..data_store import model, Postgres


class ActionNetworkSubmission(PersistedDict):
    def __init__(self, **fields):
        for key in ["form_id", "person_id"]:
            if not fields.get(key):
                raise ValueError(f"Submission must have field '{key}': {fields}")
        super().__init__(model.submission_info, **fields)

    @classmethod
    def from_hash(cls, data: dict) -> "ActionNetworkSubmission":
        uuid, created_date, modified_date = validate_hash(data)
        if person_id := data.get("action_network:person_id"):
            person_id = "action_network:" + person_id
        if form_id := data.get("action_network:form_id"):
            form_id = "action_network:" + form_id
        return cls(
            uuid=uuid,
            created_date=created_date,
            modified_date=modified_date,
            person_id=person_id,
            form_id=form_id,
        )

    @classmethod
    def from_lookup(cls, conn: Connection, uuid: str) -> "ActionNetworkSubmission":
        query = sa.select(model.submission_info).where(
            model.submission_info.c.uuid == uuid
        )
        result = lookup_objects(conn, query, lambda d: cls(**d))
        if not result:
            raise KeyError(f"No submission identified by '{uuid}'")
        return result[0]

    @classmethod
    def from_query(
        cls, conn: Connection, query: Any
    ) -> list["ActionNetworkSubmission"]:
        """
        See `.utils.lookup_hashes` for details.
        """
        return lookup_objects(conn, query, lambda d: cls(**d))


# you can't find submissions directly, you have to go through
# a person or a form.  Since there are far fewer forms than
# people, the way we load all submissions is by loading all
# forms and then finding their related submissions.  As a bonus,
# since the modified dates on forms get updated whenever the form
# is submitted, we can still restrict the query just to forms
# that have been recently submitted.
def import_submissions(
    query: Optional[str] = None,
    verbose: bool = True,
) -> int:
    return fetch_all_child_hashes(
        parent_hash_type="forms",
        child_hash_type="submissions",
        page_processor=insert_submissions_from_hashes,
        query=query,
        verbose=verbose,
    )


def insert_submissions_from_hashes(hashes: [dict]):
    with Postgres.get_global_engine().connect() as conn:  # type: Connection
        for data in hashes:
            try:
                submission = ActionNetworkSubmission.from_hash(data)
                submission.persist(conn)
            except ValueError as err:
                print(f"Skipping invalid submission: {err}")
        conn.commit()
