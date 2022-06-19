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
from typing import Optional, Any, ClassVar

import sqlalchemy as sa
from sqlalchemy.future import Connection

from .utils import validate_hash, fetch_all_child_hashes, ActionNetworkObject
from ..core.logging import get_logger
from ..data_store import model
from ..data_store.persisted_dict import lookup_objects

logger = get_logger(__name__)


class ActionNetworkSubmission(ActionNetworkObject):
    # the database table for this class
    table: ClassVar[sa.Table] = model.submission_info
    # the cache for this class
    cache: ClassVar[dict] = {}

    def __init__(self, **fields):
        for key in ["form_id", "person_id"]:
            if not fields.get(key):
                raise ValueError(f"Submission must have field '{key}': {fields}")
        super().__init__(**fields)

    def update_from_hash(self, data: dict):
        # Nothing can change about a submission, so really we should never see
        # an updated submission.  In case we do, we report it.
        uuid, _, mod_date = validate_hash(data)
        self["modified_date"] = mod_date
        logger.warning(f"Ignoring update of submission '{uuid}' dated {mod_date}")

    @classmethod
    def from_webhook(cls, data: dict) -> "ActionNetworkSubmission":
        uuid, created_date, modified_date = validate_hash(data)
        # person_id and form_id are in links, not embedded data
        links = data.get("_links", {})
        if person_id := links.get("osdi:person", {}).get("href"):
            id_part = person_id[person_id.rfind("/") + 1 :]
            person_id = "action_network:" + id_part
        else:
            raise KeyError(f"Submission webhook does not have person link")
        if form_id := links.get("osdi:form", {}).get("href"):
            id_part = form_id[form_id.rfind("/") + 1 :]
            form_id = "action_network:" + id_part
        else:
            raise KeyError(f"Submission webhook does not have form link")
        return cls(
            uuid=uuid,
            created_date=created_date,
            modified_date=modified_date,
            person_id=person_id,
            form_id=form_id,
        )

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
        See `.utils.lookup_objects` for details.
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
    ActionNetworkSubmission.initialize_cache()
    return fetch_all_child_hashes(
        parent_hash_type="forms",
        child_hash_type="submissions",
        cls=ActionNetworkSubmission,
        query=query,
        verbose=verbose,
    )
