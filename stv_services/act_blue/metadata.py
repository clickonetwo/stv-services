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
from datetime import datetime, timezone
from typing import Any, ClassVar

import sqlalchemy as sa
from dateutil.parser import parse
from sqlalchemy.future import Connection

from ..action_network.donation import ActionNetworkDonation
from ..action_network.fundraising_page import ActionNetworkFundraisingPage
from ..action_network.person import ActionNetworkPerson
from ..core.logging import get_logger
from ..data_store import model, Postgres
from ..data_store.persisted_dict import PersistedDict, lookup_objects

logger = get_logger(__name__)


def supporter_email(email: str) -> str:
    email = email.strip().lower()
    if not email:
        return ""
    if len(_parts := email.split("@")) != 2:
        return ""
    # maybe validate the email or domain
    return email


class ActBlueDonationMetadata(PersistedDict):
    existing_forms: ClassVar[set] = set()

    def __init__(self, **fields):
        for key in ["item_type", "donor_email"]:
            if not fields.get(key):
                raise ValueError(f"Donation metadata must have field '{key}': {fields}")
        super().__init__(model.donation_metadata, **fields)

    def compute_status(self, conn: Connection, force: bool = False):
        if self["item_type"] == "cancellation":
            try:
                person = ActionNetworkPerson.from_lookup(
                    conn, email=self["donor_email"]
                )
                person.notice_cancellation(conn, self)
                self["updated_date"] = datetime.now(tz=timezone.utc)
            except KeyError:
                pass
            return
        if self["item_type"] != "contribution":
            # we don't process returns or other types
            return
        # look for a person with the refcode or email.  Note that refcodes are
        # only received against general pages, whereas emails are only found
        # on supporter pages, so it can only be one or the other
        if force:
            # remove computed attributions
            self["attribution_id"] = ""
        if code := self["refcode"]:
            query = sa.select(model.person_info).where(
                model.person_info.c.funder_refcode == code
            )
        elif email := self["form_owner_email"]:
            query = sa.select(model.person_info).where(
                model.person_info.c.email == email
            )
        else:
            self["updated_date"] = datetime.now(tz=timezone.utc)
            return
        if person := conn.execute(query).mappings().first():
            self.notice_person(conn, person)

    def notice_person(self, conn: Connection, person: dict):
        if self["attribution_id"] != "":
            # we've already noticed this person, break the propagation
            return
        attribution_id = person["uuid"]
        self["attribution_id"] = attribution_id
        if person["email"] == self["form_owner_email"]:
            self.notify_fundraising_pages(conn, attribution_id)
        elif (refcode := self["refcode"]) and refcode == person["funder_refcode"]:
            self.notify_donations(conn, attribution_id)
        self["updated_date"] = datetime.now(tz=timezone.utc)

    def notify_fundraising_pages(self, conn: Connection, attribution_id: str):
        title = "actblue_146845_" + self["form_name"]
        query = sa.select(model.fundraising_page_info).where(
            model.fundraising_page_info.c.title == title
        )
        for page in ActionNetworkFundraisingPage.from_query(conn, query):
            page.notice_attribution(conn, attribution_id)

    def notify_donations(self, conn: Connection, attribution_id: str):
        query = sa.select(model.donation_info).where(
            model.donation_info.c.metadata_id == self["uuid"]
        )
        for donation in ActionNetworkDonation.from_query(conn, query):
            donation.notice_attribution(conn, attribution_id)

    def contributes_to_status(self):
        if self["item_type"] == "cancellation":
            return True
        elif self["item_type"] == "return":
            return False
        elif self["item_type"] == "contribution":
            if self["refcode"]:
                return True
            if name := self["form_name"]:
                if email := supporter_email(self["form_owner_email"]):
                    if name not in self.existing_forms:
                        self.existing_forms.add(name)
                        self["form_owner_email"] = email
                        return True
        return False

    @classmethod
    def initialize_forms(cls):
        query = sa.select(model.donation_metadata.c.form_name).where(
            model.donation_metadata.c.form_name != ""
        )
        with Postgres.get_global_engine().connect() as conn:  # type: Connection
            cls.existing_forms = {row.form_name for row in conn.execute(query)}
        pass

    @classmethod
    def from_webhook(cls, body: dict) -> "ActBlueDonationMetadata":
        donor_email = body["donor"]["email"].lower()
        if not donor_email:
            raise ValueError(f"Missing donor email in ActBlue webhook: {body}")
        contribution: dict = body["contribution"]
        if not contribution:
            raise ValueError(f"Missing contribution in ActBlue webhook: {body}")
        uuid = contribution["uniqueIdentifier"]
        if uuid:
            uuid = "act_blue:" + uuid
        else:
            raise ValueError(f"Missing unique ID in ActBlue webhook: {body}")
        order_date = contribution["createdAt"]
        order_id = contribution["orderNumber"]
        refcode = contribution["refcode"] or ""
        lineitems = body["lineitems"]
        if not lineitems:
            raise ValueError(f"No lineitems in ActBlue webhook: {body}")
        if len(lineitems) > 1:
            logger.warning(f"ActBlue webhook has multiple line items: {body}")
        line_item_ids = "+".join([str(item["lineitemId"]) for item in lineitems])
        form = body["form"]
        form_name = form["name"]
        form_owner_email = form["ownerEmail"] or ""
        if cancelled_at := contribution.get("cancelledAt"):
            item_type = "cancellation"
            created_date = parse(cancelled_at)
        else:
            if paid_at := lineitems[0].get("paidAt"):
                item_type = "contribution"
                created_date = parse(paid_at)
            elif refunded_at := lineitems[0].get("refundedAt"):
                item_type = "refund"
                created_date = parse(refunded_at)
            else:
                raise ValueError(f"Can't recognize ActBlue webhook type: {body}")
        return cls(
            uuid=uuid,
            created_date=created_date,
            modified_date=created_date,
            item_type=item_type,
            donor_email=donor_email,
            order_id=order_id,
            order_date=order_date,
            line_item_ids=line_item_ids,
            form_name=form_name,
            form_owner_email=form_owner_email,
            refcode=refcode,
        )

    @classmethod
    def from_lookup(cls, conn: Connection, uuid: str) -> "ActBlueDonationMetadata":
        query = sa.select(model.donation_metadata).where(
            model.donation_metadata.c.uuid == uuid
        )
        result = lookup_objects(conn, query, lambda d: cls(**d))
        if not result:
            raise KeyError(f"No donation metadata identified by '{uuid}'")
        return result[0]

    @classmethod
    def from_query(
        cls, conn: Connection, query: Any
    ) -> list["ActBlueDonationMetadata"]:
        """
        See `.utils.lookup_objects` for details.
        """
        return lookup_objects(conn, query, lambda d: cls(**d))


def import_metadata_from_webhooks(webhooks: list[dict]) -> int:
    with Postgres.get_global_engine().connect() as conn:  # type: Connection
        imported = 0
        for data in webhooks:
            try:
                metadata = ActBlueDonationMetadata.from_webhook(data)
                if metadata.contributes_to_status():
                    metadata.persist(conn)
                    imported += 1
            except (ValueError, KeyError) as err:
                logger.warning(f"Skipping webhook: {err}: {data}")
        conn.commit()
    return imported
