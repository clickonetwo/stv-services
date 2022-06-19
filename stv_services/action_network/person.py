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
from datetime import datetime, timezone, timedelta
from typing import Optional, Any, ClassVar

import sqlalchemy as sa
from sqlalchemy.future import Connection

from .utils import validate_hash, fetch_all_hashes, fetch_hash, ActionNetworkObject
from ..core.logging import get_logger
from ..data_store import model
from ..data_store.persisted_dict import PersistedDict, lookup_objects

logger = get_logger(__name__)

interest_table_map = {
    "2022_calls": "contact",
    "2022_doors": "contact",
    "2022_fundraise": "funder",
    "2022_recruit": "contact",
    "2022_podlead": "contact",
    "2022_branchlead": "contact",
    "branch_lead_interest_I want to help build a branch in my region!": "contact",
    "2022_notes": "contact",
    "2022_happyhour": "funder",
    "2022_fundraisepage": "funder",
    "2022_donate": "funder",
    "2022_fundraiseidea": "funder",
}


class ActionNetworkPerson(ActionNetworkObject):
    # the database table for this class
    table: ClassVar[sa.Table] = model.person_info
    # the cache for this class
    cache: ClassVar[dict] = {}

    # if you have donated on or after this date, you are a funder
    funder_cutoff_lo = datetime(2021, 11, 1, tzinfo=timezone.utc)
    # if you have filled out a form on or after this date, you are a contact
    contact_cutoff_lo = datetime(2022, 1, 1, tzinfo=timezone.utc)
    # we care specially about specific forms
    signup_form_2022 = "action_network:b399bd2b-b9a9-4916-9550-5a8a47e045fb"

    def __init__(self, **fields):
        if not fields.get("email") and not fields.get("phone"):
            raise ValueError(f"Person record must have either email or phone: {fields}")
        super().__init__(**fields)

    def compute_status(self, conn: Connection, force: bool = False):
        """
        Compute the status of a person based on their history. The status
        determines what tables they belong in, whether they have a recurring
        donation or not, and so on.

        We are careful never to remove a person from a table. We only update
        based on data since the last check unless we are forced to.
        """
        if force:
            # initialize the fields we conditionally recompute, so we are sure
            # to recompute them in this pass
            self["updated_date"] = model.epoch
            self["recur_start"] = model.epoch
            self["recur_end"] = model.epoch
            self["last_donation"] = model.epoch
            self["has_submission"] = False
        # this should always be computed on import, but in case not
        if self["created_date"] >= self.contact_cutoff_lo:
            self["is_contact"] = True
        else:
            self["is_volunteer"] = True
        # now look back at history since last update
        cutoff_lo = self.get("updated_date", model.epoch)
        self.compute_submission_status(conn, cutoff_lo)
        # because of Action Network data issues, we have to compute
        # cancellation status *before* we compute donor status
        self.compute_cancellation_status(conn, cutoff_lo)
        self.compute_donor_status(conn, cutoff_lo)
        self["updated_date"] = datetime.now(tz=timezone.utc)

    def notice_promotion(self, _conn: Connection, source: str):
        """Notice that volunteer has become a contact or that contact has
        become a funder.  Always updates the source person so that the
        checkboxes get updated in various records showing the person."""
        self["is_contact"] = True
        if source == "contact" or self.get("last_donation", model.epoch) > model.epoch:
            self["is_funder"] = True
        self["updated_date"] = datetime.now(tz=timezone.utc)

    def compute_submission_status(self, conn: Connection, cutoff_lo: datetime):
        table = model.submission_info
        query = (
            sa.select(table)
            .where(
                sa.and_(
                    table.c.form_id == self["uuid"],
                    table.c.created_date >= cutoff_lo,
                )
            )
            .order_by(table.c.created_date.desc())
        )
        signup = conn.execute(query).mappings().first()
        self.notice_submission(conn, signup)

    def notice_submission(self, conn: Connection, submission: dict = None):
        # if they have checked any of the 2022 form fields, they are contacts
        # and possibly funders (if it's a form field on the fundraising form)
        custom_fields = self.get("custom_fields", {})
        for key in custom_fields:
            if interest_table_map.get(key):
                self["has_submission"] = True
                self.notice_promotion(conn, "submission")
                break
        else:
            if submission and not self.get("has_submission"):
                is_recent = submission["created_date"] > self.contact_cutoff_lo
                is_signup = submission["form_id"] == self.signup_form_2022
                if is_signup or is_recent:
                    self["has_submission"] = True
                    self.notice_promotion(conn, "submission")
        self["updated_date"] = datetime.now(tz=timezone.utc)

    def compute_donor_status(self, conn: Connection, cutoff_lo: datetime):
        # first make sure we take into account a contact status change
        if (
            self.get("is_contact")
            and self.get("last_donation", model.epoch) > model.epoch
        ):
            self["is_funder"] = True
            self["updated_date"] = datetime.now(tz=timezone.utc)
        # then look for any new donations
        table = model.donation_info
        query = (
            sa.select(table)
            .where(
                sa.and_(
                    table.c.donor_id == self["uuid"],
                    table.c.created_date >= cutoff_lo,
                )
            )
            .order_by(table.c.created_date.asc())
        )
        donations: list[dict] = conn.execute(query).mappings().all()
        max_2021 = datetime(2022, 1, 1, tzinfo=timezone.utc)
        max_2020 = datetime(2021, 1, 1, tzinfo=timezone.utc)
        if model.epoch < cutoff_lo < max_2021:
            raise ValueError("Invalid cutoff date for historical donations")
        total_2021, entries_2021 = 0, []
        total_2020, entries_2020 = 0, []
        for donation in donations:
            self.notice_donation(conn, donation)
            donation_date = donation["created_date"]
            if donation_date >= max_2021:
                continue
            amount = float(donation["amount"])
            day = donation_date.strftime("%m/%d/%y")
            entry = f"${int(round(amount, 0))} ({day})"
            if donation_date >= max_2020:
                total_2021 += amount
                entries_2021.append(entry)
            else:
                total_2020 += amount
                entries_2020.append(entry)
        if cutoff_lo == model.epoch:
            # apply historic summaries - only first time we do this
            self["total_2021"] = total_2021
            self["summary_2021"] = ", ".join(entries_2021)
            self["total_2020"] = total_2020
            self["summary_2020"] = ", ".join(entries_2020)
            # if we haven't seen any recurrences this year,
            # then they must have been a lapsed older donor
            recur_start = self.get("recur_start", model.epoch)
            if model.epoch < recur_start < max_2021:
                if self.get("recur_end", model.epoch) <= max_2021:
                    self["recur_end"] = max_2021

    def notice_donation(self, _conn: Connection, donation: dict = None):
        """This logic expects that we see donations in chronologically
        increasing order"""
        if not donation:
            return
        donation_date = donation["created_date"]
        if donation_date >= self.get("last_donation", model.epoch):
            self["last_donation"] = donation_date
        else:
            logger.warning(f"Donation '{self['uuid']}' arrived out of order")
        if donation_date > self.funder_cutoff_lo:
            # donations on or after 11/1/2021 make them a contact and a funder
            self["is_contact"] = True
            self["is_funder"] = True
        elif self.get("is_contact"):
            # contacts who donate are funders
            self["is_funder"] = True
        # if this is a recurring donation, update their recurring start date
        recurrence_data = donation.get("recurrence_data", {})
        if recurrence_data.get("recurring"):
            if recurrence_data.get("period") == "Yearly":
                logger.warning(f"Yearly donor '{self['uuid']}' will show as lapsed")
            if donation_date > self.get("recur_start", model.epoch):
                self["recur_start"] = donation_date
        # Data problem: Action Network doesn't mark recurring donations after
        # the first one as being recurring.  So if this donation comes within
        # a month of the recur_start date, and we don't have an actual
        # cancellation, we assume that it's actually a recurring donation.
        # This only matters for donations that don't come through ActBlue,
        # so we could also check on the fundraising page origin system,
        # but that would require yet another database lookup, so we don't.
        # Testing shows this code works well enough for our purposes.
        elif self.get("recur_end", model.epoch) == model.epoch:
            delta = donation_date - self.get("recur_start", model.epoch)
            # why do we allow 64 days rather than 32 between recurring
            # monthly donations?  Because sometimes your credit card expires,
            # you miss a month, and then you fix it, so it resumes in
            # the next month!  We have several donors like this, e.g.,
            # 'action_network:259aac2e-b796-4b98-9674-c9ab86893c84'
            # and this is why it's better to be on ActBlue
            if timedelta(days=0) <= delta <= timedelta(days=64):
                self["recur_start"] = donation_date
        self["updated_date"] = datetime.now(tz=timezone.utc)

    def compute_cancellation_status(self, conn: Connection, cutoff_lo: datetime):
        # if they have a cancellation, they have a recurring end
        query = (
            sa.select(model.donation_metadata)
            .where(
                sa.and_(
                    model.donation_metadata.c.item_type == "cancellation",
                    model.donation_metadata.c.donor_email == self["email"],
                    model.donation_metadata.c.created_date > cutoff_lo,
                )
            )
            .order_by(model.donation_metadata.c.created_date.desc())
        )
        cancel = conn.execute(query).mappings().first()
        self.notice_cancellation(conn, cancel)

    def notice_cancellation(self, _conn: Connection, metadata: dict = None):
        if not metadata:
            return
        cancel_date = metadata["create_date"]
        if cancel_date > self.get("recur_end", model.epoch):
            self["recur_end"] = cancel_date
        self["updated_date"] = datetime.now(tz=timezone.utc)

    def notice_supporter_page(self, _conn: Connection):
        """This person is a supporter."""
        self["funder_has_page"] = True
        self["is_contact"] = True
        self["is_funder"] = True
        self["updated_date"] = datetime.now(tz=timezone.utc)

    def notice_refcode(self, _conn: Connection, new: str):
        """Tell a person of a refcode.  This is allowed to remove as well as
        change an existing refcode, because people make mistakes sometimes,
        and we have to allow for data updates.  Note, however, that once a
        donation has been received with a given refcode, and that refcode was
        associated at that time with a given user, changing that user's
        refcode will have no effect on past or future donations with the old
        code."""
        current = self.get("funder_refcode", "")
        if current == new:
            # this person already has this refcode, so nothing changes
            return
        if current and new != current:
            # If the new refcode is an extension of the current one, we assume
            # that we're just seeing multiple notifications during refcode entry
            if new.startswith(current):
                logger.warning(f"Extending refcode from '{current}' to '{new}'")
            else:
                logger.warning(f"Replacing refcode '{current}' with '{new}'")
        self["funder_refcode"] = new
        self["is_contact"] = True
        self["is_funder"] = True
        self["updated_date"] = datetime.now(tz=timezone.utc)
        return True

    def notice_team_lead_change(
        self, _conn: Connection, old: PersistedDict, new: PersistedDict
    ):
        """Change someone's team lead.  This actually affects three people:
        the person whose lead changed, the old lead, and the new lead, because
        all of their records have to be updated to show the lead change."""
        if old == new:
            return
        if old:
            old["updated_date"] = datetime.now(tz=timezone.utc)
        if new:
            new["updated_date"] = datetime.now(tz=timezone.utc)
            self["team_lead"] = new["uuid"]
        else:
            self["team_lead"] = ""
        self["updated_date"] = datetime.now(tz=timezone.utc)

    def notice_external_data(self):
        """Update due to external data change"""
        self["updated_date"] = datetime.now(tz=timezone.utc)

    def notice_attendance(self, conn: Connection, _attendance_: dict):
        """Update due to signing up for an event"""
        self.notice_promotion(conn, "attendance")
        self["updated_date"] = datetime.now(tz=timezone.utc)

    def notice_event(self, conn: Connection, _event: dict):
        """Update due to organizing an event"""
        self.notice_promotion(conn, "event")
        self["updated_date"] = datetime.now(tz=timezone.utc)

    def update_from_hash(self, data: dict):
        """Update data from the info in a hash (either a webhook or an import).

        This doesn't compute status, so it doesn't change the updated date.
        Anyone who calls this should compute status afterwards, exactly as
        if they had imported the data directly from Action Network."""
        new = self._parse_hash(data)
        new = {key: val for key, val in new.items() if val is not None}
        # preserve the new custom fields
        custom_fields = new["custom_fields"]
        # remove the fields that can't replace what's in self
        for key in ["uuid", "created_date", "custom_fields"]:
            del new[key]
        # replace the fields that should be replaced
        self.update(new)
        # merge the new custom fields with the existing ones
        self["custom_fields"].update(custom_fields)

    @staticmethod
    def _parse_hash(data: dict) -> dict:
        uuid, created_date, modified_date = validate_hash(data)
        is_contact, is_volunteer = None, None
        if created_date.year >= 2022:
            is_contact = True
        else:
            is_volunteer = True
        given_name: str = data.get("given_name")
        family_name: str = data.get("family_name", "")
        email: Optional[str] = None
        email_status: Optional[str] = None
        for entry in data.get("email_addresses", []):  # type: dict
            if entry.get("primary"):
                if address := entry.get("address"):
                    email = address.lower()
                email_status = entry.get("status")
                break
        phone: Optional[str] = None
        phone_type: Optional[str] = None
        phone_status: Optional[str] = None
        for entry in data.get("phone_numbers", []):  # type: dict
            if entry.get("primary"):
                phone = entry.get("number")
                phone_type = entry.get("number_type")
                phone_status = entry.get("status")
                break
        street_address: Optional[str] = None
        locality: Optional[str] = None
        region: Optional[str] = None
        postal_code: Optional[str] = None
        country: Optional[str] = None
        for entry in data.get("postal_addresses", []):  # type: dict
            if entry.get("primary"):
                if lines := entry.get("address_lines"):
                    street_address = "\n".join(lines)
                locality = entry.get("locality")
                region = entry.get("region")
                postal_code = entry.get("postal_code")
                country = entry.get("country")
                break
        custom_fields: dict = data.get("custom_fields", {})
        return dict(
            uuid=uuid,
            created_date=created_date,
            email=email,
            modified_date=modified_date,
            email_status=email_status,
            phone=phone,
            phone_type=phone_type,
            phone_status=phone_status,
            given_name=given_name,
            family_name=family_name,
            street_address=street_address,
            locality=locality,
            region=region,
            postal_code=postal_code,
            country=country,
            custom_fields=custom_fields,
            is_contact=is_contact,
            is_volunteer=is_volunteer,
        )

    @classmethod
    def from_hash(cls, data: dict) -> "ActionNetworkPerson":
        parse = cls._parse_hash(data)
        return cls(**parse)

    @classmethod
    def from_lookup(
        cls, conn: Connection, uuid: Optional[str] = None, email: Optional[str] = None
    ) -> "ActionNetworkPerson":
        if uuid:
            query = sa.select(model.person_info).where(model.person_info.c.uuid == uuid)
        elif email:
            query = sa.select(model.person_info).where(
                model.person_info.c.email == email
            )
        else:
            raise ValueError("One of uuid or email must be specified for lookup")
        result = lookup_objects(conn, query, lambda d: cls(**d))
        if not result:
            raise KeyError(f"No person identified by '{uuid or email}'")
        return result[0]

    @classmethod
    def from_query(cls, conn: Connection, query: Any) -> list["ActionNetworkPerson"]:
        """
        See `.utils.lookup_objects` for details.
        """
        return lookup_objects(conn, query, lambda d: cls(**d))

    @classmethod
    def from_action_network(cls, hash_id: str) -> "ActionNetworkPerson":
        data, _ = fetch_hash("people", hash_id)
        person = ActionNetworkPerson.from_hash(data)
        return person


def import_people(
    query: Optional[str] = None,
    verbose: bool = True,
    skip_pages: int = 0,
    max_pages: int = 0,
) -> int:
    ActionNetworkPerson.initialize_cache()
    return fetch_all_hashes(
        "people",
        cls=ActionNetworkPerson,
        query=query,
        verbose=verbose,
        skip_pages=skip_pages,
        max_pages=max_pages,
    )
