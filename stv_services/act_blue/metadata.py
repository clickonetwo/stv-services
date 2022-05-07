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
from dateutil.parser import parse

from ..data_store import model
from ..data_store.persisted_dict import PersistedDict


class ActBlueDonationMetadata(PersistedDict):
    def __init__(self, **fields):
        for key in ["item_type", "donor_email"]:
            if not fields.get(key):
                raise ValueError(f"Donation metadata must have field '{key}': {fields}")
        super().__init__(model.donation_metadata, **fields)

    @classmethod
    def from_webhook_body(cls, body: dict) -> "ActBlueDonationMetadata":
        donor: dict = body.get("donor", {})
        contribution: dict = body.get("contribution", {})
        line_items: list[dict] = body.get("lineitems", [])
        form: dict = body.get("form", {})
        if not (donor and contribution and line_items):
            raise ValueError(f"Missing required elements in body: {body}")
        uuid = "+".join([item["lineitemId"] for item in line_items])
        if not uuid:
            raise ValueError(f"Missing line items in body: {body}")
        if not (created_at := contribution.get("createdAt")):
            raise ValueError(f"No 'createdAt' in contribution: {body}")
        created_date = parse(created_at)
        modified_date = created_date
        if not (donor_email := donor.get("email")):
            raise ValueError(f"Missing donor in body: {body}")
        donor_email = donor_email.lower()
        external_uuid = contribution.get("uniqueIdentifier")
        recurrence_data = None
        ref_codes = None
        if cancelled_at := contribution.get("cancelledAt"):
            item_type = "cancellation"
            recurrence_data = {"cancelledAt": cancelled_at}
            for key in ["recurringType", "recurCompleted", "recurPledged"]:
                if val := contribution.get(key):
                    recurrence_data[key] = val
        elif contribution.get("refundedAt"):
            item_type = "refund"
            # we don't keep info on refunds
        else:
            item_type = "contribution"
            ref_codes = contribution.get("refcodes", {})
            recurrence_data = {}
            for key in [
                "recurringPeriod",
                "recurringDuration",
                "weeklyRecurringSunset",
            ]:
                if val := contribution.get(key):
                    recurrence_data[key] = val
        form_name = None
        form_owner_email = None
        if form:
            form_name = form.get("name")
            form_owner_email = form.get("ownerEmail")
        return cls(
            uuid=uuid,
            created_date=created_date,
            modified_date=modified_date,
            donor_email=donor_email,
            item_type=item_type,
            external_uuid=external_uuid,
            recurrence_data=recurrence_data,
            form_name=form_name,
            form_owner_email=form_owner_email,
            ref_codes=ref_codes,
        )
