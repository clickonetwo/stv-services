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

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql as psql

metadata = sa.MetaData()

# field type for timestamp with timezone
Timestamp = sa.TIMESTAMP(timezone=True)
epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)

# sentinel value for not-yet-computed donation totals
not_computed = -1

# Persistence for the `core.Configuration` class
configuration = sa.Table(
    "configuration",
    metadata,
    sa.Column("key", sa.Text, primary_key=True, nullable=False),
    sa.Column("value", psql.JSONB, nullable=False),
)

# Person information from Action Network
person_info = sa.Table(
    "person_info",
    metadata,
    sa.Column("uuid", sa.Text, primary_key=True, nullable=False),
    sa.Column("created_date", Timestamp, index=True, nullable=False),
    sa.Column("modified_date", Timestamp, index=True, nullable=False),
    sa.Column("email", sa.Text, unique=True, index=True, nullable=False),
    sa.Column("email_status", sa.Text, nullable=True),
    sa.Column("phone", sa.Text, index=True, nullable=True),
    sa.Column("phone_type", sa.Text, nullable=True),
    sa.Column("phone_status", sa.Text, nullable=True),
    sa.Column("given_name", sa.Text, index=True, nullable=True),
    sa.Column("family_name", sa.Text, index=True, nullable=True),
    sa.Column("street_address", sa.Text, nullable=True),
    sa.Column("locality", sa.Text, nullable=True),
    sa.Column("region", sa.Text, index=True, nullable=True),
    sa.Column("postal_code", sa.Text, index=True, nullable=True),
    sa.Column("country", sa.Text, nullable=True),
    sa.Column("custom_fields", psql.JSONB, nullable=True),
    sa.Column("has_submission", sa.Boolean, default=False),
    sa.Column("total_2020", sa.Integer, index=True, default=not_computed),
    sa.Column("summary_2020", sa.Text, default=""),
    sa.Column("total_2021", sa.Integer, index=True, default=not_computed),
    sa.Column("summary_2021", sa.Text, default=""),
    sa.Column("is_contact", sa.Boolean, index=True, default=False),
    sa.Column("contact_record_id", sa.Text, index=True, default=""),
    sa.Column("contact_last_updated", Timestamp, index=True, default=epoch),
    sa.Column("is_volunteer", sa.Boolean, index=True, default=False),
    sa.Column("volunteer_record_id", sa.Text, index=True, default=""),
    sa.Column("volunteer_last_updated", Timestamp, index=True, default=epoch),
    sa.Column("is_funder", sa.Boolean, index=True, default=False),
    sa.Column("funder_record_id", sa.Text, index=True, default=""),
    sa.Column("funder_last_updated", Timestamp, index=True, default=epoch),
)

# Externally-sourced Person info
external_info = sa.Table(
    "external_info",
    metadata,
    sa.Column("email", sa.Text, primary_key=True, nullable=False),
    sa.Column("shifts_2020", sa.Integer, default=0),
    sa.Column("events_2020", sa.Integer, default=0),
    sa.Column("connect_2020", sa.Text, default=""),
    sa.Column("assigns_2020", sa.Text, default=""),
    sa.Column("notes_2020", sa.Text, default=""),
    sa.Column("history_2020", sa.Text, default=""),
    sa.Column("fundraise_2020", sa.Boolean, default=False),
    sa.Column("doorknock_2020", sa.Boolean, default=False),
    sa.Column("phonebank_2020", sa.Boolean, default=False),
    sa.Column("recruit_2020", sa.Boolean, default=False),
    sa.Column("delegate_ga_2020", sa.Boolean, default=False),
    sa.Column("delegate_pa_2020", sa.Boolean, default=False),
    sa.Column("delegate_az_2020", sa.Boolean, default=False),
    sa.Column("delegate_fl_2020", sa.Boolean, default=False),
)

# Donation info from Action Network
donation_info = sa.Table(
    "donation_info",
    metadata,
    sa.Column("uuid", sa.Text, primary_key=True, nullable=False),
    sa.Column("created_date", Timestamp, index=True, nullable=False),
    sa.Column("modified_date", Timestamp, index=True, nullable=False),
    sa.Column("amount", sa.Text, nullable=False),
    sa.Column("recurrence_data", psql.JSONB, nullable=False),
    sa.Column("donor_id", sa.Text, index=True, nullable=False),
    sa.Column("fundraising_page_id", sa.Text, index=True, nullable=False),
    sa.Column("is_donation", sa.Boolean, index=True, default=False),
    sa.Column("donation_record_id", sa.Text, index=True, default=""),
    sa.Column("donation_last_updated", Timestamp, index=True, default=epoch),
)

# Fundraising page info from Action Network
fundraising_page_info = sa.Table(
    "fundraising_page_info",
    metadata,
    sa.Column("uuid", sa.Text, primary_key=True, nullable=False),
    sa.Column("created_date", Timestamp, nullable=False),
    sa.Column("modified_date", Timestamp, index=True, nullable=False),
    sa.Column("origin_system", sa.Text, index=True, nullable=True),
    sa.Column("title", sa.Text, index=True, nullable=False),
)

# Form submission info from Action Network
submission_info = sa.Table(
    "submission_info",
    metadata,
    sa.Column("uuid", sa.Text, primary_key=True, nullable=False),
    sa.Column("created_date", Timestamp, nullable=False),
    sa.Column("modified_date", Timestamp, index=True, nullable=False),
    sa.Column("person_id", sa.Text, index=True, nullable=False),
    sa.Column("form_id", sa.Text, index=True, nullable=False),
)
