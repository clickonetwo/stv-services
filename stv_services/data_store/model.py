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

metadata = sa.MetaData()

# field type for timestamp with timezone
Timestamp = sa.TIMESTAMP(timezone=True)

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
    sa.Column("total_2020", sa.Integer, index=True, default=-1),
    sa.Column("summary_2020", sa.Text, default=""),
    sa.Column("total_2021", sa.Integer, index=True, default=-1),
    sa.Column("summary_2021", sa.Text, default=""),
    sa.Column("is_contact", sa.Boolean, index=True, default=False),
    sa.Column("contact_record_id", sa.Text, index=True, nullable=True),
    sa.Column("contact_last_updated", Timestamp, index=True, nullable=True),
    sa.Column("is_volunteer", sa.Boolean, index=True, default=False),
    sa.Column("volunteer_record_id", sa.Text, index=True, nullable=True),
    sa.Column("volunteer_last_updated", Timestamp, index=True, nullable=True),
    sa.Column("is_funder", sa.Boolean, index=True, default=False),
    sa.Column("funder_record_id", sa.Text, index=True, nullable=True),
    sa.Column("funder_last_updated", Timestamp, index=True, nullable=True),
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
