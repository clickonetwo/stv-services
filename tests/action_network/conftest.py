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
import pytest

import sqlalchemy as sa
from stv_services.data_store import Database, model


@pytest.fixture()
def clean_db():
    with Database.get_global_engine().connect() as conn:
        conn.execute(sa.delete(model.person_info))
        conn.execute(sa.delete(model.donation_info))
        conn.execute(sa.delete(model.fundraising_page_info))
        conn.execute(sa.delete(model.submission_info))


# Lots of interesting identifiers
# historical_signup_non_donor = "action_network:35596ac2-388d-4552-8fde-14f40563ef9a"
# historical_donor = "action_network:04bfa631-fa5f-404e-8c63-a9e51a753bdc"
# historical_donor_donations = [
#     "action_network:f008946f-37d5-4d66-9f46-9336df9e5b9c",
#     "action_network:5e14edb5-5bd4-4f85-83db-74e14af15efb",
#     "action_network:146a1343-b2af-4cdb-9f31-b5fcdf5c94ce",
#     "action_network:e22be3a8-7229-4b0f-9267-bd6b9fa755ef",
#     "action_network:105a358b-cbba-4389-9d01-7c6933ced370",
#     "action_network:347bf8de-71e7-491d-a73e-4d1b79bae7d9",
#     "action_network:687b4b15-9e92-4c48-a7f4-5edaec5a0831",
#     "action_network:a0deb800-d94b-4568-ae26-d12fc6a5e363",
#     "action_network:f2a2e08d-0562-49e9-a5c6-bdc14d7dfa9d",
#     "action_network:63bd0d69-d6ff-4d93-93ee-c0f948b07988",
#     "action_network:6d5b56e7-aeb5-48ec-ae05-8b68a7e1f538",
#     "action_network:2701a5ca-1e34-46d4-bb29-4829f999f2f3",
#     "action_network:c02b9ee0-bb1a-48ec-88cb-3c0ca12ee63d",
# ]
# # this donor also has a fundraising page
# current_donor_non_signup = "action_network:870b9897-fec4-41bd-8e95-d2702c1f6ca4"
# current_donor_non_signup_donations = [
#     "action_network:bb448264-37e9-479d-a53b-30f45fa491a9"
# ]
# current_signup_non_donor = "action_network:986ac371-7e7d-4607-b0fa-b68a8a29add6"
# current_signup_non_donor_submissions = [
#     "action_network:26042188-c143-4211-863f-0d9a2b0919c7"
# ]
