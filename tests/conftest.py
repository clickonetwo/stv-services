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
import os

import pytest


#
# mark slow tests
#
def pytest_addoption(parser):
    parser.addoption(
        "--run-slow", action="store_true", default=False, help="run slow tests"
    )


def pytest_configure(config):
    config.addinivalue_line("markers", "slow: mark test as slow to run")


def pytest_collection_modifyitems(config, items):
    if config.getoption("--run-slow"):
        # --run-slow given in stv: do not skip slow tests
        return
    skip_slow = pytest.mark.skip(reason="need --run-slow option to run")
    for item in items:
        if "slow" in item.keywords:
            item.add_marker(skip_slow)


#
# testing fixtures
#
@pytest.fixture()
def clean_db():
    """Empty all Action Network data from the database"""
    from stv_services.data_store import Postgres

    Postgres.clear_all_action_network_data()


@pytest.fixture()
def test_ids() -> dict:
    """Known Action Network IDs for use in testing."""
    return dict(
        # dev@brotsky.com
        historical_signup_non_donor="action_network:35596ac2-388d-4552-8fde-14f40563ef9a",
        # voters@brotsky.com
        historical_donor="action_network:04bfa631-fa5f-404e-8c63-a9e51a753bdc",
        historical_donor_donations=[
            "action_network:f008946f-37d5-4d66-9f46-9336df9e5b9c",
            "action_network:5e14edb5-5bd4-4f85-83db-74e14af15efb",
            "action_network:146a1343-b2af-4cdb-9f31-b5fcdf5c94ce",
            "action_network:e22be3a8-7229-4b0f-9267-bd6b9fa755ef",
            "action_network:105a358b-cbba-4389-9d01-7c6933ced370",
            "action_network:347bf8de-71e7-491d-a73e-4d1b79bae7d9",
            "action_network:687b4b15-9e92-4c48-a7f4-5edaec5a0831",
            "action_network:a0deb800-d94b-4568-ae26-d12fc6a5e363",
            "action_network:f2a2e08d-0562-49e9-a5c6-bdc14d7dfa9d",
            "action_network:63bd0d69-d6ff-4d93-93ee-c0f948b07988",
            "action_network:6d5b56e7-aeb5-48ec-ae05-8b68a7e1f538",
            "action_network:2701a5ca-1e34-46d4-bb29-4829f999f2f3",
            "action_network:c02b9ee0-bb1a-48ec-88cb-3c0ca12ee63d",
        ],
        # this donor also has a fundraising page
        # dan+test-donation-1@clickonetwo.io
        current_donor_non_signup="action_network:870b9897-fec4-41bd-8e95-d2702c1f6ca4",
        current_donor_non_signup_donations=[
            "action_network:bb448264-37e9-479d-a53b-30f45fa491a9"
        ],
        # dan+uppercase-signup-1@clickonetwo.io
        current_signup_non_donor="action_network:986ac371-7e7d-4607-b0fa-b68a8a29add6",
        current_signup_non_donor_submissions=[
            "action_network:26042188-c143-4211-863f-0d9a2b0919c7"
        ],
        # dan@clickonetwo.io
        new_user_non_signup_non_donor="action_network:dec233c3-bdee-457c-95ca-055b4647b907",
        # dan+test-2022-form@clickonetwo.io
        signup_2022_non_donor="action_network:3ef41022-47b4-4ac7-a6f1-ab266d2a16d2",
    )


@pytest.fixture()
def known_db(clean_db, test_ids) -> dict:
    from stv_services.action_network import bulk
    from stv_services import external

    bulk.import_person_cluster(test_ids["historical_signup_non_donor"], verbose=False)
    bulk.import_person_cluster(test_ids["historical_donor"], verbose=False)
    bulk.import_person_cluster(test_ids["current_donor_non_signup"], verbose=False)
    bulk.import_person_cluster(test_ids["current_signup_non_donor"], verbose=False)
    bulk.import_person_cluster(test_ids["new_user_non_signup_non_donor"], verbose=False)
    bulk.import_person_cluster(test_ids["signup_2022_non_donor"], verbose=False)
    bulk.update_donor_status(verbose=False, force=True)
    bulk.update_all_classifications(verbose=False)
    external.import_spreadsheet("./tests/external/Test Spreadsheet.csv")
    return test_ids


@pytest.fixture()
def reload_db(clean_db, test_ids) -> dict:
    os.system("tests/restore-known-db.sh")
    return test_ids


@pytest.fixture(scope="session")
def ensure_schemas() -> dict:
    from stv_services.airtable.contact import verify_contact_schema
    from stv_services.airtable.volunteer import verify_volunteer_schema
    from stv_services.airtable.funder import verify_funder_schema
    from stv_services.airtable.donation import verify_donation_schema
    from stv_services.airtable.assignment import verify_assignment_schema

    return dict(
        contact_schema=verify_contact_schema(),
        volunteer_schema=verify_volunteer_schema(),
        funder_schema=verify_funder_schema(),
        donation_schema=verify_donation_schema(),
        assignment_schema=verify_assignment_schema(),
    )


@pytest.fixture(scope="session")
def ensure_webhooks(ensure_schemas):
    from stv_services.airtable import bulk
    from stv_services.core import Configuration

    bulk.register_webhooks(verbose=False)
    webhooks = Configuration.get_global_config()["airtable_webhooks"]
    ensure_schemas.update(hook_info=webhooks)
    return ensure_schemas


@pytest.fixture(scope="session")
def ensure_web_process():
    import requests, subprocess, signal

    response = requests.get("http://localhost:8080/status", timeout=0.1)
    process = None
    if response.status_code != 200:
        process = subprocess.Popen("uvicorn stv_services.web.main:app --port 8080")
    yield "http://localhost:8080"
    if process:
        process.send_signal(signal.SIGINT)
