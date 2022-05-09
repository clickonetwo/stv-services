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
from sqlalchemy.future import Connection

from ..action_network.fundraising_page import ActionNetworkFundraisingPage
from ..action_network.person import ActionNetworkPerson
from ..airtable.contact import upsert_contacts
from ..airtable.funder import upsert_funders

ORIGIN_SYSTEM = "ActBlue"
TITLE_PREFIX = "actblue_146845_"


def calculate_attribution(
    conn: Connection, page: ActionNetworkFundraisingPage, verbose: bool = False
):
    if page["attribution_status"] == "email":
        # so we have an email for the user, do we have a user for the email?
        try:
            person = ActionNetworkPerson.from_lookup(
                conn, email=page["attribution_email"]
            )
            page["attribution_id"] = person["uuid"]
            ensure_page_action(conn, person, verbose)
            republish_donations(conn, page, person, verbose)
            page["attribution_status"] = "uuid"
            page.persist(conn)
            return True
        except KeyError:
            return False
    if page["attribution_status"] in {"stv", "none"}:
        # we've already looked this up in ActBlue
        return False
    lookup_donation_page(conn, page, verbose)


def ensure_page_action(
    conn: Connection, person: ActionNetworkPerson, verbose: bool = False
):
    if not person["is_contact"] or not person["is_funder"]:
        person["is_contact"] = True
        person["is_funder"] = True
        upsert_contacts(conn, [person])
        upsert_funders(conn, [person])
    # find out if the person has a fundraising page action,
    # make one if not, and make sure it's active in any case.


def republish_donations(
    conn: Connection,
    page: ActionNetworkFundraisingPage,
    person: ActionNetworkPerson,
    verbose: bool = False,
):
    pass


def lookup_donation_page(
    conn: Connection, page: ActionNetworkFundraisingPage, verbose: bool = False
):
    if not page["title"].startswith("actblue_146845_"):
        page["attribution_status"] = "none"
        return
    act_blue_title = page["title"][len("actblue_14685_") :]
    pass
