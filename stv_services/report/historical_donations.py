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
import csv
from datetime import datetime

import sqlalchemy as sa

from sqlalchemy.future import Connection

from stv_services.action_network.donation import ActionNetworkDonation
from stv_services.action_network.fundraising_page import ActionNetworkFundraisingPage
from stv_services.action_network.person import ActionNetworkPerson
from stv_services.action_network.utils import fetch_hash
from stv_services.data_store import Postgres, model


def find_action_network_fundraisers() -> list:
    fundraiser_pages = []
    with Postgres.get_global_engine().connect() as conn:  # type: Connection
        query = sa.select(model.fundraising_page_info).where(
            model.fundraising_page_info.c.origin_system == "Action Network"
        )
        pages = ActionNetworkFundraisingPage.from_query(conn, query)
        print(f"Processing {len(pages)} fundraising pages from Action Network...")
        total, real = 0, 0
        for page in pages:
            if total > 0 and total % 50 == 0:
                print(f"Processed {total}, found {real}...")
            total += 1
            data, links = fetch_hash("fundraising_pages", page["uuid"])
            title: str = data.get("title", "")
            name: str = data.get("name", "")
            if (
                title.lower().find("fundraiser") < 0
                and name.lower().find("fundraiser") < 0
            ):
                continue
            # nav = links.get("osdi:creator")
            # if nav and nav.uri:
            #     index = nav.uri.rfind("/") + 1
            #     if index > 0:
            #         person_id = "action_network:" + nav.uri[index:]
            #         try:
            #             person = ActionNetworkPerson.from_lookup(conn, person_id)
            #         except KeyError:
            #             continue
            #         email: str = person["email"]
            entry = [page["uuid"], title, name]
            fundraiser_pages.append(entry)
            real += 1
    print(f"Processed {total}, found {real}.")
    return fundraiser_pages


def find_donations_for_fundraising_pages(pages: list) -> list:
    print(f"Reporting on donations for {len(pages)} fundraising pages...")
    rows = []
    p_count, d_count = 0, 0
    with Postgres.get_global_engine().connect() as conn:  # type: Connection
        for page in pages:
            if p_count > 0 and p_count % 5 == 0:
                print(f"Processed {p_count} pages, found {d_count} donations...")
            p_count += 1
            uuid, page_title, page_name = page
            query = (
                sa.select(model.donation_info)
                .where(model.donation_info.c.fundraising_page_id == uuid)
                .order_by(model.donation_info.c.created_date)
            )
            donations = ActionNetworkDonation.from_query(conn, query)
            for donation in donations:
                date: datetime = donation["created_date"]
                date_str = date.strftime("%Y-%m-%d")
                amount = donation["amount"]
                if amount == 0:
                    continue
                d_count += 1
                donor = ActionNetworkPerson.from_lookup(conn, uuid=donation["donor_id"])
                email = donor["email"]
                name = " ".join([donor["given_name"], donor["family_name"]])
                rows.append([page_title, page_name, date_str, amount, email, name])
    print(f"Processed {p_count} pages, found {d_count} donations...")
    return rows


def write_spreadsheet_for_donations(donations: list):
    field_names = ["Page Title", "Page Name", "Date", "Amount", "Email", "Name"]
    with open("local/report.csv", mode="w", newline="", encoding="utf-8") as out_file:
        writer = csv.DictWriter(out_file, field_names)
        writer.writeheader()
        for donation in donations:
            row = dict(zip(field_names, donation))
            writer.writerow(row)
    print("Wrote donations to spreadsheet 'local/report.csv'")
