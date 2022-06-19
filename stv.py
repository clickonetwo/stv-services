#!/usr/bin/env python3
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
"""
Command-line Interface to STV services.

This CLI provides bulk import and maintenance operations.
"""
import os.path

import click
from click_shell import shell
from sqlalchemy.future import Connection

from stv_services.act_blue import bulk as ab_bulk
from stv_services.act_blue.metadata import ActBlueDonationMetadata
from stv_services.action_network import bulk as an_bulk
from stv_services.action_network.donation import ActionNetworkDonation
from stv_services.action_network.person import ActionNetworkPerson
from stv_services.action_network.submission import ActionNetworkSubmission
from stv_services.airtable import bulk as at_bulk, sync
from stv_services.core import Configuration
from stv_services.data_store import Postgres
from stv_services.external import spreadsheet
from stv_services.mobilize import event, attendance
from stv_services.mobilize.event import MobilizeEvent
from stv_services.worker import control
from stv_services.worker.airtable import update_airtable_records


@shell(prompt="stv> ")
@click.option(
    "--verbose/--no-verbose",
    default=True,
    help="Provide progress reports during execution",
)
@click.pass_context
def stv(ctx: click.Context, verbose: bool):
    ctx.ensure_object(dict)
    ctx.obj["verbose"] = verbose
    pass


@stv.command()
@click.pass_context
def import_from_local(ctx: click.Context):
    ctx.invoke(import_external_data)
    ctx.invoke(import_donation_metadata)


@stv.command()
@click.pass_context
def import_and_update_all(ctx: click.Context):
    ctx.invoke(import_all)
    ctx.invoke(compute_status_all)
    ctx.invoke(verify_schemas)
    ctx.invoke(update_all_records)


@stv.command()
@click.pass_context
def import_all(ctx: click.Context):
    verbose = ctx.obj["verbose"]
    an_bulk.import_all(verbose)
    event.import_events(verbose)
    attendance.import_attendances(verbose)


@stv.command()
@click.option("--force/--no-force", default=False, help="Force update of all")
@click.pass_context
def update_all_records(ctx: click.Context, force: bool = False):
    verbose = ctx.obj["verbose"]
    at_bulk.update_all_records(verbose, force)


@stv.command()
@click.option("--force/--no-force", default=False, help="Force re-import of all")
@click.option("--skip-pages", default=0, help="Skip this many pages")
@click.option("--max-pages", default=0, help="Import at most this many pages")
@click.pass_context
def import_people(ctx: click.Context, force: bool, skip_pages: int, max_pages: int):
    verbose = ctx.obj["verbose"]
    an_bulk.import_people(
        verbose=verbose, force=force, skip_pages=skip_pages, max_pages=max_pages
    )


@stv.command()
@click.option("--force/--no-force", default=False, help="Force re-import of all")
@click.option("--skip-pages", default=0, help="Skip this many pages")
@click.option("--max-pages", default=0, help="Import at most this many pages")
@click.pass_context
def import_donations(ctx: click.Context, force: bool, skip_pages: int, max_pages: int):
    verbose = ctx.obj["verbose"]
    an_bulk.import_donations(verbose, force, skip_pages, max_pages)


@stv.command()
@click.option("--force/--no-force", default=False, help="Force re-import of all")
@click.option("--skip-pages", default=0, help="Skip this many pages")
@click.option("--max-pages", default=0, help="Import at most this many pages")
@click.pass_context
def import_fundraising_pages(
    ctx: click.Context, force: bool, skip_pages: int, max_pages: int
):
    verbose = ctx.obj["verbose"]
    an_bulk.import_fundraising_pages(
        verbose=verbose, force=force, skip_pages=skip_pages, max_pages=max_pages
    )


@stv.command()
@click.option("--force/--no-force", default=False, help="Force re-import of all")
@click.pass_context
def import_submissions(ctx: click.Context, force: bool):
    verbose = ctx.obj["verbose"]
    an_bulk.import_submissions(verbose=verbose, force=force)


@stv.command()
@click.option("--path", help="Import from this path")
@click.pass_context
def import_donation_metadata(ctx: click.Context, path: str = None):
    verbose = ctx.obj["verbose"]
    if not path:
        path = "./local/actblue-backfill-2022-05-12.json"
    if not os.path.isfile(path):
        raise ValueError(f"Can't find ActBlue webhooks at path '{path}'")
    ab_bulk.import_donation_metadata(path, verbose=verbose)


@stv.command()
@click.option("--force/--no-force", default=False, help="Force re-import of all")
@click.option("--skip-pages", default=0, help="Skip this many pages")
@click.option("--max-pages", default=0, help="Import at most this many pages")
@click.pass_context
def import_events(ctx: click.Context, force: bool, skip_pages: int, max_pages: int):
    verbose = ctx.obj["verbose"]
    event.import_events(
        verbose=verbose, force=force, skip_pages=skip_pages, max_pages=max_pages
    )


@stv.command()
@click.option("--force/--no-force", default=False, help="Force re-import of all")
@click.option("--skip-pages", default=0, help="Skip this many pages")
@click.option("--max-pages", default=0, help="Import at most this many pages")
@click.pass_context
def import_attendances(
    ctx: click.Context, force: bool, skip_pages: int, max_pages: int
):
    verbose = ctx.obj["verbose"]
    attendance.import_attendances(
        verbose=verbose, force=force, skip_pages=skip_pages, max_pages=max_pages
    )


@stv.command()
@click.option("--path", help="Import from this path")
@click.pass_context
def import_external_data(ctx: click.Context, path: str = None):
    verbose = ctx.obj["verbose"]
    if not path:
        path = "./local/external_data.csv"
    if not os.path.isfile(path):
        raise ValueError(f"Can't find spreadsheet at path '{path}'")
    if verbose:
        print(f"Importing from spreadsheet at '{path}'...")
    success, total = spreadsheet.import_spreadsheet(path, verbose=verbose)
    if verbose:
        print(f"Imported {success} of {total} rows successfully.")
        print(f"See error messages above for details of any errors.")


@stv.command()
@click.option("--path", help="Update from this path")
@click.pass_context
def update_external_data(ctx: click.Context, path: str = None):
    verbose = ctx.obj["verbose"]
    if not path:
        path = "./local/external_data_update.csv"
    existing_path = "./local/external_data.csv"
    new_path = "./local/updated_external_data.csv"
    email_path = "./local/updated_emails.txt"
    if not os.path.isfile(path):
        raise ValueError(f"Can't find update data file: {path}")
    if not os.path.isfile(existing_path):
        raise ValueError(f"Can't find existing data file: {existing_path}")
    if verbose:
        print(f"Updating from '{path}'...")
    success, total = spreadsheet.update_spreadsheet(
        path, existing_path, new_path, email_path, verbose=verbose
    )
    if verbose:
        print(f"Updated data for {success} of {total} emails successfully.")
        print(f"See error messages above for details of any errors.")


@stv.command()
@click.option("--force/--no-force", default=False, help="Force compute of all")
@click.pass_context
def compute_status_all(ctx: click.Context, force: bool = False):
    verbose = ctx.obj["verbose"]
    an_bulk.compute_status_all(verbose, force)
    event.compute_event_status(verbose, force)
    attendance.compute_attendance_status(verbose, force)


@stv.command()
@click.option("--force/--no-force", default=False, help="Force compute of all")
@click.option(
    "--type",
    default="people",
    help="metadata, fundraising_pages, donations, people, events, or attendances",
)
@click.pass_context
def compute_status_for_type(ctx: click.Context, type: str, force: bool = False):
    verbose = ctx.obj["verbose"]
    if type == "events":
        event.compute_event_status(verbose, force)
    elif type == "attendances":
        attendance.compute_attendance_status(verbose, force)
    elif type in ("metadata", "fundraising_pages", "donations", "people"):
        an_bulk.compute_status_for_type(type, verbose, force)
    else:
        raise ValueError(f"No such object type: {type}")


@stv.command()
@click.option("--force/--no-force", default=True, help="Force compute")
@click.option("--id", help="uuid of donation, submission, metadata, or event")
@click.option("--email", help="email of person")
@click.pass_context
def compute_status_of(
    ctx: click.Context, id: str = None, email: str = None, force: bool = True
):
    verbose = ctx.obj["verbose"]
    with Postgres.get_global_engine().connect() as conn:  # type: Connection
        if email:
            obj = ActionNetworkPerson.from_lookup(conn, email=email.lower())
        elif id.startswith("action_network"):
            try:
                obj = ActionNetworkDonation.from_lookup(conn, uuid=id)
            except KeyError:
                obj = ActionNetworkSubmission.from_lookup(conn, uuid=id)
        elif id.startswith("act_blue"):
            obj = ActBlueDonationMetadata.from_lookup(conn, uuid=id)
        elif id.isdigit():
            obj = MobilizeEvent.from_lookup(conn, uuid=int(id))
        else:
            raise ValueError(f"Can't parse object id: {id}")
        obj.compute_status(conn, force)
        obj.persist(conn)
        conn.commit()


@stv.command()
@click.pass_context
def verify_schemas(ctx: click.Context):
    verbose = ctx.obj["verbose"]
    if verbose:
        print("Verifying Airtable schemas...")
    at_bulk.verify_schemas(verbose)
    if verbose:
        print("Done.")


@stv.command()
@click.option("--force/--no-force", default=False, help="Force update of all")
@click.pass_context
def update_contacts(ctx: click.Context, force: bool = False):
    verbose = ctx.obj["verbose"]
    at_bulk.update_contact_records(verbose, force)


@stv.command()
@click.option("--force/--no-force", default=False, help="Force update of all")
@click.pass_context
def update_volunteers(ctx: click.Context, force: bool = False):
    verbose = ctx.obj["verbose"]
    at_bulk.update_volunteer_records(verbose, force)


@stv.command()
@click.option("--force/--no-force", default=False, help="Force update of all")
@click.pass_context
def update_funders(ctx: click.Context, force: bool = False):
    verbose = ctx.obj["verbose"]
    at_bulk.update_funder_records(verbose, force)


@stv.command()
@click.option("--force/--no-force", default=False, help="Force update of all")
@click.pass_context
def update_donation_records(ctx: click.Context, force: bool = False):
    verbose = ctx.obj["verbose"]
    at_bulk.update_donation_records(verbose, force)


@stv.command()
@click.option("--force/--no-force", default=False, help="Force update of all")
@click.pass_context
def update_event_records(ctx: click.Context, force: bool = False):
    verbose = ctx.obj["verbose"]
    at_bulk.update_event_records(verbose, force)


@stv.command()
@click.pass_context
def remove_contacts(ctx: click.Context):
    verbose = ctx.obj["verbose"]
    at_bulk.remove_contacts(verbose)


@stv.command()
@click.pass_context
def remove_volunteers(ctx: click.Context):
    verbose = ctx.obj["verbose"]
    at_bulk.remove_volunteers(verbose)


@stv.command()
@click.pass_context
def remove_funders(ctx: click.Context):
    verbose = ctx.obj["verbose"]
    at_bulk.remove_funders(verbose)


@stv.command()
@click.pass_context
def remove_donation_records(ctx: click.Context):
    verbose = ctx.obj["verbose"]
    at_bulk.remove_donation_records(verbose)


@stv.command()
@click.pass_context
def remove_event_records(ctx: click.Context):
    verbose = ctx.obj["verbose"]
    at_bulk.remove_event_records(verbose)


@stv.command()
@click.option("--confirm/--no-confirm", default=False, help="Yes, do it")
@click.pass_context
def delete_importable_data(ctx: click.Context, confirm: bool = False):
    verbose = ctx.obj["verbose"]
    if not confirm:
        print("You must specify the '--confirm' flag to delete importable data")
        return
    if verbose:
        print("Deleting all importable data...")
    Postgres.clear_importable_data()
    if verbose:
        print("Deleting last-update timestamps...")
    config = Configuration.get_global_config()
    to_remove = []
    for key in config.keys():  # type: str, object
        if key.endswith("_last_update_timestamp"):
            to_remove.append(key)
    for key in to_remove:
        del config[key]
    config.save_to_data_store()
    if verbose:
        print("Done.")


@stv.command()
@click.option("--path", help="Import from this path (default stdin)")
@click.pass_context
def load_config(ctx: click.Context, path: str = None):
    verbose = ctx.obj["verbose"]
    if verbose:
        print(f"Loading configuration from {path or 'stdin'}...")
    config = Configuration.get_global_config()
    config.load_from_file(path)
    config.save_to_data_store()
    if verbose:
        print(f"Loaded {len(config)} key/value pairs.")


@stv.command()
@click.option("--path", help="Dump to this path (default stdout)")
@click.pass_context
def dump_config(ctx: click.Context, path: str = None):
    verbose = ctx.obj["verbose"]
    if verbose:
        print(f"Dumping configuration to {path or 'stdout'}...")
    config = Configuration.get_global_config()
    config.save_to_file(path)
    if verbose:
        print(f"Saved {len(config)} key/value pairs.")


@stv.command()
@click.option(
    "--sync-first/--no-sync-first", default=False, help="Sync before registering"
)
@click.pass_context
def register_webhooks(ctx: click.Context, sync_first: bool = False):
    verbose = ctx.obj["verbose"]
    at_bulk.register_webhooks(verbose, sync_first)


@stv.command()
@click.option(
    "--force-remove/--no-force-remove", default=False, help="Delete all the hooks"
)
@click.pass_context
def sync_webhooks(ctx: click.Context, force_remove: bool = False):
    verbose = ctx.obj["verbose"]
    at_bulk.sync_webhooks(verbose, force_remove)


@stv.command()
@click.option("--queue", help="action_network, act_blue, airtable, or control")
@click.option("--id", help="md5 webhook ID of the request")
@click.pass_context
def resubmit_successful_webhook(ctx: click.Context, queue: str = None, id: str = None):
    verbose = ctx.obj["verbose"]
    if not queue or not id:
        raise ValueError("You must specify both the queue and hook id")
    control.resubmit_successful_requests(queue, [id])


@stv.command()
@click.option("--queue", help="Queue to resubmit (omit for all)")
@click.pass_context
def resubmit_failed_webhooks(ctx: click.Context, queue: str = None):
    verbose = ctx.obj["verbose"]
    if not queue:
        if verbose:
            print("Resubmitting all failed webhooks for re-processing")
        control.resubmit_all_failed_requests(None)
        if verbose:
            print("All failed webhooks re-submitted")
    else:
        if verbose:
            print(f"Resubmitting failed webhooks on '{queue}' for re-processing")
        control.resubmit_all_failed_requests([queue])
        if verbose:
            print(f"Failed webhooks on '{queue}' re-submitted")


@stv.command()
@click.option("--source", help="data source for update: mobilize or action_network")
@click.option("--queue/--no-queue", default=True, help="whether to queue the update")
@click.option("--force/--no-force", default=False, help="whether to re-import all data")
@click.pass_context
def update_from(
    ctx: click.Context, source: str = None, queue: bool = True, force: bool = False
):
    verbose = ctx.obj["verbose"]
    if not source:
        raise ValueError("You must specify a source")
    if verbose:
        data_word = "all" if force else "new or updated"
        action_word = "Queueing fetch of" if queue else "Fetching"
        print(f"{action_word} {data_word} data from {source}...")
    if queue:
        control.submit_update_request(source, verbose, force)
    else:
        control.execute_update_request(source, verbose, force)
    if verbose:
        data_word = "Import" if force else "Update"
        action_word = "queued" if queue else "completed"
        print(f"{data_word} {action_word}")


@stv.command()
@click.option(
    "--type", default="all", help="volunteer, contact, funder, donation, or all"
)
@click.option(
    "--remove-extra/--no-remove-extra",
    default=False,
    help="Remove unmatched records from Airtable",
)
def verify_match(type: str, remove_extra: bool = False):
    sync.verify_match(type, remove_extra=remove_extra)


@stv.command()
@click.option("--type", default="contact", help="volunteer, contact, or funder")
def analyze_match(type: str):
    report = sync.sync_report(type)
    sync.analyze_report(report)


@stv.command()
def update_airtable():
    update_airtable_records()


@stv.command()
@click.option("--email-file", help="file with one email per line")
def notice_external_data_change(email_file: str = None):
    email_file = email_file or "local/updated_emails.txt"
    if os.path.isfile(email_file):
        control.notice_external_data_change(email_file)
    else:
        print(f"No such file: {email_file}")


if __name__ == "__main__":
    stv()
