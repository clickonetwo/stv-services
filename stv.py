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

from stv_services.action_network import bulk as an_bulk
from stv_services.airtable import bulk as at_bulk
from stv_services.core import Configuration
from stv_services.data_store import Database
from stv_services.external.spreadsheet import import_spreadsheet


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
def update_all(ctx: click.Context):
    verbose = ctx.obj["verbose"]
    an_bulk.update_submissions(verbose)
    an_bulk.update_fundraising_pages(verbose)
    an_bulk.update_donations(verbose)
    an_bulk.update_people(verbose)
    an_bulk.update_donation_summaries(verbose)
    an_bulk.update_airtable_classifications(verbose)
    at_bulk.update_volunteers(verbose)
    at_bulk.update_contacts(verbose)
    at_bulk.update_funders(verbose)
    at_bulk.update_donation_records(verbose)


@stv.command()
@click.option("--force/--no-force", default=False, help="Force update of all")
@click.option("--skip-pages", default=0, help="Skip this many pages")
@click.option("--max-pages", default=0, help="Import at most this many pages")
@click.pass_context
def update_people(ctx: click.Context, force: bool, skip_pages: int, max_pages: int):
    verbose = ctx.obj["verbose"]
    an_bulk.update_people(
        verbose=verbose, force=force, skip_pages=skip_pages, max_pages=max_pages
    )


@stv.command()
@click.option("--force/--no-force", default=False, help="Force update of all")
@click.option("--skip-pages", default=0, help="Skip this many pages")
@click.option("--max-pages", default=0, help="Import at most this many pages")
@click.pass_context
def update_donations(ctx: click.Context, force: bool, skip_pages: int, max_pages: int):
    verbose = ctx.obj["verbose"]
    an_bulk.update_donations(verbose, force, skip_pages, max_pages)


@stv.command()
@click.option("--force/--no-force", default=False, help="Force update of all")
@click.option("--skip-pages", default=0, help="Skip this many pages")
@click.option("--max-pages", default=0, help="Import at most this many pages")
@click.pass_context
def update_fundraising_pages(
    ctx: click.Context, force: bool, skip_pages: int, max_pages: int
):
    verbose = ctx.obj["verbose"]
    an_bulk.update_fundraising_pages(
        verbose=verbose, force=force, skip_pages=skip_pages, max_pages=max_pages
    )


@stv.command()
@click.option("--force/--no-force", default=False, help="Force update of all")
@click.pass_context
def update_submissions(ctx: click.Context, force: bool):
    verbose = ctx.obj["verbose"]
    an_bulk.update_submissions(verbose=verbose, force=force)


@stv.command()
@click.option("--path", help="Import from this path")
@click.pass_context
def update_external_data(ctx: click.Context, path: str = None):
    verbose = ctx.obj["verbose"]
    if not path:
        path = "./local/Cleaned Up Data Spreadsheet for Integration.csv"
    if not os.path.isfile(path):
        raise ValueError("Can't find spreadsheet at path '{}'")
    if verbose:
        print(f"Importing from spreadsheet at '{path}'...")
    success, total = import_spreadsheet(path, verbose=verbose)
    if verbose:
        print(f"Imported {success} of {total} rows successfully.")
        print(f"See error messages above for details of any errors.")


@stv.command()
@click.option("--force/--no-force", default=False, help="Force re-computation")
@click.pass_context
def update_donation_summaries(ctx: click.Context, force: bool = False):
    verbose = ctx.obj["verbose"]
    an_bulk.update_donation_summaries(verbose, force)


@stv.command()
@click.pass_context
def update_airtable_classifications(ctx: click.Context):
    verbose = ctx.obj["verbose"]
    an_bulk.update_airtable_classifications(verbose)


@stv.command()
@click.pass_context
def verify_airtable_schemas(ctx: click.Context):
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
    at_bulk.update_contacts(verbose, force)


@stv.command()
@click.option("--force/--no-force", default=False, help="Force update of all")
@click.pass_context
def update_volunteers(ctx: click.Context, force: bool = False):
    verbose = ctx.obj["verbose"]
    at_bulk.update_volunteers(verbose, force)


@stv.command()
@click.option("--force/--no-force", default=False, help="Force update of all")
@click.pass_context
def update_funders(ctx: click.Context, force: bool = False):
    verbose = ctx.obj["verbose"]
    at_bulk.update_funders(verbose, force)


@stv.command()
@click.option("--force/--no-force", default=False, help="Force update of all")
@click.pass_context
def update_donation_records(ctx: click.Context, force: bool = False):
    verbose = ctx.obj["verbose"]
    at_bulk.update_donation_records(verbose, force)


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
@click.option("--confirm/--no-confirm", default=False, help="Yes, do it")
@click.pass_context
def delete_action_network_data(ctx: click.Context, confirm: bool = False):
    verbose = ctx.obj["verbose"]
    if not confirm:
        print("You must specify the '--confirm' flag to delete data")
        return
    if verbose:
        print("Deleting all Action Network data...")
    Database.clear_all_action_network_data()
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
@click.option("--path", help="Import from this path")
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
@click.option("--path", help="Dump to this path")
@click.pass_context
def dump_config(ctx: click.Context, path: str = None):
    verbose = ctx.obj["verbose"]
    if verbose:
        print(f"Dumping configuration to {path or 'stdout'}...")
    config = Configuration.get_global_config()
    config.save_to_file(path)
    if verbose:
        print(f"Saved {len(config)} key/value pairs.")


if __name__ == "__main__":
    stv()
