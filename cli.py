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

This cli provides bulk import and maintenance operations.
"""
import click

from stv_services.action_network import bulk


@click.group()
@click.option(
    "--verbose/--no-verbose",
    default=True,
    help="Provide progress reports during execution",
)
@click.pass_context
def cli(ctx: click.Context, verbose: bool):
    ctx.ensure_object(dict)
    ctx.obj["verbose"] = verbose
    pass


@cli.command()
@click.pass_context
def update_all(ctx: click.Context):
    verbose = ctx.obj["verbose"]
    bulk.update_fundraising_pages(verbose)
    bulk.update_donations(verbose)
    bulk.update_people(verbose)
    bulk.update_donation_summaries(verbose)


@cli.command()
@click.option("--skip-pages", default=0, help="Skip this many pages")
@click.option("--max-pages", default=0, help="Import at most this many pages")
@click.pass_context
def update_people(ctx: click.Context, skip_pages: int, max_pages: int):
    verbose = ctx.obj["verbose"]
    bulk.update_people(verbose, skip_pages, max_pages)


@cli.command()
@click.option("--skip-pages", default=0, help="Skip this many pages")
@click.option("--max-pages", default=0, help="Import at most this many pages")
@click.pass_context
def update_donations(ctx: click.Context, skip_pages: int, max_pages: int):
    verbose = ctx.obj["verbose"]
    bulk.update_donations(verbose, skip_pages, max_pages)


@cli.command()
@click.option("--skip-pages", default=0, help="Skip this many pages")
@click.option("--max-pages", default=0, help="Import at most this many pages")
@click.pass_context
def update_fundraising_pages(ctx: click.Context, skip_pages: int, max_pages: int):
    verbose = ctx.obj["verbose"]
    bulk.update_fundraising_pages(verbose, skip_pages, max_pages)


@cli.command()
@click.option("--force/--no-force", default=True, help="Force re-computation")
@click.pass_context
def update_donation_summaries(ctx: click.Context, force: bool = False):
    verbose = ctx.obj["verbose"]
    bulk.update_donation_summaries(verbose, force)


if __name__ == "__main__":
    cli()
