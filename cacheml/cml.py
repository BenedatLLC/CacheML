#!/usr/bin/env  python3
"""
Command line tool for Cache ML
This is not finished yet!
"""
# Copyright 2021 Benedat LLC
# Apache 2.0 license

__all__ = ["cli"]
import sys
import click
from argparse import Namespace


@click.group()
@click.option(
    "-b",
    "--batch",
    default=False,
    is_flag=True,
    help="Run in batch mode, never ask for user inputs.",
)
@click.option(
    "--verbose",
    default=False,
    is_flag=True,
    help="Print extra debugging information and ask for confirmation before running actions.",
)
@click.pass_context
def cli(ctx, batch, verbose):
    ctx.obj = Namespace()
    ctx.obj.batch = batch
    ctx.obj.verbose = verbose
    global VERBOSE_MODE
    VERBOSE_MODE = verbose


@click.command()
@click.argument("name", default="default")
@click.pass_context
def initcache(ctx, )
@click.command()
@click.argument("name", default="default")
@click.pass_context
def keygen(ctx, name):
    """Generate a new cache encryption key with the specified name.
    The default key name is 'default'.
    """
    click.echo(f"keygen {name}")

cli.add_command(keygen)

if __name__ == '__main__':
    cli()
