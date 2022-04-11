"""
The idea behind this `aw` or `aw-cli` wrapper script is to act as a collection of helper tools,
and perhaps even as a way to list and run ActivityWatch modules on a system (a bit like aw-qt, but without the GUI).
"""

from pathlib import Path
from datetime import datetime
import subprocess

import click

from aw_cli.log import find_oldest_log, print_log, LOGLEVELS


@click.group()
@click.option("--testing", is_flag=True)
def main(testing: bool = False):
    pass


@main.command()
@click.pass_context
def qt(ctx):
    return subprocess.call(
        ["aw-qt"] + (["--testing"] if ctx.parent.params["testing"] else [])
    )


@main.command()
def directories():
    # Print all directories
    from aw_core.dirs import get_data_dir, get_config_dir, get_cache_dir, get_log_dir

    print("Directory paths used")
    print(" - config: ", get_config_dir(None))
    print(" - data:   ", get_data_dir(None))
    print(" - logs:   ", get_log_dir(None))
    print(" - cache:  ", get_cache_dir(None))


@main.command()
@click.pass_context
@click.argument("module_name", type=str, required=False)
@click.option(
    "--since",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    help="Only show logs since this date",
)
@click.option(
    "--level",
    type=click.Choice(LOGLEVELS),
    help="Only show logs of this level, or higher.",
)
def logs(ctx, module_name: str = None, since: datetime = None, level: str = None):
    from aw_core.dirs import get_log_dir

    testing = ctx.parent.params["testing"]
    logdir: Path = Path(get_log_dir(None))

    # find the oldest logfile in each of the subdirectories in the logging directory, and print the last lines in each one.

    if module_name:
        print_oldest_log(logdir / module_name, testing, since, level)
    else:
        for subdir in sorted(logdir.iterdir()):
            if subdir.is_dir():
                print_oldest_log(subdir, testing, since, level)


def print_oldest_log(path, testing, since, level):
    path = find_oldest_log(path, testing)
    if path:
        print_log(path, since, level)
    else:
        print(f"No logfile found in {path}")


if __name__ == "__main__":
    main()
