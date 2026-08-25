"""
The idea behind this `aw` or `aw-cli` wrapper script is to act as a collection of helper tools,
and perhaps even as a way to list and run ActivityWatch modules on a system (a bit like aw-qt, but without the GUI).
"""

from pathlib import Path
from datetime import datetime
import os
import subprocess

import click

from aw_cli.log import find_oldest_log, print_log, LOGLEVELS
from typing import Optional


def _resolve_profile(profile: Optional[str], testing: bool) -> str:
    """Return the active profile name, resolving --testing as an alias for 'testing'."""
    if profile:
        return profile
    if testing:
        return "testing"
    return ""


@click.group()
@click.option(
    "--profile",
    default=None,
    help="Named instance profile (e.g. 'testing', 'research').",
)
@click.option("--testing", is_flag=True, help="Alias for --profile testing.")
def main(profile: Optional[str] = None, testing: bool = False):
    active = _resolve_profile(profile, testing)
    if active:
        os.environ["AW_PROFILE"] = active
    # If no explicit flag, preserve any inherited AW_PROFILE from the environment.


@main.command()
@click.pass_context
def qt(ctx):
    profile = ctx.parent.params.get("profile")
    testing = ctx.parent.params.get("testing")
    active = _resolve_profile(profile, testing)
    args = ["aw-qt"]
    if active:
        args += ["--profile", active]
    return subprocess.call(args)


@main.command()
def directories():
    # Print all directories (respects AW_PROFILE set by the group callback)
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
def logs(
    ctx,
    module_name: Optional[str] = None,
    since: Optional[datetime] = None,
    level: Optional[str] = None,
):
    from aw_core.dirs import get_log_dir

    # AW_PROFILE was set by the group callback, so get_log_dir returns the profile-specific dir.
    logdir: Path = Path(get_log_dir(None))

    if module_name:
        _print_newest_log(logdir / module_name, since, level)
    else:
        for subdir in sorted(logdir.iterdir()):
            if subdir.is_dir():
                _print_newest_log(subdir, since, level)


def _print_newest_log(
    path: Path, since: Optional[datetime], level: Optional[str]
) -> None:
    logfile = find_oldest_log(path)
    if logfile:
        print_log(logfile, since, level)
    else:
        print(f"No logfile found in {path}")


if __name__ == "__main__":
    main()
