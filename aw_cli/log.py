from pathlib import Path
from datetime import datetime
from typing import Optional


LOGLEVELS = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]


def print_log(
    path: Path, since: Optional[datetime] = None, level: Optional[str] = None
):
    if not path.is_file():
        return

    show_levels = LOGLEVELS[LOGLEVELS.index(level) :] if level else None

    lines_printed = 0
    with path.open("r") as f:
        lines = f.readlines()
        print(f"Logs for module {path.parent.name} ({path.name}, {len(lines)} lines)")
        for line in lines:
            if since:
                try:
                    linedate = datetime.strptime(line.split(" ")[0], "%Y-%m-%d")
                except ValueError:
                    # Could not parse the date, so skip this line
                    # NOTE: Just because the date could not be parsed, doesn't mean there isn't meaningful info there.
                    #       Would be better to find the first line after the cutoff, and then just print everything past that.
                    continue
                # Skip lines before the date
                if linedate < since:
                    continue
            if level:
                if not any(level in line for level in show_levels):
                    continue
            print(line, end="")
            lines_printed += 1

    print(f"  (Filtered {lines_printed}/{len(lines)} lines)")


def find_oldest_log(path: Path) -> Optional[Path]:
    """Return the most-recently-modified log file under *path*.

    Since aw-core #149 each profile runs in its own platform directory
    (``activitywatch-<profile>``), so no filename-based profile filtering is
    needed here — every ``.log`` file in *path* belongs to the active profile.
    """
    if not path.is_dir():
        return None

    logfiles = [f for f in path.iterdir() if f.is_file() and f.name.endswith(".log")]
    if not logfiles:
        return None

    logfiles.sort(key=lambda f: f.stat().st_mtime)
    return logfiles[-1]
