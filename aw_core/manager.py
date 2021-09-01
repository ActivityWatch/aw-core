import os
import sys
import logging
import subprocess
import platform
from pathlib import Path
from glob import glob
from time import sleep
from typing import Optional, List, Hashable, Union

import aw_core

logger = logging.getLogger(__name__)

# The path of aw_qt
_module_dir = os.path.dirname(os.path.realpath(__file__))

# The path of the aw-qt executable (when using PyInstaller)
_parent_dir = os.path.abspath(os.path.join(_module_dir, os.pardir))

# Required for Manager use in aw_server for custom_watcher_pages
_parent_parent_dir = os.path.abspath(os.path.join(_parent_dir, os.pardir))


def _log_modules(modules: List["Module"]) -> None:
    for module in modules:
        logger.info(" - {} at {}".format(module.name, module.path))


def is_executable(path: str, filename: str) -> bool:
    if not os.path.isfile(path):
        return False
    # On windows all files ending with .exe are executables
    if platform.system() == "Windows":
        return filename.endswith(".exe")
    # On Unix platforms all files having executable permissions are executables
    # We do not however want to include .desktop files
    else:  # Assumes Unix
        if not os.access(path, os.X_OK):
            return False
        if filename.endswith(".desktop"):
            return False
        return True


def _discover_modules_in_directory(path: str) -> List["Module"]:
    """Look for modules in given directory path and recursively in subdirs matching aw-*"""
    modules = []
    matches = glob(os.path.join(path, "aw-*"))
    for path in matches:
        basename = os.path.basename(path)
        if is_executable(path, basename) and basename.startswith("aw-"):
            name = _filename_to_name(basename)
            modules.append(Module(name, Path(path), "bundled"))
        elif os.path.isdir(path) and os.access(path, os.X_OK):
            modules.extend(_discover_modules_in_directory(path))
        else:
            logger.warning(
                "Found matching file but was not executable: {}".format(path)
            )
    return modules


def _filename_to_name(filename: str) -> str:
    return filename.replace(".exe", "")


def _discover_modules_bundled(use_parent_parent) -> List["Module"]:
    """Use ``_discover_modules_in_directory`` to find all bundled modules """
    _search_paths = [_module_dir, _parent_dir]

    if use_parent_parent:
        _search_paths.append(_parent_parent_dir)

    logger.info("Searching for bundled modules in: {}".format(_search_paths))
    modules: List[Module] = []
    for path in _search_paths:
        modules += _discover_modules_in_directory(path)

    logger.info("Found bundled modules:")
    _log_modules(modules)
    return modules


def _discover_modules_system() -> List["Module"]:
    """Find all aw- modules in PATH"""
    search_paths = os.get_exec_path()

    # Needed because PyInstaller adds the executable dir to the PATH
    if _parent_dir in search_paths:
        search_paths.remove(_parent_dir)

    logger.debug("Searching for system modules in PATH: {}".format(search_paths))
    modules: List["Module"] = []
    paths = [p for p in search_paths if os.path.isdir(p)]
    for path in paths:
        for basename in os.listdir(path):
            if not basename.startswith("aw-"):
                continue
            if not is_executable(os.path.join(path, basename), basename):
                continue
            name = _filename_to_name(basename)
            # Only pick the first match (to respect PATH priority)
            if name not in [m.name for m in modules]:
                modules.append(Module(name, Path(path) / basename, "system"))

    logger.info("Found system modules:")
    _log_modules(modules)
    return modules


class Module:
    def __init__(self, name: str, path: Path, type: str) -> None:
        self.name = name
        self.path = path
        assert type in ["system", "bundled"]
        self.type = type
        self.started = (
            False  # Should be True if module is supposed to be running, else False
        )
        # assert location in ["system", "bundled"]
        # self.location = "system" if _is_system_module(name) else "bundled"
        self._process: Optional[subprocess.Popen[str]] = None
        self._last_process: Optional[subprocess.Popen[str]] = None

    def __hash__(self) -> int:
        return hash((self.name, self.path))

    def __eq__(self, other: Hashable) -> bool:
        return hash(self) == hash(other)

    def __repr__(self) -> str:
        return "<Module {} at {}>".format(self.name, self.path)

    def start(self, testing: bool) -> None:
        logger.info("Starting module {}".format(self.name))

        # Create a process group, become its leader
        # TODO: This shouldn't go here
        if sys.platform != "win32":
            os.setpgrp()

        exec_cmd = [str(self.path)]
        if testing:
            exec_cmd.append("--testing")
        # logger.debug("Running: {}".format(exec_cmd))

        # Don't display a console window on Windows
        # See: https://github.com/ActivityWatch/activitywatch/issues/212
        startupinfo = None
        if sys.platform == "win32" or sys.platform == "cygwin":
            startupinfo = subprocess.STARTUPINFO()
            startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
        elif sys.platform == "darwin":
            logger.info("macOS: Disable dock icon")
            import AppKit

            AppKit.NSBundle.mainBundle().infoDictionary()["LSBackgroundOnly"] = "1"

        # There is a very good reason stdout and stderr is not PIPE here
        # See: https://github.com/ActivityWatch/aw-server/issues/27
        self._process = subprocess.Popen(
            exec_cmd, universal_newlines=True, startupinfo=startupinfo
        )
        self.started = True

    def stop(self) -> None:
        """
        Stops a module, and waits until it terminates.
        """
        # TODO: What if a module doesn't stop? Add timeout to p.wait() and then do a p.kill() if timeout is hit
        if not self.started:
            logger.warning(
                "Tried to stop module {}, but it hasn't been started".format(self.name)
            )
            return
        elif not self.is_alive():
            logger.warning(
                "Tried to stop module {}, but it wasn't running".format(self.name)
            )
        else:
            if not self._process:
                logger.error("No reference to process object")
            logger.debug("Stopping module {}".format(self.name))
            if self._process:
                self._process.terminate()
            logger.debug("Waiting for module {} to shut down".format(self.name))
            if self._process:
                self._process.wait()
            logger.info("Stopped module {}".format(self.name))

        assert not self.is_alive()
        self._last_process = self._process
        self._process = None
        self.started = False

    def toggle(self, testing: bool) -> None:
        if self.started:
            self.stop()
        else:
            self.start(testing)

    def is_alive(self) -> bool:
        if self._process is None:
            return False

        self._process.poll()
        # If returncode is none after p.poll(), module is still running
        return True if self._process.returncode is None else False

    def read_log(self, testing: bool) -> str:
        """Useful if you want to retrieve the logs of a module"""
        log_path = aw_core.log.get_latest_log_file(self.name, testing)
        if log_path:
            with open(log_path) as f:
                return f.read()
        else:
            return "No log file found"

    @property
    def base_directory(self) -> Path:
        return self.path.parent

    @property
    def static_directory(self) -> Union[Path, None]:
        static_dir = self.base_directory / "static"

        if static_dir.is_dir():
            return static_dir

        return None


class Manager:
    def __init__(self, testing: bool = False, use_parent_parent: bool = False) -> None:
        self.modules: List[Module] = []
        self.testing = testing

        self.discover_modules(use_parent_parent)

    @property
    def modules_system(self) -> List[Module]:
        return [m for m in self.modules if m.type == "system"]

    @property
    def modules_bundled(self) -> List[Module]:
        return [m for m in self.modules if m.type == "bundled"]

    def discover_modules(self, use_parent_parent: bool = False) -> None:
        # These should always be bundled with aw-qt
        found_modules = set(_discover_modules_bundled(use_parent_parent))
        found_modules |= set(_discover_modules_system())
        found_modules = {
            m
            for m in found_modules
            if m.name not in ["aw-qt", "aw-qt.desktop", "aw-client"]
        }  # Exclude self

        for m in found_modules:
            if m not in self.modules:
                self.modules.append(m)

    def get_unexpected_stops(self) -> List[Module]:
        return list(filter(lambda x: x.started and not x.is_alive(), self.modules))

    def start(self, module_name: str) -> None:
        # NOTE: Will always prefer a bundled version, if available. This will not affect the
        #       aw-qt menu since it directly calls the module's start() method.
        bundled = [m for m in self.modules_bundled if m.name == module_name]
        system = [m for m in self.modules_system if m.name == module_name]
        if bundled:
            bundled[0].start(self.testing)
        elif system:
            system[0].start(self.testing)
        else:
            logger.error(
                "Manager tried to start nonexistent module {}".format(module_name)
            )

    def autostart(self, autostart_modules: List[str]) -> None:
        # NOTE: Currently impossible to autostart a system module if a bundled module with the same name exists

        # We only want to autostart modules that are both in found modules and are asked to autostart.
        for name in autostart_modules:
            if name not in [m.name for m in self.modules]:
                logger.error(f"Module {name} not found")
        autostart_modules = list(set(autostart_modules))

        # Start aw-server-rust first
        if "aw-server-rust" in autostart_modules:
            self.start("aw-server-rust")
        elif "aw-server" in autostart_modules:
            self.start("aw-server")

        autostart_modules = list(
            set(autostart_modules) - {"aw-server", "aw-server-rust"}
        )
        for name in autostart_modules:
            self.start(name)

    def stop_all(self) -> None:
        for module in filter(lambda m: m.is_alive(), self.modules):
            module.stop()


if __name__ == "__main__":
    manager = Manager()
    for module in manager.modules:
        module.start(testing=True)
        sleep(2)
        assert module.is_alive()
        module.stop()
