import os
import re
import logging
import shlex
import shutil
import sys
from pathlib import Path


REZ_DELIMITER_PATTERN = re.compile(r"(-|==|>=|>|<=|<)")


def getResolvedVersionsDict():
    """ Get a dict {packageName: version} corresponding to the current context """
    resolvedPackages = os.environ.get('REZ_RESOLVE', '').split()
    resolvedVersions = {}
    for r in resolvedPackages:
        if r.startswith('~'):  # remove implicit packages
            continue
        v = r.split('-')
        if len(v) == 2:
            resolvedVersions[v[0]] = v[1]
        elif len(v) > 2:  # Handle case with multiple hyphen-minus
            resolvedVersions[v[0]] = "-".join(v[1:])
    return resolvedVersions


def getRequestPackages(packagesDelimiter="=="):
    """ 
    Get list of packages required for the job
    Depends on env var and current rez context

    By default we use the "==" delimiter to make sure we have the same version
    in the job that the one we have in the env where meshroom is launched
    """
    reqPackages = set()
    if 'REZ_REQUEST' in os.environ:
        # Get the names of the packages that have been requested
        requestedPackages = os.environ.get('REZ_USED_REQUEST', '').split()
        usedPackages = set()  # Use set to remove duplicates
        for p in requestedPackages:
            if p.startswith('~') or p.startswith("!"):
                continue
            v = REZ_DELIMITER_PATTERN.split(p)
            usedPackages.add(v[0])
        # Add requested packages to the reqPackages set
        resolvedVersions = getResolvedVersionsDict()
        for p in usedPackages:
            reqPackages.add(packagesDelimiter.join([p, resolvedVersions[p]]))
        logging.debug(f"TractorSubmitter: REZ Packages: {str(reqPackages)}")
    elif 'REZ_MESHROOM_VERSION' in os.environ:
        reqPackages.add(f"meshroom{packagesDelimiter}{os.environ.get('REZ_MESHROOM_VERSION', '')}")
    return list(reqPackages)


class CommandArgsBuilder:
    def __init__(self, cmd):
        self.cmd = cmd
        self.settings = None
        self.packages = []
        self.useCurrentContext=False
        self.useRequestedContext=True
        self.rezPkgDelimiter="=="
        self.tractorWrapper = None

    def setSubmissionSettings(self, settings):
        self.settings = settings

    def setRequiredPackages(self, packages: list[str] = None):
        self.packages = packages

    def setRezSettings(self, 
                       useCurrentContext: bool = False, 
                       useRequestedContext: bool = True, 
                       rezPkgDelimiter: str = "=="):
        """Set additional settings for rez

        Args:
            useCurrentContext: Use current rez context to retrieve a list of rez packages.
            useRequestedContext: Use rez packages that have been requested (not the full context)
            rezPkgDelimiter: Delimiter used for the request.
        """
        self.useCurrentContext = useCurrentContext
        self.useRequestedContext = useRequestedContext
        self.rezPkgDelimiter = rezPkgDelimiter
    
    def setTractorWrapper(self, wrapperPath):
        """ Sets a python script wrapper to wrap the command executed on farm

        It needs to be used on several occasions for example on the expanding tasks, 
        it is wrapping the whodl process so that we only write tractor TCL commands
        on the output.

        Example
            cmd = "rez env PKGS -- meshroom_createChunks ARGS"
            -> cmd = "python tractorWrapper.py rez env PKGS -- meshroom_createChunks ARGS"
        """
        self.tractorWrapper = wrapperPath

    def getRezPackages(self):
        """ Get list of packages depending on current environment and rez settings. """
        packages = set()
        if self.useCurrentContext:
            packages.update([p for p in os.environ.get('REZ_RESOLVE', '').split(" ") if p])
        elif self.useRequestedContext:
            packages.update(getRequestPackages(packagesDelimiter=self.rezPkgDelimiter))
        if self.packages:
            packages.update(self.packages)
        return [p for p in packages if p]

    def getRezExecutable(self) -> str:
        """ Find path to rez executable. If not found, use the alias "rez". """
        rezBin = "rez"
        if "REZ_BIN" in os.environ and os.environ["REZ_BIN"]:
            rezBin = os.environ["REZ_BIN"]
        elif "REZ_PACKAGES_ROOT" in os.environ and os.environ["REZ_PACKAGES_ROOT"]:
            rezBin = os.path.join(os.environ["REZ_PACKAGES_ROOT"], "bin/rez")
        elif shutil.which("rez"):
            rezBin = shutil.which("rez")
        if Path(rezBin).exists():
            return str(Path(rezBin).resolve())
        return rezBin

    def getWrappedCommand(self) -> list[str]:
        """ Wraps the rez command. 
        If a "rezWrapper" is found on the settings, call it to build the command.
        """
        # First split the command to execute
        if "target_os" in self.settings and self.settings.target_os == "windows":
            args = shlex.split(self.cmd, posix=False)
        else:
            args = shlex.split(self.cmd)
        # Get rez executable and packages
        rez_bin = self.getRezExecutable()
        rez_packages = self.getRezPackages()
        # Use the task-specific wrapper if we find one
        if "rezWrapper" in self.settings:
            args = self.settings.rezWrapper(
                rez_bin = rez_bin,
                rez_packages=rez_packages,
                args=args,
                tractor_wrapper=self.tractorWrapper
            )
        else:
            # Default : "rez env PKGS -- CMD"
            args = [rez_bin, "env"] + rez_packages + ["--"] + args
            if self.tractorWrapper:
                args = [sys.executable, self.tractorWrapper] + args
        return args
