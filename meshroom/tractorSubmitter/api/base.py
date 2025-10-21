#!/usr/bin/env python

"""

Here goes all the boilerplate code
- Common or global variables
- Functions to generate tags, metadata
- Functions to wrap the commands
- ...

"""

import os
import sys
import re
import json
import getpass
import logging
import shlex
from collections import namedtuple
import tempfile


TRACTOR_JOB_URL = "http://tractor-engine/tv/#jid={jid}"
Chunk = namedtuple("chunk", ["iteration", "start", "end"])

REZ_DELIMITER_PATTERN = re.compile(r"(-|==|>=|>|<=|<)")
LICENSES_MAP = {
    'mtoa': 'arnold',
    'houdiniE': 'houdinie', 
}

PRIORITY_DICT = {
        "low": 4000,
        "normal": 5000,
        "high": 10000,
    }


def createTmpFolder(create=False):
    # PY-3.12 : d = tempfile.TemporaryDirectory(suffix=None, prefix="meshroom_expand_task", delete=False)
    tmpFolder = tempfile.mktemp(prefix="meshroom_expand_task")
    if not create:
        os.makedirs(tmpFolder)
    return tmpFolder


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


def rezWrapCommand(cmd, useCurrentContext=False, useRequestedContext=True, otherRezPkg: list[str] = None):
    """ Wrap command to be runned using rez
    :param cmd: command to run
    :type cmd: bool
    :param useCurrentContext: use current rez context to retrieve a list of rez packages
    :type useCurrentContext: bool
    :param useRequestedContext: use rez packages that have been requested (not the full context)  # TODO : remove it
    :type useRequestedContext: bool
    :param otherRezPkg: Additionnal rez packages
    :type otherRezPkg: list[str]
    """
    packages = set()
    if useCurrentContext:
        # In this case we want to use the full context
        packages.update([p for p in os.environ.get('REZ_RESOLVE', '').split(" ") if p])
    elif useRequestedContext:
        # In this case we want to use only packages in the rez request
        packages.update(getRequestPackages())
    # Add additional packages
    if otherRezPkg:
        packages.update(otherRezPkg)
    packagesStr = " ".join([p for p in packages if p])
    if packagesStr:
        rezBin = "rez"
        if "REZ_BIN" in os.environ:
            rezBin = os.environ["REZ_BIN"]
        elif "REZ_PACKAGES_ROOT" in os.environ:
            rezBin = os.path.join(os.environ["REZ_PACKAGES_ROOT"], "/bin/rez")
        return f"{rezBin} env {packagesStr} -- {cmd}"
    return cmd


def toTractorEnv(environment):
    """ Format env for Tractor """
    return [f"setenv {k}={v}" for k, v in environment.items()]


# 
# Job and Task boilerplate code
# Here are objects that can be used to prepare args for jobs and tasks
# Because they rely a lot of args and the args are often generated
# through execution context.
# 
# Here are some infos on how the jobs and tasks are created :
# 
# [JOB]
# - A job has an internal representation of a graph of tasks
# - Additionally jobs have metadata and settings
# - A job has a "job task" that does nothing, it's just there to be at the root of the graph
# - When we build the job we create the job, then the job task
# - Then we cook the job : we go through the tasks, create them through the tractor author API 
#   and add them as children to the job task or to other tasks
# 
# [TASKS]
# - When a task is cooked we prepare the task metadata and settings
# - A task can be either an "expanded task" : this task will create chunk tasks, or it's a process/chunk task
# 
# > Expanded task
# - We create the task
# - The task wraps the meshroom process that will do the necessary to create other tasks
#   The task is created through instructions sent to the stdout at the end of this task
# - When the task is finished if the stdout is correct, then the task expands and 
#   children tasks are executed
# 
# > Chunk/Process task
# - The task simply executes the meshroom_compute command
# 

class JobInfos:
    def __init__(self, name, share=None, service=None, environment=None, tags=None, user=None, comment="", paused=False):
        self.name = name
        self.share = self.getShare(share)
        self.requirements = service or {}
        self.service = service or os.environ.get('DEFAULT_TRACTOR_SERVICE')
        if not self.service:
            raise EnvironmentError('Could not find DEFAULT_TRACTOR_SERVICE in env')
        self.tags = tags or {}
        self.paused = paused
        self.comment = comment
        self.user = user or getpass.getuser()
        # auto. add FARM_USER user
        self.environment = environment or {}
        self.environment['FARM_USER'] = self.user

    @staticmethod
    def getShare(share):
        if share:
            if isinstance(share, (str, bytes)):
                share = [share]
        elif 'DEFAULT_FARM_SHARE_TRACTOR' in os.environ:
            share = os.environ['DEFAULT_FARM_SHARE_TRACTOR'].split(',')
        return share

    def cook(self):
        tags = self.tags.copy()
        env = self.environment.copy()
        return {
            "title": self.name,
            "service": self.service,
            "metadata": json.dumps(tags),
            "envkey": toTractorEnv(env),
            "paused": self.paused,
            "comment": self.comment,
            "spoolcwd": '/tmp',
            "projects": [self.share],
        }


class TaskInfos:
    def __init__(self, name, cmdArgs, nodeUid, cacheFolder="", 
                 environment=None, rezPackages=None, 
                 service=None, licenses=None, tags=None, 
                 expandingTask=False, chunkParams=None):
        self.name = name
        self.uid = nodeUid
        self.taskCommandArgs = cmdArgs
        # Env
        self.environment = environment or {}
        # Rez packages
        self.rezPackages = rezPackages or []
        # self.limits
        self.service = service or os.environ.get('DEFAULT_TRACTOR_SERVICE')
        if not self.service:
            raise EnvironmentError('Could not find DEFAULT_TRACTOR_SERVICE in env')
        self.limits = self.getLimits(licenses)
        # Tags
        self.tags = tags or {}
        self.tags["nodeUid"] = nodeUid
        # Expanding / Chunks
        self.expandingTask = expandingTask
        # self.expandingFile = self._setExpandingTaskFile(cacheFolder)
        self.chunks = [] if self.expandingTask else self.getChunks(chunkParams)

    @staticmethod
    def getLimits(licenses=None):
        licenses = [] if licenses is None else licenses
        taskLimits = [LICENSES_MAP.get(license, license) for license in licenses]
        if 'DEFAULT_TRACTOR_LIMIT' in os.environ:
            taskLimits.append(os.environ['DEFAULT_TRACTOR_LIMIT'])
        return taskLimits

    @staticmethod
    def getChunks(chunkParams) -> list[Chunk]:
        """ Get list of chunks """
        it = None
        if chunkParams:
            start, end = chunkParams.get("start", -1), chunkParams.get("end", -2)
            size = chunkParams.get("packetSize", 1)
            frameRange = list(range(start, end+1, 1))
            if frameRange:
                slices = [frameRange[i:i + size] for i in range(0, len(frameRange), size)]
                it = [Chunk(i, item[0], item[-1]) for i, item in enumerate(slices)]
        return it

    def _setExpandingTaskFile(self, cacheFolder):
        """ Doesn't work with current python API ! 
        It should be possible starting Tractor 1.7 to give a file path to cmd.expand
        But it doesn't seem to work in python
        Therefore it is not used now
        """
        if not self.expandingTask:
            return None
        if not cacheFolder:
            cacheFolder = createTmpFolder()
        if not os.path.exists(cacheFolder):
            os.makedirs(cacheFolder)
        expandingFile = os.path.join(cacheFolder, "_expand")
        with open(expandingFile, "w") as fo:
            fo.write("# Tractor commands")
        # Update env to be able to write the tractor commands on the file
        self.environment["EXPAND_FILE"] = expandingFile
        return expandingFile

    @property
    def envkey(self):
        return toTractorEnv(self.environment)
    
    def getEpandWrappedCmd(self):
        cmd = self.taskCommandArgs
        # Wrap with create_chunks
        cmd = f"meshroom_createChunks --submitter Tractor {cmd}"
        # Wrap with rez
        cmd = rezWrapCommand(cmd, otherRezPkg=self.rezPackages)
        # Wrap with tractor wrapper (will redirect stdout to stderr)
        # to make sure stdout only has the 
        wrapperModule = "tractorSubtaskWrapper.py"
        wrapperPath = os.path.join(os.environ["MR_SUBMITTERS_SCRITPS"], wrapperModule)
        cmd = f"{sys.executable} {wrapperPath} {cmd}"
        return cmd

    def cook(self):
        if self.expandingTask:
            # Chunks are not created yet so we use the wrapper and the task will expand itself
            cmd = self.getEpandWrappedCmd()
            
        elif self.chunks:
            # Empty task with multiple commands (sub-tasks) to execute in parallel
            cmd = None
        else:
            # Simple task with only one command to execute
            cmd = f"meshroom_compute {self.taskCommandArgs}"
            cmd = rezWrapCommand(cmd, otherRezPkg=self.rezPackages)
        return {
            "title": self.name,
            "argv": shlex.split(cmd) if cmd else cmd,
            "service": self.service,
            "metadata": json.dumps(self.tags)
        }


class ChunkTaskInfos:
    """
    In the case where chunks are already created, and that there are multiple chunks
    we will create the chunks from the submitter process.
    Here the taskInfos corresponds to the task for the node, and we create an instance of 
    ChunkTaskInfos per chunk that handles generating infos for the chunk task
    """
    def __init__(self, taskInfos, chunk):
        self.taskInfos: TaskInfos = taskInfos
        self.chunk: Chunk = chunk

    def cook(self):
        title = f"{self.taskInfos.name}_{self.chunk.start}_{self.chunk.end}"
        # Update cmd
        cmd = f"meshroom_compute {self.taskInfos.taskCommandArgs}"
        cmd = f"{cmd} --iteration {self.chunk.iteration}"
        cmd = rezWrapCommand(cmd, otherRezPkg=self.taskInfos.rezPackages)
        # Update tags
        chunkTags = self.taskInfos.tags.copy()
        chunkTags["iteration"] = self.chunk.iteration
        return {
            "title": title,
            "argv": shlex.split(cmd),  # Never None
            "service": self.taskInfos.service,
            "metadata": json.dumps(chunkTags),
        }
