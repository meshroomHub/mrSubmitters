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
import shutil
import copy
from collections import namedtuple
import tempfile
from typing import Optional, Tuple, TYPE_CHECKING

if TYPE_CHECKING:
    from types import ModuleType
    from meshroom.core.node import BaseNode

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
    """ Get a dict {packageName: version} corresponding to the current context. """

    resolvedPackages = os.environ.get('REZ_USED_RESOLVE', '').split()
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
    if 'REZ_USED_REQUEST' in os.environ:
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


def rezWrapCommand(cmd, useCurrentContext=False, useRequestedContext=True,
                   otherRezPkg: list[str] = None):
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
        packages.update([p for p in os.environ.get('REZ_USED_RESOLVE', '').split(" ") if p])
    elif useRequestedContext:
        # In this case we want to use only packages in the rez request
        packages.update(getRequestPackages())
    # Add additional packages
    if otherRezPkg:
        packages.update(otherRezPkg)
    packagesStr = " ".join([p for p in packages if p])
    if packagesStr:
        rezBin = "rez"
        if "REZ_BIN" in os.environ and os.environ["REZ_BIN"]:
            rezBin = os.environ["REZ_BIN"]
        elif "REZ_PACKAGES_ROOT" in os.environ and os.environ["REZ_PACKAGES_ROOT"]:
            rezBin = os.path.join(os.environ["REZ_PACKAGES_ROOT"], "bin/rez")
        elif shutil.which("rez"):
            rezBin = shutil.which("rez")
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
# Here are some information on how the jobs and tasks are created :
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
# - A task can be either an "expanded task" : this task will create chunk tasks, or it's a
#   process/chunk task
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

class JobInfo:
    def __init__(self, name, share=None, serviceKey=None, environment=None, tags=None, user=None,
                 comment="", paused=False):
        self.name = name
        self.share = self.getShare(share)
        self.serviceKey = serviceKey or os.environ.get("DEFAULT_TRACTOR_SERVICE", "")
        self.tags = tags or {}
        self.paused = paused
        self.comment = comment
        self.user = user or getpass.getuser()
        # auto. add FARM_USER user
        self.environment = environment or {}
        self.environment['FARM_USER'] = self.user
        if "PROD_MOUNT" in os.environ:
            self.environment['PROD_MOUNT'] = os.environ["PROD_MOUNT"]

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
            "service": self.serviceKey,
            "metadata": json.dumps(tags),
            "envkey": toTractorEnv(env),
            "paused": self.paused,
            "comment": self.comment,
            "spoolcwd": '/tmp',
            "projects": [self.share],
        }


class TaskInfo:
    def __init__(self, 
                 name: str, 
                 node: "BaseNode",
                 cmdArgs: str, 
                 cacheFolder: str="",
                 environment: dict=None, 
                 reqPackages: list=None, 
                 config: "ModuleType" = None,
                 taskType:Optional[Tuple]=None, 
                 tags=None):
        """Object to gather, manipulate, generate task infos

        Args:
            name: name of the task (usually the node name). 
                  For the final title we add the task type (chunk index)
            node: Node
            cmdArgs: Command to execute
            cacheFolder: Folder containing the node cache.
            environment: Environment to set. Dict with key:value.
            reqPackages: List of requested packages.
            config: Tractor configuration module.
            taskType: Task type and iteration if needed. Tuple[task type, iteration]
            tags: Additional metadata to set on the task.
        """
        # Check task type : placeholder, expanding, stage, etc
        taskType_, iteration_ = taskType or ("placeholder", None)
        self.placeholderTask = (taskType_ == "placeholder")
        self.expandingTask = (taskType_ == "expanding")
        self.preprocessTask = (taskType_ == "preprocess")
        self.postprocessTask = (taskType_ == "postprocess")
        self.chunkTask = (taskType_ == "chunk")  
        self.iteration = iteration_

        # Save the node
        self.node = node
        # Submitter settings - needed for getLimits
        self.taskSubmitterSettings = self._getTaskSubmissionSettings()
        
        # Fill other info
        self.name = name
        self.taskCommandArgs = cmdArgs
        self.config = config
        # Env
        self.environment = environment or {}
        # Requested packages
        self.reqPackages = reqPackages or []
        # Limits (rez, licenses, ...)
        self.limits = self.getLimits()
        # Tags
        self.tags = self.getTags(tags)

    def _getTaskSubmissionSettings(self):
        if self.placeholderTask:
            return None
        nodeSubmitSettings = self.node.nodeDesc.getSubmissionSettings(self.node)
        if self.preprocessTask:
            return nodeSubmitSettings.preprocess
        elif self.postprocessTask:
            return nodeSubmitSettings.postprocess
        else:
            return nodeSubmitSettings.process

    def getLimits(self):
        licenses = []
        if self.taskSubmitterSettings and "licenses" in self.taskSubmitterSettings:
            licenses = self.taskSubmitterSettings.licenses
        taskLimits = [LICENSES_MAP.get(license, license) for license in licenses]
        if 'DEFAULT_TRACTOR_LIMIT' in os.environ:
            taskLimits.append(os.environ['DEFAULT_TRACTOR_LIMIT'])
        return taskLimits

    def getTags(self, tags=None):
        tags = tags or {}
        if self.taskSubmitterSettings and "tags" in self.taskSubmitterSettings:
            tags.update(self.taskSubmitterSettings.tags)
        if self.node:
            tags["nodeUid"] = self.node._uid
        return tags

    @property
    def service(self):
        """ Get the service key for the task """
        if self.placeholderTask:
            return ""
        if self.expandingTask:
            return self.config.GLOBAL_KEY
        # Get submitter settings
        settings = self.taskSubmitterSettings
        if not settings:
            return ""  # Only for placeholder, should be handled before
        # If an explicit service key is set use it
        if "service_key" in settings:
            return settings.service_key
        # Else try to build from the config file
        kwargs = {}
        if "cuda_tag" in settings:
            kwargs["cuda_tag"] = settings.cuda_tag
        if "excluded_hosts" in settings:
            hosts = settings.excluded_hosts
            hosts = list(hosts) if isinstance(hosts, (list, tuple)) else [hosts]
            kwargs["excluded_hosts"] = hosts
        service = self.config.get_config(
            cpu=settings.cpu,
            ram=settings.ram,
            gpu=settings.gpu,
            **kwargs
        )
        return service

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
        env = copy.deepcopy(self.environment)
        if self.taskSubmitterSettings and "env" in self.taskSubmitterSettings:
            env.update(self.taskSubmitterSettings.env)
        return toTractorEnv(env)

    def cook(self):
        title = f"{self.name}"
        tags = self.tags
        cmd = self.taskCommandArgs
        if self.preprocessTask:
            cmd += f" --preprocess"
            title += "_preprocess"
            tags["iteration"] = "preprocess"
        elif self.postprocessTask:
            cmd += f" --postprocess"
            title += "_postprocess"
            tags["iteration"] = "postprocess"
        elif self.chunkTask:
            if self.iteration >= 0:
                title += f"_{self.iteration}"
            else:
                title += f"_0"
            cmd += f" --iteration {self.iteration}"
            tags["iteration"] = self.iteration
        
        if self.expandingTask:
            cmd = rezWrapCommand(cmd, otherRezPkg=self.reqPackages)
            # Wrap with tractor wrapper (will redirect stdout to stderr)
            # to make sure stdout only has the
            wrapperModule = "tractorExpander.py"
            wrapperPath = os.path.join(os.environ["MR_SUBMITTERS_SCRITPS"], wrapperModule)
            cmd = f"{sys.executable} {wrapperPath} {cmd}"
        elif self.placeholderTask:
            cmd = None
        else:
            cmd = rezWrapCommand(cmd, otherRezPkg=self.reqPackages)

        return {
            "title": title,
            "argv": shlex.split(cmd) if cmd else cmd,
            "service": self.service,
            "metadata": json.dumps(self.tags)
        }
