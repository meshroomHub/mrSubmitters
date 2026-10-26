#!/usr/bin/env python

"""

Here goes all the boilerplate code
- Common or global variables
- Functions to generate tags, metadata
- Functions to wrap the commands
- ...

"""

import os
import json
import getpass
import tempfile
from collections import namedtuple
from typing import Optional, Tuple

from tractorSubmitter.rezUtils import CommandArgsBuilder

from meshroom.core.node import BaseNode


TRACTOR_JOB_URL = "http://tractor-engine/tv/#jid={jid}"
Chunk = namedtuple("chunk", ["iteration", "start", "end"])

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
                 node: BaseNode,
                 cmdArgs: str, 
                 cacheFolder: str="",
                 environment: dict=None, 
                 reqPackages: list=None, 
                 config: str = None,
                 licenses=None, 
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
            licenses: Eequired licenses.
            taskType: Task type and iteration if needed. Tuple[task type, iteration]
            tags: Additional metadata to set on the task.
        """
        self.node = node
        self.name = name
        self.taskCommandArgs = cmdArgs
        self.config = config
        # Env
        self.environment = environment or {}
        # Requested packages
        self.reqPackages = reqPackages or []
        # self.limits
        self.limits = self.getLimits(licenses)
        # Tags
        self.tags = tags or {}
        if node:
            self.tags["nodeUid"] = node._uid

        # Expanding / Chunks
        taskType_, iteration_ = taskType or ("placeholder", None)
        self.placeholderTask = (taskType_ == "placeholder")
        self.expandingTask = (taskType_ == "expanding")
        self.preprocessTask = (taskType_ == "preprocess")
        self.postprocessTask = (taskType_ == "postprocess")
        self.chunkTask = (taskType_ == "chunk")  
        self.iteration = iteration_
        
        # Submitter settings
        self.taskSubmitterSettings = self._getTaskSubmitterSettings()

    def _getTaskSubmitterSettings(self):
        nodeSubmitSettings = self.node.nodeDesc.getSubmitSettings(self.node)
        if self.preprocessTask:
            return nodeSubmitSettings.preprocess
        elif self.postprocessTask:
            return nodeSubmitSettings.postprocess
        else:
            return nodeSubmitSettings.process

    @staticmethod
    def getLimits(licenses=None):
        licenses = [] if licenses is None else licenses
        taskLimits = [LICENSES_MAP.get(license, license) for license in licenses]
        if 'DEFAULT_TRACTOR_LIMIT' in os.environ:
            taskLimits.append(os.environ['DEFAULT_TRACTOR_LIMIT'])
        return taskLimits

    @property
    def service(self):
        """ Get the service key for the task """
        if self.placeholderTask:
            return ""
        if self.expandingTask:
            return self.config.GLOBAL_KEY
        # Get submitter settings
        settings = self.taskSubmitterSettings
        # If an explicit service key is set use it
        if "service_key" in settings:
            return settings.service_key
        # Else try to build from the config file
        kwargs = {}
        if "cuda_tag" in settings:
            kwargs["cuda_tag"] = settings.cuda_tag
        if "excluded_hosts" in settings:
            hosts = settings.excluded_hosts
            hosts = hosts if isinstance(hosts, list) else [hosts]
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
        settings = self.taskSubmitterSettings
        env = self.environment
        print("env", env)
        if "env" in settings:
            env.update(settings.env)
        return toTractorEnv(env)

    def get_kwargs(self):
        title = f"{self.name}"
        tags = self.tags
        if self.preprocessTask:
            title += "_preprocess"
            tags["iteration"] = "preprocess"
        elif self.postprocessTask:
            title += "_postprocess"
            tags["iteration"] = "postprocess"
        elif self.chunkTask:
            if self.iteration >= 0:
                title += f"_{self.iteration}"
            else:
                title += f"_0"
            tags["iteration"] = self.iteration
        return {
            "title": title,
            "service": self.service,
            "metadata": json.dumps(self.tags)
        }

    def get_commands(self):
        """Build list of commands

        Note:
            seatup/teardown commands do not work for tractor expand commands
        """
        cmd = self.taskCommandArgs
        if self.placeholderTask or not cmd:
            return []
        settings = self.taskSubmitterSettings
        commands = []

        # Setup
        if "setup_command" in settings and not self.expandingTask:
            commands.append(settings.setup_command)

        # Build process command
        processCommand = CommandArgsBuilder(cmd)
        processCommand.setRequiredPackages(self.reqPackages)
        processCommand.setSubmissionSettings(settings)
        if self.expandingTask:
            wrapperModule = "tractorExpander.py"
            wrapperPath = os.path.join(os.environ["MR_SUBMITTERS_SCRITPS"], wrapperModule)
            processCommand.setTractorWrapper(wrapperPath)
            commands.append(processCommand.getWrappedCommand())
        else:
            if self.preprocessTask:
                processCommand.cmd += f" --preprocess"
            elif self.postprocessTask:
                processCommand.cmd += f" --postprocess"
            elif self.chunkTask:
                processCommand.cmd += f" --iteration {self.iteration}"
            commands.append(processCommand.getWrappedCommand())

        # Teardown
        if "teardown_command" in settings and not self.expandingTask:
            commands.append(settings.teardown_command)

        return commands
