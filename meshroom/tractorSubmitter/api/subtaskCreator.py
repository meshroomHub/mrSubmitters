#!/usr/bin/env python

"""
Helper functions to create subtasks

Provides queueSubtask() to write Tractor subtask definitions to stdout.
Works with tractorExpander.py to ensure proper stream handling.

Example :
>>> from tractorSubmitter.api.subtaskCreator import queueSubtask
>>> queueSubtask(command1, **args)
>>> queueSubtask(command2, **args)
>>> ...
"""

import sys
import os
import json
import shlex
from tractorSubmitter.api.base import TaskInfo


# Original stdout file descriptor
# Cached to avoid reopening file descriptor multiple times
EXPAND_MODE = "stdout"  # Or "file"
_stdout = None
_expandTaskFile = None


def log(*text):
    text = " ".join(text)
    sys.stderr.write(text + "\n")


def _getCachedSubtaskStdout():
    """
    Get cached subtask stdout
    """
    global _stdout
    if _stdout is None:
        if 'TRACTOR_STDOUT_FD' in os.environ:
            try:
                fd = int(os.environ['TRACTOR_STDOUT_FD'])
                # Open the file descriptor for writing
                _stdout = os.fdopen(fd, 'w', buffering=1)
            except (ValueError, OSError):
                raise RuntimeError("(_getCachedSubtaskStdout) Could not open TRACTOR_STDOUT_FD")
            log(f"(_getCachedSubtaskStdout) stdout={_stdout}")
        else:
            raise FileNotFoundError("(_getCachedSubtaskStdout) Could not find TRACTOR_STDOUT_FD")
    return _stdout


def _getCachedTaskFile():
    """
    Not used ! It would be a better alternative but since we cannot
    pass a string to cmd.expand (although it should be possible since tractor 1.7)
    we cannot use this
    """
    global _expandTaskFile
    if _expandTaskFile is None:
        if 'EXPAND_FILE' in os.environ:
            try:
                _expandTaskFile = os.environ['EXPAND_FILE']
            except (ValueError, OSError):
                raise RuntimeError("(_getCachedTaskFile) Could not open EXPAND_FILE")
            log(f"(_getCachedTaskFile) expand file: {_expandTaskFile}")
        else:
            raise FileNotFoundError("(_getCachedTaskFile) Could not find EXPAND_FILE")
    return _expandTaskFile


def sendTractorCmd(task_def):
    """ Write the tractor command to the stdout """
    if EXPAND_MODE == "stdout":
        tractor_stdout = _getCachedSubtaskStdout()
        tractor_stdout.write(task_def)
        tractor_stdout.flush()
    elif EXPAND_MODE == "file":
        expandFile = _getCachedTaskFile()
        with open(expandFile, "a+") as f:
            f.write("\n" + task_def + "\n")


def queueSubtask(title, argv, service="", limits=None, metadata=None, envkey=None):
    """
    Queue a subtask to be created in Tractor.

    Args:
        title (str): Task title
        cmd (str or list): Command to run (string or argv list)
        service (str): Tractor service key
        limits (list): Limit tags (e.g. ["blender", "nuke"])
        metadata (dict): Metadata as key:value pairs
        envkey (list): Environment key list

    # TODO : Add possibility to specify blades ?

    Example:
        queueSubtask(
            title="render_frame_0001",
            cmd="render --frame 1 scene.ma",
            service="mikrosRender",
            limits=["blender"],
            metadata={'user': 'john', 'iteration': '1', 'prod': 'mvg'}
        )
    """

    # Parse command
    if isinstance(argv, str):
        cmd_argv = shlex.split(argv)
    else:
        cmd_argv = list(argv)

    cmd_str = " ".join(cmd_argv)

    # Build tags string
    tags_str = ""
    if limits:
        tags_str = f"-tags {{{' '.join(limits)}}}"

    # Build metadata string
    if isinstance(metadata, dict):
        metadata = json.dumps(metadata)
    metadata_str = f"-metadata {{{metadata}}}"

    # Build envkey string
    envkey_str = ""
    if envkey:
        envkey_str = f"-envkey {{{' '.join(envkey)}}}"

    # Build service string
    service_str = f"-service {{{service}}}" if service else ""

    # Write Alfred task definition
    # TODO : we can use tractor API to convert a Task into alf (asTcl)
    task_def = f"""
Task -title {{{title}}} {service_str} {metadata_str} -cmds {{
    RemoteCmd {{{cmd_str}}} {service_str} {tags_str} {envkey_str}
}}
"""
    print(task_def)
    sendTractorCmd(task_def)
    log(f"Queued subtask: {title}")


def getChunks(chunkParams):
    it = None
    ignoreIterations = chunkParams.get("ignoreIterations", [])
    if chunkParams:
        start, end = chunkParams.get("start", -1), chunkParams.get("end", -2)
        size = 1
        frameRange = list(range(start, end+1, 1))
        if frameRange:
            it = [
                Chunk(i, )
            ]
            slices = [frameRange[i : i+1] for i in range(0, len(frameRange))]
            it = [Chunk(i, item[0], item[-1]) for i, item in enumerate(slices)
                    if i not in ignoreIterations]
    return it


def queueChunkTask(node, cmdArgs, service, tags=None, reqPackages=None, environment=None):
    blockSize, fullSize, nbBlocks = node.nodeDesc.parallelization.getSizes(node)
    if nbBlocks <= 0:
        return
    licenses = node.nodeDesc._licenses
    
    for iteration in range(nbBlocks):
        taskInfo = TaskInfo(
            name=node.name, 
            cmdArgs=cmdArgs,
            nodeUid=node._uid,
            environment=environment,
            reqPackages=reqPackages,
            service=service,
            licenses=licenses,
            taskType=("chunk", iteration),
            tags=tags.copy() if tags else None,
        )
        # title, argv, service, metadata
        taskArgs = taskInfo.cook()
        # limits, envkey
        taskArgs['limits'] = taskInfo.limits
        taskArgs['envkey'] = taskInfo.envkey
        queueSubtask(**taskArgs)
