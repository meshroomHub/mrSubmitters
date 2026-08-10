#!/usr/bin/env python

"""
Helper functions to create subtasks

Provides queueSubtask() to write Tractor subtask definitions to the file
standing in for Tractor's stdout, which tractorExpander.py forwards to the
real stdout to ensure proper stream handling.

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


# Env var set by tractorExpander.py, holding the path of the file standing in
# for Tractor's stdout: everything written there, and nothing else, ends up on
# the real stdout Tractor parses to expand the task.
STDOUT_FILE_VAR = "TRACTOR_STDOUT_FILE"
# Legacy channel: a pipe file descriptor inherited from tractorExpander.py.
# Unreliable, since any intermediate process spawning with close_fds=True
# (`rez env` does, on python3) closes it, hence the file based channel above.
STDOUT_FD_VAR = "TRACTOR_STDOUT_FD"

# Output stream, cached to avoid reopening it on every subtask
_stdout = None


def log(*text):
    text = " ".join(text)
    sys.stderr.write(text + "\n")


def _openStdoutFile():
    """
    Open the file tractorExpander.py reads the task definitions from,
    or return None if no such file is declared in the environment
    """
    path = os.environ.get(STDOUT_FILE_VAR)
    if not path:
        return None
    stream = open(path, 'a', buffering=1)
    log(f"(_getCachedSubtaskStdout) using {STDOUT_FILE_VAR}={path}")
    return stream


def _openStdoutFd():
    """
    Open the legacy pipe file descriptor inherited from tractorExpander.py,
    or return None if it is not declared or no longer usable.

    The descriptor is checked with os.fstat first : it does not survive
    processes spawning with close_fds=True (`rez env` does), and reusing a
    stale number would write into whatever file took its place
    """
    raw = os.environ.get(STDOUT_FD_VAR)
    if not raw:
        return None
    try:
        fd = int(raw)
        os.fstat(fd)  # Raises OSError(EBADF) if the fd did not survive
        stream = os.fdopen(fd, 'w', buffering=1)
    except (ValueError, OSError) as err:
        log(f"(_getCachedSubtaskStdout) unusable {STDOUT_FD_VAR}={raw}: {err}")
        return None
    log(f"(_getCachedSubtaskStdout) using {STDOUT_FD_VAR}={fd}")
    return stream


def _getCachedSubtaskStdout():
    """
    Get cached subtask stdout, the stdout file being preferred over the
    legacy inherited file descriptor
    """
    global _stdout
    if _stdout is None:
        _stdout = _openStdoutFile() or _openStdoutFd()
        if _stdout is None:
            raise RuntimeError(
                "(_getCachedSubtaskStdout) No usable Tractor stdout channel: "
                f"neither {STDOUT_FILE_VAR} nor {STDOUT_FD_VAR} is set to "
                "something writable. The command must be launched through "
                "tractorExpander.py."
            )
    return _stdout


def sendTractorCmd(task_def):
    """ Write the tractor command to the stdout """
    tractor_stdout = _getCachedSubtaskStdout()
    tractor_stdout.write(task_def)
    tractor_stdout.flush()


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
        metadata = json.dumps(metadata) if metadata else ""
    metadata_str = f"-metadata {{{metadata}}}" if metadata else ""

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
