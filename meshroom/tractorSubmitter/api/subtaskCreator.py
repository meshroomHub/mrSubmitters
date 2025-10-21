#!/usr/bin/env python

"""
Helper functions to create subtasks

Provides queueSubtask() to write Tractor subtask definitions to stdout.
Works with tractorSubtaskWrapper.py to ensure proper stream handling.

Example :
>>> from tractorSubmitter.api.subtaskCreator import queueSubtask
>>> queueSubtask(command1, **args)
>>> queueSubtask(command2, **args)
>>> ...
"""

import sys
import os
import shlex


# Original stdout file descriptor
# Cached to avoid reopening file descriptor multiple times
_stdout = None


def log(*text):
    text = " ".join(text)
    sys.stderr.write(text + "\n")


def _getCachedSubtaskStdout():
    """
    Get cached subtask stdout
    """
    global _stdout
    if _stdout is None:
        if 'TRACTOR_SUBTASK_STDOUT_FD' in os.environ:
            try:
                fd = int(os.environ['TRACTOR_SUBTASK_STDOUT_FD'])
                # Open the file descriptor for writing
                _stdout = os.fdopen(fd, 'w', buffering=1)
            except (ValueError, OSError):
                _stdout = sys.stdout
            log(f"(_getCachedSubtaskStdout) stdout={_stdout}")
        else:
            raise FileNotFoundError("(_getCachedSubtaskStdout) Could not find TRACTOR_SUBTASK_STDOUT_FD")
    return _stdout


def queueSubtask(title, cmd, service="", limits=None, metadata=None, envkey=None):
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
    # Get the correct stdout for Tractor
    tractor_stdout = _getCachedSubtaskStdout()

    # Parse command
    if isinstance(cmd, str):
        cmd_argv = shlex.split(cmd)
    else:
        cmd_argv = list(cmd)

    cmd_str = " ".join(cmd_argv)

    # Build tags string
    tags_str = ""
    if limits:
        tags_str = f"-tags {{{' '.join(limits)}}}"

    # Build metadata string
    metadata_str = ""
    if metadata:
        metadata_list = [f"{k}:{v}" for k, v in metadata.items()]
        metadata_str = f"-metadata {{{' '.join(metadata_list)}}}"

    # Build envkey string
    envkey_str = ""
    if envkey:
        envkey_str = f"-envkey {{{' '.join(envkey)}}}"

    # Build service string
    service_str = f"-service {{{service}}}" if service else ""

    # Write Alfred task definition
    task_def = f"""
Task -title {{{title}}} {service_str} {metadata_str} -cmds {{
    RemoteCmd {{{cmd_str}}} {service_str} {tags_str} {envkey_str}
}}
"""

    tractor_stdout.write(task_def)
    tractor_stdout.flush()

    log(f"Queued subtask: {title}")
