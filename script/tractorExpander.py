#!/usr/bin/env python

"""
Tractor Expander
Redirects all normal output to stderr, leaving stdout for Tractor commands.

The wrapped command sends its Tractor task definitions through a dedicated
file standing in for Tractor's stdout, whose path is passed down in the
TRACTOR_STDOUT_FILE env var (see tractorSubmitter.api.subtaskCreator). Once the
command is done, the content of that file is copied to the real stdout, which
is what Tractor parses to expand the task.

An inherited pipe file descriptor cannot be used here: the wrapped command is
usually launched through `rez env -- ...`, and rez spawns its sub-shell with
subprocess' default close_fds=True (rez/shells.py), which closes every fd > 2.

Usage:
    python tractorExpander.py script.py arg1 arg2 --option=value
"""

import os
import shlex
import subprocess
import sys
import tempfile


class TractorTaskReturnCode:
    SUCCESS = 0
    ERROR = 1
    ERROR_NO_RETRY = -999

    @classmethod
    def kill_current_process(cls, allow_auto_retry=True):
        """ I'm not sure if Tractor will immediatly kill the process so to make sure 
        we need to call sys.exit after too
        """
        return_code = cls.ERROR
        if not allow_auto_retry:
            return_code = cls.ERROR_NO_RETRY
            print(f"This job return '{return_code}' error code in order to prevent Tractor autoretry")
        # Farm trick to force exit status and prevent auto retry
        sys.stdout.write(f'TR_EXIT_STATUS {return_code}')
        sys.stdout.flush()


def main():
    if len(sys.argv) < 2:
        sys.stderr.write("Usage: tractorExpander.py <script> [args...]\n")
        sys.exit(1)

    command = sys.argv[1:]

    # Save original stdout (for Tractor commands output)
    original_stdout = sys.stdout

    # Create the file standing in for Tractor's stdout, the expand script will
    # write its task definitions there. A file (and not an inherited fd) is
    # used because the command is run through processes that close every
    # inherited fd (see module docstring).
    stdout_fd, stdout_file = tempfile.mkstemp(prefix="tractorStdout_", suffix=".alf")
    os.close(stdout_fd)

    # Set environment variable so the expand script can find the file
    os.environ['TRACTOR_STDOUT_FILE'] = stdout_file

    # Log to stderr
    sys.stderr.write(f"[tractorExpander] Executing: {' '.join(command)}\n")
    sys.stderr.write(f"[tractorExpander] Tractor stdout file: {stdout_file}\n")
    sys.stderr.flush()

    try:
        # Convert command list to shell string for alias expansion
        command_string = shlex.join(command)

        # Execute the command with stderr going to stderr, stdout going to stderr too
        # (so print statements go to stderr)
        # The expand script will write to stdout_file
        process = subprocess.Popen(
            command_string,
            stdout=sys.stderr,  # Normal output goes to stderr
            stderr=sys.stderr,
            env=os.environ.copy(),
            shell=True,
            executable='/bin/bash',
        )

        # Wait for subprocess to complete
        returncode = process.wait()

        # Forward the task definitions to the real stdout, the only stream
        # Tractor reads to expand the task
        with open(stdout_file, 'r') as stdout_reader:
            for line in stdout_reader:
                original_stdout.write(line)
        original_stdout.flush()

        sys.stderr.write(f"[tractorExpander] Command completed with exit code {returncode}\n")
        sys.stderr.flush()

        if returncode == TractorTaskReturnCode.ERROR_NO_RETRY:
            TractorTaskReturnCode.kill_current_process(allow_auto_retry=False)

        # Exit with the same code as the subprocess
        sys.exit(returncode)

    except Exception as e:
        sys.stderr.write(f"Error running command {command}: \n{e}\n")
        import traceback
        traceback.print_exc(file=sys.stderr)
        sys.exit(1)

    finally:
        # Restore stdout (cleanup)
        sys.stdout = original_stdout
        try:
            os.remove(stdout_file)
        except OSError:
            pass

if __name__ == "__main__":
    main()
