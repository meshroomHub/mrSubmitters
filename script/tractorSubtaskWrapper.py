#!/usr/bin/env python

"""
Tractor Subtask Wrapper
Redirects all normal output to stderr, leaving stdout for Tractor subtask definitions.

Usage:
    python tractorSubtaskWrapper.py createTasks.py arg1 arg2 --option=value
"""

import sys
import os
import shlex
import subprocess


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
        sys.stdout.write('TR_EXIT_STATUS {}'.format(return_code))
        sys.stdout.flush()


def main():
    if len(sys.argv) < 2:
        sys.stderr.write("Usage: tractorSubtaskWrapper.py <script> [args...]\n")
        sys.exit(1)

    command = sys.argv[1:]
    
    # Save original stdout (for Tractor subtask output)
    original_stdout = sys.stdout

    # Create a pipe for capturing subtask output
    read_fd, write_fd = os.pipe()
    
    # Set environment variable so subtaskCreator.py can find the write end
    os.environ['TRACTOR_SUBTASK_STDOUT_FD'] = str(write_fd)
    
    # Log to stderr
    sys.stderr.write(f"[tractorSubtaskWrapper] Executing: {' '.join(command)}\n")
    sys.stderr.flush()

    try:
        # Convert command list to shell string for alias expansion
        command_string = shlex.join(command)
        
        # Execute the command with stderr going to stderr, stdout going to stderr too
        # (so print statements go to stderr)
        # The subtaskCreator will write to write_fd
        process = subprocess.Popen(
            command_string,
            stdout=sys.stderr,  # Normal output goes to stderr
            stderr=sys.stderr,
            env=os.environ.copy(),
            shell=True,
            executable='/bin/bash',
            pass_fds=(write_fd,)  # Pass the write_fd to subprocess
        )
        
        # Close write end in parent (subprocess has it)
        os.close(write_fd)
        
        # Read from pipe and write to original stdout
        with os.fdopen(read_fd, 'r') as pipe_reader:
            for line in pipe_reader:
                original_stdout.write(line)
                original_stdout.flush()
        
        # Wait for subprocess to complete
        returncode = process.wait()
        
        sys.stderr.write(f"[tractorSubtaskWrapper] Command completed with exit code {returncode}\n")
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

if __name__ == "__main__":
    main()
