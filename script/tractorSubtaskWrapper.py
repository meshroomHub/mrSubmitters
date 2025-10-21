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

def main():
    if len(sys.argv) < 2:
        sys.stderr.write("Usage: tractorSubtaskWrapper.py <script> [args...]\n")
        sys.exit(1)

    command = sys.argv[1:]
    
    # Save original stdout (for Tractor subtask output)
    original_stdout = sys.stdout

    # Write Alfred header to stdout FIRST
    original_stdout.write("##AlfredToDo 3.0\n")
    original_stdout.flush()

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
