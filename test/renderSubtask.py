#!/usr/bin/env python

"""
In this script we will execute a small process
"""

import os
import time
import argparse


def render(frame):
    print(f"RENDER frame {frame}")
    print("-> Sleeping for 2 seconds")
    time.sleep(2)
    print("-> Done !")


def printenv():
    print("")
    print("Env:\n")
    import pprint
    pprint.pprint(dict(os.environ))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-f", "--frame", type=int, default=0)
    args = parser.parse_args()
    render(args.frame)
    # printenv()
