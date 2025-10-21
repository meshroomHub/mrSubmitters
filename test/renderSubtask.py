#!/usr/bin/env python

"""
In this script we will execute a small process
"""

import time
import argparse


def main(frame):
    print("Sleeping for 2 seconds")
    time.sleep(2)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-f", "--frame", type=int, default=0)
    args = parser.parse_args()
    frame = args.frame
    print(f"RENDER frame {frame}")
    main(frame)
    print("Done !")
