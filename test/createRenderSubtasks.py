#!/usr/bin/env python

"""
We can execute anything here.

The stdout is redirected to stderr with the wrapper so if we want to spool subtasks 
we can use  queueSubtask :
>>> from subtaskCreator import queueSubtask
>>> queueSubtask(title, cmd, service, tags, metadata, envkey)
"""

import os
import re
import getpass
import logging
from tractorSubmitter.api.subtaskCreator import queueSubtask

REZ_DELIMITER_PATTERN = re.compile(r"-|==|>=|>|<=|<")


# Configure logging - will go to stderr (Tractor log)
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s: %(message)s'
)
logger = logging.getLogger(__name__)


def rezWrapCommand(cmd):
    rezPackages = set()
    if 'REZ_REQUEST' in os.environ:
        packages = os.environ.get('REZ_USED_REQUEST', '').split()
        resolvedPackages = os.environ.get('REZ_RESOLVE', '').split()
        resolvedVersions = {}
        for r in resolvedPackages:
            if r.startswith('~'):  # remove implicit packages
                continue
            v = r.split('-')
            if len(v) == 2:
                resolvedVersions[v[0]] = v[1]
            elif len(v) > 2:  # Handle case with multiple hyphen-minus
                resolvedVersions[v[0]] = "-".join(v[1:])
        usedPackages = set()  # Use set to remove duplicates
        for p in packages:
            if p.startswith('~') or p.startswith("!"):
                continue
            v = REZ_DELIMITER_PATTERN.split(p)
            usedPackages.add(v[0])
        for p in usedPackages:
            # Use "==" to make sure we have the same version in the job that the one we have in the env
            # where meshroom is launched
            rezPackages.add("==".join([p, resolvedVersions[p]]))
    packagesStr = " ".join([p for p in rezPackages if p])
    if packagesStr:
        rezBin = "rez"
        if "REZ_BIN" in os.environ:
            rezBin = os.environ["REZ_BIN"]
        elif "REZ_PACKAGES_ROOT" in os.environ:
            rezBin = os.path.join(os.environ["REZ_PACKAGES_ROOT"], "/bin/rez")
        return f"{rezBin} env {packagesStr} -- {cmd}"
    return cmd


def get_envkey():
    environment = {}
    if 'REZ_DEV_PACKAGES_ROOT' in os.environ:
        environment['REZ_DEV_PACKAGES_ROOT'] = os.environ['REZ_DEV_PACKAGES_ROOT']
    if 'REZ_PROD_PACKAGES_PATH' in os.environ:
        environment['REZ_PROD_PACKAGES_PATH'] = os.environ['REZ_PROD_PACKAGES_PATH']
    if 'PROD' in os.environ:
        environment['PROD'] = os.environ['PROD']
    if 'PROD_ROOT' in os.environ:
        environment['PROD_ROOT'] = os.environ['PROD_ROOT']
    environment["FARM_USER"] = os.environ.get('FARM_USER', os.environ.get('USER', getpass.getuser()))
    return [f"setenv {k}={v}" for k, v in environment.items()]



def main(nb_subtasks):
    print("[MAIN]")
    logger.info("Starting subtask creation...")
    
    name = "[Tractor test](Subtask)Render"
    user = os.environ.get('FARM_USER', os.environ.get('USER', getpass.getuser()))
    limits = ["blender"]
    service = "mikrosRender"
    
    # Create subtasks
    for index in range(nb_subtasks):
        metadata = {
            'prod': "mvg",
            'comment': "",
            'iteration': str(index),
            'user': user
        }
        cmd = f"testRenderSubtask --frame {index}"
        cmd = rezWrapCommand(cmd)
        
        queueSubtask(
            title=f"{name}_{index:04d}",
            cmd=cmd,
            service=service,
            limits=limits,
            metadata=metadata,
            envkey=get_envkey()
        )
    
    logger.info(f"Successfully queued {nb_subtasks} subtasks")
    print(f"Done! Created subtasks for frames 0-{nb_subtasks-1}")


if __name__ == "__main__":
    import sys
    nb_subtasks = int(sys.argv[1])
    main(nb_subtasks)
