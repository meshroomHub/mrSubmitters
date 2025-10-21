#!/usr/bin/env python

import os
import json
import re
import shlex
import getpass

REZ_DELIMITER_PATTERN = re.compile(r"-|==|>=|>|<=|<")
TRACTOR_JOB_URL = "http://tractor-engine/tv/#jid={jid}"


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


def createJob(tractorAuthor, nb_subtasks, priority=5000):
    """
    Create a job with a single task
    The goal of this task is to spool additional tasks
    """
    mainTags = {'prod': "mvg", 'nbFrames': "5", 'comment': "test job"}
    allRequirements = ["mikrosRender"]
    projects = ["vfx"]
    
    # Create job
    job = tractorAuthor.Job(
        title="[Tractor test] (Job) creating tasks", 
        service=",".join(allRequirements), 
        metadata=json.dumps(mainTags),
        envkey=get_envkey(),
        paused=False,
        comment="",
        spoolcwd='/tmp',
        projects=projects
    )
    # Job task
    jobTask = job.newTask(title="[Tractor test] (Job task) creating tasks", 
                          argv=None, serialsubtasks=True)
    
    # Pre-render task
    jobStart = tractorAuthor.Task(
        title="[Tractor test] (Task) Start",
        argv=None,
        service=",".join(allRequirements), 
        metadata=json.dumps(mainTags)
    )
    prerenderTask = tractorAuthor.Task(
        title="[Tractor test] (Task) Pre-render",
        argv=shlex.split(rezWrapCommand("testPrerender")),
        service=",".join(allRequirements), 
        metadata=json.dumps(mainTags)
    )
    
    # Render task
    cmd = f"tractorSubtaskWrapper testCreateRenderSubtasks {nb_subtasks}" 
    cmd = rezWrapCommand(cmd)
    tractorCmd = shlex.split(cmd)
    renderTask = tractorAuthor.Task(
        title="[Tractor test] (Task) Render",
        argv=tractorCmd,
        service=",".join(allRequirements), 
        metadata=json.dumps(mainTags)
    )
    for cmd in renderTask.cmds:
        cmd.tags = []
        cmd.envkey = get_envkey()
        cmd.expand = True
    
    # Post-render task
    postrenderTask = tractorAuthor.Task(
        title="[Tractor test] (Task) Post-render",
        argv=shlex.split(rezWrapCommand("testPostrender")),
        service=",".join(allRequirements), 
        metadata=json.dumps(mainTags)
    )
        
    jobTask.addChild(postrenderTask)
    postrenderTask.addChild(renderTask)
    renderTask.addChild(prerenderTask)
    prerenderTask.addChild(jobStart)
    
    # Submit
    user = os.environ.get('FARM_USER', os.environ.get('USER', getpass.getuser()))
    job.priority = priority
    jid = job.spool(block=False, owner=user)
    return jid


def main():
    nb_subtasks = 5
    from tractor.api import author as tractorAuthor
    jid = createJob(tractorAuthor, nb_subtasks, priority=9000)
    print(f"Created job: {jid}")
    print(f"-> {TRACTOR_JOB_URL.format(jid=jid)}")
    

if __name__ == "__main__":
    main()
