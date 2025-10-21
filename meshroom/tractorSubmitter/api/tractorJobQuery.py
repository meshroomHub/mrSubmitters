#!/usr/bin/env python

import json
from collections import namedtuple

TRACTOR_JOB_URL = "http://tractor-engine/tv/#jid={jid}"
Chunk = namedtuple("chunk", ["iteration", "start", "end"])


tlm = None
_tq = None
try:
    # tractorLoginManager is a private python package
    # that handles authentification to Tractor
    # If not available, we offer a fallback method where you need to provide
    # credentials so that Meshroom can connect to tractor
    from tractorLoginManager import TractorLoginManager
    tlm = TractorLoginManager()
except ImportError:
    import tractor.api.query
    from submitterCredentialUi import getCredentials
    credentials = getCredentials()
    if not credentials:
        raise SystemError("Could not find credentials to use tractor")
    _tq = tractor.api.query
    _tq.setEngineClientParam(user=credentials['username'], password=credentials['password'])


def tractorQuery(func):
    def wrapper(*args, **kwargs):
        if tlm is None:
            res = func(_tq, *args, **kwargs)
            return res
        else:
            tq = tlm.start_query()
            res = func(tq, *args, **kwargs)
            tq.closeEngineClient()
        return res
    return wrapper


JOB_KEYS = [
    "jid", "title", "spoolhost", "numactive", "numready", "numdone", "numerror", "maxtid", "priority", "afterjids"
]

@tractorQuery
def getJob(tq, jobId):
    """ Request follows : 'CONDITION1 and CONDITION 2 and ...' """
    request = f"jid={jobId}"
    # jobsList = tq.jobs(request)
    jobsList = tq.jobs(request, columns=JOB_KEYS)
    if jobsList:
        job = jobsList[0]
        # return {k:v for k,v in job.items() if k in JOB_KEYS}
        return job
    return None


TASK_KEYS = [
    "jid", "title", "state", "tid", "ptids", "progress", "retrycount", "currcid", "cids", "metadata"
]

@tractorQuery
def getJobTasks(tq, jobId):
    request = f"jid={jobId}"
    tasks = tq.tasks(request, columns=TASK_KEYS)
    tractorTasks = {}
    for task in tasks:
        tid = task.get("tid")
        if "metadata" in task and task["metadata"]:
            task["metadata"] = json.loads(task["metadata"])
        # tractorTasks[tid] = {k:v for k,v in task.items() if k in TASK_KEYS}
        tractorTasks[tid] = task
    return tractorTasks


@tractorQuery
def blockTask(tq, tid, jid):
    raise NotImplementedError("Cannot block a task with current tractor API")


def waitForJob(jid):
    print(f"[TractorSubmitter] Block the current process to wait for completion of {jid}")
    import time
    while True:
        time.sleep(5)
        job = getJob(jid)
        if job.get("numactive", 0) != 0:
            continue
        if job.get("numerror", 0) != 0:
            continue
        if job.get("numdone", 0) == job.get("maxtid"):
            break
