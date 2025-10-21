#!/usr/bin/env python

import os
import json


_tlm = None
_tq = None
try:
    # tractorLoginManager is a private python package
    # that handles authentification to Tractor
    # If not available, we offer a fallback method where you need to provide
    # credentials so that Meshroom can connect to tractor
    from tractorLoginManager import TractorLoginManager
    _tlm = TractorLoginManager()
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
        if _tlm is None:
            res = func(_tq, *args, **kwargs)
            return res
        else:
            tq = _tlm.start_query()
            res = func(tq, *args, **kwargs)
            tq.closeEngineClient()
        return res
    return wrapper


# 
# Query job/task infos
# 

JOB_KEYS = [
    "jid", "title", "spoolhost", "numactive", "numready", "numdone", "numerror", "maxtid", "priority", "afterjids"
]

@tractorQuery
def getJob(tq, jid):
    """ Request follows : 'CONDITION1 and CONDITION 2 and ...' """
    request = {"jid": jid}
    jobs = tq.jobs(request, columns=JOB_KEYS, limit=1)
    if jobs:
        job = jobs[0]
        return job
    return None

def getCurrentRunningJobInfos():
    jid = int(os.environ.get("TR_ENV_JID", -1))
    if jid < 0:
        return None
    return getJob(jid=jid)


TASK_KEYS = [
    "jid", "title", "state", "tid", "ptids", "progress", "retrycount", "currcid", "cids", "metadata"
]

def _formatTask(task):
    tractorTask = {}
    # TODO : python 3.10 : replace by match / case
    for k, v in task.items():
        if k == "jid":
            v = int(v)
        if k == "tid":
            v = int(v)
        if k == "metadata":
            v = json.loads(v)
        tractorTask[k] = v
    return tractorTask

@tractorQuery
def getJobTasks(tq, jid):
    request = {"jid": jid}
    limit = 1000
    tasks = tq.tasks(request, columns=TASK_KEYS, limit=limit)
    if len(tasks) >= limit:
        print(f"[Warning] Tractor might have not collected all the tasks ! Limit of {limit} reached")
    tractorTasks = {}
    for task in tasks:
        task = _formatTask(task)
        tid = task.get("tid")
        tractorTasks[tid] = task
    return tractorTasks

@tractorQuery
def getTask(tq, jid, tid):
    request = {"jid": jid, "tid": tid}
    tasks = tq.tasks(request, columns=TASK_KEYS, limit=1)
    if tasks:
        task = _formatTask(tasks[0])
        return task
    return None

def getCurrentRunningTaskInfos():
    jid = int(os.environ.get("TR_ENV_JID", -1))
    tid = int(os.environ.get("TR_ENV_TID", -1))
    if jid < 0 or tid < 0:
        return None
    return getTask(jid=jid, tid=tid)


# 
# Job actions
# 

@tractorQuery
def pauseJob(tq, jid):
    """ Pause job : scheduled tasks won't be launched """
    if isinstance(jid, list):
        jobs = [{"jid": j} for j in jid]
    else:
        jobs = {"jid": jid}
    tq.pause(jobs)

@tractorQuery
def unpauseJob(tq, jid):
    """ Unpause the job : Allow scheduled tasks to be launched """
    if isinstance(jid, list):
        jobs = [{"jid": j} for j in jid]
    else:
        jobs = {"jid": jid}
    tq.unpause(jobs)

@tractorQuery
def interruptJob(tq, jid):
    """ Interrupt all running tasks and block the job """
    if isinstance(jid, list):
        jobs = [{"jid": j} for j in jid]
    else:
        jobs = {"jid": jid}
    tq.interrupt(jobs)

@tractorQuery
def restartJob(tq, jid):
    """ Respool the job """
    if isinstance(jid, list):
        jobs = [{"jid": j} for j in jid]
    else:
        jobs = {"jid": jid}
    tq.interrupt(jobs)

@tractorQuery
def retryErrorTasks(tq, jid):
    """ Retry all error tasks """
    if isinstance(jid, list):
        jobs = [{"jid": j} for j in jid]
    else:
        jobs = {"jid": jid}
    tq.retryerrors(jobs)


# 
# Task actions
# 

@tractorQuery
def retryTask(tq, jid, tid):
    """ Relaunch a task """
    if isinstance(tid, list):
        tasks = [{"jid": jid, "tid": t} for t in tid]
    else:
        tasks = {"jid": jid, "tid": tid}
    tq.retry(tasks)

@tractorQuery
def resumeTask(tq, jid, tid):
    """ Resume a tasks (that have been killed, paused or interrupted I guess ? We should test this one) """
    if isinstance(tid, list):
        tasks = [{"jid": jid, "tid": t} for t in tid]
    else:
        tasks = {"jid": jid, "tid": tid}
    tq.resume(tasks)

@tractorQuery
def killTask(tq, jid, tid):
    """ Kills a running task """
    if isinstance(tid, list):
        tasks = [{"jid": jid, "tid": t} for t in tid]
    else:
        tasks = {"jid": jid, "tid": tid}
    tq.kill(tasks)

@tractorQuery
def skipTask(tq, jid, tid):
    """ Skips a task : job won't be blocked by this task and considers it as done """
    if isinstance(tid, list):
        tasks = [{"jid": jid, "tid": t} for t in tid]
    else:
        tasks = {"jid": jid, "tid": tid}
    tq.skip(tasks)
