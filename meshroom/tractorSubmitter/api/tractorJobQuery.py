#!/usr/bin/env python

import re
import os
import json
import getpass
import logging
import shlex
from collections import namedtuple
from meshroom.core.submitter import BaseSubmittedJob

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


@tractorQuery
def getJob(tq, jobId):
    """ Request follows : 'CONDITION1 and CONDITION 2 and ...' """
    JOB_KEYS = []
    request = f"jid={jobId}"
    jobsList = tq.jobs(request)
    if jobsList:
        job = jobsList[0]
        return {k:v for k,v in job.items() if k in JOB_KEYS}}
    return None




class TractorJob(BaseSubmittedJob):
    """
    Interface to manipulate the job via Meshroom
    """

    def __init__(self, jid, submitter):
        super().__init__(jid, submitter)
        self.jid = jid
        # self.jobUrl = TRACTOR_JOB_URL.format(jid=jid)
        self.__tractorJob = None

    def __getTractorJob(self):
        """ Find job """
        return getJob(self.jid)

    @property
    def tractorJob(self):
        if not self.__tractorJob:
            self.__tractorJob = self.__getTractorJob()
        return self.__tractorJob

    def interrupt(self):
        raise NotImplementedError("[TractorJob] 'interrupt' is not implemented yet")

    def resume(self):
        raise NotImplementedError("[TractorJob] 'resume' is not implemented yet")
