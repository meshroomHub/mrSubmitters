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


class TractorJob(BaseSubmittedJob):
    """
    Interface to manipulate the job via Meshroom
    """
    
    def __init__(self, jid, submitter):
        super().__init__(jid, submitter)
        self.jobUrl = TRACTOR_JOB_URL.format(jid=jid)
        self.__tractorJob = None
    
    def __getTractorJob(self):
        """ Find job """
        return 0
    
    @property
    def tractorJob(self):
        if not self.__tractorJob:
            self.tractorJob = self.__getTractorJob()
        return self.__tractorJob
    
    @tractorJob.setter
    def tractorJob(self, job):
        self.__tractorJob = job
    
    def interrupt(self):
        raise NotImplementedError("[TractorJob] 'interrupt' is not implemented yet")
    
    def resume(self):
        raise NotImplementedError("[TractorJob] 'resume' is not implemented yet")
