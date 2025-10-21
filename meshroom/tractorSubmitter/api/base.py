#!/usr/bin/env python

import re
import os
import json
import getpass
import logging
import shlex
from collections import namedtuple

TRACTOR_JOB_URL = "http://tractor-engine/tv/#jid={jid}"
Chunk = namedtuple("chunk", ["iteration", "start", "end"])

# Put here all common code shared between author and query
