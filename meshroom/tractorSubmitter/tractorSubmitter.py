#!/usr/bin/env python

import os
import shutil
import json
import getpass
import logging
import importlib
from meshroom.core.submitter import BaseSubmitter, SubmitterOptions, SubmitterOptionsEnum
import tractorSubmitter.api.tractorJobQuery as tq
from tractorSubmitter.api.base import getRequestPackages
from tractorSubmitter.api.base import TaskInfos, ChunkTaskInfos
from tractorSubmitter.api.tractorJobCreation import Task, Job
from tractorSubmitter.api.subtaskCreator import queueChunkTask
from meshroom.core.submitter import BaseSubmittedJob

currentDir = os.path.dirname(os.path.realpath(__file__))
binDir = os.path.dirname(os.path.dirname(os.path.dirname(currentDir)))


class TractorJob(BaseSubmittedJob):
    """
    Interface to manipulate the job via Meshroom
    """

    def __init__(self, jid, submitter):
        super().__init__(jid, submitter)
        self.jid = jid
        self.submitter: TractorSubmitter = submitter
        # self.jobUrl = TRACTOR_JOB_URL.format(jid=jid)
        self.__tractorJob = None
        self.__tractorJobTasks = None

    def printInfos(self):
        print(f"[Tractor Job] {self.jid}")
        print(f"        job : {self.tractorJob}")
        print(f"      tasks : ")
        for _, task in self.tractorJobTasks.items():
            meta = task.get('metadata')
            uid = None
            if meta:
                uid = meta.get("uid")
            print(f"            - [{uid}] {task}")

    def __getTractorInfos(self):
        """ Find job """
        self.__tractorJob = tq.getJob(self.jid)
        self.__tractorJobTasks = tq.getJobTasks(self.jid)

    @property
    def tractorJob(self):
        if not self.__tractorJob:
            self.__getTractorInfos()
        return self.__tractorJob

    @property
    def tractorJobTasks(self):
        if not self.__tractorJobTasks:
            self.__getTractorInfos()
        return self.__tractorJobTasks

    def __getChunkTasks(self, nodeUid, iteration):
        tasks = []
        for _, task in self.tractorJobTasks.items():
            taskNodeUid = task["metadata"].get("nodeUid", None)
            taskIt = task["metadata"].get("iteration", -1)
            if taskNodeUid == nodeUid and taskIt == iteration:
                tasks.append(task)
        return tasks

    # Task actions

    def stopChunkTask(self, node, iteration):
        """ This will kill one task """
        tasks = self.__getChunkTasks(node._uid, iteration)
        for task in tasks:
            tq.killTask(self.jid, task["tid"])

    def skipChunkTask(self, node, iteration):
        """ This will kill one task """
        tasks = self.__getChunkTasks(node._uid, iteration)
        for task in tasks:
            tq.skipTask(self.jid, task["tid"])

    def restartChunkTask(self, node, iteration):
        """ This will kill one task """
        tasks = self.__getChunkTasks(node._uid, iteration)
        for task in tasks:
            tq.retryTask(self.jid, task["tid"])  # or resumeTask ?

    # Job actions

    def pauseJob(self):
        """ This will pause the job : new tasks will not be processed """
        tq.pauseJob(self.jid)

    def resumeJob(self):
        """ This will unpause the job """
        tq.unpauseJob(self.jid)

    def interruptJob(self):
        """ This will interrupt the job (and kill running tasks) """
        tq.interruptJob(self.jid)

    def restartJob(self):
        """ Restarts the whole job """
        tq.restartJob(self.jid)
    
    def restartErrorTasks(self):
        """ Restart all error tasks on the job """
        tq.retryErrorTasks(self.jid)


def loadConfig(configpath):
    if not configpath:
        raise FileNotFoundError(f"Could not load tractor config from file {configpath}")
    import importlib.util
    spec = importlib.util.spec_from_file_location("tractorConfig", configpath)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class TractorSubmitter(BaseSubmitter):
    """
    Meshroom submitter to tractor
    """
    
    _name = "Tractor"
    _options = SubmitterOptions(SubmitterOptionsEnum.ALL)
    
    dryRun = False
    environment = {}
    DEFAULT_TAGS = {"prod": ""}

    configpath = os.environ.get("TRACTORCONFIG")
    if not configpath:
        configpath = os.path.join(os.environ.get("MR_SUBMITTERS_CONFIGS"), "tractorConfig.py")
    config = loadConfig(configpath)
    
    def __init__(self, parent=None):
        super().__init__(parent=parent)
        self.share = os.environ.get("MESHROOM_TRACTOR_SHARE", "vfx")
        self.prod = os.environ.get("PROD", "mvg")
        self.reqPackages = getRequestPackages()
        if "REZ_DEV_PACKAGES_ROOT" in os.environ:
            self.environment["REZ_DEV_PACKAGES_ROOT"] = os.environ["REZ_DEV_PACKAGES_ROOT"]
        if "REZ_PROD_PACKAGES_PATH" in os.environ:
            self.environment["REZ_PROD_PACKAGES_PATH"] = os.environ["REZ_PROD_PACKAGES_PATH"]
        if "PROD" in os.environ:
            self.environment["PROD"] = os.environ["PROD"]
        if "PROD_ROOT" in os.environ:
            self.environment["PROD_ROOT"] = os.environ["PROD_ROOT"]
    
    def getTaskService(self, node):
        service = self.config.get_config(
            cpu=node.nodeDesc.cpu.value,
            ram=node.nodeDesc.ram.value,
            gpu=node.nodeDesc.gpu.value,
            excludeHosts=[]
        )
        return service
    
    def retrieveJob(self, jid) -> TractorJob:
        job = TractorJob(jid, self)
        return job

    def createTask(self, job: Job, meshroomFile: str, node) -> Task:
        print(f"Tractor Submitter : Add node {node.name} ({node})")
        tags = self.DEFAULT_TAGS.copy()  # copy to not modify default tags
        optionalArgs = {}
        if not (hasattr(node, '_chunksCreated') and node._chunksCreated):
            # Chunks will be created by the process
            optionalArgs["expandingTask"] = True
        elif node.isParallelized:
            blockSize, fullSize, nbBlocks = node.nodeDesc.parallelization.getSizes(node)
            if nbBlocks > 1:  # Is it better like this ?
                optionalArgs["chunkParams"] = {'start': 0, 'end': nbBlocks - 1, 'step': 1}
        tags['nbFrames'] = node.size
        tags['prod'] = self.prod
        # Fetch licenses
        licenses = node.nodeDesc._licenses
        cmdArgs = f"--node {node.name} \"{meshroomFile}\" --extern"
        task = job.createTask(
            name=node.name,
            commandArgs=cmdArgs,
            uid=node._uid,  # Provide unicity info
            nodeCache=node._internalFolder, 
            tags=tags,
            rezPackages=self.reqPackages,
            service=self.getTaskService(node),
            licenses=licenses,
            **optionalArgs
        )
        return task

    def createJob(self, nodes, edges, filepath, submitLabel="{projectName}"):
        projectName = os.path.splitext(os.path.basename(filepath))[0]
        name = submitLabel.format(projectName=projectName)
        comment = filepath
        maxNodeSize = max([node.size for node in nodes])
        mainTags = {
            'prod': self.prod,
            'nbFrames': str(maxNodeSize),
            'comment': comment,
        }
        # Create job
        job = Job(
            name,
            tags=mainTags,
            environment=self.environment,
            user=os.environ.get('FARM_USER', os.environ.get('USER', getpass.getuser())),
        )
        # Create tasks
        nodeUidToTask: dict[str, Task] = {}
        for node in nodes:
            if node._uid in nodeUidToTask:
                continue  # HACK: Should not be necessary
            task = self.createTask(job, filepath, node)
            nodeUidToTask[node._uid] = task
        # Connect tasks
        for u, v in edges:
            nodeUidToTask[u._uid].addChild(nodeUidToTask[v._uid])
        # Submit job
        res = job.submit(share=self.share, dryRun=self.dryRun)
        if self.dryRun:
            return True
        if len(res) == 0:
            return False
        submittedJob = TractorJob(res.get("id"), TractorSubmitter)
        return submittedJob

    def createChunkTask(self, node, graphFile, **kwargs):
        """
        Keyword args : cache, forceStatus, forceCompute
        """
        taskTags = self.DEFAULT_TAGS.copy()
        taskTags['nbFrames'] = node.size
        taskTags['prod'] = self.prod
        # Environment
        environment = self.environment.copy()
        environment['FARM_USER'] = os.environ.get('FARM_USER', os.environ.get('USER', getpass.getuser()))
        # Command
        cmdArgs = f"--node {node.name} \"{graphFile}\" --extern"
        # Add task to the queue
        queueChunkTask(
            node=node,
            cmdArgs=cmdArgs,
            service=self.getTaskService(node),
            tags=taskTags,
            rezPackages=self.reqPackages,
            environment=environment
        )
