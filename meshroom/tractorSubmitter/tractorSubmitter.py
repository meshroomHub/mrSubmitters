#!/usr/bin/env python

import os
import shutil
import json
import getpass
import logging
from meshroom.core.submitter import BaseSubmitter, SubmitterOptions, SubmitterOptionsEnum
import tractorSubmitter.api.tractorJobQuery as tq
from tractorSubmitter.api.tractorJobCreation import get_job_packages, Task, Job
from meshroom.core.submitter import BaseSubmittedJob
from tractor.api import author as tractorAuthor

currentDir = os.path.dirname(os.path.realpath(__file__))
binDir = os.path.dirname(os.path.dirname(os.path.dirname(currentDir)))


class TractorJob(BaseSubmittedJob):
    """
    Interface to manipulate the job via Meshroom
    """

    def __init__(self, jid, submitter):
        super().__init__(jid, submitter)
        self.jid = jid
        self.submitter = submitter
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

    def interrupt(self):
        raise NotImplementedError("[TractorJob] 'interrupt' is not implemented yet")

    def resume(self):
        raise NotImplementedError("[TractorJob] 'resume' is not implemented yet")
    
    def _createChunkTask(self, node, filepath, submitLabel="{projectName}"):
        projectName = os.path.splitext(os.path.basename(filepath))[0]
        name = submitLabel.format(projectName=projectName)
        comment = filepath
        mainTags = {'prod': self.submitter.prod, 'nbFrames': str(node.size), 'comment': comment}
        allRequirements = list(self.submitter.config.get('BASE', []))
        # Create job
        job = Job(name, tags=mainTags, requirements={'service': str(','.join(allRequirements))},
                  environment=self.submitter.environment, 
                  user=os.environ.get('FARM_USER', os.environ.get('USER', getpass.getuser())))
        # Create task
        taskTags = self.submitter.DEFAULT_TAGS.copy()
        optionalArgs = {}
        exe = "meshroom_compute" if self.submitter.reqPackages else os.path.join(binDir, "meshroom_compute")
        blockSize, fullSize, nbBlocks = node.nodeDesc.parallelization.getSizes(node)
        if nbBlocks > 1:  # Is it better like this ?
            optionalArgs["chunks"] = {'start': 0, 'end': nbBlocks - 1, 'step': 1}
        taskTags['nbFrames'] = node.size
        taskTags['prod'] = self.submitter.prod
        allRequirements = set()
        allRequirements.update(self.submitter.config['CPU'].get(node.nodeDesc.cpu.name, []))
        allRequirements.update(self.submitter.config['RAM'].get(node.nodeDesc.ram.name, []))
        allRequirements.update(self.submitter.config['GPU'].get(node.nodeDesc.gpu.name, []))
        taskCommand = f"{exe} --node {node.name} \"{filepath}\" --extern"
        task = Task(
            name=node.name,
            uid=node._uid,  # Provide unicity info
            command=taskCommand,
            tags=taskTags,
            rezPackages=self.submitter.reqPackages,
            requirements={'service': str(','.join(allRequirements))},
            **optionalArgs)
        task = job.addTask(task)
        # Submit
        res = job.submit(share=self.submitter.share, dryRun=self.submitter.dryRun)
        if len(res) == 0:
            return False
        return res.get("id")
    
    def getPreviousTasksId(self, node):
        for _, task in self.tractorJobTasks.items():
            meta = task.get('metadata')
            uid = None
            if not meta:
                continue
            uid = meta.get("uid")
            if uid == node._uid:
                return task.get("ptids", [])
        print(f"[TractorJob] (get_previous_tasks) Could not find the tractor task for node {node._uid}")
        return []
    
    def addChunkTask(self, node, submitLabel="{projectName}", **kwargs):
        # Create job that will execute chunks
        filepath = kwargs.get("graphFile")
        jid = self._createChunkTask(node, filepath, submitLabel)
        if not jid:
            print("[TractorJob] (addChunkTask) Failed to create the job")
            return False
        # Get previous task IDs (tasks to block)
        ptids = self.getPreviousTasksId(node)
        print(f"[TractorJob] (addChunkTask) ptids={ptids}")
        # Block previous tasks
        for tid in ptids:
            # task = self.tractorJobTasks.get(tid)
            # Block the task
            # tq.blockTask(tid, jid)
            # HACK: We cannot delay tasks so instead we are going to block the current process
            tq.waitForJob(jid)


class TractorSubmitter(BaseSubmitter):
    """
    Meshroom submitter to tractor
    """
    
    _name = "Tractor"
    _options = SubmitterOptions(SubmitterOptionsEnum.ALL)
    
    dryRun = False
    environment = {}
    DEFAULT_TAGS = {'prod': ''}

    filepath = os.environ.get('TRACTORCONFIG', os.path.join(currentDir, 'tractorConfig.json'))
    config = json.load(open(filepath))
    
    def __init__(self, parent=None):
        super().__init__(parent=parent)
        self.share = os.environ.get('MESHROOM_TRACTOR_SHARE', 'vfx')
        self.prod = os.environ.get('PROD', 'mvg')
        self.reqPackages = get_job_packages()
        if 'REZ_DEV_PACKAGES_ROOT' in os.environ:
            self.environment['REZ_DEV_PACKAGES_ROOT'] = os.environ['REZ_DEV_PACKAGES_ROOT']
        if 'REZ_PROD_PACKAGES_PATH' in os.environ:
            self.environment['REZ_PROD_PACKAGES_PATH'] = os.environ['REZ_PROD_PACKAGES_PATH']
        if 'PROD' in os.environ:
            self.environment['PROD'] = os.environ['PROD']
        if 'PROD_ROOT' in os.environ:
            self.environment['PROD_ROOT'] = os.environ['PROD_ROOT']
    
    def retrieveJob(self, jid) -> TractorJob:
        job = TractorJob(jid, self)
        return job

    def createTask(self, meshroomFile, node):
        tags = self.DEFAULT_TAGS.copy()  # copy to not modify default tags
        optionalArgs = {}
        logging.debug(f"TractorSubmitter: node: {node.name} ({node._uid})")
        exe = "meshroom_compute" if self.reqPackages else os.path.join(binDir, "meshroom_compute")
        if not (hasattr(node, '_chunksCreated') and node._chunksCreated):
            # Chunks will be created by the process
            optionalArgs["createChunks"] = True
            exe = "meshroom_submitterWrapper"
        elif node.isParallelized:
            blockSize, fullSize, nbBlocks = node.nodeDesc.parallelization.getSizes(node)
            if nbBlocks > 1:  # Is it better like this ?
                optionalArgs["chunks"] = {'start': 0, 'end': nbBlocks - 1, 'step': 1}
        tags['nbFrames'] = node.size
        tags['prod'] = self.prod
        allRequirements = set()
        allRequirements.update(self.config['CPU'].get(node.nodeDesc.cpu.name, []))
        allRequirements.update(self.config['RAM'].get(node.nodeDesc.ram.name, []))
        allRequirements.update(self.config['GPU'].get(node.nodeDesc.gpu.name, []))
        taskCommand = f"{exe} --node {node.name} \"{meshroomFile}\" --extern"
        task = Task(
            name=node.name,
            uid=node._uid,  # Provide unicity info
            command=taskCommand,
            tags=tags,
            rezPackages=self.reqPackages,
            requirements={'service': str(','.join(allRequirements))},
            **optionalArgs)
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
        allRequirements = list(self.config.get('BASE', []))

        # Create Job Graph
        job = Job(
            name,
            tags=mainTags,
            requirements={'service': str(','.join(allRequirements))},
            environment=self.environment,
            user=os.environ.get('FARM_USER', os.environ.get('USER', getpass.getuser())),
        )

        nodeUidToTask = {}
        for node in nodes:
            if node._uid in nodeUidToTask:
                continue  # HACK: Should not be necessary
            task = self.createTask(filepath, node)
            task = job.addTask(task)  # Should not be necessary but we never know
            nodeUidToTask[node._uid] = task

        for u, v in edges:
            nodeUidToTask[u._uid].connect(nodeUidToTask[v._uid])

        res = job.submit(share=self.share, dryRun=self.dryRun)
        if self.dryRun:
            return True
        if len(res) == 0:
            return False
        submittedJob = TractorJob(res.get("id"), TractorSubmitter)
        return submittedJob
