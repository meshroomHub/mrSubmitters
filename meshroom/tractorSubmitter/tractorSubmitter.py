#!/usr/bin/env python

import os
import shutil
import json
import getpass
import logging
from meshroom.core.submitter import BaseSubmitter, SubmitterOptions, SubmitterOptionsEnum
from tractorSubmitter.api.tractorJobQuery import TractorJob
from tractorSubmitter.api.tractorJobCreation import get_job_packages, Task, Job

currentDir = os.path.dirname(os.path.realpath(__file__))
binDir = os.path.dirname(os.path.dirname(os.path.dirname(currentDir)))


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
        return TractorJob(jid, TractorSubmitter)

    def createTask(self, meshroomFile, node):
        tags = self.DEFAULT_TAGS.copy()  # copy to not modify default tags
        optionalArgs = {}
        logging.debug(f"TractorSubmitter: node: {node.name} ({node._uid})")
        if node.isParallelized:
            blockSize, fullSize, nbBlocks = node.nodeDesc.parallelization.getSizes(node)
            if nbBlocks > 1:  # Is it better like this ?
                optionalArgs["chunks"] = {'start': 0, 'end': nbBlocks - 1, 'step': 1}
        tags['nbFrames'] = node.size
        tags['prod'] = self.prod
        allRequirements = set()
        allRequirements.update(self.config['CPU'].get(node.nodeDesc.cpu.name, []))
        allRequirements.update(self.config['RAM'].get(node.nodeDesc.ram.name, []))
        allRequirements.update(self.config['GPU'].get(node.nodeDesc.gpu.name, []))
        exe = "meshroom_compute" if self.reqPackages else os.path.join(binDir, "meshroom_compute")
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
