#!/usr/bin/env python

import os
import sys
import shutil
import getpass
import logging
from typing import Dict, List, Union

# ========== Tractor ==========
from tractor.api import author as tractorAuthor
from tractorSubmitter.rezUtils import getRequestPackages
from tractorSubmitter.api.base import (
    TaskInfo, JobInfo,
    TRACTOR_JOB_URL, PRIORITY_DICT
)
import tractorSubmitter.api.tractorJobQuery as tq
from tractorSubmitter.api.subtaskCreator import queueChunkTask

# ========== Meshroom ========== 
import meshroom
from meshroom import _MESHROOM_ROOT
from meshroom.core.node import Status
from meshroom.core.submitter import (
    BaseSubmitter, BaseSubmittedJob, 
    SubmitterOptions, SubmitterOptionsEnum,
    OrderedTask, OrderedTasks, OrderedTaskType
)

currentDir = os.path.dirname(os.path.realpath(__file__))
binDir = os.path.dirname(os.path.dirname(os.path.dirname(currentDir)))


def wrapMeshroomBin(_bin):
    if shutil.which(_bin):
        # The alias exists so use it directly
        return _bin
    binFolder = str(_MESHROOM_ROOT / "bin")
    return os.path.join(binFolder, _bin)


class Task:
    def __init__(self, node, command, defaultName="", nodeCache="", tags=None, 
                 reqPackages=None, config=None, licenses=None, taskType=None):
        self.taskInfos = TaskInfo(
            name=node.name if node else defaultName,
            node=node,
            cmdArgs=command,
            cacheFolder=nodeCache, 
            reqPackages=reqPackages, 
            config=config, 
            licenses=licenses, 
            taskType=taskType, 
            tags=tags.copy() if tags else None, 
        )
        # Create task
        taskKwargs = self.taskInfos.get_kwargs()
        logging.info(f"Task {self.taskInfos.name} -> kwargs: {taskKwargs}")
        logging.info(f"Task {self.taskInfos.name} -> envkey: {self.taskInfos.envkey}")
        self.tractorTask: tractorAuthor.Task = tractorAuthor.Task(**taskKwargs)
        # Add commands
        for i, cmd in enumerate(self.taskInfos.get_commands()):
            # All attrs:
            # msg, tags, service, metrics, id, refersto, expand, atleast, atmost, 
            # minrunsecs, maxrunsecs, samehost, envkey, retryrc, when, resumewhile, 
            # resumepin, metadata, 
            logging.info(f"Task {self.taskInfos.name} -> Command {i}: {cmd}")
            self.tractorTask.newCommand(
                argv=cmd,
                service=taskKwargs.get("service"),
                envkey=self.taskInfos.envkey,
                tags=self.taskInfos.limits,
                expand=self.taskInfos.expandingTask,
                # If we use a file for expanding task instead we could use this :
                # expand = taskInfos.expandingFile
            )


class Job:
    def __init__(self, name, tags=None, serviceKey=None, environment=None, user=None, comment="", paused=False):
        self.jobInfo = JobInfo(
            name, 
            share="", 
            serviceKey=serviceKey, 
            environment=environment, 
            tags=tags, 
            user=user, 
            comment=comment, 
            paused=paused
        )
        self.tasks : List[Task] = []
        self.taskDependencies = {}  # task: [tasks that the task depends on]
    
    def addTask(self, task: Task):
        self.tasks.append(task)
        self.taskDependencies[task] = []

    def addTaskDependency(self, parentTask: Task, childTask: Task):
        parentTask.tractorTask.addChild(childTask.tractorTask)
        self.taskDependencies[parentTask].append(childTask)

    def getRootTasks(self):
        """ Get all tasks that are not children of other tasks """
        tasksWithoutDeps = set(self.tasks)
        for _, childTasks in self.taskDependencies.items():
            for task in childTasks:
                if task not in tasksWithoutDeps:
                    continue
                tasksWithoutDeps.remove(task)
        return list(tasksWithoutDeps)

    @staticmethod
    def createDummyTask(tractorJob: tractorAuthor.Job):
        """ Tractor API will raise a RequiredValueError if no task is 
        in the job so we add a dummy one. 
        Note that the job will not even appear in Tractor web ui.
        """
        return tractorJob.newTask(title='dummy')
    
    def cook(self, tractorJob: tractorAuthor.Job):
        if len(self.tasks) == 0:
            self.createDummyTask(tractorJob)
            return
        # Create the job task (no command, at the graph root)
        rootTasks = self.getRootTasks()
        serialsubtasks = len(rootTasks) == 1
        if not serialsubtasks:
            rootTaskName = self.jobInfo.name + " (root)"
            rootTask = tractorJob.newTask(title=rootTaskName, argv=None, serialsubtasks=serialsubtasks)
            for task in rootTasks:
                rootTask.addChild(task.tractorTask)
        # Cook tasks
        taskToTractorTask = {}
        for task in self.tasks:
            tractorTask = task.tractorTask
            tractorJob.addChild(tractorTask)
            taskToTractorTask[task] = tractorTask

    def submit(self, priority="normal", share="", dryRun=False, block=False):
        """Submit to Tractor, or print TCL if dryRun."""
        if share:
            self.jobInfo.share = share

        # Create job
        tractorJob = tractorAuthor.Job(**self.jobInfo.cook())
        self.cook(tractorJob)
        tractorJob.priority = PRIORITY_DICT.get(priority, PRIORITY_DICT["normal"])

        if dryRun:
            logging.info("TractorSubmitter: Job in TCL format")
            logging.info(tractorJob.asTcl())
            return {}
        else:
            jid = tractorJob.spool(block=block, owner=self.jobInfo.user)
            return {"id": jid, "url": TRACTOR_JOB_URL.format(jid=jid)}


class TractorTaskReturnCode:
    SUCCESS = 0
    ERROR = 1
    ERROR_NO_RETRY = -999

    @classmethod
    def kill_current_process(cls, allow_auto_retry=True):
        return_code = cls.ERROR
        if not allow_auto_retry:
            return_code = cls.ERROR_NO_RETRY
            print(f"This job returns '{return_code}' error code in order to prevent "
                  f"Tractor autoretry")
        # Farm trick to force exit status and prevent auto retry
        sys.stdout.write(f"TR_EXIT_STATUS {return_code}")
        sys.stdout.flush()


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

    def printInfo(self):
        print(f"[Tractor Job] {self.jid}")
        print(f"        job : {self.tractorJob}")
        print(f"      tasks : ")
        for _, task in self.tractorJobTasks.items():
            meta = task.get('metadata')
            uid = None
            if meta:
                uid = meta.get("uid")
            print(f"            - [{uid}] {task}")

    def __getTractorInfo(self):
        """ Find job """
        self.__tractorJob = tq.getJob(self.jid)
        self.__tractorJobTasks = tq.getJobTasks(self.jid)

    @property
    def tractorJob(self):
        if not self.__tractorJob:
            self.__getTractorInfo()
        return self.__tractorJob

    @property
    def tractorJobTasks(self):
        if not self.__tractorJobTasks:
            self.__getTractorInfo()
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
        if "FARM_REZ_VERSION" in os.environ:
            self.environment["REZ_VERSION"] = os.environ["FARM_REZ_VERSION"]
        elif "REZ_VERSION" in os.environ:
            self.environment["REZ_VERSION"] = os.environ["REZ_VERSION"]

    def getTaskService(self, node):
        kwargs = {
            "excludeHosts": []
        }
        if hasattr(node.nodeDesc, "_cuda_tag"):
            kwargs["cuda_tag"] = node.nodeDesc._cuda_tag
        service = self.config.get_config(
            cpu=node.cpu.value,
            ram=node.ram.value,
            gpu=node.gpu.value,
            **kwargs
        )
        return service
    
    def getTaskProcessCommands(self, node, iteration):
        """ process is one of 'preprocess', 'process', 'postprocess' """
        commands = []
        settings = node.getSubmitSettings(node).process
        # Setup
        if "setup_command" in settings:
            commands.append(settings.teardown_command)
        # Process
        taskCommand = ...  # build from infos on node and iteration
        if "command_wrapper" in settings:
            taskCommand = settings.command_wrapper(taskCommand)
        # Teardown
        if "teardown_command":
            commands.append(settings.teardown_command)
        return commands

    def retrieveJob(self, jid) -> TractorJob:
        job = TractorJob(jid, self)
        return job

    def createTask(self, meshroomFile: str, orderedTask: OrderedTask, createdTasks: Dict[OrderedTask, Task], **kwargs) -> Task:
        node = orderedTask.node
        if orderedTask.taskType == OrderedTaskType.PLACEHOLDER:
            defaultName = kwargs.get("jobName", "") + " (placeholder)"
            return Task(
                node=node,
                defaultName=defaultName,
                command="",
            )

        if orderedTask.taskType == OrderedTaskType.CHUNK:
            taskType = ("chunk", orderedTask.iteration)
        elif orderedTask.taskType == OrderedTaskType.PREPROCESS:
            taskType = ("preprocess", None)
        elif orderedTask.taskType == OrderedTaskType.POSTPROCESS:
            taskType = ("postprocess", None)
        elif orderedTask.taskType == OrderedTaskType.EXPANDING:
            taskType = ("expanding", None)
        else:
            raise ValueError(f"Unknown OrderedTaskType type {orderedTask.taskType}")

        tags = self.DEFAULT_TAGS.copy()  # copy to not modify default tags
        tags['prod'] = self.prod
        
        taskParams = {
            "node": node,
            "nodeCache": node._internalFolder,
            "tags": tags,
            "reqPackages": self.reqPackages,
            "config": self.config,
            "licenses": node.nodeDesc._licenses,
            "taskType": taskType
        }

        cmdArgs = f"--node {orderedTask.node.name} \"{meshroomFile}\" --extern"
        
        if orderedTask.taskType == OrderedTaskType.EXPANDING:
            cmdBin = "meshroom_createChunks"
            cmdArgs = f"--submitter {self._name} {cmdArgs}"
        else:
            cmdBin = "meshroom_compute"
        cmdBin = wrapMeshroomBin(cmdBin)
        
        cmdArgs = f"{cmdBin} {cmdArgs}"
        task = Task(command=cmdArgs, **taskParams)
        return task

    def createJob(self, orderedTasks: OrderedTasks, filepath, submitLabel="{projectName}") -> Union[TractorJob, bool]:
        # Create job
        projectName = os.path.splitext(os.path.basename(filepath))[0]
        name = submitLabel.format(projectName=projectName)
        comment = filepath
        mainTags = {
            'prod': self.prod,
            'comment': comment,
        }
        # Create job
        job = Job(
            name,
            tags=mainTags,
            environment=self.environment,
            user=os.environ.get('FARM_USER', os.environ.get('USER', getpass.getuser())),
        )
        # Add tasks
        logging.debug("Ordered Tasks:")
        orderedTasks.display()
        createdTasks: Dict[OrderedTask, Task] = dict()
        for taskToCreate in orderedTasks.iterOnTasks(skipRootTask=True):
            if taskToCreate in createdTasks.keys():
                continue
            createdTask = self.createTask(filepath, taskToCreate, createdTasks, jobName=name)
            job.addTask(createdTask)
            createdTasks[taskToCreate] = createdTask
        
        for orderedTask, task in createdTasks.items():
            deps = [createdTasks.get(t) for t in orderedTask.dependencies]
            for dependency in deps:
                job.addTaskDependency(task, dependency)
                
        res = job.submit(share=self.share, dryRun=self.dryRun)
        if self.dryRun:
            return True
        if len(res) == 0:
            return False
        submittedJob = TractorJob(res.get("id"), TractorSubmitter)
        return submittedJob

    def createChunkTask(self, node, graphFile, environment=None, **kwargs):
        """
        Create chunk tasks for the given node
        Keyword args : cache, forceStatus, forceCompute
        """
        taskTags = self.DEFAULT_TAGS.copy()
        taskTags['prod'] = self.prod
        # Environment
        environment = environment or {}
        # Command
        taskCommand = f"meshroom_compute --node {node.name} \"{graphFile}\" --extern"
        # Add task to the queue
        queueChunkTask(
            node=node,
            taskCommand=taskCommand,
            config=self.config,
            tags=taskTags,
            reqPackages=self.reqPackages,
            environment=environment
        )

    @staticmethod
    def killRunningJob():
        """ Kill the current job and prevent """
        TractorTaskReturnCode.kill_current_process(allow_auto_retry=False)
        sys.exit(meshroom.MeshroomExitStatus.ERROR_NO_RETRY)
