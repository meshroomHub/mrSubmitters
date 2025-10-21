#!/usr/bin/env python

import logging

from tractorSubmitter.api.base import Chunk
from tractorSubmitter.api.base import TRACTOR_JOB_URL, PRIORITY_DICT
from tractorSubmitter.api.base import toTractorEnv
from tractorSubmitter.api.base import TaskInfos, ChunkTaskInfos, JobInfos

from tractor.api import author as tractorAuthor


class TractorTask:
    """ Stores a task and the additional tasks spawned for each chunks
    Will be helpful later to resubmit only failed chunks for example
    """
    
    def __init__(self, task):
        self.task: tractorAuthor.Task = task
        self.chunkTasks: dict[Chunk, tractorAuthor.Task] = {}
    
    def addChunkTask(self, chunk: Chunk, task: tractorAuthor.Task):
        self.chunkTasks[chunk] = task


def cookTractorTask(taskInfos: TaskInfos) -> TractorTask:
    """
    Cook a tractor task depending on taskInfos
    Returns a TractorTask object with the tractor task, and chunk tasks
    
    # TODO : there is only one command for each task so looping over .cmds seems useless
    """
    taskKwargs = taskInfos.cook()
    tractorTask = tractorAuthor.Task(**taskKwargs)
    res = TractorTask(tractorTask)
    if taskInfos.chunks:
        for chk in taskInfos.chunks:
            chunkTaskKwargs = ChunkTaskInfos(taskInfos, chk).cook()
            chunkTractorTask = tractorTask.newTask(**chunkTaskKwargs)
            for cmd in chunkTractorTask.cmds:
                cmd.tags = taskInfos.limits
                cmd.envkey = taskInfos.envkey
            res.addChunkTask(chk, chunkTractorTask)
    else:
        for cmd in tractorTask.cmds:
            cmd.tags = taskInfos.limits
            cmd.envkey = taskInfos.envkey
            cmd.expand = taskInfos.expandingTask
    return res


class Task:
    """ Object that represent a Node in meshroom that has been submitted to the farm.
    Each node should be created as a Task in the tractor submitter.
    However one Task object can spool multiple tractor task because we will create individual 
    tasks for chunks.
    """
    
    def __init__(self, taskInfos: TaskInfos):
        self.taskInfos = taskInfos
        self._children = set()
        self._parents = set()
    
    def __repr__(self):
        return f"<Task {self.taskInfos.name} {self.taskInfos.uid}>"
    
    def __hash__(self):
        return hash(frozenset(["TractorTask", self.taskInfos.name, self.taskInfos.uid]))
    
    def __eq__(self, __value: object) -> bool:
        return hash(self) == hash(__value)

    def addChild(self, task):
        """ Add a task in the children of the current task
        """
        if isinstance(task, (tuple, list)):
            for t in task:
                self.addChild(t)
        else:
            self._children.add(task)  # Add task as current object children
            task._parents.add(self)   # Add current object as task parent


class TaskGraph:
    """ Graph with Task objects
    The point of this class is to delegate task creation and to make sure we 
    don't create multiple times the same task.
    Also we store the created tasks and chunks info so that might be useful in the future
    """
    
    def __init__(self, job):
        self.job = job
        self._tasks = set()
        self.__cooked = {}
    
    def __len__(self):
        return len(self._tasks)
    
    @property
    def roots(self):
        return [task for task in self._tasks if not task._parents]

    @property
    def leaves(self):
        return [task for task in self._tasks if not task._children]
    
    def cookTask(self, task: Task):
        """ Cook task, chunk tasks, and set tasks dependencies """
        if task.taskInfos.uid not in self.__cooked:
            logging.info(f"TractorSubmitter: Create Tractor Task: {task.taskInfos.name}")
            tractorTask = cookTractorTask(task.taskInfos)
            self.__cooked[task.taskInfos.uid] = tractorTask
            for child in task._children:
                childTask = self.cookTask(child)
                if tractorTask.chunkTasks:
                    for chkTask in tractorTask.chunkTasks.values():
                        chkTask.addChild(childTask)
                else:
                    tractorTask.task.addChild(childTask)
        return self.__cooked[task.taskInfos.uid].task
    
    def cook(self, jobTask):
        """ Cook the graph (i.e. create all tractor tasks) and dependencies
        jobTask is the root task for the whole job
        """
        for task in self.roots:
            child = self.cookTask(task)
            jobTask.addChild(child)


class Job:
    def __init__(self, name, tags=None, requirements=None, environment=None, user=None, comment="", paused=False):
        self.jobInfos = JobInfos(
            name, 
            share="", 
            service=requirements, 
            environment=environment, 
            tags=tags, 
            user=user, 
            comment=comment, 
            paused=paused
        )
        self._graph = TaskGraph(self)
    
    def createTask(self, name, commandArgs, uid, tags=None, rezPackages=None, service=None, 
                   licenses=None, expandingTask=None, chunkParams=None) -> Task:
        """ Add task and make sure it is unique """
        taskInfos = TaskInfos(
            name=name,
            cmdArgs=commandArgs,
            nodeUid=uid,
            rezPackages=rezPackages, 
            service=service, 
            licenses=licenses, 
            tags=tags.copy() if tags else None, 
            expandingTask=expandingTask, 
            chunkParams=chunkParams
        )
        task = Task(taskInfos)
        # Dont add the task if it has already been created
        for t in self._graph._tasks:
            if t == task:
                logging.error(f"TractorSubmitter: Task already created : {t}")
                return t
        self._graph._tasks.add(task)
        return task
    
    def cook(self):
        """ Cook job and tasks graph """
        # Create job
        tractorJob = tractorAuthor.Job(**self.jobInfos.cook())
        serialsubtasks = (len(self._graph.leaves) == 1)
        # Create the job task (no command, at the graph root)
        jobTask = tractorJob.newTask(title=self.jobInfos.name, argv=None, serialsubtasks=serialsubtasks)
        self._graph.cook(jobTask)
        if len(self._graph) == 0:
            # tractor API will raise a RequiredValueError if no task are in job so we add a dummy one
            # note that the job will not even appear in Tractor web ui
            _ = tractorJob.newTask(title='dummy')
        return tractorJob
    
    def submit(self, priority="normal", share="", dryRun=False, block=False):
        """Submit to Tractor, or print TCL if dryRun."""
        if share:
            self.jobInfos.share = share

        job = self.cook()
        job.priority = PRIORITY_DICT.get(priority, 5000)

        if dryRun:
            logging.info("TractorSubmitter: Job in TCL format :")
            logging.info(job.asTcl())
            return {}
        else:
            jid = job.spool(block=block, owner=self.jobInfos.user)
            return {"id": jid, "url": TRACTOR_JOB_URL.format(jid=jid)}
