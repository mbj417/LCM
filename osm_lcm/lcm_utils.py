# -*- coding: utf-8 -*-

##
# Copyright 2018 Telefonica S.A.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.
##

import asyncio
from collections import OrderedDict
# from osm_common.dbbase import DbException

__author__ = "Alfonso Tierno"


class LcmException(Exception):
    pass


class LcmExceptionNoMgmtIP(LcmException):
    pass


class LcmExceptionExit(LcmException):
    pass


def versiontuple(v):
    """utility for compare dot separate versions. Fills with zeros to proper number comparison
    package version will be something like 4.0.1.post11+gb3f024d.dirty-1. Where 4.0.1 is the git tag, postXX is the
    number of commits from this tag, and +XXXXXXX is the git commit short id. Total length is 16 with until 999 commits
    """
    filled = []
    for point in v.split("."):
        filled.append(point.zfill(16))
    return tuple(filled)


# LcmBase must be listed before TaskRegistry, as it is a dependency.
class LcmBase:

    def __init__(self, db, msg, fs, logger):
        """

        :param db: database connection
        """
        self.db = db
        self.msg = msg
        self.fs = fs
        self.logger = logger

    def update_db_2(self, item, _id, _desc):
        """
        Updates database with _desc information. If success _desc is cleared
        :param item:
        :param _id:
        :param _desc: dictionary with the content to update. Keys are dot separated keys for
        :return: None. Exception is raised on error
        """
        if not _desc:
            return
        self.db.set_one(item, {"_id": _id}, _desc)
        _desc.clear()
        # except DbException as e:
        #     self.logger.error("Updating {} _id={} with '{}'. Error: {}".format(item, _id, _desc, e))


class TaskRegistry(LcmBase):
    """
    Implements a registry of task needed for later cancelation, look for related tasks that must be completed before
    etc. It stores a four level dict
    First level is the topic, ns, vim_account, sdn
    Second level is the _id
    Third level is the operation id
    Fourth level is a descriptive name, the value is the task class

    The HA (High-Availability) methods are used when more than one LCM instance is running.
    To register the current task in the external DB, use LcmBase as base class, to be able
    to reuse LcmBase.update_db_2()
    The DB registry uses the following fields to distinguish a task:
    - op_type: operation type ("nslcmops" or "nsilcmops")
    - op_id:   operation ID
    - worker:  the worker ID for this process
    """

    instance_id_label_dict = {'ns': 'nsInstanceId', 'nsi': 'netsliceInstanceId'}

    def __init__(self, worker_id=None, db=None, logger=None):
        self.task_registry = {
            "ns": {},
            "nsi": {},
            "vim_account": {},
            "wim_account": {},
            "sdn": {},
        }
        self.worker_id = worker_id
        self.db = db
        self.logger = logger

    def register(self, topic, _id, op_id, task_name, task):
        """
        Register a new task
        :param topic: Can be "ns", "nsi", "vim_account", "sdn"
        :param _id: _id of the related item
        :param op_id: id of the operation of the related item
        :param task_name: Task descriptive name, as create, instantiate, terminate. Must be unique in this op_id
        :param task: Task class
        :return: none
        """
        if _id not in self.task_registry[topic]:
            self.task_registry[topic][_id] = OrderedDict()
        if op_id not in self.task_registry[topic][_id]:
            self.task_registry[topic][_id][op_id] = {task_name: task}
        else:
            self.task_registry[topic][_id][op_id][task_name] = task
        # print("registering task", topic, _id, op_id, task_name, task)

    def remove(self, topic, _id, op_id, task_name=None):
        """
        When task is ended, it should be removed. It ignores missing tasks. It also removes tasks done with this _id
        :param topic: Can be "ns", "nsi", "vim_account", "sdn"
        :param _id: _id of the related item
        :param op_id: id of the operation of the related item
        :param task_name: Task descriptive name. If none it deletes all tasks with same _id and op_id
        :return: None
        """
        if not self.task_registry[topic].get(_id):
            return
        if not task_name:
            self.task_registry[topic][_id].pop(op_id, None)
        elif self.task_registry[topic][_id].get(op_id):
            self.task_registry[topic][_id][op_id].pop(task_name, None)

        # delete done tasks
        for op_id_ in list(self.task_registry[topic][_id]):
            for name, task in self.task_registry[topic][_id][op_id_].items():
                if not task.done():
                    break
            else:
                del self.task_registry[topic][_id][op_id_]
        if not self.task_registry[topic][_id]:
            del self.task_registry[topic][_id]

    def lookfor_related(self, topic, _id, my_op_id=None):
        task_list = []
        task_name_list = []
        if _id not in self.task_registry[topic]:
            return "", task_name_list
        for op_id in reversed(self.task_registry[topic][_id]):
            if my_op_id:
                if my_op_id == op_id:
                    my_op_id = None  # so that the next task is taken
                continue

            for task_name, task in self.task_registry[topic][_id][op_id].items():
                if not task.done():
                    task_list.append(task)
                    task_name_list.append(task_name)
            break
        return ", ".join(task_name_list), task_list

    def cancel(self, topic, _id, target_op_id=None, target_task_name=None):
        """
        Cancel all active tasks of a concrete ns, nsi, vim_account, sdn identified for _id. If op_id is supplied only
        this is cancelled, and the same with task_name
        """
        if not self.task_registry[topic].get(_id):
            return
        for op_id in reversed(self.task_registry[topic][_id]):
            if target_op_id and target_op_id != op_id:
                continue
            for task_name, task in self.task_registry[topic][_id][op_id].items():
                if target_task_name and target_task_name != task_name:
                    continue
                # result =
                task.cancel()
                # if result:
                #     self.logger.debug("{} _id={} order_id={} task={} cancelled".format(topic, _id, op_id, task_name))

    def lock_HA(self, topic, op_type, op_id):
        """
        Lock an task, if possible, to indicate to the HA system that
        the task will be executed in this LCM instance.
        :param topic: Can be "ns", "nsi"
        :param op_type: Operation type, can be "nslcmops", "nsilcmops"
        :param op_id: id of the operation of the related item
        :return:
        True=lock successful => execute the task (not registered by any other LCM instance)
        False=lock failed => do NOT execute the task (already registered by another LCM instance)
        """

        db_lock_task = self.db.set_one(op_type,
                                       q_filter={'_id': op_id, '_admin.worker': None},
                                       update_dict={'_admin.worker': self.worker_id},
                                       fail_on_empty=False)

        if db_lock_task is None:
            self.logger.debug("Task {} operation={} already locked by another worker".format(topic, op_id))
            return False
        else:
            return True

    async def waitfor_related_HA(self, topic, op_type, op_id=None):
        """
        Wait for any pending related HA tasks
        """

        # InstanceId label
        instance_id_label = self.instance_id_label_dict.get(topic)

        # Get 'startTime' timestamp for this operation
        step = "Getting timestamp for op_id={} from db".format(op_id)
        db_lcmop = self.db.get_one(op_type,
                                   {"_id": op_id},
                                   fail_on_empty=False)
        if not db_lcmop:
            return
        starttime_this_op = db_lcmop.get("startTime")
        instance_id = db_lcmop.get(instance_id_label)

        # For HA, get list of tasks from DB instead of from dictionary (in-memory) variable.
        timeout_wait_for_task = 3600   # Max time (seconds) to wait for a related task to finish
        # interval_wait_for_task = 30    #  A too long polling interval slows things down considerably
        interval_wait_for_task = 10       # Interval in seconds for polling related tasks
        time_left = timeout_wait_for_task
        old_num_related_tasks = 0
        while True:
            # Get related tasks (operations within the same NS or NSI instance) which are
            # still running (operationState='PROCESSING') and which were started before this task.
            _filter = {instance_id_label: instance_id,
                       'operationState': 'PROCESSING',
                       'startTime.lt': starttime_this_op}
            db_waitfor_related_task = self.db.get_list(op_type,
                                                       q_filter=_filter)
            new_num_related_tasks = len(db_waitfor_related_task)
            if not new_num_related_tasks:
                # There are no related tasks, no need to wait, so return.
                return
            # If number of pending related tasks have changed,
            # update the 'detailed-status' field and log the change.
            if new_num_related_tasks != old_num_related_tasks:
                db_lcmops_update = {}
                step = db_lcmops_update["detailed-status"] = \
                    "Waiting for {} related tasks to be completed.".format(
                        new_num_related_tasks)
                self.logger.debug("Task {} operation={} {}".format(topic, op_id, step))
                self.update_db_2(op_type, op_id, db_lcmops_update)
                old_num_related_tasks = new_num_related_tasks
            time_left -= interval_wait_for_task
            if time_left < 0:
                raise LcmException(
                    "Timeout ({}) when waiting for related tasks to be completed".format(
                        timeout_wait_for_task))
            await asyncio.sleep(interval_wait_for_task)

        return
