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
import logging
import logging.handlers
import ROclient
from lcm_utils import LcmException, LcmBase
from osm_common.dbbase import DbException
from copy import deepcopy

__author__ = "Alfonso Tierno"


class VimLcm(LcmBase):
    # values that are encrypted at vim config because they are passwords
    vim_config_encrypted = ("admin_password", "nsx_password", "vcenter_password")

    def __init__(self, db, msg, fs, lcm_tasks, ro_config, loop):
        """
        Init, Connect to database, filesystem storage, and messaging
        :param config: two level dictionary with configuration. Top level should contain 'database', 'storage',
        :return: None
        """

        self.logger = logging.getLogger('lcm.vim')
        self.loop = loop
        self.lcm_tasks = lcm_tasks
        self.ro_config = ro_config

        super().__init__(db, msg, fs, self.logger)

    async def create(self, vim_content, order_id):
        vim_id = vim_content["_id"]
        vim_content.pop("op_id", None)
        logging_text = "Task vim_create={} ".format(vim_id)
        self.logger.debug(logging_text + "Enter")
        db_vim = None
        db_vim_update = {}
        exc = None
        RO_sdn_id = None
        try:
            step = "Getting vim-id='{}' from db".format(vim_id)
            db_vim = self.db.get_one("vim_accounts", {"_id": vim_id})
            if vim_content.get("config") and vim_content["config"].get("sdn-controller"):
                step = "Getting sdn-controller-id='{}' from db".format(vim_content["config"]["sdn-controller"])
                db_sdn = self.db.get_one("sdns", {"_id": vim_content["config"]["sdn-controller"]})
                if db_sdn.get("_admin") and db_sdn["_admin"].get("deployed") and db_sdn["_admin"]["deployed"].get("RO"):
                    RO_sdn_id = db_sdn["_admin"]["deployed"]["RO"]
                else:
                    raise LcmException("sdn-controller={} is not available. Not deployed at RO".format(
                        vim_content["config"]["sdn-controller"]))

            step = "Creating vim at RO"
            db_vim_update["_admin.deployed.RO"] = None
            db_vim_update["_admin.detailed-status"] = step
            self.update_db_2("vim_accounts", vim_id, db_vim_update)
            RO = ROclient.ROClient(self.loop, **self.ro_config)
            vim_RO = deepcopy(vim_content)
            vim_RO.pop("_id", None)
            vim_RO.pop("_admin", None)
            schema_version = vim_RO.pop("schema_version", None)
            vim_RO.pop("schema_type", None)
            vim_RO.pop("vim_tenant_name", None)
            vim_RO["type"] = vim_RO.pop("vim_type")
            vim_RO.pop("vim_user", None)
            vim_RO.pop("vim_password", None)
            if RO_sdn_id:
                vim_RO["config"]["sdn-controller"] = RO_sdn_id
            desc = await RO.create("vim", descriptor=vim_RO)
            RO_vim_id = desc["uuid"]
            db_vim_update["_admin.deployed.RO"] = RO_vim_id
            self.logger.debug(logging_text + "VIM created at RO_vim_id={}".format(RO_vim_id))

            step = "Creating vim_account at RO"
            db_vim_update["_admin.detailed-status"] = step
            self.update_db_2("vim_accounts", vim_id, db_vim_update)

            if vim_content.get("vim_password"):
                vim_content["vim_password"] = self.db.decrypt(vim_content["vim_password"],
                                                              schema_version=schema_version,
                                                              salt=vim_id)
            vim_account_RO = {"vim_tenant_name": vim_content["vim_tenant_name"],
                              "vim_username": vim_content["vim_user"],
                              "vim_password": vim_content["vim_password"]
                              }
            if vim_RO.get("config"):
                vim_account_RO["config"] = vim_RO["config"]
                if "sdn-controller" in vim_account_RO["config"]:
                    del vim_account_RO["config"]["sdn-controller"]
                if "sdn-port-mapping" in vim_account_RO["config"]:
                    del vim_account_RO["config"]["sdn-port-mapping"]
                for p in self.vim_config_encrypted:
                    if vim_account_RO["config"].get(p):
                        vim_account_RO["config"][p] = self.db.decrypt(vim_account_RO["config"][p],
                                                                      schema_version=schema_version,
                                                                      salt=vim_id)

            desc = await RO.attach("vim_account", RO_vim_id, descriptor=vim_account_RO)
            db_vim_update["_admin.deployed.RO-account"] = desc["uuid"]
            db_vim_update["_admin.operationalState"] = "ENABLED"
            db_vim_update["_admin.detailed-status"] = "Done"

            # await asyncio.sleep(15)   # TODO remove. This is for test
            self.logger.debug(logging_text + "Exit Ok VIM account created at RO_vim_account_id={}".format(desc["uuid"]))
            return

        except (ROclient.ROClientException, DbException) as e:
            self.logger.error(logging_text + "Exit Exception {}".format(e))
            exc = e
        except Exception as e:
            self.logger.critical(logging_text + "Exit Exception {}".format(e), exc_info=True)
            exc = e
        finally:
            if exc and db_vim:
                db_vim_update["_admin.operationalState"] = "ERROR"
                db_vim_update["_admin.detailed-status"] = "ERROR {}: {}".format(step, exc)
            try:
                if db_vim_update:
                    self.update_db_2("vim_accounts", vim_id, db_vim_update)
            except DbException as e:
                self.logger.error(logging_text + "Cannot update database: {}".format(e))

            self.lcm_tasks.remove("vim_account", vim_id, order_id)

    async def edit(self, vim_content, order_id):
        vim_id = vim_content["_id"]
        vim_content.pop("op_id", None)
        logging_text = "Task vim_edit={} ".format(vim_id)
        self.logger.debug(logging_text + "Enter")
        db_vim = None
        exc = None
        RO_sdn_id = None
        RO_vim_id = None
        db_vim_update = {}
        step = "Getting vim-id='{}' from db".format(vim_id)
        try:
            db_vim = self.db.get_one("vim_accounts", {"_id": vim_id})

            # look if previous tasks in process
            task_name, task_dependency = self.lcm_tasks.lookfor_related("vim_account", vim_id, order_id)
            if task_dependency:
                step = "Waiting for related tasks to be completed: {}".format(task_name)
                self.logger.debug(logging_text + step)
                # TODO write this to database
                _, pending = await asyncio.wait(task_dependency, timeout=3600)
                if pending:
                    raise LcmException("Timeout waiting related tasks to be completed")

            if db_vim.get("_admin") and db_vim["_admin"].get("deployed") and db_vim["_admin"]["deployed"].get("RO"):
                if vim_content.get("config") and vim_content["config"].get("sdn-controller"):
                    step = "Getting sdn-controller-id='{}' from db".format(vim_content["config"]["sdn-controller"])
                    db_sdn = self.db.get_one("sdns", {"_id": vim_content["config"]["sdn-controller"]})

                    # look if previous tasks in process
                    task_name, task_dependency = self.lcm_tasks.lookfor_related("sdn", db_sdn["_id"])
                    if task_dependency:
                        step = "Waiting for related tasks to be completed: {}".format(task_name)
                        self.logger.debug(logging_text + step)
                        # TODO write this to database
                        _, pending = await asyncio.wait(task_dependency, timeout=3600)
                        if pending:
                            raise LcmException("Timeout waiting related tasks to be completed")

                    if db_sdn.get("_admin") and db_sdn["_admin"].get("deployed") and db_sdn["_admin"]["deployed"].get(
                            "RO"):
                        RO_sdn_id = db_sdn["_admin"]["deployed"]["RO"]
                    else:
                        raise LcmException("sdn-controller={} is not available. Not deployed at RO".format(
                            vim_content["config"]["sdn-controller"]))

                RO_vim_id = db_vim["_admin"]["deployed"]["RO"]
                step = "Editing vim at RO"
                RO = ROclient.ROClient(self.loop, **self.ro_config)
                vim_RO = deepcopy(vim_content)
                vim_RO.pop("_id", None)
                vim_RO.pop("_admin", None)
                schema_version = vim_RO.pop("schema_version", None)
                vim_RO.pop("schema_type", None)
                vim_RO.pop("vim_tenant_name", None)
                if "vim_type" in vim_RO:
                    vim_RO["type"] = vim_RO.pop("vim_type")
                vim_RO.pop("vim_user", None)
                vim_RO.pop("vim_password", None)
                if RO_sdn_id:
                    vim_RO["config"]["sdn-controller"] = RO_sdn_id
                # TODO make a deep update of sdn-port-mapping 
                if vim_RO:
                    await RO.edit("vim", RO_vim_id, descriptor=vim_RO)

                step = "Editing vim-account at RO tenant"
                vim_account_RO = {}
                if "config" in vim_content:
                    if "sdn-controller" in vim_content["config"]:
                        del vim_content["config"]["sdn-controller"]
                    if "sdn-port-mapping" in vim_content["config"]:
                        del vim_content["config"]["sdn-port-mapping"]
                    if not vim_content["config"]:
                        del vim_content["config"]
                if "vim_tenant_name" in vim_content:
                    vim_account_RO["vim_tenant_name"] = vim_content["vim_tenant_name"]
                if "vim_password" in vim_content:
                    vim_account_RO["vim_password"] = vim_content["vim_password"]
                if vim_content.get("vim_password"):
                    vim_account_RO["vim_password"] = self.db.decrypt(vim_content["vim_password"],
                                                                     schema_version=schema_version,
                                                                     salt=vim_id)
                if "config" in vim_content:
                    vim_account_RO["config"] = vim_content["config"]
                if vim_content.get("config"):
                    for p in self.vim_config_encrypted:
                        if vim_content["config"].get(p):
                            vim_account_RO["config"][p] = self.db.decrypt(vim_content["config"][p],
                                                                          schema_version=schema_version,
                                                                          salt=vim_id)

                if "vim_user" in vim_content:
                    vim_content["vim_username"] = vim_content["vim_user"]
                # vim_account must be edited always even if empty in order to ensure changes are translated to RO
                # vim_thread. RO will remove and relaunch a new thread for this vim_account
                await RO.edit("vim_account", RO_vim_id, descriptor=vim_account_RO)
                db_vim_update["_admin.operationalState"] = "ENABLED"

            self.logger.debug(logging_text + "Exit Ok RO_vim_id={}".format(RO_vim_id))
            return

        except (ROclient.ROClientException, DbException) as e:
            self.logger.error(logging_text + "Exit Exception {}".format(e))
            exc = e
        except Exception as e:
            self.logger.critical(logging_text + "Exit Exception {}".format(e), exc_info=True)
            exc = e
        finally:
            if exc and db_vim:
                db_vim_update["_admin.operationalState"] = "ERROR"
                db_vim_update["_admin.detailed-status"] = "ERROR {}: {}".format(step, exc)
            try:
                if db_vim_update:
                    self.update_db_2("vim_accounts", vim_id, db_vim_update)
            except DbException as e:
                self.logger.error(logging_text + "Cannot update database: {}".format(e))

            self.lcm_tasks.remove("vim_account", vim_id, order_id)

    async def delete(self, vim_id, order_id):
        logging_text = "Task vim_delete={} ".format(vim_id)
        self.logger.debug(logging_text + "Enter")
        db_vim = None
        db_vim_update = {}
        exc = None
        step = "Getting vim from db"
        try:
            db_vim = self.db.get_one("vim_accounts", {"_id": vim_id})
            if db_vim.get("_admin") and db_vim["_admin"].get("deployed") and db_vim["_admin"]["deployed"].get("RO"):
                RO_vim_id = db_vim["_admin"]["deployed"]["RO"]
                RO = ROclient.ROClient(self.loop, **self.ro_config)
                step = "Detaching vim from RO tenant"
                try:
                    await RO.detach("vim_account", RO_vim_id)
                except ROclient.ROClientException as e:
                    if e.http_code == 404:  # not found
                        self.logger.debug(logging_text + "RO_vim_id={} already detached".format(RO_vim_id))
                    else:
                        raise

                step = "Deleting vim from RO"
                try:
                    await RO.delete("vim", RO_vim_id)
                except ROclient.ROClientException as e:
                    if e.http_code == 404:  # not found
                        self.logger.debug(logging_text + "RO_vim_id={} already deleted".format(RO_vim_id))
                    else:
                        raise
            else:
                # nothing to delete
                self.logger.error(logging_text + "Nohing to remove at RO")
            self.db.del_one("vim_accounts", {"_id": vim_id})
            db_vim = None
            self.logger.debug(logging_text + "Exit Ok")
            return

        except (ROclient.ROClientException, DbException) as e:
            self.logger.error(logging_text + "Exit Exception {}".format(e))
            exc = e
        except Exception as e:
            self.logger.critical(logging_text + "Exit Exception {}".format(e), exc_info=True)
            exc = e
        finally:
            self.lcm_tasks.remove("vim_account", vim_id, order_id)
            if exc and db_vim:
                db_vim_update["_admin.operationalState"] = "ERROR"
                db_vim_update["_admin.detailed-status"] = "ERROR {}: {}".format(step, exc)
            try:
                if db_vim and db_vim_update:
                    self.update_db_2("vim_accounts", vim_id, db_vim_update)
            except DbException as e:
                self.logger.error(logging_text + "Cannot update database: {}".format(e))
            self.lcm_tasks.remove("vim_account", vim_id, order_id)


class WimLcm(LcmBase):
    # values that are encrypted at wim config because they are passwords
    wim_config_encrypted = ()

    def __init__(self, db, msg, fs, lcm_tasks, ro_config, loop):
        """
        Init, Connect to database, filesystem storage, and messaging
        :param config: two level dictionary with configuration. Top level should contain 'database', 'storage',
        :return: None
        """

        self.logger = logging.getLogger('lcm.vim')
        self.loop = loop
        self.lcm_tasks = lcm_tasks
        self.ro_config = ro_config

        super().__init__(db, msg, fs, self.logger)

    async def create(self, wim_content, order_id):
        wim_id = wim_content["_id"]
        wim_content.pop("op_id", None)
        logging_text = "Task wim_create={} ".format(wim_id)
        self.logger.debug(logging_text + "Enter")
        db_wim = None
        db_wim_update = {}
        exc = None
        try:
            step = "Getting wim-id='{}' from db".format(wim_id)
            db_wim = self.db.get_one("wim_accounts", {"_id": wim_id})
            db_wim_update["_admin.deployed.RO"] = None

            step = "Creating wim at RO"
            db_wim_update["_admin.detailed-status"] = step
            self.update_db_2("wim_accounts", wim_id, db_wim_update)
            RO = ROclient.ROClient(self.loop, **self.ro_config)
            wim_RO = deepcopy(wim_content)
            wim_RO.pop("_id", None)
            wim_RO.pop("_admin", None)
            schema_version = wim_RO.pop("schema_version", None)
            wim_RO.pop("schema_type", None)
            wim_RO.pop("wim_tenant_name", None)
            wim_RO["type"] = wim_RO.pop("wim_type")
            wim_RO.pop("wim_user", None)
            wim_RO.pop("wim_password", None)
            desc = await RO.create("wim", descriptor=wim_RO)
            RO_wim_id = desc["uuid"]
            db_wim_update["_admin.deployed.RO"] = RO_wim_id
            self.logger.debug(logging_text + "WIM created at RO_wim_id={}".format(RO_wim_id))

            step = "Creating wim_account at RO"
            db_wim_update["_admin.detailed-status"] = step
            self.update_db_2("wim_accounts", wim_id, db_wim_update)

            if wim_content.get("wim_password"):
                wim_content["wim_password"] = self.db.decrypt(wim_content["wim_password"],
                                                              schema_version=schema_version,
                                                              salt=wim_id)
            wim_account_RO = {"name": wim_content["name"],
                              "user": wim_content["user"],
                              "password": wim_content["password"]
                              }
            if wim_RO.get("config"):
                wim_account_RO["config"] = wim_RO["config"]
                if "wim_port_mapping" in wim_account_RO["config"]:
                    del wim_account_RO["config"]["wim_port_mapping"]
                for p in self.wim_config_encrypted:
                    if wim_account_RO["config"].get(p):
                        wim_account_RO["config"][p] = self.db.decrypt(wim_account_RO["config"][p],
                                                                      schema_version=schema_version,
                                                                      salt=wim_id)

            desc = await RO.attach("wim_account", RO_wim_id, descriptor=wim_account_RO)
            db_wim_update["_admin.deployed.RO-account"] = desc["uuid"]
            db_wim_update["_admin.operationalState"] = "ENABLED"
            db_wim_update["_admin.detailed-status"] = "Done"

            self.logger.debug(logging_text + "Exit Ok WIM account created at RO_wim_account_id={}".format(desc["uuid"]))
            return

        except (ROclient.ROClientException, DbException) as e:
            self.logger.error(logging_text + "Exit Exception {}".format(e))
            exc = e
        except Exception as e:
            self.logger.critical(logging_text + "Exit Exception {}".format(e), exc_info=True)
            exc = e
        finally:
            if exc and db_wim:
                db_wim_update["_admin.operationalState"] = "ERROR"
                db_wim_update["_admin.detailed-status"] = "ERROR {}: {}".format(step, exc)
            try:
                if db_wim_update:
                    self.update_db_2("wim_accounts", wim_id, db_wim_update)
            except DbException as e:
                self.logger.error(logging_text + "Cannot update database: {}".format(e))
            self.lcm_tasks.remove("wim_account", wim_id, order_id)

    async def edit(self, wim_content, order_id):
        wim_id = wim_content["_id"]
        wim_content.pop("op_id", None)
        logging_text = "Task wim_edit={} ".format(wim_id)
        self.logger.debug(logging_text + "Enter")
        db_wim = None
        exc = None
        RO_wim_id = None
        db_wim_update = {}
        step = "Getting wim-id='{}' from db".format(wim_id)
        try:
            db_wim = self.db.get_one("wim_accounts", {"_id": wim_id})

            # look if previous tasks in process
            task_name, task_dependency = self.lcm_tasks.lookfor_related("wim_account", wim_id, order_id)
            if task_dependency:
                step = "Waiting for related tasks to be completed: {}".format(task_name)
                self.logger.debug(logging_text + step)
                # TODO write this to database
                _, pending = await asyncio.wait(task_dependency, timeout=3600)
                if pending:
                    raise LcmException("Timeout waiting related tasks to be completed")

            if db_wim.get("_admin") and db_wim["_admin"].get("deployed") and db_wim["_admin"]["deployed"].get("RO"):

                RO_wim_id = db_wim["_admin"]["deployed"]["RO"]
                step = "Editing wim at RO"
                RO = ROclient.ROClient(self.loop, **self.ro_config)
                wim_RO = deepcopy(wim_content)
                wim_RO.pop("_id", None)
                wim_RO.pop("_admin", None)
                schema_version = wim_RO.pop("schema_version", None)
                wim_RO.pop("schema_type", None)
                wim_RO.pop("wim_tenant_name", None)
                if "wim_type" in wim_RO:
                    wim_RO["type"] = wim_RO.pop("wim_type")
                wim_RO.pop("wim_user", None)
                wim_RO.pop("wim_password", None)
                # TODO make a deep update of wim_port_mapping
                if wim_RO:
                    await RO.edit("wim", RO_wim_id, descriptor=wim_RO)

                step = "Editing wim-account at RO tenant"
                wim_account_RO = {}
                if "config" in wim_content:
                    if "wim_port_mapping" in wim_content["config"]:
                        del wim_content["config"]["wim_port_mapping"]
                    if not wim_content["config"]:
                        del wim_content["config"]
                if "wim_tenant_name" in wim_content:
                    wim_account_RO["wim_tenant_name"] = wim_content["wim_tenant_name"]
                if "wim_password" in wim_content:
                    wim_account_RO["wim_password"] = wim_content["wim_password"]
                if wim_content.get("wim_password"):
                    wim_account_RO["wim_password"] = self.db.decrypt(wim_content["wim_password"],
                                                                     schema_version=schema_version,
                                                                     salt=wim_id)
                if "config" in wim_content:
                    wim_account_RO["config"] = wim_content["config"]
                if wim_content.get("config"):
                    for p in self.wim_config_encrypted:
                        if wim_content["config"].get(p):
                            wim_account_RO["config"][p] = self.db.decrypt(wim_content["config"][p],
                                                                          schema_version=schema_version,
                                                                          salt=wim_id)

                if "wim_user" in wim_content:
                    wim_content["wim_username"] = wim_content["wim_user"]
                # wim_account must be edited always even if empty in order to ensure changes are translated to RO
                # wim_thread. RO will remove and relaunch a new thread for this wim_account
                await RO.edit("wim_account", RO_wim_id, descriptor=wim_account_RO)
                db_wim_update["_admin.operationalState"] = "ENABLED"

            self.logger.debug(logging_text + "Exit Ok RO_wim_id={}".format(RO_wim_id))
            return

        except (ROclient.ROClientException, DbException) as e:
            self.logger.error(logging_text + "Exit Exception {}".format(e))
            exc = e
        except Exception as e:
            self.logger.critical(logging_text + "Exit Exception {}".format(e), exc_info=True)
            exc = e
        finally:
            if exc and db_wim:
                db_wim_update["_admin.operationalState"] = "ERROR"
                db_wim_update["_admin.detailed-status"] = "ERROR {}: {}".format(step, exc)
            try:
                if db_wim_update:
                    self.update_db_2("wim_accounts", wim_id, db_wim_update)
            except DbException as e:
                self.logger.error(logging_text + "Cannot update database: {}".format(e))
            self.lcm_tasks.remove("wim_account", wim_id, order_id)

    async def delete(self, wim_id, order_id):
        logging_text = "Task wim_delete={} ".format(wim_id)
        self.logger.debug(logging_text + "Enter")
        db_wim = None
        db_wim_update = {}
        exc = None
        step = "Getting wim from db"
        try:
            db_wim = self.db.get_one("wim_accounts", {"_id": wim_id})
            if db_wim.get("_admin") and db_wim["_admin"].get("deployed") and db_wim["_admin"]["deployed"].get("RO"):
                RO_wim_id = db_wim["_admin"]["deployed"]["RO"]
                RO = ROclient.ROClient(self.loop, **self.ro_config)
                step = "Detaching wim from RO tenant"
                try:
                    await RO.detach("wim_account", RO_wim_id)
                except ROclient.ROClientException as e:
                    if e.http_code == 404:  # not found
                        self.logger.debug(logging_text + "RO_wim_id={} already detached".format(RO_wim_id))
                    else:
                        raise

                step = "Deleting wim from RO"
                try:
                    await RO.delete("wim", RO_wim_id)
                except ROclient.ROClientException as e:
                    if e.http_code == 404:  # not found
                        self.logger.debug(logging_text + "RO_wim_id={} already deleted".format(RO_wim_id))
                    else:
                        raise
            else:
                # nothing to delete
                self.logger.error(logging_text + "Nohing to remove at RO")
            self.db.del_one("wim_accounts", {"_id": wim_id})
            db_wim = None
            self.logger.debug(logging_text + "Exit Ok")
            return

        except (ROclient.ROClientException, DbException) as e:
            self.logger.error(logging_text + "Exit Exception {}".format(e))
            exc = e
        except Exception as e:
            self.logger.critical(logging_text + "Exit Exception {}".format(e), exc_info=True)
            exc = e
        finally:
            self.lcm_tasks.remove("wim_account", wim_id, order_id)
            if exc and db_wim:
                db_wim_update["_admin.operationalState"] = "ERROR"
                db_wim_update["_admin.detailed-status"] = "ERROR {}: {}".format(step, exc)
            try:
                if db_wim and db_wim_update:
                    self.update_db_2("wim_accounts", wim_id, db_wim_update)
            except DbException as e:
                self.logger.error(logging_text + "Cannot update database: {}".format(e))
            self.lcm_tasks.remove("wim_account", wim_id, order_id)


class SdnLcm(LcmBase):

    def __init__(self, db, msg, fs, lcm_tasks, ro_config, loop):
        """
        Init, Connect to database, filesystem storage, and messaging
        :param config: two level dictionary with configuration. Top level should contain 'database', 'storage',
        :return: None
        """

        self.logger = logging.getLogger('lcm.sdn')
        self.loop = loop
        self.lcm_tasks = lcm_tasks
        self.ro_config = ro_config

        super().__init__(db, msg, fs, self.logger)

    async def create(self, sdn_content, order_id):
        sdn_id = sdn_content["_id"]
        sdn_content.pop("op_id", None)
        logging_text = "Task sdn_create={} ".format(sdn_id)
        self.logger.debug(logging_text + "Enter")
        db_sdn = None
        db_sdn_update = {}
        RO_sdn_id = None
        exc = None
        try:
            step = "Getting sdn from db"
            db_sdn = self.db.get_one("sdns", {"_id": sdn_id})
            db_sdn_update["_admin.deployed.RO"] = None

            step = "Creating sdn at RO"
            db_sdn_update["_admin.detailed-status"] = step
            self.update_db_2("sdns", sdn_id, db_sdn_update)

            RO = ROclient.ROClient(self.loop, **self.ro_config)
            sdn_RO = deepcopy(sdn_content)
            sdn_RO.pop("_id", None)
            sdn_RO.pop("_admin", None)
            schema_version = sdn_RO.pop("schema_version", None)
            sdn_RO.pop("schema_type", None)
            sdn_RO.pop("description", None)
            if sdn_RO.get("password"):
                sdn_RO["password"] = self.db.decrypt(sdn_RO["password"], schema_version=schema_version, salt=sdn_id)

            desc = await RO.create("sdn", descriptor=sdn_RO)
            RO_sdn_id = desc["uuid"]
            db_sdn_update["_admin.deployed.RO"] = RO_sdn_id
            db_sdn_update["_admin.operationalState"] = "ENABLED"
            self.logger.debug(logging_text + "Exit Ok RO_sdn_id={}".format(RO_sdn_id))
            return

        except (ROclient.ROClientException, DbException) as e:
            self.logger.error(logging_text + "Exit Exception {}".format(e))
            exc = e
        except Exception as e:
            self.logger.critical(logging_text + "Exit Exception {}".format(e), exc_info=True)
            exc = e
        finally:
            if exc and db_sdn:
                db_sdn_update["_admin.operationalState"] = "ERROR"
                db_sdn_update["_admin.detailed-status"] = "ERROR {}: {}".format(step, exc)
            try:
                if db_sdn and db_sdn_update:
                    self.update_db_2("sdns", sdn_id, db_sdn_update)
            except DbException as e:
                self.logger.error(logging_text + "Cannot update database: {}".format(e))
            self.lcm_tasks.remove("sdn", sdn_id, order_id)

    async def edit(self, sdn_content, order_id):
        sdn_id = sdn_content["_id"]
        sdn_content.pop("op_id", None)
        logging_text = "Task sdn_edit={} ".format(sdn_id)
        self.logger.debug(logging_text + "Enter")
        db_sdn = None
        db_sdn_update = {}
        exc = None
        step = "Getting sdn from db"
        try:
            db_sdn = self.db.get_one("sdns", {"_id": sdn_id})
            RO_sdn_id = None
            if db_sdn.get("_admin") and db_sdn["_admin"].get("deployed") and db_sdn["_admin"]["deployed"].get("RO"):
                RO_sdn_id = db_sdn["_admin"]["deployed"]["RO"]
                RO = ROclient.ROClient(self.loop, **self.ro_config)
                step = "Editing sdn at RO"
                sdn_RO = deepcopy(sdn_content)
                sdn_RO.pop("_id", None)
                sdn_RO.pop("_admin", None)
                schema_version = sdn_RO.pop("schema_version", None)
                sdn_RO.pop("schema_type", None)
                sdn_RO.pop("description", None)
                if sdn_RO.get("password"):
                    sdn_RO["password"] = self.db.decrypt(sdn_RO["password"], schema_version=schema_version, salt=sdn_id)
                if sdn_RO:
                    await RO.edit("sdn", RO_sdn_id, descriptor=sdn_RO)
                db_sdn_update["_admin.operationalState"] = "ENABLED"

            self.logger.debug(logging_text + "Exit Ok RO_sdn_id={}".format(RO_sdn_id))
            return

        except (ROclient.ROClientException, DbException) as e:
            self.logger.error(logging_text + "Exit Exception {}".format(e))
            exc = e
        except Exception as e:
            self.logger.critical(logging_text + "Exit Exception {}".format(e), exc_info=True)
            exc = e
        finally:
            if exc and db_sdn:
                db_sdn["_admin.operationalState"] = "ERROR"
                db_sdn["_admin.detailed-status"] = "ERROR {}: {}".format(step, exc)
            try:
                if db_sdn_update:
                    self.update_db_2("sdns", sdn_id, db_sdn_update)
            except DbException as e:
                self.logger.error(logging_text + "Cannot update database: {}".format(e))
            self.lcm_tasks.remove("sdn", sdn_id, order_id)

    async def delete(self, sdn_id, order_id):
        logging_text = "Task sdn_delete={} ".format(sdn_id)
        self.logger.debug(logging_text + "Enter")
        db_sdn = None
        db_sdn_update = {}
        exc = None
        step = "Getting sdn from db"
        try:
            db_sdn = self.db.get_one("sdns", {"_id": sdn_id})
            if db_sdn.get("_admin") and db_sdn["_admin"].get("deployed") and db_sdn["_admin"]["deployed"].get("RO"):
                RO_sdn_id = db_sdn["_admin"]["deployed"]["RO"]
                RO = ROclient.ROClient(self.loop, **self.ro_config)
                step = "Deleting sdn from RO"
                try:
                    await RO.delete("sdn", RO_sdn_id)
                except ROclient.ROClientException as e:
                    if e.http_code == 404:  # not found
                        self.logger.debug(logging_text + "RO_sdn_id={} already deleted".format(RO_sdn_id))
                    else:
                        raise
            else:
                # nothing to delete
                self.logger.error(logging_text + "Skipping. There is not RO information at database")
            self.db.del_one("sdns", {"_id": sdn_id})
            db_sdn = None
            self.logger.debug("sdn_delete task sdn_id={} Exit Ok".format(sdn_id))
            return

        except (ROclient.ROClientException, DbException) as e:
            self.logger.error(logging_text + "Exit Exception {}".format(e))
            exc = e
        except Exception as e:
            self.logger.critical(logging_text + "Exit Exception {}".format(e), exc_info=True)
            exc = e
        finally:
            if exc and db_sdn:
                db_sdn["_admin.operationalState"] = "ERROR"
                db_sdn["_admin.detailed-status"] = "ERROR {}: {}".format(step, exc)
            try:
                if db_sdn and db_sdn_update:
                    self.update_db_2("sdns", sdn_id, db_sdn_update)
            except DbException as e:
                self.logger.error(logging_text + "Cannot update database: {}".format(e))
            self.lcm_tasks.remove("sdn", sdn_id, order_id)
