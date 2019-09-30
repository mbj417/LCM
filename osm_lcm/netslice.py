# -*- coding: utf-8 -*-

import asyncio
import logging
import logging.handlers
import traceback
import ns
from ns import populate_dict as populate_dict
import ROclient
from lcm_utils import LcmException, LcmBase
from osm_common.dbbase import DbException
from time import time
from copy import deepcopy


__author__ = "Felipe Vicens, Pol Alemany, Alfonso Tierno"


def get_iterable(in_dict, in_key):
    """
    Similar to <dict>.get(), but if value is None, False, ..., An empty tuple is returned instead
    :param in_dict: a dictionary
    :param in_key: the key to look for at in_dict
    :return: in_dict[in_var] or () if it is None or not present
    """
    if not in_dict.get(in_key):
        return ()
    return in_dict[in_key]


class NetsliceLcm(LcmBase):

    total_deploy_timeout = 2 * 3600   # global timeout for deployment

    def __init__(self, db, msg, fs, lcm_tasks, ro_config, vca_config, loop):
        """
        Init, Connect to database, filesystem storage, and messaging
        :param config: two level dictionary with configuration. Top level should contain 'database', 'storage',
        :return: None
        """
        # logging
        self.logger = logging.getLogger('lcm.netslice')
        self.loop = loop
        self.lcm_tasks = lcm_tasks
        self.ns = ns.NsLcm(db, msg, fs, lcm_tasks, ro_config, vca_config, loop)
        self.ro_config = ro_config

        super().__init__(db, msg, fs, self.logger)

    def nsi_update_nsir(self, nsi_update_nsir, db_nsir, nsir_desc_RO):
        """
        Updates database nsir with the RO info for the created vld
        :param nsi_update_nsir: dictionary to be filled with the updated info
        :param db_nsir: content of db_nsir. This is also modified
        :param nsir_desc_RO: nsir descriptor from RO
        :return: Nothing, LcmException is raised on errors
        """

        for vld_index, vld in enumerate(get_iterable(db_nsir, "vld")):
            for net_RO in get_iterable(nsir_desc_RO, "nets"):
                if vld["id"] != net_RO.get("ns_net_osm_id"):
                    continue
                vld["vim-id"] = net_RO.get("vim_net_id")
                vld["name"] = net_RO.get("vim_name")
                vld["status"] = net_RO.get("status")
                vld["status-detailed"] = net_RO.get("error_msg")
                nsi_update_nsir["vld.{}".format(vld_index)] = vld
                break
            else:
                raise LcmException("ns_update_nsir: Not found vld={} at RO info".format(vld["id"]))

    async def instantiate(self, nsir_id, nsilcmop_id):

        # Try to lock HA task here
        task_is_locked_by_me = self.lcm_tasks.lock_HA('nsi', 'nsilcmops', nsilcmop_id)
        if not task_is_locked_by_me:
            return

        logging_text = "Task netslice={} instantiate={} ".format(nsir_id, nsilcmop_id)
        self.logger.debug(logging_text + "Enter")
        # get all needed from database
        exc = None
        db_nsir = None
        db_nsilcmop = None
        db_nsir_update = {"_admin.nsilcmop": nsilcmop_id}
        db_nsilcmop_update = {}
        nsilcmop_operation_state = None
        vim_2_RO = {}
        RO = ROclient.ROClient(self.loop, **self.ro_config)

        def ip_profile_2_RO(ip_profile):
            RO_ip_profile = deepcopy((ip_profile))
            if "dns-server" in RO_ip_profile:
                if isinstance(RO_ip_profile["dns-server"], list):
                    RO_ip_profile["dns-address"] = []
                    for ds in RO_ip_profile.pop("dns-server"):
                        RO_ip_profile["dns-address"].append(ds['address'])
                else:
                    RO_ip_profile["dns-address"] = RO_ip_profile.pop("dns-server")
            if RO_ip_profile.get("ip-version") == "ipv4":
                RO_ip_profile["ip-version"] = "IPv4"
            if RO_ip_profile.get("ip-version") == "ipv6":
                RO_ip_profile["ip-version"] = "IPv6"
            if "dhcp-params" in RO_ip_profile:
                RO_ip_profile["dhcp"] = RO_ip_profile.pop("dhcp-params")
            return RO_ip_profile

        def vim_account_2_RO(vim_account):
            """
            Translate a RO vim_account from OSM vim_account params
            :param ns_params: OSM instantiate params
            :return: The RO ns descriptor
            """
            if vim_account in vim_2_RO:
                return vim_2_RO[vim_account]

            db_vim = self.db.get_one("vim_accounts", {"_id": vim_account})
            if db_vim["_admin"]["operationalState"] != "ENABLED":
                raise LcmException("VIM={} is not available. operationalState={}".format(
                    vim_account, db_vim["_admin"]["operationalState"]))
            RO_vim_id = db_vim["_admin"]["deployed"]["RO"]
            vim_2_RO[vim_account] = RO_vim_id
            return RO_vim_id

        async def netslice_scenario_create(self, vld_item, nsir_id, db_nsir, db_nsir_admin, db_nsir_update):
            """
            Create a network slice VLD through RO Scenario
            :param vld_id The VLD id inside nsir to be created
            :param nsir_id The nsir id
            """
            ip_vld = None
            mgmt_network = False
            RO_vld_sites = []
            vld_id = vld_item["id"]
            netslice_vld = vld_item
            # logging_text = "Task netslice={} instantiate_vld={} ".format(nsir_id, vld_id)
            # self.logger.debug(logging_text + "Enter")

            vld_shared = None
            for shared_nsrs_item in get_iterable(vld_item, "shared-nsrs-list"):
                _filter = {"_id.ne": nsir_id, "_admin.nsrs-detailed-list.ANYINDEX.nsrId": shared_nsrs_item}
                shared_nsi = self.db.get_one("nsis", _filter, fail_on_empty=False, fail_on_more=False)
                if shared_nsi:
                    for vlds in get_iterable(shared_nsi["_admin"]["deployed"], "RO"):
                        if vld_id == vlds["vld_id"]:
                            vld_shared = {"instance_scenario_id": vlds["netslice_scenario_id"], "osm_id": vld_id}
                            break
                    break

            # Creating netslice-vld at RO
            RO_nsir = db_nsir["_admin"].get("deployed", {}).get("RO", [])

            if vld_id in RO_nsir:
                db_nsir_update["_admin.deployed.RO"] = RO_nsir

            # If netslice-vld doesn't exists then create it
            else:
                # TODO: Check VDU type in all descriptors finding SRIOV / PT
                # Updating network names and datacenters from instantiation parameters for each VLD
                RO_ns_params = {}
                RO_ns_params["name"] = netslice_vld["name"]
                RO_ns_params["datacenter"] = vim_account_2_RO(db_nsir["instantiation_parameters"]["vimAccountId"])
                for instantiation_params_vld in get_iterable(db_nsir["instantiation_parameters"], "netslice-vld"):
                    if instantiation_params_vld.get("name") == netslice_vld["name"]:
                        ip_vld = deepcopy(instantiation_params_vld)

                if netslice_vld.get("mgmt-network"):
                    mgmt_network = True

                # Creating scenario if vim-network-name / vim-network-id are present as instantiation parameter
                # Use vim-network-id instantiation parameter
                vim_network_option = None
                if ip_vld:
                    if ip_vld.get("vim-network-id"):
                        vim_network_option = "vim-network-id"
                    elif ip_vld.get("vim-network-name"):
                        vim_network_option = "vim-network-name"
                    if ip_vld.get("ip-profile"):
                        populate_dict(RO_ns_params, ("networks", netslice_vld["name"], "ip-profile"),
                                      ip_profile_2_RO(ip_vld["ip-profile"]))

                if vim_network_option:
                    if ip_vld.get(vim_network_option):
                        if isinstance(ip_vld.get(vim_network_option), list):
                            for vim_net_id in ip_vld.get(vim_network_option):
                                for vim_account, vim_net in vim_net_id.items():
                                    RO_vld_sites.append({
                                        "netmap-use": vim_net,
                                        "datacenter": vim_account_2_RO(vim_account)
                                    })
                        elif isinstance(ip_vld.get(vim_network_option), dict):
                            for vim_account, vim_net in ip_vld.get(vim_network_option).items():
                                RO_vld_sites.append({
                                    "netmap-use": vim_net,
                                    "datacenter": vim_account_2_RO(vim_account)
                                })
                        else:
                            RO_vld_sites.append({
                                "netmap-use": ip_vld[vim_network_option],
                                "datacenter": vim_account_2_RO(netslice_vld["vimAccountId"])})
                        
                # Use default netslice vim-network-name from template
                else:
                    for nss_conn_point_ref in get_iterable(netslice_vld, "nss-connection-point-ref"):
                        if nss_conn_point_ref.get("vimAccountId"):
                            if nss_conn_point_ref["vimAccountId"] != netslice_vld["vimAccountId"]:
                                RO_vld_sites.append({
                                    "netmap-create": None,
                                    "datacenter": vim_account_2_RO(nss_conn_point_ref["vimAccountId"])})

                if vld_shared:
                    populate_dict(RO_ns_params, ("networks", netslice_vld["name"], "use-network"), vld_shared)

                if RO_vld_sites:
                    populate_dict(RO_ns_params, ("networks", netslice_vld["name"], "sites"), RO_vld_sites)

                RO_ns_params["scenario"] = {"nets": [{"name": netslice_vld["name"],
                                            "external": mgmt_network, "type": "bridge"}]}

                # self.logger.debug(logging_text + step)
                db_nsir_update["detailed-status"] = "Creating netslice-vld at RO"
                desc = await RO.create("ns", descriptor=RO_ns_params)
                db_nsir_update_RO = {}
                db_nsir_update_RO["netslice_scenario_id"] = desc["uuid"]
                db_nsir_update_RO["vld_id"] = RO_ns_params["name"]
                db_nsir_update["_admin.deployed.RO"].append(db_nsir_update_RO)

        def overwrite_nsd_params(self, db_nsir, nslcmop):
            RO_list = []
            vld_op_list = []
            vld = None
            nsr_id = nslcmop.get("nsInstanceId")
            # Overwrite instantiation parameters in netslice runtime
            if db_nsir.get("_admin"):
                if db_nsir["_admin"].get("deployed"):
                    db_admin_deployed_nsir = db_nsir["_admin"].get("deployed")
                    if db_admin_deployed_nsir.get("RO"):
                        RO_list = db_admin_deployed_nsir["RO"]

            for RO_item in RO_list:
                for netslice_vld in get_iterable(db_nsir["_admin"], "netslice-vld"):
                    # if is equal vld of _admin with vld of netslice-vld then go for the CPs
                    if RO_item.get("vld_id") == netslice_vld.get("id"):
                        # Search the cp of netslice-vld that match with nst:netslice-subnet
                        for nss_cp_item in get_iterable(netslice_vld, "nss-connection-point-ref"):
                            # Search the netslice-subnet of nst that match
                            for nss in get_iterable(db_nsir["_admin"], "netslice-subnet"):
                                # Compare nss-ref equal nss from nst
                                if nss_cp_item["nss-ref"] == nss["nss-id"]:
                                    db_nsds = self.db.get_one("nsds", {"_id": nss["nsdId"]})
                                    # Go for nsd, and search the CP that match with nst:CP to get vld-id-ref
                                    for cp_nsd in db_nsds["connection-point"]:
                                        if cp_nsd["name"] == nss_cp_item["nsd-connection-point-ref"]:
                                            if nslcmop.get("operationParams"):
                                                if nslcmop["operationParams"].get("nsName") == nss["nsName"]:
                                                    vld_id = RO_item["vld_id"]
                                                    netslice_scenario_id = RO_item["netslice_scenario_id"]
                                                    nslcmop_vld = {}
                                                    nslcmop_vld["ns-net"] = {vld_id: netslice_scenario_id}
                                                    nslcmop_vld["name"] = cp_nsd["vld-id-ref"]
                                                    for vld in get_iterable(nslcmop["operationParams"], "vld"):
                                                        if vld["name"] == cp_nsd["vld-id-ref"]:
                                                            nslcmop_vld.update(vld)
                                                    vld_op_list.append(nslcmop_vld)
            nslcmop["operationParams"]["vld"] = vld_op_list
            self.update_db_2("nslcmops", nslcmop["_id"], nslcmop)
            return nsr_id, nslcmop

        try:
            # wait for any previous tasks in process
            await self.lcm_tasks.waitfor_related_HA('nsi', 'nsilcmops', nsilcmop_id)

            step = "Getting nsir={} from db".format(nsir_id)
            db_nsir = self.db.get_one("nsis", {"_id": nsir_id})
            step = "Getting nsilcmop={} from db".format(nsilcmop_id)
            db_nsilcmop = self.db.get_one("nsilcmops", {"_id": nsilcmop_id})

            # Empty list to keep track of network service records status in the netslice
            nsir_admin = db_nsir_admin = db_nsir.get("_admin")

            # Slice status Creating
            db_nsir_update["detailed-status"] = "creating"
            db_nsir_update["operational-status"] = "init"
            self.update_db_2("nsis", nsir_id, db_nsir_update)

            # Creating netslice VLDs networking before NS instantiation
            db_nsir_update["_admin.deployed.RO"] = db_nsir_admin["deployed"]["RO"]
            for vld_item in get_iterable(nsir_admin, "netslice-vld"):
                await netslice_scenario_create(self, vld_item, nsir_id, db_nsir, db_nsir_admin, db_nsir_update)
            self.update_db_2("nsis", nsir_id, db_nsir_update)

            db_nsir_update["detailed-status"] = "Creating netslice subnets at RO"
            self.update_db_2("nsis", nsir_id, db_nsir_update)

            db_nsir = self.db.get_one("nsis", {"_id": nsir_id})

            # Check status of the VLDs and wait for creation
            # netslice_scenarios = db_nsir["_admin"]["deployed"]["RO"]
            # db_nsir_update_RO = deepcopy(netslice_scenarios)
            # for netslice_scenario in netslice_scenarios:
            #    await netslice_scenario_check(self, netslice_scenario["netslice_scenario_id"],
            #                                  nsir_id, db_nsir_update_RO)

            # db_nsir_update["_admin.deployed.RO"] = db_nsir_update_RO
            # self.update_db_2("nsis", nsir_id, db_nsir_update)

            # Iterate over the network services operation ids to instantiate NSs
            # TODO: (future improvement) look another way check the tasks instead of keep asking
            # -> https://docs.python.org/3/library/asyncio-task.html#waiting-primitives
            # steps: declare ns_tasks, add task when terminate is called, await asyncio.wait(vca_task_list, timeout=300)
            db_nsir = self.db.get_one("nsis", {"_id": nsir_id})
            nslcmop_ids = db_nsilcmop["operationParams"].get("nslcmops_ids")
            for nslcmop_id in nslcmop_ids:
                nslcmop = self.db.get_one("nslcmops", {"_id": nslcmop_id})
                # Overwriting netslice-vld vim-net-id to ns
                nsr_id, nslcmop = overwrite_nsd_params(self, db_nsir, nslcmop)
                step = "Launching ns={} instantiate={} task".format(nsr_id, nslcmop_id)
                task = asyncio.ensure_future(self.ns.instantiate(nsr_id, nslcmop_id))
                self.lcm_tasks.register("ns", nsr_id, nslcmop_id, "ns_instantiate", task)

            # Wait until Network Slice is ready
            step = nsir_status_detailed = " Waiting nsi ready. nsi_id={}".format(nsir_id)
            nsrs_detailed_list_old = None
            self.logger.debug(logging_text + step)

            # TODO: substitute while for await (all task to be done or not)
            deployment_timeout = 2 * 3600   # Two hours
            while deployment_timeout > 0:
                # Check ns instantiation status
                nsi_ready = True
                nsir = self.db.get_one("nsis", {"_id": nsir_id})
                nsir_admin = nsir["_admin"]
                nsrs_detailed_list = nsir["_admin"]["nsrs-detailed-list"]
                nsrs_detailed_list_new = []
                for nslcmop_item in nslcmop_ids:
                    nslcmop = self.db.get_one("nslcmops", {"_id": nslcmop_item})
                    status = nslcmop.get("operationState")
                    # TODO: (future improvement) other possible status: ROLLING_BACK,ROLLED_BACK
                    for nss in nsrs_detailed_list:
                        if nss["nsrId"] == nslcmop["nsInstanceId"]:
                            nss.update({"nsrId": nslcmop["nsInstanceId"], "status": nslcmop["operationState"],
                                        "detailed-status":
                                        nsir_status_detailed + "; {}".format(nslcmop.get("detailed-status")),
                                        "instantiated": True})
                            nsrs_detailed_list_new.append(nss)
                    if status not in ["COMPLETED", "PARTIALLY_COMPLETED", "FAILED", "FAILED_TEMP"]:  
                        nsi_ready = False

                if nsrs_detailed_list_new != nsrs_detailed_list_old:
                    nsir_admin["nsrs-detailed-list"] = nsrs_detailed_list_new
                    nsrs_detailed_list_old = nsrs_detailed_list_new
                    db_nsir_update["_admin"] = nsir_admin
                    self.update_db_2("nsis", nsir_id, db_nsir_update)

                if nsi_ready:
                    step = "Network Slice Instance is ready. nsi_id={}".format(nsir_id)
                    for items in nsrs_detailed_list:
                        if "FAILED" in items.values():
                            raise LcmException("Error deploying NSI: {}".format(nsir_id))
                    break
 
                # TODO: future improvement due to synchronism -> await asyncio.wait(vca_task_list, timeout=300)
                await asyncio.sleep(5, loop=self.loop)
                deployment_timeout -= 5
            
            if deployment_timeout <= 0:
                raise LcmException("Timeout waiting nsi to be ready. nsi_id={}".format(nsir_id))

            db_nsir_update["operational-status"] = "running"
            db_nsir_update["detailed-status"] = "done"
            db_nsir_update["config-status"] = "configured"
            db_nsilcmop_update["operationState"] = nsilcmop_operation_state = "COMPLETED"
            db_nsilcmop_update["statusEnteredTime"] = time()
            db_nsilcmop_update["detailed-status"] = "done"
            return

        except (LcmException, DbException) as e:
            self.logger.error(logging_text + "Exit Exception while '{}': {}".format(step, e))
            exc = e
        except asyncio.CancelledError:
            self.logger.error(logging_text + "Cancelled Exception while '{}'".format(step))
            exc = "Operation was cancelled"
        except Exception as e:
            exc = traceback.format_exc()
            self.logger.critical(logging_text + "Exit Exception {} while '{}': {}".format(type(e).__name__, step, e),
                                 exc_info=True)
        finally:
            if exc:
                if db_nsir:
                    db_nsir_update["detailed-status"] = "ERROR {}: {}".format(step, exc)
                    db_nsir_update["operational-status"] = "failed"
                if db_nsilcmop:
                    db_nsilcmop_update["detailed-status"] = "FAILED {}: {}".format(step, exc)
                    db_nsilcmop_update["operationState"] = nsilcmop_operation_state = "FAILED"
                    db_nsilcmop_update["statusEnteredTime"] = time()
            try:
                if db_nsir:
                    db_nsir_update["_admin.nsiState"] = "INSTANTIATED"
                    db_nsir_update["_admin.nsilcmop"] = None
                    self.update_db_2("nsis", nsir_id, db_nsir_update)
                if db_nsilcmop:
                    self.update_db_2("nsilcmops", nsilcmop_id, db_nsilcmop_update)
            except DbException as e:
                self.logger.error(logging_text + "Cannot update database: {}".format(e))
            if nsilcmop_operation_state:
                try:
                    await self.msg.aiowrite("nsi", "instantiated", {"nsir_id": nsir_id, "nsilcmop_id": nsilcmop_id,
                                                                    "operationState": nsilcmop_operation_state})
                except Exception as e:
                    self.logger.error(logging_text + "kafka_write notification Exception {}".format(e))
            self.logger.debug(logging_text + "Exit")
            self.lcm_tasks.remove("nsi", nsir_id, nsilcmop_id, "nsi_instantiate")

    async def terminate(self, nsir_id, nsilcmop_id):

        # Try to lock HA task here
        task_is_locked_by_me = self.lcm_tasks.lock_HA('nsi', 'nsilcmops', nsilcmop_id)
        if not task_is_locked_by_me:
            return

        logging_text = "Task nsi={} terminate={} ".format(nsir_id, nsilcmop_id)
        self.logger.debug(logging_text + "Enter")
        exc = None
        db_nsir = None
        db_nsilcmop = None
        db_nsir_update = {"_admin.nsilcmop": nsilcmop_id}
        db_nsilcmop_update = {}
        RO = ROclient.ROClient(self.loop, **self.ro_config)
        nsir_deployed = None
        failed_detail = []   # annotates all failed error messages
        nsilcmop_operation_state = None
        autoremove = False  # autoremove after terminated
        try:
            # wait for any previous tasks in process
            await self.lcm_tasks.waitfor_related_HA('nsi', 'nsilcmops', nsilcmop_id)

            step = "Getting nsir={} from db".format(nsir_id)
            db_nsir = self.db.get_one("nsis", {"_id": nsir_id})
            nsir_deployed = deepcopy(db_nsir["_admin"].get("deployed"))
            step = "Getting nsilcmop={} from db".format(nsilcmop_id)
            db_nsilcmop = self.db.get_one("nsilcmops", {"_id": nsilcmop_id})

            # TODO: Check if makes sense check the nsiState=NOT_INSTANTIATED when terminate
            # CASE: Instance was terminated but there is a second request to terminate the instance
            if db_nsir["_admin"]["nsiState"] == "NOT_INSTANTIATED":
                return

            # Slice status Terminating
            db_nsir_update["operational-status"] = "terminating"
            db_nsir_update["config-status"] = "terminating"
            db_nsir_update["detailed-status"] = "Terminating Netslice subnets"
            self.update_db_2("nsis", nsir_id, db_nsir_update)

            # Gets the list to keep track of network service records status in the netslice
            nsir_admin = db_nsir["_admin"]
            nsrs_detailed_list = []

            # Iterate over the network services operation ids to terminate NSs
            # TODO: (future improvement) look another way check the tasks instead of keep asking
            # -> https://docs.python.org/3/library/asyncio-task.html#waiting-primitives
            # steps: declare ns_tasks, add task when terminate is called, await asyncio.wait(vca_task_list, timeout=300)
            nslcmop_ids = db_nsilcmop["operationParams"].get("nslcmops_ids")
            nslcmop_new = []
            for nslcmop_id in nslcmop_ids:
                nslcmop = self.db.get_one("nslcmops", {"_id": nslcmop_id})
                nsr_id = nslcmop["operationParams"].get("nsInstanceId")
                nss_in_use = self.db.get_list("nsis", {"_admin.netslice-vld.ANYINDEX.shared-nsrs-list": nsr_id,
                                                       "operational-status": {"$nin": ["terminated", "failed"]}})
                if len(nss_in_use) < 2:
                    task = asyncio.ensure_future(self.ns.terminate(nsr_id, nslcmop_id))
                    self.lcm_tasks.register("ns", nsr_id, nslcmop_id, "ns_instantiate", task)
                    nslcmop_new.append(nslcmop_id)
                else:
                    # Update shared nslcmop shared with active nsi
                    netsliceInstanceId = db_nsir["_id"]
                    for nsis_item in nss_in_use:
                        if db_nsir["_id"] != nsis_item["_id"]:
                            netsliceInstanceId = nsis_item["_id"]
                            break
                    self.db.set_one("nslcmops", {"_id": nslcmop_id},
                                    {"operationParams.netsliceInstanceId": netsliceInstanceId})
            self.db.set_one("nsilcmops", {"_id": nsilcmop_id}, {"operationParams.nslcmops_ids": nslcmop_new})

            # Wait until Network Slice is terminated
            step = nsir_status_detailed = " Waiting nsi terminated. nsi_id={}".format(nsir_id)
            nsrs_detailed_list_old = None
            self.logger.debug(logging_text + step)

            termination_timeout = 2 * 3600   # Two hours
            while termination_timeout > 0:
                # Check ns termination status
                nsi_ready = True
                db_nsir = self.db.get_one("nsis", {"_id": nsir_id})
                nsir_admin = db_nsir["_admin"]
                nsrs_detailed_list = db_nsir["_admin"].get("nsrs-detailed-list")
                nsrs_detailed_list_new = []
                for nslcmop_item in nslcmop_ids:
                    nslcmop = self.db.get_one("nslcmops", {"_id": nslcmop_item})
                    status = nslcmop["operationState"]
                    # TODO: (future improvement) other possible status: ROLLING_BACK,ROLLED_BACK 
                    for nss in nsrs_detailed_list:
                        if nss["nsrId"] == nslcmop["nsInstanceId"]:
                            nss.update({"nsrId": nslcmop["nsInstanceId"], "status": nslcmop["operationState"],
                                        "detailed-status":
                                        nsir_status_detailed + "; {}".format(nslcmop.get("detailed-status"))})
                            nsrs_detailed_list_new.append(nss)
                    if status not in ["COMPLETED", "PARTIALLY_COMPLETED", "FAILED", "FAILED_TEMP"]:
                        nsi_ready = False

                if nsrs_detailed_list_new != nsrs_detailed_list_old:
                    nsir_admin["nsrs-detailed-list"] = nsrs_detailed_list_new
                    nsrs_detailed_list_old = nsrs_detailed_list_new
                    db_nsir_update["_admin"] = nsir_admin
                    self.update_db_2("nsis", nsir_id, db_nsir_update)
                    
                if nsi_ready:
                    # Check if it is the last used nss and mark isinstantiate: False
                    db_nsir = self.db.get_one("nsis", {"_id": nsir_id})
                    nsir_admin = db_nsir["_admin"]
                    nsrs_detailed_list = db_nsir["_admin"].get("nsrs-detailed-list")
                    for nss in nsrs_detailed_list:
                        _filter = {"_admin.nsrs-detailed-list.ANYINDEX.nsrId": nss["nsrId"],
                                   "operational-status.ne": "terminated",
                                   "_id.ne": nsir_id}
                        nsis_list = self.db.get_one("nsis", _filter, fail_on_empty=False, fail_on_more=False)
                        if not nsis_list:
                            nss.update({"instantiated": False})
                    db_nsir_update["_admin"] = nsir_admin
                    self.update_db_2("nsis", nsir_id, db_nsir_update)

                    step = "Network Slice Instance is terminated. nsi_id={}".format(nsir_id)
                    for items in nsrs_detailed_list:
                        if "FAILED" in items.values():
                            raise LcmException("Error terminating NSI: {}".format(nsir_id))
                    break

                await asyncio.sleep(5, loop=self.loop)
                termination_timeout -= 5

            if termination_timeout <= 0:
                raise LcmException("Timeout waiting nsi to be terminated. nsi_id={}".format(nsir_id))

            # Delete netslice-vlds
            RO_nsir_id = RO_delete_action = None
            for nsir_deployed_RO in get_iterable(nsir_deployed, "RO"):
                RO_nsir_id = nsir_deployed_RO.get("netslice_scenario_id")
                try:
                    step = db_nsir_update["detailed-status"] = "Deleting netslice-vld at RO"
                    db_nsilcmop_update["detailed-status"] = "Deleting netslice-vld at RO"
                    self.logger.debug(logging_text + step)
                    desc = await RO.delete("ns", RO_nsir_id)
                    RO_delete_action = desc["action_id"]
                    nsir_deployed_RO["vld_delete_action_id"] = RO_delete_action
                    nsir_deployed_RO["vld_status"] = "DELETING"
                    db_nsir_update["_admin.deployed"] = nsir_deployed
                    self.update_db_2("nsis", nsir_id, db_nsir_update)
                    if RO_delete_action:
                        # wait until NS is deleted from VIM
                        step = "Waiting ns deleted from VIM. RO_id={}".format(RO_nsir_id)
                        self.logger.debug(logging_text + step)
                except ROclient.ROClientException as e:
                    if e.http_code == 404:  # not found
                        nsir_deployed_RO["vld_id"] = None
                        nsir_deployed_RO["vld_status"] = "DELETED"
                        self.logger.debug(logging_text + "RO_ns_id={} already deleted".format(RO_nsir_id))
                    elif e.http_code == 409:   # conflict
                        failed_detail.append("RO_ns_id={} delete conflict: {}".format(RO_nsir_id, e))
                        self.logger.debug(logging_text + failed_detail[-1])
                    else:
                        failed_detail.append("RO_ns_id={} delete error: {}".format(RO_nsir_id, e))
                        self.logger.error(logging_text + failed_detail[-1])

                if failed_detail:
                    self.logger.error(logging_text + " ;".join(failed_detail))
                    db_nsir_update["operational-status"] = "failed"
                    db_nsir_update["detailed-status"] = "Deletion errors " + "; ".join(failed_detail)
                    db_nsilcmop_update["detailed-status"] = "; ".join(failed_detail)
                    db_nsilcmop_update["operationState"] = nsilcmop_operation_state = "FAILED"
                    db_nsilcmop_update["statusEnteredTime"] = time()
                else:
                    db_nsir_update["operational-status"] = "terminating"
                    db_nsir_update["config-status"] = "terminating"
                    db_nsir_update["_admin.nsiState"] = "NOT_INSTANTIATED"
                    db_nsilcmop_update["operationState"] = nsilcmop_operation_state = "COMPLETED"
                    db_nsilcmop_update["statusEnteredTime"] = time()
                    if db_nsilcmop["operationParams"].get("autoremove"):
                        autoremove = True

            db_nsir_update["detailed-status"] = "done"
            db_nsir_update["operational-status"] = "terminated"
            db_nsir_update["config-status"] = "terminated"
            db_nsilcmop_update["statusEnteredTime"] = time()
            db_nsilcmop_update["detailed-status"] = "done"
            return

        except (LcmException, DbException) as e:
            self.logger.error(logging_text + "Exit Exception while '{}': {}".format(step, e))
            exc = e
        except asyncio.CancelledError:
            self.logger.error(logging_text + "Cancelled Exception while '{}'".format(step))
            exc = "Operation was cancelled"
        except Exception as e:
            exc = traceback.format_exc()
            self.logger.critical(logging_text + "Exit Exception {} while '{}': {}".format(type(e).__name__, step, e),
                                 exc_info=True)
        finally:
            if exc:
                if db_nsir:
                    db_nsir_update["_admin.deployed"] = nsir_deployed
                    db_nsir_update["detailed-status"] = "ERROR {}: {}".format(step, exc)
                    db_nsir_update["operational-status"] = "failed"
                if db_nsilcmop:
                    db_nsilcmop_update["detailed-status"] = "FAILED {}: {}".format(step, exc)
                    db_nsilcmop_update["operationState"] = nsilcmop_operation_state = "FAILED"
                    db_nsilcmop_update["statusEnteredTime"] = time()
            try:
                if db_nsir:
                    db_nsir_update["_admin.deployed"] = nsir_deployed
                    db_nsir_update["_admin.nsilcmop"] = None
                    self.update_db_2("nsis", nsir_id, db_nsir_update)
                if db_nsilcmop:
                    self.update_db_2("nsilcmops", nsilcmop_id, db_nsilcmop_update)
            except DbException as e:
                self.logger.error(logging_text + "Cannot update database: {}".format(e))

            if nsilcmop_operation_state:
                try:
                    await self.msg.aiowrite("nsi", "terminated", {"nsir_id": nsir_id, "nsilcmop_id": nsilcmop_id,
                                                                  "operationState": nsilcmop_operation_state,
                                                                  "autoremove": autoremove},
                                            loop=self.loop)
                except Exception as e:
                    self.logger.error(logging_text + "kafka_write notification Exception {}".format(e))
            self.logger.debug(logging_text + "Exit")
            self.lcm_tasks.remove("nsi", nsir_id, nsilcmop_id, "nsi_terminate")
