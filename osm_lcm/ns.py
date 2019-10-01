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
import yaml
import json
import logging
import logging.handlers
import functools
import traceback
from jinja2 import Environment, Template, meta, TemplateError, TemplateNotFound, TemplateSyntaxError

import ROclient
from lcm_utils import LcmException, LcmExceptionNoMgmtIP, LcmBase

from osm_common.dbbase import DbException
from osm_common.fsbase import FsException
from n2vc.vnf import N2VC, N2VCPrimitiveExecutionFailed, NetworkServiceDoesNotExist, PrimitiveDoesNotExist

from copy import copy, deepcopy
from http import HTTPStatus
from time import time
from uuid import uuid4

__author__ = "Alfonso Tierno"


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


def populate_dict(target_dict, key_list, value):
    """
    Upate target_dict creating nested dictionaries with the key_list. Last key_list item is asigned the value.
    Example target_dict={K: J}; key_list=[a,b,c];  target_dict will be {K: J, a: {b: {c: value}}}
    :param target_dict: dictionary to be changed
    :param key_list: list of keys to insert at target_dict
    :param value:
    :return: None
    """
    for key in key_list[0:-1]:
        if key not in target_dict:
            target_dict[key] = {}
        target_dict = target_dict[key]
    target_dict[key_list[-1]] = value


class NsLcm(LcmBase):
    timeout_vca_on_error = 5 * 60   # Time for charm from first time at blocked,error status to mark as failed
    total_deploy_timeout = 2 * 3600   # global timeout for deployment
    timeout_charm_delete = 10 * 60
    timeout_primitive = 10 * 60  # timeout for primitive execution

    def __init__(self, db, msg, fs, lcm_tasks, ro_config, vca_config, loop, lcm=None):
        """
        Init, Connect to database, filesystem storage, and messaging
        :param config: two level dictionary with configuration. Top level should contain 'database', 'storage',
        :return: None
        """
        # logging
        self.logger = logging.getLogger('lcm.ns')
        self.loop = loop
        self.lcm_tasks = lcm_tasks
        self.lcm = lcm

        super().__init__(db, msg, fs, self.logger)

        self.ro_config = ro_config

        self.n2vc = N2VC(
            log=self.logger,
            server=vca_config['host'],
            port=vca_config['port'],
            user=vca_config['user'],
            secret=vca_config['secret'],
            # TODO: This should point to the base folder where charms are stored,
            # if there is a common one (like object storage). Otherwise, leave
            # it unset and pass it via DeployCharms
            # artifacts=vca_config[''],
            artifacts=None,
            juju_public_key=vca_config.get('pubkey'),
            ca_cert=vca_config.get('cacert'),
        )

    def vnfd2RO(self, vnfd, new_id=None, additionalParams=None, nsrId=None):
        """
        Converts creates a new vnfd descriptor for RO base on input OSM IM vnfd
        :param vnfd: input vnfd
        :param new_id: overrides vnf id if provided
        :param additionalParams: Instantiation params for VNFs provided
        :param nsrId: Id of the NSR
        :return: copy of vnfd
        """
        try:
            vnfd_RO = deepcopy(vnfd)
            # remove unused by RO configuration, monitoring, scaling and internal keys
            vnfd_RO.pop("_id", None)
            vnfd_RO.pop("_admin", None)
            vnfd_RO.pop("vnf-configuration", None)
            vnfd_RO.pop("monitoring-param", None)
            vnfd_RO.pop("scaling-group-descriptor", None)
            if new_id:
                vnfd_RO["id"] = new_id

            # parse cloud-init or cloud-init-file with the provided variables using Jinja2
            for vdu in get_iterable(vnfd_RO, "vdu"):
                cloud_init_file = None
                if vdu.get("cloud-init-file"):
                    base_folder = vnfd["_admin"]["storage"]
                    cloud_init_file = "{}/{}/cloud_init/{}".format(base_folder["folder"], base_folder["pkg-dir"],
                                                                   vdu["cloud-init-file"])
                    with self.fs.file_open(cloud_init_file, "r") as ci_file:
                        cloud_init_content = ci_file.read()
                    vdu.pop("cloud-init-file", None)
                elif vdu.get("cloud-init"):
                    cloud_init_content = vdu["cloud-init"]
                else:
                    continue

                env = Environment()
                ast = env.parse(cloud_init_content)
                mandatory_vars = meta.find_undeclared_variables(ast)
                if mandatory_vars:
                    for var in mandatory_vars:
                        if not additionalParams or var not in additionalParams.keys():
                            raise LcmException("Variable '{}' defined at vnfd[id={}]:vdu[id={}]:cloud-init/cloud-init-"
                                               "file, must be provided in the instantiation parameters inside the "
                                               "'additionalParamsForVnf' block".format(var, vnfd["id"], vdu["id"]))
                template = Template(cloud_init_content)
                cloud_init_content = template.render(additionalParams or {})
                vdu["cloud-init"] = cloud_init_content

            return vnfd_RO
        except FsException as e:
            raise LcmException("Error reading vnfd[id={}]:vdu[id={}]:cloud-init-file={}: {}".
                               format(vnfd["id"], vdu["id"], cloud_init_file, e))
        except (TemplateError, TemplateNotFound, TemplateSyntaxError) as e:
            raise LcmException("Error parsing Jinja2 to cloud-init content at vnfd[id={}]:vdu[id={}]: {}".
                               format(vnfd["id"], vdu["id"], e))

    def n2vc_callback(self, model_name, application_name, status, message, n2vc_info, task=None):
        """
        Callback both for charm status change and task completion
        :param model_name: Charm model name
        :param application_name: Charm application name
        :param status: Can be
            - blocked: The unit needs manual intervention
            - maintenance: The unit is actively deploying/configuring
            - waiting: The unit is waiting for another charm to be ready
            - active: The unit is deployed, configured, and ready
            - error: The charm has failed and needs attention.
            - terminated: The charm has been destroyed
            - removing,
            - removed
        :param message: detailed message error
        :param n2vc_info: dictionary with information shared with instantiate task. It contains:
            nsr_id:
            nslcmop_id:
            lcmOperationType: currently "instantiate"
            deployed: dictionary with {<application>: {operational-status: <status>, detailed-status: <text>}}
            db_update: dictionary to be filled with the changes to be wrote to database with format key.key.key: value
            n2vc_event: event used to notify instantiation task that some change has been produced
        :param task: None for charm status change, or task for completion task callback
        :return:
        """
        try:
            nsr_id = n2vc_info["nsr_id"]
            deployed = n2vc_info["deployed"]
            db_nsr_update = n2vc_info["db_update"]
            nslcmop_id = n2vc_info["nslcmop_id"]
            ns_operation = n2vc_info["lcmOperationType"]
            n2vc_event = n2vc_info["n2vc_event"]
            logging_text = "Task ns={} {}={} [n2vc_callback] application={}".format(nsr_id, ns_operation, nslcmop_id,
                                                                                    application_name)
            for vca_index, vca_deployed in enumerate(deployed):
                if not vca_deployed:
                    continue
                if model_name == vca_deployed["model"] and application_name == vca_deployed["application"]:
                    break
            else:
                self.logger.error(logging_text + " Not present at nsr._admin.deployed.VCA. Received model_name={}".
                                  format(model_name))
                return
            if task:
                if task.cancelled():
                    self.logger.debug(logging_text + " task Cancelled")
                    vca_deployed['operational-status'] = "error"
                    db_nsr_update["_admin.deployed.VCA.{}.operational-status".format(vca_index)] = "error"
                    vca_deployed['detailed-status'] = "Task Cancelled"
                    db_nsr_update["_admin.deployed.VCA.{}.detailed-status".format(vca_index)] = "Task Cancelled"

                elif task.done():
                    exc = task.exception()
                    if exc:
                        self.logger.error(logging_text + " task Exception={}".format(exc))
                        vca_deployed['operational-status'] = "error"
                        db_nsr_update["_admin.deployed.VCA.{}.operational-status".format(vca_index)] = "error"
                        vca_deployed['detailed-status'] = str(exc)
                        db_nsr_update["_admin.deployed.VCA.{}.detailed-status".format(vca_index)] = str(exc)
                    else:
                        self.logger.debug(logging_text + " task Done")
                        # task is Done, but callback is still ongoing. So ignore
                        return
            elif status:
                self.logger.debug(logging_text + " Enter status={} message={}".format(status, message))
                if vca_deployed['operational-status'] == status:
                    return  # same status, ignore
                vca_deployed['operational-status'] = status
                db_nsr_update["_admin.deployed.VCA.{}.operational-status".format(vca_index)] = status
                vca_deployed['detailed-status'] = str(message)
                db_nsr_update["_admin.deployed.VCA.{}.detailed-status".format(vca_index)] = str(message)
            else:
                self.logger.critical(logging_text + " Enter with bad parameters", exc_info=True)
                return
            # wake up instantiate task
            n2vc_event.set()
        except Exception as e:
            self.logger.critical(logging_text + " Exception {}".format(e), exc_info=True)

    def ns_params_2_RO(self, ns_params, nsd, vnfd_dict, n2vc_key_list):
        """
        Creates a RO ns descriptor from OSM ns_instantiate params
        :param ns_params: OSM instantiate params
        :return: The RO ns descriptor
        """
        vim_2_RO = {}
        wim_2_RO = {}
        # TODO feature 1417: Check that no instantiation is set over PDU
        # check if PDU forces a concrete vim-network-id and add it
        # check if PDU contains a SDN-assist info (dpid, switch, port) and pass it to RO

        def vim_account_2_RO(vim_account):
            if vim_account in vim_2_RO:
                return vim_2_RO[vim_account]

            db_vim = self.db.get_one("vim_accounts", {"_id": vim_account})
            if db_vim["_admin"]["operationalState"] != "ENABLED":
                raise LcmException("VIM={} is not available. operationalState={}".format(
                    vim_account, db_vim["_admin"]["operationalState"]))
            RO_vim_id = db_vim["_admin"]["deployed"]["RO"]
            vim_2_RO[vim_account] = RO_vim_id
            return RO_vim_id

        def wim_account_2_RO(wim_account):
            if isinstance(wim_account, str):
                if wim_account in wim_2_RO:
                    return wim_2_RO[wim_account]

                db_wim = self.db.get_one("wim_accounts", {"_id": wim_account})
                if db_wim["_admin"]["operationalState"] != "ENABLED":
                    raise LcmException("WIM={} is not available. operationalState={}".format(
                        wim_account, db_wim["_admin"]["operationalState"]))
                RO_wim_id = db_wim["_admin"]["deployed"]["RO-account"]
                wim_2_RO[wim_account] = RO_wim_id
                return RO_wim_id
            else:
                return wim_account

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

        if not ns_params:
            return None
        RO_ns_params = {
            # "name": ns_params["nsName"],
            # "description": ns_params.get("nsDescription"),
            "datacenter": vim_account_2_RO(ns_params["vimAccountId"]),
            "wim_account": wim_account_2_RO(ns_params.get("wimAccountId")),
            # "scenario": ns_params["nsdId"],
        }
        if n2vc_key_list:
            for vnfd_ref, vnfd in vnfd_dict.items():
                vdu_needed_access = []
                mgmt_cp = None
                if vnfd.get("vnf-configuration"):
                    if vnfd.get("mgmt-interface"):
                        if vnfd["mgmt-interface"].get("vdu-id"):
                            vdu_needed_access.append(vnfd["mgmt-interface"]["vdu-id"])
                        elif vnfd["mgmt-interface"].get("cp"):
                            mgmt_cp = vnfd["mgmt-interface"]["cp"]

                for vdu in vnfd.get("vdu", ()):
                    if vdu.get("vdu-configuration"):
                        vdu_needed_access.append(vdu["id"])
                    elif mgmt_cp:
                        for vdu_interface in vdu.get("interface"):
                            if vdu_interface.get("external-connection-point-ref") and \
                                    vdu_interface["external-connection-point-ref"] == mgmt_cp:
                                vdu_needed_access.append(vdu["id"])
                                mgmt_cp = None
                                break

                if vdu_needed_access:
                    for vnf_member in nsd.get("constituent-vnfd"):
                        if vnf_member["vnfd-id-ref"] != vnfd_ref:
                            continue
                        for vdu in vdu_needed_access:
                            populate_dict(RO_ns_params,
                                          ("vnfs", vnf_member["member-vnf-index"], "vdus", vdu, "mgmt_keys"),
                                          n2vc_key_list)

        if ns_params.get("vduImage"):
            RO_ns_params["vduImage"] = ns_params["vduImage"]

        if ns_params.get("ssh_keys"):
            RO_ns_params["cloud-config"] = {"key-pairs": ns_params["ssh_keys"]}
        for vnf_params in get_iterable(ns_params, "vnf"):
            for constituent_vnfd in nsd["constituent-vnfd"]:
                if constituent_vnfd["member-vnf-index"] == vnf_params["member-vnf-index"]:
                    vnf_descriptor = vnfd_dict[constituent_vnfd["vnfd-id-ref"]]
                    break
            else:
                raise LcmException("Invalid instantiate parameter vnf:member-vnf-index={} is not present at nsd:"
                                   "constituent-vnfd".format(vnf_params["member-vnf-index"]))
            if vnf_params.get("vimAccountId"):
                populate_dict(RO_ns_params, ("vnfs", vnf_params["member-vnf-index"], "datacenter"),
                              vim_account_2_RO(vnf_params["vimAccountId"]))

            for vdu_params in get_iterable(vnf_params, "vdu"):
                # TODO feature 1417: check that this VDU exist and it is not a PDU
                if vdu_params.get("volume"):
                    for volume_params in vdu_params["volume"]:
                        if volume_params.get("vim-volume-id"):
                            populate_dict(RO_ns_params, ("vnfs", vnf_params["member-vnf-index"], "vdus",
                                                         vdu_params["id"], "devices", volume_params["name"], "vim_id"),
                                          volume_params["vim-volume-id"])
                if vdu_params.get("interface"):
                    for interface_params in vdu_params["interface"]:
                        if interface_params.get("ip-address"):
                            populate_dict(RO_ns_params, ("vnfs", vnf_params["member-vnf-index"], "vdus",
                                                         vdu_params["id"], "interfaces", interface_params["name"],
                                                         "ip_address"),
                                          interface_params["ip-address"])
                        if interface_params.get("mac-address"):
                            populate_dict(RO_ns_params, ("vnfs", vnf_params["member-vnf-index"], "vdus",
                                                         vdu_params["id"], "interfaces", interface_params["name"],
                                                         "mac_address"),
                                          interface_params["mac-address"])
                        if interface_params.get("floating-ip-required"):
                            populate_dict(RO_ns_params, ("vnfs", vnf_params["member-vnf-index"], "vdus",
                                                         vdu_params["id"], "interfaces", interface_params["name"],
                                                         "floating-ip"),
                                          interface_params["floating-ip-required"])

            for internal_vld_params in get_iterable(vnf_params, "internal-vld"):
                if internal_vld_params.get("vim-network-name"):
                    populate_dict(RO_ns_params, ("vnfs", vnf_params["member-vnf-index"], "networks",
                                                 internal_vld_params["name"], "vim-network-name"),
                                  internal_vld_params["vim-network-name"])
                if internal_vld_params.get("vim-network-id"):
                    populate_dict(RO_ns_params, ("vnfs", vnf_params["member-vnf-index"], "networks",
                                                 internal_vld_params["name"], "vim-network-id"),
                                  internal_vld_params["vim-network-id"])
                if internal_vld_params.get("ip-profile"):
                    populate_dict(RO_ns_params, ("vnfs", vnf_params["member-vnf-index"], "networks",
                                                 internal_vld_params["name"], "ip-profile"),
                                  ip_profile_2_RO(internal_vld_params["ip-profile"]))

                for icp_params in get_iterable(internal_vld_params, "internal-connection-point"):
                    # look for interface
                    iface_found = False
                    for vdu_descriptor in vnf_descriptor["vdu"]:
                        for vdu_interface in vdu_descriptor["interface"]:
                            if vdu_interface.get("internal-connection-point-ref") == icp_params["id-ref"]:
                                if icp_params.get("ip-address"):
                                    populate_dict(RO_ns_params, ("vnfs", vnf_params["member-vnf-index"], "vdus",
                                                                 vdu_descriptor["id"], "interfaces",
                                                                 vdu_interface["name"], "ip_address"),
                                                  icp_params["ip-address"])

                                if icp_params.get("mac-address"):
                                    populate_dict(RO_ns_params, ("vnfs", vnf_params["member-vnf-index"], "vdus",
                                                                 vdu_descriptor["id"], "interfaces",
                                                                 vdu_interface["name"], "mac_address"),
                                                  icp_params["mac-address"])
                                iface_found = True
                                break
                        if iface_found:
                            break
                    else:
                        raise LcmException("Invalid instantiate parameter vnf:member-vnf-index[{}]:"
                                           "internal-vld:id-ref={} is not present at vnfd:internal-"
                                           "connection-point".format(vnf_params["member-vnf-index"],
                                                                     icp_params["id-ref"]))

        for vld_params in get_iterable(ns_params, "vld"):
            if "ip-profile" in vld_params:
                populate_dict(RO_ns_params, ("networks", vld_params["name"], "ip-profile"),
                              ip_profile_2_RO(vld_params["ip-profile"]))

            if "wimAccountId" in vld_params and vld_params["wimAccountId"] is not None:
                populate_dict(RO_ns_params, ("networks", vld_params["name"], "wim_account"),
                              wim_account_2_RO(vld_params["wimAccountId"])),
            if vld_params.get("vim-network-name"):
                RO_vld_sites = []
                if isinstance(vld_params["vim-network-name"], dict):
                    for vim_account, vim_net in vld_params["vim-network-name"].items():
                        RO_vld_sites.append({
                            "netmap-use": vim_net,
                            "datacenter": vim_account_2_RO(vim_account)
                        })
                else:  # isinstance str
                    RO_vld_sites.append({"netmap-use": vld_params["vim-network-name"]})
                if RO_vld_sites:
                    populate_dict(RO_ns_params, ("networks", vld_params["name"], "sites"), RO_vld_sites)
            if vld_params.get("vim-network-id"):
                RO_vld_sites = []
                if isinstance(vld_params["vim-network-id"], dict):
                    for vim_account, vim_net in vld_params["vim-network-id"].items():
                        RO_vld_sites.append({
                            "netmap-use": vim_net,
                            "datacenter": vim_account_2_RO(vim_account)
                        })
                else:  # isinstance str
                    RO_vld_sites.append({"netmap-use": vld_params["vim-network-id"]})
                if RO_vld_sites:
                    populate_dict(RO_ns_params, ("networks", vld_params["name"], "sites"), RO_vld_sites)
            if vld_params.get("ns-net"):
                if isinstance(vld_params["ns-net"], dict):
                    for vld_id, instance_scenario_id in vld_params["ns-net"].items():
                        RO_vld_ns_net = {"instance_scenario_id": instance_scenario_id, "osm_id": vld_id}
                if RO_vld_ns_net:
                    populate_dict(RO_ns_params, ("networks", vld_params["name"], "use-network"), RO_vld_ns_net)            
            if "vnfd-connection-point-ref" in vld_params:
                for cp_params in vld_params["vnfd-connection-point-ref"]:
                    # look for interface
                    for constituent_vnfd in nsd["constituent-vnfd"]:
                        if constituent_vnfd["member-vnf-index"] == cp_params["member-vnf-index-ref"]:
                            vnf_descriptor = vnfd_dict[constituent_vnfd["vnfd-id-ref"]]
                            break
                    else:
                        raise LcmException(
                            "Invalid instantiate parameter vld:vnfd-connection-point-ref:member-vnf-index-ref={} "
                            "is not present at nsd:constituent-vnfd".format(cp_params["member-vnf-index-ref"]))
                    match_cp = False
                    for vdu_descriptor in vnf_descriptor["vdu"]:
                        for interface_descriptor in vdu_descriptor["interface"]:
                            if interface_descriptor.get("external-connection-point-ref") == \
                                    cp_params["vnfd-connection-point-ref"]:
                                match_cp = True
                                break
                        if match_cp:
                            break
                    else:
                        raise LcmException(
                            "Invalid instantiate parameter vld:vnfd-connection-point-ref:member-vnf-index-ref={}:"
                            "vnfd-connection-point-ref={} is not present at vnfd={}".format(
                                cp_params["member-vnf-index-ref"],
                                cp_params["vnfd-connection-point-ref"],
                                vnf_descriptor["id"]))
                    if cp_params.get("ip-address"):
                        populate_dict(RO_ns_params, ("vnfs", cp_params["member-vnf-index-ref"], "vdus",
                                                     vdu_descriptor["id"], "interfaces",
                                                     interface_descriptor["name"], "ip_address"),
                                      cp_params["ip-address"])
                    if cp_params.get("mac-address"):
                        populate_dict(RO_ns_params, ("vnfs", cp_params["member-vnf-index-ref"], "vdus",
                                                     vdu_descriptor["id"], "interfaces",
                                                     interface_descriptor["name"], "mac_address"),
                                      cp_params["mac-address"])
        return RO_ns_params

    def scale_vnfr(self, db_vnfr, vdu_create=None, vdu_delete=None):
        # make a copy to do not change
        vdu_create = copy(vdu_create)
        vdu_delete = copy(vdu_delete)

        vdurs = db_vnfr.get("vdur")
        if vdurs is None:
            vdurs = []
        vdu_index = len(vdurs)
        while vdu_index:
            vdu_index -= 1
            vdur = vdurs[vdu_index]
            if vdur.get("pdu-type"):
                continue
            vdu_id_ref = vdur["vdu-id-ref"]
            if vdu_create and vdu_create.get(vdu_id_ref):
                for index in range(0, vdu_create[vdu_id_ref]):
                    vdur = deepcopy(vdur)
                    vdur["_id"] = str(uuid4())
                    vdur["count-index"] += 1
                    vdurs.insert(vdu_index+1+index, vdur)
                del vdu_create[vdu_id_ref]
            if vdu_delete and vdu_delete.get(vdu_id_ref):
                del vdurs[vdu_index]
                vdu_delete[vdu_id_ref] -= 1
                if not vdu_delete[vdu_id_ref]:
                    del vdu_delete[vdu_id_ref]
        # check all operations are done
        if vdu_create or vdu_delete:
            raise LcmException("Error scaling OUT VNFR for {}. There is not any existing vnfr. Scaled to 0?".format(
                vdu_create))
        if vdu_delete:
            raise LcmException("Error scaling IN VNFR for {}. There is not any existing vnfr. Scaled to 0?".format(
                vdu_delete))

        vnfr_update = {"vdur": vdurs}
        db_vnfr["vdur"] = vdurs
        self.update_db_2("vnfrs", db_vnfr["_id"], vnfr_update)

    def ns_update_nsr(self, ns_update_nsr, db_nsr, nsr_desc_RO):
        """
        Updates database nsr with the RO info for the created vld
        :param ns_update_nsr: dictionary to be filled with the updated info
        :param db_nsr: content of db_nsr. This is also modified
        :param nsr_desc_RO: nsr descriptor from RO
        :return: Nothing, LcmException is raised on errors
        """

        for vld_index, vld in enumerate(get_iterable(db_nsr, "vld")):
            for net_RO in get_iterable(nsr_desc_RO, "nets"):
                if vld["id"] != net_RO.get("ns_net_osm_id"):
                    continue
                vld["vim-id"] = net_RO.get("vim_net_id")
                vld["name"] = net_RO.get("vim_name")
                vld["status"] = net_RO.get("status")
                vld["status-detailed"] = net_RO.get("error_msg")
                ns_update_nsr["vld.{}".format(vld_index)] = vld
                break
            else:
                raise LcmException("ns_update_nsr: Not found vld={} at RO info".format(vld["id"]))

    def ns_update_vnfr(self, db_vnfrs, nsr_desc_RO):
        """
        Updates database vnfr with the RO info, e.g. ip_address, vim_id... Descriptor db_vnfrs is also updated
        :param db_vnfrs: dictionary with member-vnf-index: vnfr-content
        :param nsr_desc_RO: nsr descriptor from RO
        :return: Nothing, LcmException is raised on errors
        """
        for vnf_index, db_vnfr in db_vnfrs.items():
            for vnf_RO in nsr_desc_RO["vnfs"]:
                if vnf_RO["member_vnf_index"] != vnf_index:
                    continue
                vnfr_update = {}
                if vnf_RO.get("ip_address"):
                    db_vnfr["ip-address"] = vnfr_update["ip-address"] = vnf_RO["ip_address"].split(";")[0]
                elif not db_vnfr.get("ip-address"):
                    raise LcmExceptionNoMgmtIP("ns member_vnf_index '{}' has no IP address".format(vnf_index))

                for vdu_index, vdur in enumerate(get_iterable(db_vnfr, "vdur")):
                    vdur_RO_count_index = 0
                    if vdur.get("pdu-type"):
                        continue
                    for vdur_RO in get_iterable(vnf_RO, "vms"):
                        if vdur["vdu-id-ref"] != vdur_RO["vdu_osm_id"]:
                            continue
                        if vdur["count-index"] != vdur_RO_count_index:
                            vdur_RO_count_index += 1
                            continue
                        vdur["vim-id"] = vdur_RO.get("vim_vm_id")
                        if vdur_RO.get("ip_address"):
                            vdur["ip-address"] = vdur_RO["ip_address"].split(";")[0]
                        else:
                            vdur["ip-address"] = None
                        vdur["vdu-id-ref"] = vdur_RO.get("vdu_osm_id")
                        vdur["name"] = vdur_RO.get("vim_name")
                        vdur["status"] = vdur_RO.get("status")
                        vdur["status-detailed"] = vdur_RO.get("error_msg")
                        for ifacer in get_iterable(vdur, "interfaces"):
                            for interface_RO in get_iterable(vdur_RO, "interfaces"):
                                if ifacer["name"] == interface_RO.get("internal_name"):
                                    ifacer["ip-address"] = interface_RO.get("ip_address")
                                    ifacer["mac-address"] = interface_RO.get("mac_address")
                                    break
                            else:
                                raise LcmException("ns_update_vnfr: Not found member_vnf_index={} vdur={} interface={} "
                                                   "at RO info".format(vnf_index, vdur["vdu-id-ref"], ifacer["name"]))
                        vnfr_update["vdur.{}".format(vdu_index)] = vdur
                        break
                    else:
                        raise LcmException("ns_update_vnfr: Not found member_vnf_index={} vdur={} count_index={} at "
                                           "RO info".format(vnf_index, vdur["vdu-id-ref"], vdur["count-index"]))

                for vld_index, vld in enumerate(get_iterable(db_vnfr, "vld")):
                    for net_RO in get_iterable(nsr_desc_RO, "nets"):
                        if vld["id"] != net_RO.get("vnf_net_osm_id"):
                            continue
                        vld["vim-id"] = net_RO.get("vim_net_id")
                        vld["name"] = net_RO.get("vim_name")
                        vld["status"] = net_RO.get("status")
                        vld["status-detailed"] = net_RO.get("error_msg")
                        vnfr_update["vld.{}".format(vld_index)] = vld
                        break
                    else:
                        raise LcmException("ns_update_vnfr: Not found member_vnf_index={} vld={} at RO info".format(
                            vnf_index, vld["id"]))

                self.update_db_2("vnfrs", db_vnfr["_id"], vnfr_update)
                break

            else:
                raise LcmException("ns_update_vnfr: Not found member_vnf_index={} at RO info".format(vnf_index))

    async def do_placement(self, nslcmop,  nsr, vnfrs):
        operationParams = nslcmop.get('operationParams', {})
        additionalNsParams = operationParams.get('additionalParamsForNs', {})
        placementEngine = additionalNsParams.get('placementEngine', None)
        if placementEngine == "LCM_PLA":
            self.logger.debug("Use LCM_PLA...");
            pla_suggestions = { 'received' : False, 'suggestions' : [] }
            def pla_callback(topic, command, data):
                pla_suggestions['suggestions'] = data['suggestions']
                pla_suggestions['received'] = True

            self.lcm.kafka_subscribe("pla", "suggestions", pla_callback)
            await self.msg.aiowrite("pla", "get_suggestions", { 'nsParams': nsr['instantiate_params'] }, loop=self.loop)
            wait = 15
            while not pla_suggestions['received']:
                await asyncio.sleep(1)
                wait -= 1
                if wait <= 0:
                    self.logger.debug("Placement timeout!")
                    return
            self.lcm.kafka_unsubscribe("pla", "suggestions")
            self.logger.debug("Placement suggestions received: {}".format(json.dumps(pla_suggestions['suggestions'])))
            # use first suggestion
            nsr_update = {'instantiate_params' : nsr['instantiate_params'] }
            nsr_update['instantiate_params'].update(pla_suggestions['suggestions'][0])
            nsr.update(nsr_update)
            self.update_db_2("nsrs", nsr["_id"], nsr_update)
            operationParams.update(pla_suggestions['suggestions'][0])
            nslcmop_update = { 'operationParams' : operationParams }
            nslcmop.update(nslcmop_update)
            self.update_db_2("nslcmops", nslcmop['_id'], nslcmop_update)
            for vnfr in vnfrs:
                for vnf in nsr['instantiate_params']['vnf']:
                    if vnfr['member-vnf-index-ref'] != vnf['member-vnf-index']:
                        continue
                    if vnfr['vim-account-id'] != vnf['vimAccountId']:
                        self.logger.debug("Updating VIM account for VNF {} to {}".format(vnf['member-vnf-index'], vnf['vimAccountId']))
                        vnfr_update = { 'vim-account-id' : vnf['vimAccountId'] }
                        vnfr.update(vnfr_update)
                        self.update_db_2("vnfrs", vnfr["_id"], vnfr_update)
        return
            
    async def instantiate(self, nsr_id, nslcmop_id):

        # Try to lock HA task here
        task_is_locked_by_me = self.lcm_tasks.lock_HA('ns', 'nslcmops', nslcmop_id)
        if not task_is_locked_by_me:
            return

        logging_text = "Task ns={} instantiate={} ".format(nsr_id, nslcmop_id)
        self.logger.debug(logging_text + "Enter")
        # get all needed from database
        start_deploy = time()
        db_nsr = None
        db_nslcmop = None
        db_nsr_update = {"_admin.nslcmop": nslcmop_id}
        db_nslcmop_update = {}
        nslcmop_operation_state = None
        db_vnfrs = {}
        RO_descriptor_number = 0   # number of descriptors created at RO
        vnf_index_2_RO_id = {}    # map between vnfd/nsd id to the id used at RO
        n2vc_info = {}
        n2vc_key_list = []  # list of public keys to be injected as authorized to VMs
        exc = None
        try:
            # wait for any previous tasks in process
            await self.lcm_tasks.waitfor_related_HA('ns', 'nslcmops', nslcmop_id)

            step = "Getting nslcmop={} from db".format(nslcmop_id)
            db_nslcmop = self.db.get_one("nslcmops", {"_id": nslcmop_id})
            step = "Getting nsr={} from db".format(nsr_id)
            db_nsr = self.db.get_one("nsrs", {"_id": nsr_id})
            ns_params = db_nslcmop.get("operationParams")
            nsd = db_nsr["nsd"]
            nsr_name = db_nsr["name"]   # TODO short-name??

            self.logger.debug("Instantiate:")
            self.logger.debug("db_nslcmop = {}".format(json.dumps(db_nslcmop)))
            self.logger.debug("nsr = {}".format(json.dumps(db_nsr)))
            
            step = "Getting vnfrs from db"
            db_vnfrs_list = self.db.get_list("vnfrs", {"nsr-id-ref": nsr_id})
            db_vnfds_ref = {}
            db_vnfds = {}
            db_vnfds_index = {}
            for vnfr in db_vnfrs_list:
                self.logger.debug("vnfr = {}".format(json.dumps(vnfr)))
                db_vnfrs[vnfr["member-vnf-index-ref"]] = vnfr
                vnfd_id = vnfr["vnfd-id"]
                vnfd_ref = vnfr["vnfd-ref"]
                if vnfd_id not in db_vnfds:
                    step = "Getting vnfd={} id='{}' from db".format(vnfd_id, vnfd_ref)
                    vnfd = self.db.get_one("vnfds", {"_id": vnfd_id})
                    db_vnfds_ref[vnfd_ref] = vnfd
                    db_vnfds[vnfd_id] = vnfd
                db_vnfds_index[vnfr["member-vnf-index-ref"]] = db_vnfds[vnfd_id]

            await self.do_placement(db_nslcmop, db_nsr, db_vnfrs_list)
            ns_params = db_nslcmop.get("operationParams")
            self.logger.debug("Placement done:")
            self.logger.debug("db_nslcmop = {}".format(json.dumps(db_nslcmop)))
            self.logger.debug("nsr = {}".format(json.dumps(db_nsr)))
            for vnfr in db_vnfrs_list:
                self.logger.debug("vnfr = {}".format(json.dumps(vnfr)))

            # Get or generates the _admin.deployed,VCA list
            vca_deployed_list = None
            vca_model_name = None
            if db_nsr["_admin"].get("deployed"):
                vca_deployed_list = db_nsr["_admin"]["deployed"].get("VCA")
                vca_model_name = db_nsr["_admin"]["deployed"].get("VCA-model-name")
            if vca_deployed_list is None:
                vca_deployed_list = []
                db_nsr_update["_admin.deployed.VCA"] = vca_deployed_list
                populate_dict(db_nsr, ("_admin", "deployed", "VCA"), vca_deployed_list)
            elif isinstance(vca_deployed_list, dict):
                # maintain backward compatibility. Change a dict to list at database
                vca_deployed_list = list(vca_deployed_list.values())
                db_nsr_update["_admin.deployed.VCA"] = vca_deployed_list
                populate_dict(db_nsr, ("_admin", "deployed", "VCA"), vca_deployed_list)

            db_nsr_update["detailed-status"] = "creating"
            db_nsr_update["operational-status"] = "init"
            if not db_nsr["_admin"].get("deployed") or not db_nsr["_admin"]["deployed"].get("RO") or \
                    not db_nsr["_admin"]["deployed"]["RO"].get("vnfd"):
                populate_dict(db_nsr, ("_admin", "deployed", "RO", "vnfd"), [])
                db_nsr_update["_admin.deployed.RO.vnfd"] = []

            # set state to INSTANTIATED. When instantiated NBI will not delete directly
            db_nsr_update["_admin.nsState"] = "INSTANTIATED"
            self.update_db_2("nsrs", nsr_id, db_nsr_update)

            # Deploy charms
            # The parameters we'll need to deploy a charm
            number_to_configure = 0

            def deploy_charm(vnf_index, vdu_id, vdu_name, vdu_count_index, charm_params, n2vc_info, native_charm=False):
                """An inner function to deploy the charm from either ns, vnf or vdu
                For ns both vnf_index and vdu_id are None.
                For vnf only vdu_id is None
                For vdu both vnf_index and vdu_id contain a value
                """
                # if not charm_params.get("rw_mgmt_ip") and vnf_index:  # if NS skip mgmt_ip checking
                #     raise LcmException("ns/vnfd/vdu has not management ip address to configure it")

                machine_spec = {}
                if native_charm:
                    machine_spec["username"] = charm_params.get("username"),
                    machine_spec["hostname"] = charm_params.get("rw_mgmt_ip")

                # Note: The charm needs to exist on disk at the location
                # specified by charm_path.
                descriptor = vnfd if vnf_index else nsd
                base_folder = descriptor["_admin"]["storage"]
                storage_params = self.fs.get_params()
                charm_path = "{}{}/{}/charms/{}".format(
                    storage_params["path"],
                    base_folder["folder"],
                    base_folder["pkg-dir"],
                    proxy_charm
                )

                # ns_name will be ignored in the current version of N2VC
                # but will be implemented for the next point release.
                model_name = nsr_id
                vdu_id_text = (str(vdu_id) if vdu_id else "") + "-"
                vnf_index_text = (str(vnf_index) if vnf_index else "") + "-"
                application_name = self.n2vc.FormatApplicationName(nsr_name, vnf_index_text, vdu_id_text)

                vca_index = len(vca_deployed_list)
                # trunk name and add two char index at the end to ensure that it is unique. It is assumed no more than
                # 26*26 charm in the same NS
                application_name = application_name[0:48]
                application_name += chr(97 + vca_index // 26) + chr(97 + vca_index % 26)
                vca_deployed_ = {
                    "member-vnf-index": vnf_index,
                    "vdu_id": vdu_id,
                    "model": model_name,
                    "application": application_name,
                    "operational-status": "init",
                    "detailed-status": "",
                    "step": "initial-deploy",
                    "vnfd_id": vnfd_id,
                    "vdu_name": vdu_name,
                    "vdu_count_index": vdu_count_index,
                }
                vca_deployed_list.append(vca_deployed_)
                db_nsr_update["_admin.deployed.VCA.{}".format(vca_index)] = vca_deployed_
                self.update_db_2("nsrs", nsr_id, db_nsr_update)

                self.logger.debug("Task create_ns={} Passing artifacts path '{}' for {}".format(nsr_id, charm_path,
                                                                                                proxy_charm))
                if not n2vc_info:
                    n2vc_info["nsr_id"] = nsr_id
                    n2vc_info["nslcmop_id"] = nslcmop_id
                    n2vc_info["n2vc_event"] = asyncio.Event(loop=self.loop)
                    n2vc_info["lcmOperationType"] = "instantiate"
                    n2vc_info["deployed"] = vca_deployed_list
                    n2vc_info["db_update"] = db_nsr_update
                task = asyncio.ensure_future(
                    self.n2vc.DeployCharms(
                        model_name,          # The network service name
                        application_name,    # The application name
                        descriptor,          # The vnf/nsd descriptor
                        charm_path,          # Path to charm
                        charm_params,        # Runtime params, like mgmt ip
                        machine_spec,        # for native charms only
                        self.n2vc_callback,  # Callback for status changes
                        n2vc_info,           # Callback parameter
                        None,                # Callback parameter (task)
                    )
                )
                task.add_done_callback(functools.partial(self.n2vc_callback, model_name, application_name, None, None,
                                                         n2vc_info))
                self.lcm_tasks.register("ns", nsr_id, nslcmop_id, "create_charm:" + application_name, task)

            step = "Looking for needed vnfd to configure with proxy charm"
            self.logger.debug(logging_text + step)

            for c_vnf in get_iterable(nsd, "constituent-vnfd"):
                vnfd_id = c_vnf["vnfd-id-ref"]
                vnf_index = str(c_vnf["member-vnf-index"])
                vnfd = db_vnfds_ref[vnfd_id]

                # Get additional parameters
                vnfr_params = {}
                if db_vnfrs[vnf_index].get("additionalParamsForVnf"):
                    vnfr_params = db_vnfrs[vnf_index]["additionalParamsForVnf"].copy()
                for k, v in vnfr_params.items():
                    if isinstance(v, str) and v.startswith("!!yaml "):
                        vnfr_params[k] = yaml.safe_load(v[7:])

                step = "deploying proxy charms for configuration"
                # Check if this VNF has a charm configuration
                vnf_config = vnfd.get("vnf-configuration")
                if vnf_config and vnf_config.get("juju"):
                    proxy_charm = vnf_config["juju"]["charm"]
                    if vnf_config["juju"].get("proxy") is False:
                        # native_charm, will be deployed after VM. Skip
                        proxy_charm = None

                    if proxy_charm:
                        if not vca_model_name:
                            step = "creating VCA model name '{}'".format(nsr_id)
                            self.logger.debug(logging_text + step)
                            await self.n2vc.CreateNetworkService(nsr_id)
                            vca_model_name = nsr_id
                            db_nsr_update["_admin.deployed.VCA-model-name"] = nsr_id
                            self.update_db_2("nsrs", nsr_id, db_nsr_update)
                        step = "deploying proxy charm to configure vnf {}".format(vnf_index)
                        vnfr_params["rw_mgmt_ip"] = db_vnfrs[vnf_index]["ip-address"]
                        charm_params = {
                            "user_values": vnfr_params,
                            "rw_mgmt_ip": db_vnfrs[vnf_index]["ip-address"],
                            "initial-config-primitive": {}    # vnf_config.get('initial-config-primitive') or {}
                        }

                        # Login to the VCA. If there are multiple calls to login(),
                        # subsequent calls will be a nop and return immediately.
                        await self.n2vc.login()

                        deploy_charm(vnf_index, None, None, None, charm_params, n2vc_info)
                        number_to_configure += 1

                # Deploy charms for each VDU that supports one.
                for vdu_index, vdu in enumerate(get_iterable(vnfd, 'vdu')):
                    vdu_config = vdu.get('vdu-configuration')
                    proxy_charm = None

                    if vdu_config and vdu_config.get("juju"):
                        proxy_charm = vdu_config["juju"]["charm"]
                        if vdu_config["juju"].get("proxy") is False:
                            # native_charm, will be deployed after VM. Skip
                            proxy_charm = None

                        if proxy_charm:
                            if not vca_model_name:
                                step = "creating VCA model name"
                                await self.n2vc.CreateNetworkService(nsr_id)
                                vca_model_name = nsr_id
                                db_nsr_update["_admin.deployed.VCA-model-name"] = nsr_id
                                self.update_db_2("nsrs", nsr_id, db_nsr_update)
                            step = "deploying proxy charm to configure member_vnf_index={} vdu={}".format(vnf_index,
                                                                                                          vdu["id"])
                            await self.n2vc.login()
                            vdur = db_vnfrs[vnf_index]["vdur"][vdu_index]
                            # TODO for the moment only first vdu_id contains a charm deployed
                            if vdur["vdu-id-ref"] != vdu["id"]:
                                raise LcmException("Mismatch vdur {}, vdu {} at index {} for member_vnf_index={}"
                                                   .format(vdur["vdu-id-ref"], vdu["id"], vdu_index, vnf_index))
                            vnfr_params["rw_mgmt_ip"] = vdur["ip-address"]
                            charm_params = {
                                "user_values": vnfr_params,
                                "rw_mgmt_ip": vdur["ip-address"],
                                "initial-config-primitive": {}  # vdu_config.get('initial-config-primitive') or {}
                            }
                            deploy_charm(vnf_index, vdu["id"], vdur.get("name"), vdur["count-index"],
                                         charm_params, n2vc_info)
                            number_to_configure += 1

            # Check if this NS has a charm configuration

            ns_config = nsd.get("ns-configuration")
            if ns_config and ns_config.get("juju"):
                proxy_charm = ns_config["juju"]["charm"]
                if ns_config["juju"].get("proxy") is False:
                    # native_charm, will be deployed after VM. Skip
                    proxy_charm = None

                if proxy_charm:
                    step = "deploying proxy charm to configure ns"
                    # TODO is NS magmt IP address needed?

                    # Get additional parameters
                    additional_params = {}
                    if db_nsr.get("additionalParamsForNs"):
                        additional_params = db_nsr["additionalParamsForNs"].copy()
                    for k, v in additional_params.items():
                        if isinstance(v, str) and v.startswith("!!yaml "):
                            additional_params[k] = yaml.safe_load(v[7:])

                    # additional_params["rw_mgmt_ip"] = db_nsr["ip-address"]
                    charm_params = {
                        "user_values": additional_params,
                        # "rw_mgmt_ip": db_nsr["ip-address"],
                        "initial-config-primitive": {}   # ns_config.get('initial-config-primitive') or {}
                    }

                    # Login to the VCA. If there are multiple calls to login(),
                    # subsequent calls will be a nop and return immediately.
                    await self.n2vc.login()
                    deploy_charm(None, None, None, None, charm_params, n2vc_info)
                    number_to_configure += 1

            db_nsr_update["operational-status"] = "running"

            # Wait until all charms has reached blocked or active status
            step = "waiting proxy charms to be ready"
            if number_to_configure:
                # wait until all charms are configured.
                # steps are:
                #       initial-deploy
                #       get-ssh-public-key
                #       generate-ssh-key
                #       retry-get-ssh-public-key
                #       ssh-public-key-obtained
                while time() <= start_deploy + self.total_deploy_timeout:
                    if db_nsr_update:
                        self.update_db_2("nsrs", nsr_id, db_nsr_update)
                    if db_nslcmop_update:
                        self.update_db_2("nslcmops", nslcmop_id, db_nslcmop_update)

                    all_active = True
                    for vca_index, vca_deployed in enumerate(vca_deployed_list):
                        database_entry = "_admin.deployed.VCA.{}.".format(vca_index)
                        if vca_deployed["step"] == "initial-deploy":
                            if vca_deployed["operational-status"] in ("active", "blocked"):
                                step = "execute charm primitive get-ssh-public-key for member_vnf_index={} vdu_id={}" \
                                       .format(vca_deployed["member-vnf-index"],
                                               vca_deployed["vdu_id"])
                                self.logger.debug(logging_text + step)
                                try:
                                    primitive_id = await self.n2vc.ExecutePrimitive(
                                        vca_deployed["model"],
                                        vca_deployed["application"],
                                        "get-ssh-public-key",
                                        None,
                                    )
                                    vca_deployed["step"] = db_nsr_update[database_entry + "step"] = "get-ssh-public-key"
                                    vca_deployed["primitive_id"] = db_nsr_update[database_entry + "primitive_id"] =\
                                        primitive_id
                                    db_nsr_update[database_entry + "operational-status"] =\
                                        vca_deployed["operational-status"]
                                except PrimitiveDoesNotExist:
                                    ssh_public_key = None
                                    vca_deployed["step"] = db_nsr_update[database_entry + "step"] =\
                                        "ssh-public-key-obtained"
                                    vca_deployed["ssh-public-key"] = db_nsr_update[database_entry + "ssh-public-key"] =\
                                        ssh_public_key
                                    step = "charm ssh-public-key for  member_vnf_index={} vdu_id={} not needed".format(
                                        vca_deployed["member-vnf-index"], vca_deployed["vdu_id"])
                                    self.logger.debug(logging_text + step)

                        elif vca_deployed["step"] in ("get-ssh-public-key", "retry-get-ssh-public-key"):
                            primitive_id = vca_deployed["primitive_id"]
                            primitive_status = await self.n2vc.GetPrimitiveStatus(vca_deployed["model"],
                                                                                  primitive_id)
                            if primitive_status in ("completed", "failed"):
                                primitive_result = await self.n2vc.GetPrimitiveOutput(vca_deployed["model"],
                                                                                      primitive_id)
                                vca_deployed["primitive_id"] = db_nsr_update[database_entry + "primitive_id"] = None
                                if primitive_status == "completed" and isinstance(primitive_result, dict) and \
                                        primitive_result.get("pubkey"):
                                    ssh_public_key = primitive_result.get("pubkey")
                                    vca_deployed["step"] = db_nsr_update[database_entry + "step"] =\
                                        "ssh-public-key-obtained"
                                    vca_deployed["ssh-public-key"] = db_nsr_update[database_entry + "ssh-public-key"] =\
                                        ssh_public_key
                                    n2vc_key_list.append(ssh_public_key)
                                    step = "charm ssh-public-key for  member_vnf_index={} vdu_id={} is '{}'".format(
                                        vca_deployed["member-vnf-index"], vca_deployed["vdu_id"], ssh_public_key)
                                    self.logger.debug(logging_text + step)
                                else:  # primitive_status == "failed":
                                    if vca_deployed["step"] == "get-ssh-public-key":
                                        step = "execute charm primitive generate-ssh-public-key for member_vnf_index="\
                                               "{} vdu_id={}".format(vca_deployed["member-vnf-index"],
                                                                     vca_deployed["vdu_id"])
                                        self.logger.debug(logging_text + step)
                                        vca_deployed["step"] = db_nsr_update[database_entry + "step"] =\
                                            "generate-ssh-key"
                                        primitive_id = await self.n2vc.ExecutePrimitive(
                                            vca_deployed["model"],
                                            vca_deployed["application"],
                                            "generate-ssh-key",
                                            None,
                                        )
                                        vca_deployed["primitive_id"] = db_nsr_update[database_entry + "primitive_id"] =\
                                            primitive_id
                                    else:  # failed for second time
                                        raise LcmException(
                                            "error executing primitive get-ssh-public-key: {}".format(primitive_result))

                        elif vca_deployed["step"] == "generate-ssh-key":
                            primitive_id = vca_deployed["primitive_id"]
                            primitive_status = await self.n2vc.GetPrimitiveStatus(vca_deployed["model"],
                                                                                  primitive_id)
                            if primitive_status in ("completed", "failed"):
                                primitive_result = await self.n2vc.GetPrimitiveOutput(vca_deployed["model"],
                                                                                      primitive_id)
                                vca_deployed["primitive_id"] = db_nsr_update[
                                    database_entry + "primitive_id"] = None
                                if primitive_status == "completed":
                                    step = "execute primitive get-ssh-public-key again for member_vnf_index={} "\
                                           "vdu_id={}".format(vca_deployed["member-vnf-index"],
                                                              vca_deployed["vdu_id"])
                                    self.logger.debug(logging_text + step)
                                    vca_deployed["step"] = db_nsr_update[database_entry + "step"] = \
                                        "retry-get-ssh-public-key"
                                    primitive_id = await self.n2vc.ExecutePrimitive(
                                        vca_deployed["model"],
                                        vca_deployed["application"],
                                        "get-ssh-public-key",
                                        None,
                                    )
                                    vca_deployed["primitive_id"] = db_nsr_update[database_entry + "primitive_id"] =\
                                        primitive_id

                                else:  # primitive_status == "failed":
                                    raise LcmException("error executing primitive  generate-ssh-key: {}"
                                                       .format(primitive_result))

                        if vca_deployed["step"] != "ssh-public-key-obtained":
                            all_active = False

                    if all_active:
                        break
                    await asyncio.sleep(5)
                else:   # total_deploy_timeout
                    raise LcmException("Timeout waiting charm to be initialized for member_vnf_index={} vdu_id={}"
                                       .format(vca_deployed["member-vnf-index"], vca_deployed["vdu_id"]))

            # deploy RO
            RO = ROclient.ROClient(self.loop, **self.ro_config)
            # get vnfds, instantiate at RO
            for c_vnf in nsd.get("constituent-vnfd", ()):
                member_vnf_index = c_vnf["member-vnf-index"]
                vnfd = db_vnfds_ref[c_vnf['vnfd-id-ref']]
                vnfd_ref = vnfd["id"]
                step = db_nsr_update["detailed-status"] = "Creating vnfd='{}' member_vnf_index='{}' at RO".format(
                    vnfd_ref, member_vnf_index)
                # self.logger.debug(logging_text + step)
                vnfd_id_RO = "{}.{}.{}".format(nsr_id, RO_descriptor_number, member_vnf_index[:23])
                vnf_index_2_RO_id[member_vnf_index] = vnfd_id_RO
                RO_descriptor_number += 1

                # look position at deployed.RO.vnfd if not present it will be appended at the end
                for index, vnf_deployed in enumerate(db_nsr["_admin"]["deployed"]["RO"]["vnfd"]):
                    if vnf_deployed["member-vnf-index"] == member_vnf_index:
                        break
                else:
                    index = len(db_nsr["_admin"]["deployed"]["RO"]["vnfd"])
                    db_nsr["_admin"]["deployed"]["RO"]["vnfd"].append(None)

                # look if present
                RO_update = {"member-vnf-index": member_vnf_index}
                vnfd_list = await RO.get_list("vnfd", filter_by={"osm_id": vnfd_id_RO})
                if vnfd_list:
                    RO_update["id"] = vnfd_list[0]["uuid"]
                    self.logger.debug(logging_text + "vnfd='{}'  member_vnf_index='{}' exists at RO. Using RO_id={}".
                                      format(vnfd_ref, member_vnf_index, vnfd_list[0]["uuid"]))
                else:
                    vnfd_RO = self.vnfd2RO(vnfd, vnfd_id_RO, db_vnfrs[c_vnf["member-vnf-index"]].
                                           get("additionalParamsForVnf"), nsr_id)
                    desc = await RO.create("vnfd", descriptor=vnfd_RO)
                    RO_update["id"] = desc["uuid"]
                    self.logger.debug(logging_text + "vnfd='{}' member_vnf_index='{}' created at RO. RO_id={}".format(
                        vnfd_ref, member_vnf_index, desc["uuid"]))
                db_nsr_update["_admin.deployed.RO.vnfd.{}".format(index)] = RO_update
                db_nsr["_admin"]["deployed"]["RO"]["vnfd"][index] = RO_update
                self.update_db_2("nsrs", nsr_id, db_nsr_update)

            # create nsd at RO
            nsd_ref = nsd["id"]
            step = db_nsr_update["detailed-status"] = "Creating nsd={} at RO".format(nsd_ref)
            # self.logger.debug(logging_text + step)

            RO_osm_nsd_id = "{}.{}.{}".format(nsr_id, RO_descriptor_number, nsd_ref[:23])
            RO_descriptor_number += 1
            nsd_list = await RO.get_list("nsd", filter_by={"osm_id": RO_osm_nsd_id})
            if nsd_list:
                db_nsr_update["_admin.deployed.RO.nsd_id"] = RO_nsd_uuid = nsd_list[0]["uuid"]
                self.logger.debug(logging_text + "nsd={} exists at RO. Using RO_id={}".format(
                    nsd_ref, RO_nsd_uuid))
            else:
                nsd_RO = deepcopy(nsd)
                nsd_RO["id"] = RO_osm_nsd_id
                nsd_RO.pop("_id", None)
                nsd_RO.pop("_admin", None)
                for c_vnf in nsd_RO.get("constituent-vnfd", ()):
                    member_vnf_index = c_vnf["member-vnf-index"]
                    c_vnf["vnfd-id-ref"] = vnf_index_2_RO_id[member_vnf_index]
                for c_vld in nsd_RO.get("vld", ()):
                    for cp in c_vld.get("vnfd-connection-point-ref", ()):
                        member_vnf_index = cp["member-vnf-index-ref"]
                        cp["vnfd-id-ref"] = vnf_index_2_RO_id[member_vnf_index]

                desc = await RO.create("nsd", descriptor=nsd_RO)
                db_nsr_update["_admin.nsState"] = "INSTANTIATED"
                db_nsr_update["_admin.deployed.RO.nsd_id"] = RO_nsd_uuid = desc["uuid"]
                self.logger.debug(logging_text + "nsd={} created at RO. RO_id={}".format(nsd_ref, RO_nsd_uuid))
            self.update_db_2("nsrs", nsr_id, db_nsr_update)

            # Crate ns at RO
            # if present use it unless in error status
            RO_nsr_id = db_nsr["_admin"].get("deployed", {}).get("RO", {}).get("nsr_id")
            if RO_nsr_id:
                try:
                    step = db_nsr_update["detailed-status"] = "Looking for existing ns at RO"
                    # self.logger.debug(logging_text + step + " RO_ns_id={}".format(RO_nsr_id))
                    desc = await RO.show("ns", RO_nsr_id)
                except ROclient.ROClientException as e:
                    if e.http_code != HTTPStatus.NOT_FOUND:
                        raise
                    RO_nsr_id = db_nsr_update["_admin.deployed.RO.nsr_id"] = None
                if RO_nsr_id:
                    ns_status, ns_status_info = RO.check_ns_status(desc)
                    db_nsr_update["_admin.deployed.RO.nsr_status"] = ns_status
                    if ns_status == "ERROR":
                        step = db_nsr_update["detailed-status"] = "Deleting ns at RO. RO_ns_id={}".format(RO_nsr_id)
                        self.logger.debug(logging_text + step)
                        await RO.delete("ns", RO_nsr_id)
                        RO_nsr_id = db_nsr_update["_admin.deployed.RO.nsr_id"] = None
            if not RO_nsr_id:
                step = db_nsr_update["detailed-status"] = "Checking dependencies"
                # self.logger.debug(logging_text + step)

                # check if VIM is creating and wait  look if previous tasks in process
                task_name, task_dependency = self.lcm_tasks.lookfor_related("vim_account", ns_params["vimAccountId"])
                if task_dependency:
                    step = "Waiting for related tasks to be completed: {}".format(task_name)
                    self.logger.debug(logging_text + step)
                    await asyncio.wait(task_dependency, timeout=3600)
                if ns_params.get("vnf"):
                    for vnf in ns_params["vnf"]:
                        if "vimAccountId" in vnf:
                            task_name, task_dependency = self.lcm_tasks.lookfor_related("vim_account",
                                                                                        vnf["vimAccountId"])
                        if task_dependency:
                            step = "Waiting for related tasks to be completed: {}".format(task_name)
                            self.logger.debug(logging_text + step)
                            await asyncio.wait(task_dependency, timeout=3600)

                step = db_nsr_update["detailed-status"] = "Checking instantiation parameters"

                # feature 1429. Add n2vc public key to needed VMs
                n2vc_key = await self.n2vc.GetPublicKey()
                n2vc_key_list.append(n2vc_key)
                RO_ns_params = self.ns_params_2_RO(ns_params, nsd, db_vnfds_ref, n2vc_key_list)

                step = db_nsr_update["detailed-status"] = "Creating ns at RO"
                desc = await RO.create("ns", descriptor=RO_ns_params,
                                       name=db_nsr["name"],
                                       scenario=RO_nsd_uuid)
                RO_nsr_id = db_nsr_update["_admin.deployed.RO.nsr_id"] = desc["uuid"]
                db_nsr_update["_admin.nsState"] = "INSTANTIATED"
                db_nsr_update["_admin.deployed.RO.nsr_status"] = "BUILD"
                self.logger.debug(logging_text + "ns created at RO. RO_id={}".format(desc["uuid"]))
            self.update_db_2("nsrs", nsr_id, db_nsr_update)

            # wait until NS is ready
            step = ns_status_detailed = detailed_status = "Waiting ns ready at RO. RO_id={}".format(RO_nsr_id)
            detailed_status_old = None
            self.logger.debug(logging_text + step)

            while time() <= start_deploy + self.total_deploy_timeout:
                desc = await RO.show("ns", RO_nsr_id)
                ns_status, ns_status_info = RO.check_ns_status(desc)
                db_nsr_update["_admin.deployed.RO.nsr_status"] = ns_status
                if ns_status == "ERROR":
                    raise ROclient.ROClientException(ns_status_info)
                elif ns_status == "BUILD":
                    detailed_status = ns_status_detailed + "; {}".format(ns_status_info)
                elif ns_status == "ACTIVE":
                    step = detailed_status = "Waiting for management IP address reported by the VIM. Updating VNFRs"
                    try:
                        self.ns_update_vnfr(db_vnfrs, desc)
                        break
                    except LcmExceptionNoMgmtIP:
                        pass
                else:
                    assert False, "ROclient.check_ns_status returns unknown {}".format(ns_status)
                if detailed_status != detailed_status_old:
                    detailed_status_old = db_nsr_update["detailed-status"] = detailed_status
                    self.update_db_2("nsrs", nsr_id, db_nsr_update)
                await asyncio.sleep(5, loop=self.loop)
            else:   # total_deploy_timeout
                raise ROclient.ROClientException("Timeout waiting ns to be ready")

            step = "Updating NSR"
            self.ns_update_nsr(db_nsr_update, db_nsr, desc)

            db_nsr_update["operational-status"] = "running"
            db_nsr["detailed-status"] = "Configuring vnfr"
            self.update_db_2("nsrs", nsr_id, db_nsr_update)

            # Configure proxy charms once VMs are up
            for vca_index, vca_deployed in enumerate(vca_deployed_list):
                vnf_index = vca_deployed.get("member-vnf-index")
                vdu_id = vca_deployed.get("vdu_id")
                vdu_name = None
                vdu_count_index = None

                step = "executing proxy charm initial primitives for member_vnf_index={} vdu_id={}".format(vnf_index,
                                                                                                           vdu_id)
                add_params = {}
                initial_config_primitive_list = []
                if vnf_index:
                    if db_vnfrs[vnf_index].get("additionalParamsForVnf"):
                        add_params = db_vnfrs[vnf_index]["additionalParamsForVnf"].copy()
                    vnfd = db_vnfds_index[vnf_index]

                    if vdu_id:
                        for vdu_index, vdu in enumerate(get_iterable(vnfd, 'vdu')):
                            if vdu["id"] == vdu_id:
                                initial_config_primitive_list = vdu['vdu-configuration'].get(
                                    'initial-config-primitive', [])
                                break
                        else:
                            raise LcmException("Not found vdu_id={} at vnfd:vdu".format(vdu_id))
                        vdur = db_vnfrs[vnf_index]["vdur"][vdu_index]
                        # TODO for the moment only first vdu_id contains a charm deployed
                        if vdur["vdu-id-ref"] != vdu["id"]:
                            raise LcmException("Mismatch vdur {}, vdu {} at index {} for vnf {}"
                                               .format(vdur["vdu-id-ref"], vdu["id"], vdu_index, vnf_index))
                        add_params["rw_mgmt_ip"] = vdur["ip-address"]
                    else:
                        add_params["rw_mgmt_ip"] = db_vnfrs[vnf_index]["ip-address"]
                        initial_config_primitive_list = vnfd["vnf-configuration"].get('initial-config-primitive', [])
                else:
                    if db_nsr.get("additionalParamsForNs"):
                        add_params = db_nsr["additionalParamsForNs"].copy()
                    for k, v in add_params.items():
                        if isinstance(v, str) and v.startswith("!!yaml "):
                            add_params[k] = yaml.safe_load(v[7:])
                    add_params["rw_mgmt_ip"] = None
                    initial_config_primitive_list = nsd["ns-configuration"].get('initial-config-primitive', [])

                # add primitive verify-ssh-credentials to the list after config only when is a vnf or vdu charm
                initial_config_primitive_list = initial_config_primitive_list.copy()
                if initial_config_primitive_list and vnf_index and vca_deployed.get("ssh-public-key"):
                    initial_config_primitive_list.insert(1, {"name": "verify-ssh-credentials", "parameter": []})

                for initial_config_primitive in initial_config_primitive_list:
                    primitive_params_ = self._map_primitive_params(initial_config_primitive, {}, add_params)
                    self.logger.debug(logging_text + step + " primitive '{}' params '{}'"
                                      .format(initial_config_primitive["name"], primitive_params_))
                    primitive_result, primitive_detail = await self._ns_execute_primitive(
                        db_nsr["_admin"]["deployed"], vnf_index, vdu_id, vdu_name, vdu_count_index,
                        initial_config_primitive["name"],
                        primitive_params_,
                        retries=10 if initial_config_primitive["name"] == "verify-ssh-credentials" else 0,
                        retries_interval=30)
                    if primitive_result != "COMPLETED":
                        raise LcmException("charm error executing primitive {} for member_vnf_index={} vdu_id={}: '{}'"
                                           .format(initial_config_primitive["name"], vca_deployed["member-vnf-index"],
                                                   vca_deployed["vdu_id"], primitive_detail))

            # Deploy native charms
            step = "Looking for needed vnfd to configure with native charm"
            self.logger.debug(logging_text + step)

            for c_vnf in get_iterable(nsd, "constituent-vnfd"):
                vnfd_id = c_vnf["vnfd-id-ref"]
                vnf_index = str(c_vnf["member-vnf-index"])
                vnfd = db_vnfds_ref[vnfd_id]

                # Get additional parameters
                vnfr_params = {}
                if db_vnfrs[vnf_index].get("additionalParamsForVnf"):
                    vnfr_params = db_vnfrs[vnf_index]["additionalParamsForVnf"].copy()
                for k, v in vnfr_params.items():
                    if isinstance(v, str) and v.startswith("!!yaml "):
                        vnfr_params[k] = yaml.safe_load(v[7:])

                # Check if this VNF has a charm configuration
                vnf_config = vnfd.get("vnf-configuration")
                if vnf_config and vnf_config.get("juju"):
                    native_charm = vnf_config["juju"].get("proxy") is False

                    if native_charm:
                        if not vca_model_name:
                            step = "creating VCA model name '{}'".format(nsr_id)
                            self.logger.debug(logging_text + step)
                            await self.n2vc.CreateNetworkService(nsr_id)
                            vca_model_name = nsr_id
                            db_nsr_update["_admin.deployed.VCA-model-name"] = nsr_id
                            self.update_db_2("nsrs", nsr_id, db_nsr_update)
                        step = "deploying native charm for vnf_member_index={}".format(vnf_index)
                        vnfr_params["rw_mgmt_ip"] = db_vnfrs[vnf_index]["ip-address"]
                        charm_params = {
                            "user_values": vnfr_params,
                            "rw_mgmt_ip": db_vnfrs[vnf_index]["ip-address"],
                            "initial-config-primitive": vnf_config.get('initial-config-primitive') or {},
                        }

                        # get username
                        # TODO remove this when changes on IM regarding config-access:ssh-access:default-user were
                        #  merged. Meanwhile let's get username from initial-config-primitive
                        if vnf_config.get("initial-config-primitive"):
                            for param in vnf_config["initial-config-primitive"][0].get("parameter", ()):
                                if param["name"] == "ssh-username":
                                    charm_params["username"] = param["value"]
                        if vnf_config.get("config-access") and vnf_config["config-access"].get("ssh-access"):
                            if vnf_config["config-access"]["ssh-access"].get("required"):
                                charm_params["username"] = vnf_config["config-access"]["ssh-access"].get("default-user")

                        # Login to the VCA. If there are multiple calls to login(),
                        # subsequent calls will be a nop and return immediately.
                        await self.n2vc.login()

                        deploy_charm(vnf_index, None, None, None, charm_params, n2vc_info, native_charm)
                        number_to_configure += 1

                # Deploy charms for each VDU that supports one.
                for vdu_index, vdu in enumerate(get_iterable(vnfd, 'vdu')):
                    vdu_config = vdu.get('vdu-configuration')
                    native_charm = False

                    if vdu_config and vdu_config.get("juju"):
                        native_charm = vdu_config["juju"].get("proxy") is False

                        if native_charm:
                            if not vca_model_name:
                                step = "creating VCA model name"
                                await self.n2vc.CreateNetworkService(nsr_id)
                                vca_model_name = nsr_id
                                db_nsr_update["_admin.deployed.VCA-model-name"] = nsr_id
                                self.update_db_2("nsrs", nsr_id, db_nsr_update)
                            step = "deploying native charm for vnf_member_index={} vdu_id={}".format(vnf_index,
                                                                                                     vdu["id"])
                            await self.n2vc.login()
                            vdur = db_vnfrs[vnf_index]["vdur"][vdu_index]
                            # TODO for the moment only first vdu_id contains a charm deployed
                            if vdur["vdu-id-ref"] != vdu["id"]:
                                raise LcmException("Mismatch vdur {}, vdu {} at index {} for vnf {}"
                                                   .format(vdur["vdu-id-ref"], vdu["id"], vdu_index, vnf_index))
                            vnfr_params["rw_mgmt_ip"] = vdur["ip-address"]
                            charm_params = {
                                "user_values": vnfr_params,
                                "rw_mgmt_ip": vdur["ip-address"],
                                "initial-config-primitive": vdu_config.get('initial-config-primitive') or {}
                            }

                            # get username
                            # TODO remove this when changes on IM regarding config-access:ssh-access:default-user were
                            #  merged. Meanwhile let's get username from initial-config-primitive
                            if vdu_config.get("initial-config-primitive"):
                                for param in vdu_config["initial-config-primitive"][0].get("parameter", ()):
                                    if param["name"] == "ssh-username":
                                        charm_params["username"] = param["value"]
                            if vdu_config.get("config-access") and vdu_config["config-access"].get("ssh-access"):
                                if vdu_config["config-access"]["ssh-access"].get("required"):
                                    charm_params["username"] = vdu_config["config-access"]["ssh-access"].get(
                                        "default-user")

                            deploy_charm(vnf_index, vdu["id"], vdur.get("name"), vdur["count-index"],
                                         charm_params, n2vc_info, native_charm)
                            number_to_configure += 1

            # Check if this NS has a charm configuration

            ns_config = nsd.get("ns-configuration")
            if ns_config and ns_config.get("juju"):
                native_charm = ns_config["juju"].get("proxy") is False

                if native_charm:
                    step = "deploying native charm to configure ns"
                    # TODO is NS magmt IP address needed?

                    # Get additional parameters
                    additional_params = {}
                    if db_nsr.get("additionalParamsForNs"):
                        additional_params = db_nsr["additionalParamsForNs"].copy()
                    for k, v in additional_params.items():
                        if isinstance(v, str) and v.startswith("!!yaml "):
                            additional_params[k] = yaml.safe_load(v[7:])

                    # additional_params["rw_mgmt_ip"] = db_nsr["ip-address"]
                    charm_params = {
                        "user_values": additional_params,
                        "rw_mgmt_ip": db_nsr.get("ip-address"),
                        "initial-config-primitive": ns_config.get('initial-config-primitive') or {}
                    }

                    # get username
                    # TODO remove this when changes on IM regarding config-access:ssh-access:default-user were
                    #  merged. Meanwhile let's get username from initial-config-primitive
                    if ns_config.get("initial-config-primitive"):
                        for param in ns_config["initial-config-primitive"][0].get("parameter", ()):
                            if param["name"] == "ssh-username":
                                charm_params["username"] = param["value"]
                    if ns_config.get("config-access") and ns_config["config-access"].get("ssh-access"):
                        if ns_config["config-access"]["ssh-access"].get("required"):
                            charm_params["username"] = ns_config["config-access"]["ssh-access"].get("default-user")

                    # Login to the VCA. If there are multiple calls to login(),
                    # subsequent calls will be a nop and return immediately.
                    await self.n2vc.login()
                    deploy_charm(None, None, None, None, charm_params, n2vc_info, native_charm)
                    number_to_configure += 1

            # waiting all charms are ok
            configuration_failed = False
            if number_to_configure:
                step = "Waiting all charms are active"
                old_status = "configuring: init: {}".format(number_to_configure)
                db_nsr_update["config-status"] = old_status
                db_nsr_update["detailed-status"] = old_status
                db_nslcmop_update["detailed-status"] = old_status

                # wait until all are configured.
                while time() <= start_deploy + self.total_deploy_timeout:
                    if db_nsr_update:
                        self.update_db_2("nsrs", nsr_id, db_nsr_update)
                    if db_nslcmop_update:
                        self.update_db_2("nslcmops", nslcmop_id, db_nslcmop_update)
                    # TODO add a fake task that set n2vc_event after some time
                    await n2vc_info["n2vc_event"].wait()
                    n2vc_info["n2vc_event"].clear()
                    all_active = True
                    status_map = {}
                    n2vc_error_text = []  # contain text error list. If empty no one is in error status
                    now = time()
                    for vca_deployed in vca_deployed_list:
                        vca_status = vca_deployed["operational-status"]
                        if vca_status not in status_map:
                            # Initialize it
                            status_map[vca_status] = 0
                        status_map[vca_status] += 1

                        if vca_status == "active":
                            vca_deployed.pop("time_first_error", None)
                            vca_deployed.pop("status_first_error", None)
                            continue

                        all_active = False
                        if vca_status in ("error", "blocked"):
                            vca_deployed["detailed-status-error"] = vca_deployed["detailed-status"]
                            # if not first time in this status error
                            if not vca_deployed.get("time_first_error"):
                                vca_deployed["time_first_error"] = now
                                continue
                        if vca_deployed.get("time_first_error") and \
                                now <= vca_deployed["time_first_error"] + self.timeout_vca_on_error:
                            n2vc_error_text.append("member_vnf_index={} vdu_id={} {}: {}"
                                                   .format(vca_deployed["member-vnf-index"],
                                                           vca_deployed["vdu_id"], vca_status,
                                                           vca_deployed["detailed-status-error"]))

                    if all_active:
                        break
                    elif n2vc_error_text:
                        db_nsr_update["config-status"] = "failed"
                        error_text = "fail configuring " + ";".join(n2vc_error_text)
                        db_nsr_update["detailed-status"] = error_text
                        db_nslcmop_update["operationState"] = nslcmop_operation_state = "FAILED_TEMP"
                        db_nslcmop_update["detailed-status"] = error_text
                        db_nslcmop_update["statusEnteredTime"] = time()
                        configuration_failed = True
                        break
                    else:
                        cs = "configuring: "
                        separator = ""
                        for status, num in status_map.items():
                            cs += separator + "{}: {}".format(status, num)
                            separator = ", "
                        if old_status != cs:
                            db_nsr_update["config-status"] = cs
                            db_nsr_update["detailed-status"] = cs
                            db_nslcmop_update["detailed-status"] = cs
                            old_status = cs
                else:   # total_deploy_timeout
                    raise LcmException("Timeout waiting ns to be configured")

            if not configuration_failed:
                # all is done
                db_nslcmop_update["operationState"] = nslcmop_operation_state = "COMPLETED"
                db_nslcmop_update["statusEnteredTime"] = time()
                db_nslcmop_update["detailed-status"] = "done"
                db_nsr_update["config-status"] = "configured"
                db_nsr_update["detailed-status"] = "done"

            return

        except (ROclient.ROClientException, DbException, LcmException) as e:
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
                if db_nsr:
                    db_nsr_update["detailed-status"] = "ERROR {}: {}".format(step, exc)
                    db_nsr_update["operational-status"] = "failed"
                if db_nslcmop:
                    db_nslcmop_update["detailed-status"] = "FAILED {}: {}".format(step, exc)
                    db_nslcmop_update["operationState"] = nslcmop_operation_state = "FAILED"
                    db_nslcmop_update["statusEnteredTime"] = time()
            try:
                if db_nsr:
                    db_nsr_update["_admin.nslcmop"] = None
                    self.update_db_2("nsrs", nsr_id, db_nsr_update)
                if db_nslcmop_update:
                    self.update_db_2("nslcmops", nslcmop_id, db_nslcmop_update)
            except DbException as e:
                self.logger.error(logging_text + "Cannot update database: {}".format(e))
            if nslcmop_operation_state:
                try:
                    await self.msg.aiowrite("ns", "instantiated", {"nsr_id": nsr_id, "nslcmop_id": nslcmop_id,
                                                                   "operationState": nslcmop_operation_state},
                                            loop=self.loop)
                except Exception as e:
                    self.logger.error(logging_text + "kafka_write notification Exception {}".format(e))

            self.logger.debug(logging_text + "Exit")
            self.lcm_tasks.remove("ns", nsr_id, nslcmop_id, "ns_instantiate")

    async def _destroy_charm(self, model, application):
        """
        Order N2VC destroy a charm
        :param model:
        :param application:
        :return: True if charm does not exist. False if it exist
        """
        if not await self.n2vc.HasApplication(model, application):
            return True  # Already removed
        await self.n2vc.RemoveCharms(model, application)
        return False

    async def _wait_charm_destroyed(self, model, application, timeout):
        """
        Wait until charm does not exist
        :param model:
        :param application:
        :param timeout:
        :return: True if not exist, False if timeout
        """
        while True:
            if not await self.n2vc.HasApplication(model, application):
                return True
            if timeout < 0:
                return False
            await asyncio.sleep(10)
            timeout -= 10

    # Check if this VNFD has a configured terminate action
    def _has_terminate_config_primitive(self, vnfd):
        vnf_config = vnfd.get("vnf-configuration")
        if vnf_config and vnf_config.get("terminate-config-primitive"):
            return True
        else:
            return False

    @staticmethod
    def _get_terminate_config_primitive_seq_list(vnfd):
        """ Get a numerically sorted list of the sequences for this VNFD's terminate action """
        # No need to check for existing primitive twice, already done before
        vnf_config = vnfd.get("vnf-configuration")
        seq_list = vnf_config.get("terminate-config-primitive")
        # Get all 'seq' tags in seq_list, order sequences numerically, ascending.
        seq_list_sorted = sorted(seq_list, key=lambda x: int(x['seq']))
        return seq_list_sorted

    @staticmethod
    def _create_nslcmop(nsr_id, operation, params):
        """
        Creates a ns-lcm-opp content to be stored at database.
        :param nsr_id: internal id of the instance
        :param operation: instantiate, terminate, scale, action, ...
        :param params: user parameters for the operation
        :return: dictionary following SOL005 format
        """
        # Raise exception if invalid arguments
        if not (nsr_id and operation and params):
            raise LcmException(
                "Parameters 'nsr_id', 'operation' and 'params' needed to create primitive not provided")
        now = time()
        _id = str(uuid4())
        nslcmop = {
            "id": _id,
            "_id": _id,
            # COMPLETED,PARTIALLY_COMPLETED,FAILED_TEMP,FAILED,ROLLING_BACK,ROLLED_BACK
            "operationState": "PROCESSING",
            "statusEnteredTime": now,
            "nsInstanceId": nsr_id,
            "lcmOperationType": operation,
            "startTime": now,
            "isAutomaticInvocation": False,
            "operationParams": params,
            "isCancelPending": False,
            "links": {
                "self": "/osm/nslcm/v1/ns_lcm_op_occs/" + _id,
                "nsInstance": "/osm/nslcm/v1/ns_instances/" + nsr_id,
            }
        }
        return nslcmop

    async def _terminate_action(self, db_nslcmop, nslcmop_id, nsr_id):
        """ Create a primitive with params from VNFD
            Called from terminate() before deleting instance
            Calls action() to execute the primitive """
        logging_text = "Task ns={} _terminate_action={} ".format(nsr_id, nslcmop_id)
        db_vnfds = {}
        db_vnfrs_list = self.db.get_list("vnfrs", {"nsr-id-ref": nsr_id})
        # Loop over VNFRs
        for vnfr in db_vnfrs_list:
            vnfd_id = vnfr["vnfd-id"]
            vnf_index = vnfr["member-vnf-index-ref"]
            if vnfd_id not in db_vnfds:
                step = "Getting vnfd={} id='{}' from db".format(vnfd_id, vnfd_id)
                vnfd = self.db.get_one("vnfds", {"_id": vnfd_id})
                db_vnfds[vnfd_id] = vnfd
            vnfd = db_vnfds[vnfd_id]
            if not self._has_terminate_config_primitive(vnfd):
                continue
            # Get the primitive's sorted sequence list
            seq_list = self._get_terminate_config_primitive_seq_list(vnfd)
            for seq in seq_list:
                # For each sequence in list, call terminate action
                step = "Calling terminate action for vnf_member_index={} primitive={}".format(
                    vnf_index, seq.get("name"))
                self.logger.debug(logging_text + step)
                # Create the primitive for each sequence
                operation = "action"
                # primitive, i.e. "primitive": "touch"
                primitive = seq.get('name')
                primitive_params = {}
                params = {
                    "member_vnf_index": vnf_index,
                    "primitive": primitive,
                    "primitive_params": primitive_params,
                }
                nslcmop_primitive = self._create_nslcmop(nsr_id, operation, params)
                # Get a copy of db_nslcmop 'admin' part
                db_nslcmop_action = {"_admin": deepcopy(db_nslcmop["_admin"])}
                # Update db_nslcmop with the primitive data
                db_nslcmop_action.update(nslcmop_primitive)
                # Create a new db entry for the created primitive, returns the new ID.
                # (The ID is normally obtained from Kafka.)
                nslcmop_terminate_action_id = self.db.create(
                    "nslcmops", db_nslcmop_action)
                # Execute the primitive
                nslcmop_operation_state, nslcmop_operation_state_detail = await self.action(
                    nsr_id, nslcmop_terminate_action_id)
                # Launch Exception if action() returns other than ['COMPLETED', 'PARTIALLY_COMPLETED']
                nslcmop_operation_states_ok = ['COMPLETED', 'PARTIALLY_COMPLETED']
                if nslcmop_operation_state not in nslcmop_operation_states_ok:
                    raise LcmException(
                        "terminate_primitive_action for vnf_member_index={}",
                        " primitive={} fails with error {}".format(
                            vnf_index, seq.get("name"), nslcmop_operation_state_detail))

    async def terminate(self, nsr_id, nslcmop_id):

        # Try to lock HA task here
        task_is_locked_by_me = self.lcm_tasks.lock_HA('ns', 'nslcmops', nslcmop_id)
        if not task_is_locked_by_me:
            return

        logging_text = "Task ns={} terminate={} ".format(nsr_id, nslcmop_id)
        self.logger.debug(logging_text + "Enter")
        db_nsr = None
        db_nslcmop = None
        exc = None
        failed_detail = []   # annotates all failed error messages
        vca_time_destroy = None   # time of where destroy charm order
        db_nsr_update = {"_admin.nslcmop": nslcmop_id}
        db_nslcmop_update = {}
        nslcmop_operation_state = None
        autoremove = False  # autoremove after terminated
        try:
            # wait for any previous tasks in process
            await self.lcm_tasks.waitfor_related_HA("ns", 'nslcmops', nslcmop_id)

            step = "Getting nslcmop={} from db".format(nslcmop_id)
            db_nslcmop = self.db.get_one("nslcmops", {"_id": nslcmop_id})
            step = "Getting nsr={} from db".format(nsr_id)
            db_nsr = self.db.get_one("nsrs", {"_id": nsr_id})
            # nsd = db_nsr["nsd"]
            nsr_deployed = deepcopy(db_nsr["_admin"].get("deployed"))
            if db_nsr["_admin"]["nsState"] == "NOT_INSTANTIATED":
                return
            # #TODO check if VIM is creating and wait
            # RO_vim_id = db_vim["_admin"]["deployed"]["RO"]
            # Call internal terminate action
            await self._terminate_action(db_nslcmop, nslcmop_id, nsr_id)

            db_nsr_update["operational-status"] = "terminating"
            db_nsr_update["config-status"] = "terminating"

            if nsr_deployed and nsr_deployed.get("VCA-model-name"):
                vca_model_name = nsr_deployed["VCA-model-name"]
                step = "deleting VCA model name '{}' and all charms".format(vca_model_name)
                self.logger.debug(logging_text + step)
                try:
                    await self.n2vc.DestroyNetworkService(vca_model_name)
                except NetworkServiceDoesNotExist:
                    pass
                db_nsr_update["_admin.deployed.VCA-model-name"] = None
                if nsr_deployed.get("VCA"):
                    for vca_index in range(0, len(nsr_deployed["VCA"])):
                        db_nsr_update["_admin.deployed.VCA.{}".format(vca_index)] = None
                self.update_db_2("nsrs", nsr_id, db_nsr_update)
            # for backward compatibility if charm have been created with "default"  model name delete one by one
            elif nsr_deployed and nsr_deployed.get("VCA"):
                try:
                    step = "Scheduling configuration charms removing"
                    db_nsr_update["detailed-status"] = "Deleting charms"
                    self.logger.debug(logging_text + step)
                    self.update_db_2("nsrs", nsr_id, db_nsr_update)
                    # for backward compatibility
                    if isinstance(nsr_deployed["VCA"], dict):
                        nsr_deployed["VCA"] = list(nsr_deployed["VCA"].values())
                        db_nsr_update["_admin.deployed.VCA"] = nsr_deployed["VCA"]
                        self.update_db_2("nsrs", nsr_id, db_nsr_update)

                    for vca_index, vca_deployed in enumerate(nsr_deployed["VCA"]):
                        if vca_deployed:
                            if await self._destroy_charm(vca_deployed['model'], vca_deployed["application"]):
                                vca_deployed.clear()
                                db_nsr["_admin.deployed.VCA.{}".format(vca_index)] = None
                            else:
                                vca_time_destroy = time()
                except Exception as e:
                    self.logger.debug(logging_text + "Failed while deleting charms: {}".format(e))

            # remove from RO
            RO_fail = False
            RO = ROclient.ROClient(self.loop, **self.ro_config)

            # Delete ns
            RO_nsr_id = RO_delete_action = None
            if nsr_deployed and nsr_deployed.get("RO"):
                RO_nsr_id = nsr_deployed["RO"].get("nsr_id")
                RO_delete_action = nsr_deployed["RO"].get("nsr_delete_action_id")
            try:
                if RO_nsr_id:
                    step = db_nsr_update["detailed-status"] = db_nslcmop_update["detailed-status"] = "Deleting ns at RO"
                    self.update_db_2("nslcmops", nslcmop_id, db_nslcmop_update)
                    self.update_db_2("nsrs", nsr_id, db_nsr_update)
                    self.logger.debug(logging_text + step)
                    desc = await RO.delete("ns", RO_nsr_id)
                    RO_delete_action = desc["action_id"]
                    db_nsr_update["_admin.deployed.RO.nsr_delete_action_id"] = RO_delete_action
                    db_nsr_update["_admin.deployed.RO.nsr_id"] = None
                    db_nsr_update["_admin.deployed.RO.nsr_status"] = "DELETED"
                if RO_delete_action:
                    # wait until NS is deleted from VIM
                    step = detailed_status = "Waiting ns deleted from VIM. RO_id={} RO_delete_action={}".\
                        format(RO_nsr_id, RO_delete_action)
                    detailed_status_old = None
                    self.logger.debug(logging_text + step)

                    delete_timeout = 20 * 60   # 20 minutes
                    while delete_timeout > 0:
                        desc = await RO.show("ns", item_id_name=RO_nsr_id, extra_item="action",
                                             extra_item_id=RO_delete_action)
                        ns_status, ns_status_info = RO.check_action_status(desc)
                        if ns_status == "ERROR":
                            raise ROclient.ROClientException(ns_status_info)
                        elif ns_status == "BUILD":
                            detailed_status = step + "; {}".format(ns_status_info)
                        elif ns_status == "ACTIVE":
                            db_nsr_update["_admin.deployed.RO.nsr_delete_action_id"] = None
                            db_nsr_update["_admin.deployed.RO.nsr_status"] = "DELETED"
                            break
                        else:
                            assert False, "ROclient.check_action_status returns unknown {}".format(ns_status)
                        if detailed_status != detailed_status_old:
                            detailed_status_old = db_nslcmop_update["detailed-status"] = \
                                db_nsr_update["detailed-status"] = detailed_status
                            self.update_db_2("nslcmops", nslcmop_id, db_nslcmop_update)
                            self.update_db_2("nsrs", nsr_id, db_nsr_update)
                        await asyncio.sleep(5, loop=self.loop)
                        delete_timeout -= 5
                    else:  # delete_timeout <= 0:
                        raise ROclient.ROClientException("Timeout waiting ns deleted from VIM")

            except ROclient.ROClientException as e:
                if e.http_code == 404:  # not found
                    db_nsr_update["_admin.deployed.RO.nsr_id"] = None
                    db_nsr_update["_admin.deployed.RO.nsr_status"] = "DELETED"
                    db_nsr_update["_admin.deployed.RO.nsr_delete_action_id"] = None
                    self.logger.debug(logging_text + "RO_ns_id={} already deleted".format(RO_nsr_id))
                elif e.http_code == 409:   # conflict
                    failed_detail.append("RO_ns_id={} delete conflict: {}".format(RO_nsr_id, e))
                    self.logger.debug(logging_text + failed_detail[-1])
                    RO_fail = True
                else:
                    failed_detail.append("RO_ns_id={} delete error: {}".format(RO_nsr_id, e))
                    self.logger.error(logging_text + failed_detail[-1])
                    RO_fail = True

            # Delete nsd
            if not RO_fail and nsr_deployed and nsr_deployed.get("RO") and nsr_deployed["RO"].get("nsd_id"):
                RO_nsd_id = nsr_deployed["RO"]["nsd_id"]
                try:
                    step = db_nsr_update["detailed-status"] = db_nslcmop_update["detailed-status"] =\
                        "Deleting nsd at RO"
                    await RO.delete("nsd", RO_nsd_id)
                    self.logger.debug(logging_text + "RO_nsd_id={} deleted".format(RO_nsd_id))
                    db_nsr_update["_admin.deployed.RO.nsd_id"] = None
                except ROclient.ROClientException as e:
                    if e.http_code == 404:  # not found
                        db_nsr_update["_admin.deployed.RO.nsd_id"] = None
                        self.logger.debug(logging_text + "RO_nsd_id={} already deleted".format(RO_nsd_id))
                    elif e.http_code == 409:   # conflict
                        failed_detail.append("RO_nsd_id={} delete conflict: {}".format(RO_nsd_id, e))
                        self.logger.debug(logging_text + failed_detail[-1])
                        RO_fail = True
                    else:
                        failed_detail.append("RO_nsd_id={} delete error: {}".format(RO_nsd_id, e))
                        self.logger.error(logging_text + failed_detail[-1])
                        RO_fail = True

            if not RO_fail and nsr_deployed and nsr_deployed.get("RO") and nsr_deployed["RO"].get("vnfd"):
                for index, vnf_deployed in enumerate(nsr_deployed["RO"]["vnfd"]):
                    if not vnf_deployed or not vnf_deployed["id"]:
                        continue
                    try:
                        RO_vnfd_id = vnf_deployed["id"]
                        step = db_nsr_update["detailed-status"] = db_nslcmop_update["detailed-status"] =\
                            "Deleting member_vnf_index={} RO_vnfd_id={} from RO".format(
                                vnf_deployed["member-vnf-index"], RO_vnfd_id)
                        await RO.delete("vnfd", RO_vnfd_id)
                        self.logger.debug(logging_text + "RO_vnfd_id={} deleted".format(RO_vnfd_id))
                        db_nsr_update["_admin.deployed.RO.vnfd.{}.id".format(index)] = None
                    except ROclient.ROClientException as e:
                        if e.http_code == 404:  # not found
                            db_nsr_update["_admin.deployed.RO.vnfd.{}.id".format(index)] = None
                            self.logger.debug(logging_text + "RO_vnfd_id={} already deleted ".format(RO_vnfd_id))
                        elif e.http_code == 409:   # conflict
                            failed_detail.append("RO_vnfd_id={} delete conflict: {}".format(RO_vnfd_id, e))
                            self.logger.debug(logging_text + failed_detail[-1])
                        else:
                            failed_detail.append("RO_vnfd_id={} delete error: {}".format(RO_vnfd_id, e))
                            self.logger.error(logging_text + failed_detail[-1])

            # wait until charm deleted
            if vca_time_destroy:
                db_nsr_update["detailed-status"] = db_nslcmop_update["detailed-status"] = step = \
                    "Waiting for deletion of configuration charms"
                self.update_db_2("nslcmops", nslcmop_id, db_nslcmop_update)
                self.update_db_2("nsrs", nsr_id, db_nsr_update)
                for vca_index, vca_deployed in enumerate(nsr_deployed["VCA"]):
                    if not vca_deployed:
                        continue
                    step = "Waiting for deletion of charm application_name={}".format(vca_deployed["application"])
                    timeout = self.timeout_charm_delete - int(time() - vca_time_destroy)
                    if not await self._wait_charm_destroyed(vca_deployed['model'], vca_deployed["application"],
                                                            timeout):
                        failed_detail.append("VCA[application_name={}] Deletion timeout".format(
                            vca_deployed["application"]))
                    else:
                        db_nsr["_admin.deployed.VCA.{}".format(vca_index)] = None

            if failed_detail:
                self.logger.error(logging_text + " ;".join(failed_detail))
                db_nsr_update["operational-status"] = "failed"
                db_nsr_update["detailed-status"] = "Deletion errors " + "; ".join(failed_detail)
                db_nslcmop_update["detailed-status"] = "; ".join(failed_detail)
                db_nslcmop_update["operationState"] = nslcmop_operation_state = "FAILED"
                db_nslcmop_update["statusEnteredTime"] = time()
            else:
                db_nsr_update["operational-status"] = "terminated"
                db_nsr_update["detailed-status"] = "Done"
                db_nsr_update["_admin.nsState"] = "NOT_INSTANTIATED"
                db_nslcmop_update["detailed-status"] = "Done"
                db_nslcmop_update["operationState"] = nslcmop_operation_state = "COMPLETED"
                db_nslcmop_update["statusEnteredTime"] = time()
                if db_nslcmop["operationParams"].get("autoremove"):
                    autoremove = True

        except (ROclient.ROClientException, DbException, LcmException) as e:
            self.logger.error(logging_text + "Exit Exception {}".format(e))
            exc = e
        except asyncio.CancelledError:
            self.logger.error(logging_text + "Cancelled Exception while '{}'".format(step))
            exc = "Operation was cancelled"
        except Exception as e:
            exc = traceback.format_exc()
            self.logger.critical(logging_text + "Exit Exception {}".format(e), exc_info=True)
        finally:
            if exc and db_nslcmop:
                db_nslcmop_update["detailed-status"] = "FAILED {}: {}".format(step, exc)
                db_nslcmop_update["operationState"] = nslcmop_operation_state = "FAILED"
                db_nslcmop_update["statusEnteredTime"] = time()
            try:
                if db_nslcmop and db_nslcmop_update:
                    self.update_db_2("nslcmops", nslcmop_id, db_nslcmop_update)
                if db_nsr:
                    db_nsr_update["_admin.nslcmop"] = None
                    self.update_db_2("nsrs", nsr_id, db_nsr_update)
            except DbException as e:
                self.logger.error(logging_text + "Cannot update database: {}".format(e))
            if nslcmop_operation_state:
                try:
                    await self.msg.aiowrite("ns", "terminated", {"nsr_id": nsr_id, "nslcmop_id": nslcmop_id,
                                                                 "operationState": nslcmop_operation_state,
                                                                 "autoremove": autoremove},
                                            loop=self.loop)
                except Exception as e:
                    self.logger.error(logging_text + "kafka_write notification Exception {}".format(e))
            self.logger.debug(logging_text + "Exit")
            self.lcm_tasks.remove("ns", nsr_id, nslcmop_id, "ns_terminate")

    @staticmethod
    def _map_primitive_params(primitive_desc, params, instantiation_params):
        """
        Generates the params to be provided to charm before executing primitive. If user does not provide a parameter,
        The default-value is used. If it is between < > it look for a value at instantiation_params
        :param primitive_desc: portion of VNFD/NSD that describes primitive
        :param params: Params provided by user
        :param instantiation_params: Instantiation params provided by user
        :return: a dictionary with the calculated params
        """
        calculated_params = {}
        for parameter in primitive_desc.get("parameter", ()):
            param_name = parameter["name"]
            if param_name in params:
                calculated_params[param_name] = params[param_name]
            elif "default-value" in parameter or "value" in parameter:
                if "value" in parameter:
                    calculated_params[param_name] = parameter["value"]
                else:
                    calculated_params[param_name] = parameter["default-value"]
                if isinstance(calculated_params[param_name], str) and calculated_params[param_name].startswith("<") \
                        and calculated_params[param_name].endswith(">"):
                    if calculated_params[param_name][1:-1] in instantiation_params:
                        calculated_params[param_name] = instantiation_params[calculated_params[param_name][1:-1]]
                    else:
                        raise LcmException("Parameter {} needed to execute primitive {} not provided".
                                           format(parameter["default-value"], primitive_desc["name"]))
            else:
                raise LcmException("Parameter {} needed to execute primitive {} not provided".
                                   format(param_name, primitive_desc["name"]))

            if isinstance(calculated_params[param_name], (dict, list, tuple)):
                calculated_params[param_name] = yaml.safe_dump(calculated_params[param_name], default_flow_style=True,
                                                               width=256)
            elif isinstance(calculated_params[param_name], str) and calculated_params[param_name].startswith("!!yaml "):
                calculated_params[param_name] = calculated_params[param_name][7:]
        return calculated_params

    async def _ns_execute_primitive(self, db_deployed, member_vnf_index, vdu_id, vdu_name, vdu_count_index,
                                    primitive, primitive_params, retries=0, retries_interval=30):
        start_primitive_time = time()
        try:
            for vca_deployed in db_deployed["VCA"]:
                if not vca_deployed:
                    continue
                if member_vnf_index != vca_deployed["member-vnf-index"] or vdu_id != vca_deployed["vdu_id"]:
                    continue
                if vdu_name and vdu_name != vca_deployed["vdu_name"]:
                    continue
                if vdu_count_index and vdu_count_index != vca_deployed["vdu_count_index"]:
                    continue
                break
            else:
                raise LcmException("charm for member_vnf_index={} vdu_id={} vdu_name={} vdu_count_index={} is not "
                                   "deployed".format(member_vnf_index, vdu_id, vdu_name, vdu_count_index))
            model_name = vca_deployed.get("model")
            application_name = vca_deployed.get("application")
            if not model_name or not application_name:
                raise LcmException("charm for member_vnf_index={} vdu_id={} vdu_name={} vdu_count_index={} has not "
                                   "model or application name" .format(member_vnf_index, vdu_id, vdu_name,
                                                                       vdu_count_index))
            # if vca_deployed["operational-status"] != "active":
            #   raise LcmException("charm for member_vnf_index={} vdu_id={} operational_status={} not 'active'".format(
            #   member_vnf_index, vdu_id, vca_deployed["operational-status"]))
            callback = None  # self.n2vc_callback
            callback_args = ()  # [db_nsr, db_nslcmop, member_vnf_index, None]
            await self.n2vc.login()
            if primitive == "config":
                primitive_params = {"params": primitive_params}
            while retries >= 0:
                primitive_id = await self.n2vc.ExecutePrimitive(
                    model_name,
                    application_name,
                    primitive,
                    callback,
                    *callback_args,
                    **primitive_params
                )
                while time() - start_primitive_time < self.timeout_primitive:
                    primitive_result_ = await self.n2vc.GetPrimitiveStatus(model_name, primitive_id)
                    if primitive_result_ in ("completed", "failed"):
                        primitive_result = "COMPLETED" if primitive_result_ == "completed" else "FAILED"
                        detailed_result = await self.n2vc.GetPrimitiveOutput(model_name, primitive_id)
                        break
                    elif primitive_result_ is None and primitive == "config":
                        primitive_result = "COMPLETED"
                        detailed_result = None
                        break
                    else:  # ("running", "pending", None):
                        pass
                    await asyncio.sleep(5)
                else:
                    raise LcmException("timeout after {} seconds".format(self.timeout_primitive))
                if primitive_result == "COMPLETED":
                    break
                retries -= 1
                if retries >= 0:
                    await asyncio.sleep(retries_interval)

            return primitive_result, detailed_result
        except (N2VCPrimitiveExecutionFailed, LcmException) as e:
            return "FAILED", str(e)

    async def action(self, nsr_id, nslcmop_id):

        # Try to lock HA task here
        task_is_locked_by_me = self.lcm_tasks.lock_HA('ns', 'nslcmops', nslcmop_id)
        if not task_is_locked_by_me:
            return

        logging_text = "Task ns={} action={} ".format(nsr_id, nslcmop_id)
        self.logger.debug(logging_text + "Enter")
        # get all needed from database
        db_nsr = None
        db_nslcmop = None
        db_nsr_update = {"_admin.nslcmop": nslcmop_id}
        db_nslcmop_update = {}
        nslcmop_operation_state = None
        nslcmop_operation_state_detail = None
        exc = None
        try:
            # wait for any previous tasks in process
            await self.lcm_tasks.waitfor_related_HA('ns', 'nslcmops', nslcmop_id)

            step = "Getting information from database"
            db_nslcmop = self.db.get_one("nslcmops", {"_id": nslcmop_id})
            db_nsr = self.db.get_one("nsrs", {"_id": nsr_id})

            nsr_deployed = db_nsr["_admin"].get("deployed")
            vnf_index = db_nslcmop["operationParams"].get("member_vnf_index")
            vdu_id = db_nslcmop["operationParams"].get("vdu_id")
            vdu_count_index = db_nslcmop["operationParams"].get("vdu_count_index")
            vdu_name = db_nslcmop["operationParams"].get("vdu_name")

            if vnf_index:
                step = "Getting vnfr from database"
                db_vnfr = self.db.get_one("vnfrs", {"member-vnf-index-ref": vnf_index, "nsr-id-ref": nsr_id})
                step = "Getting vnfd from database"
                db_vnfd = self.db.get_one("vnfds", {"_id": db_vnfr["vnfd-id"]})
            else:
                if db_nsr.get("nsd"):
                    db_nsd = db_nsr.get("nsd")    # TODO this will be removed
                else:
                    step = "Getting nsd from database"
                    db_nsd = self.db.get_one("nsds", {"_id": db_nsr["nsd-id"]})

            # for backward compatibility
            if nsr_deployed and isinstance(nsr_deployed.get("VCA"), dict):
                nsr_deployed["VCA"] = list(nsr_deployed["VCA"].values())
                db_nsr_update["_admin.deployed.VCA"] = nsr_deployed["VCA"]
                self.update_db_2("nsrs", nsr_id, db_nsr_update)

            primitive = db_nslcmop["operationParams"]["primitive"]
            primitive_params = db_nslcmop["operationParams"]["primitive_params"]

            # look for primitive
            config_primitive_desc = None
            if vdu_id:
                for vdu in get_iterable(db_vnfd, "vdu"):
                    if vdu_id == vdu["id"]:
                        for config_primitive in vdu.get("vdu-configuration", {}).get("config-primitive", ()):
                            if config_primitive["name"] == primitive:
                                config_primitive_desc = config_primitive
                                break
            elif vnf_index:
                for config_primitive in db_vnfd.get("vnf-configuration", {}).get("config-primitive", ()):
                    if config_primitive["name"] == primitive:
                        config_primitive_desc = config_primitive
                        break
            else:
                for config_primitive in db_nsd.get("ns-configuration", {}).get("config-primitive", ()):
                    if config_primitive["name"] == primitive:
                        config_primitive_desc = config_primitive
                        break

            if not config_primitive_desc:
                raise LcmException("Primitive {} not found at [ns|vnf|vdu]-configuration:config-primitive ".
                                   format(primitive))

            desc_params = {}
            if vnf_index:
                if db_vnfr.get("additionalParamsForVnf"):
                    desc_params.update(db_vnfr["additionalParamsForVnf"])
            else:
                if db_nsr.get("additionalParamsForVnf"):
                    desc_params.update(db_nsr["additionalParamsForNs"])

            # TODO check if ns is in a proper status
            result, result_detail = await self._ns_execute_primitive(
                nsr_deployed, vnf_index, vdu_id, vdu_name, vdu_count_index, primitive,
                self._map_primitive_params(config_primitive_desc, primitive_params, desc_params))
            db_nslcmop_update["detailed-status"] = nslcmop_operation_state_detail = result_detail
            db_nslcmop_update["operationState"] = nslcmop_operation_state = result
            db_nslcmop_update["statusEnteredTime"] = time()
            self.logger.debug(logging_text + " task Done with result {} {}".format(result, result_detail))
            return  # database update is called inside finally

        except (DbException, LcmException) as e:
            self.logger.error(logging_text + "Exit Exception {}".format(e))
            exc = e
        except asyncio.CancelledError:
            self.logger.error(logging_text + "Cancelled Exception while '{}'".format(step))
            exc = "Operation was cancelled"
        except Exception as e:
            exc = traceback.format_exc()
            self.logger.critical(logging_text + "Exit Exception {} {}".format(type(e).__name__, e), exc_info=True)
        finally:
            if exc and db_nslcmop:
                db_nslcmop_update["detailed-status"] = nslcmop_operation_state_detail = \
                    "FAILED {}: {}".format(step, exc)
                db_nslcmop_update["operationState"] = nslcmop_operation_state = "FAILED"
                db_nslcmop_update["statusEnteredTime"] = time()
            try:
                if db_nslcmop_update:
                    self.update_db_2("nslcmops", nslcmop_id, db_nslcmop_update)
                if db_nsr:
                    db_nsr_update["_admin.nslcmop"] = None
                    self.update_db_2("nsrs", nsr_id, db_nsr_update)
            except DbException as e:
                self.logger.error(logging_text + "Cannot update database: {}".format(e))
            self.logger.debug(logging_text + "Exit")
            if nslcmop_operation_state:
                try:
                    await self.msg.aiowrite("ns", "actioned", {"nsr_id": nsr_id, "nslcmop_id": nslcmop_id,
                                                               "operationState": nslcmop_operation_state},
                                            loop=self.loop)
                except Exception as e:
                    self.logger.error(logging_text + "kafka_write notification Exception {}".format(e))
            self.logger.debug(logging_text + "Exit")
            self.lcm_tasks.remove("ns", nsr_id, nslcmop_id, "ns_action")
            return nslcmop_operation_state, nslcmop_operation_state_detail

    async def scale(self, nsr_id, nslcmop_id):

        # Try to lock HA task here
        task_is_locked_by_me = self.lcm_tasks.lock_HA('ns', 'nslcmops', nslcmop_id)
        if not task_is_locked_by_me:
            return

        logging_text = "Task ns={} scale={} ".format(nsr_id, nslcmop_id)
        self.logger.debug(logging_text + "Enter")
        # get all needed from database
        db_nsr = None
        db_nslcmop = None
        db_nslcmop_update = {}
        nslcmop_operation_state = None
        db_nsr_update = {"_admin.nslcmop": nslcmop_id}
        exc = None
        # in case of error, indicates what part of scale was failed to put nsr at error status
        scale_process = None
        old_operational_status = ""
        old_config_status = ""
        vnfr_scaled = False
        try:
            # wait for any previous tasks in process
            await self.lcm_tasks.waitfor_related_HA('ns', 'nslcmops', nslcmop_id)

            step = "Getting nslcmop from database"
            self.logger.debug(step + " after having waited for previous tasks to be completed")
            db_nslcmop = self.db.get_one("nslcmops", {"_id": nslcmop_id})
            step = "Getting nsr from database"
            db_nsr = self.db.get_one("nsrs", {"_id": nsr_id})

            old_operational_status = db_nsr["operational-status"]
            old_config_status = db_nsr["config-status"]
            step = "Parsing scaling parameters"
            # self.logger.debug(step)
            db_nsr_update["operational-status"] = "scaling"
            self.update_db_2("nsrs", nsr_id, db_nsr_update)
            nsr_deployed = db_nsr["_admin"].get("deployed")
            RO_nsr_id = nsr_deployed["RO"]["nsr_id"]
            vnf_index = db_nslcmop["operationParams"]["scaleVnfData"]["scaleByStepData"]["member-vnf-index"]
            scaling_group = db_nslcmop["operationParams"]["scaleVnfData"]["scaleByStepData"]["scaling-group-descriptor"]
            scaling_type = db_nslcmop["operationParams"]["scaleVnfData"]["scaleVnfType"]
            # scaling_policy = db_nslcmop["operationParams"]["scaleVnfData"]["scaleByStepData"].get("scaling-policy")

            # for backward compatibility
            if nsr_deployed and isinstance(nsr_deployed.get("VCA"), dict):
                nsr_deployed["VCA"] = list(nsr_deployed["VCA"].values())
                db_nsr_update["_admin.deployed.VCA"] = nsr_deployed["VCA"]
                self.update_db_2("nsrs", nsr_id, db_nsr_update)

            step = "Getting vnfr from database"
            db_vnfr = self.db.get_one("vnfrs", {"member-vnf-index-ref": vnf_index, "nsr-id-ref": nsr_id})
            step = "Getting vnfd from database"
            db_vnfd = self.db.get_one("vnfds", {"_id": db_vnfr["vnfd-id"]})

            step = "Getting scaling-group-descriptor"
            for scaling_descriptor in db_vnfd["scaling-group-descriptor"]:
                if scaling_descriptor["name"] == scaling_group:
                    break
            else:
                raise LcmException("input parameter 'scaleByStepData':'scaling-group-descriptor':'{}' is not present "
                                   "at vnfd:scaling-group-descriptor".format(scaling_group))

            # cooldown_time = 0
            # for scaling_policy_descriptor in scaling_descriptor.get("scaling-policy", ()):
            #     cooldown_time = scaling_policy_descriptor.get("cooldown-time", 0)
            #     if scaling_policy and scaling_policy == scaling_policy_descriptor.get("name"):
            #         break

            # TODO check if ns is in a proper status
            step = "Sending scale order to RO"
            nb_scale_op = 0
            if not db_nsr["_admin"].get("scaling-group"):
                self.update_db_2("nsrs", nsr_id, {"_admin.scaling-group": [{"name": scaling_group, "nb-scale-op": 0}]})
                admin_scale_index = 0
            else:
                for admin_scale_index, admin_scale_info in enumerate(db_nsr["_admin"]["scaling-group"]):
                    if admin_scale_info["name"] == scaling_group:
                        nb_scale_op = admin_scale_info.get("nb-scale-op", 0)
                        break
                else:  # not found, set index one plus last element and add new entry with the name
                    admin_scale_index += 1
                    db_nsr_update["_admin.scaling-group.{}.name".format(admin_scale_index)] = scaling_group
            RO_scaling_info = []
            vdu_scaling_info = {"scaling_group_name": scaling_group, "vdu": []}
            if scaling_type == "SCALE_OUT":
                # count if max-instance-count is reached
                if "max-instance-count" in scaling_descriptor and scaling_descriptor["max-instance-count"] is not None:
                    max_instance_count = int(scaling_descriptor["max-instance-count"])

                    # self.logger.debug("MAX_INSTANCE_COUNT is {}".format(scaling_descriptor["max-instance-count"]))
                    if nb_scale_op >= max_instance_count:
                        raise LcmException("reached the limit of {} (max-instance-count) "
                                           "scaling-out operations for the "
                                           "scaling-group-descriptor '{}'".format(nb_scale_op, scaling_group))
                        
                nb_scale_op += 1
                vdu_scaling_info["scaling_direction"] = "OUT"
                vdu_scaling_info["vdu-create"] = {}
                for vdu_scale_info in scaling_descriptor["vdu"]:
                    RO_scaling_info.append({"osm_vdu_id": vdu_scale_info["vdu-id-ref"], "member-vnf-index": vnf_index,
                                            "type": "create", "count": vdu_scale_info.get("count", 1)})
                    vdu_scaling_info["vdu-create"][vdu_scale_info["vdu-id-ref"]] = vdu_scale_info.get("count", 1)

            elif scaling_type == "SCALE_IN":
                # count if min-instance-count is reached
                min_instance_count = 0
                if "min-instance-count" in scaling_descriptor and scaling_descriptor["min-instance-count"] is not None:
                    min_instance_count = int(scaling_descriptor["min-instance-count"])
                if nb_scale_op <= min_instance_count:
                    raise LcmException("reached the limit of {} (min-instance-count) scaling-in operations for the "
                                       "scaling-group-descriptor '{}'".format(nb_scale_op, scaling_group))
                nb_scale_op -= 1
                vdu_scaling_info["scaling_direction"] = "IN"
                vdu_scaling_info["vdu-delete"] = {}
                for vdu_scale_info in scaling_descriptor["vdu"]:
                    RO_scaling_info.append({"osm_vdu_id": vdu_scale_info["vdu-id-ref"], "member-vnf-index": vnf_index,
                                            "type": "delete", "count": vdu_scale_info.get("count", 1)})
                    vdu_scaling_info["vdu-delete"][vdu_scale_info["vdu-id-ref"]] = vdu_scale_info.get("count", 1)

            # update VDU_SCALING_INFO with the VDUs to delete ip_addresses
            vdu_create = vdu_scaling_info.get("vdu-create")
            vdu_delete = copy(vdu_scaling_info.get("vdu-delete"))
            if vdu_scaling_info["scaling_direction"] == "IN":
                for vdur in reversed(db_vnfr["vdur"]):
                    if vdu_delete.get(vdur["vdu-id-ref"]):
                        vdu_delete[vdur["vdu-id-ref"]] -= 1
                        vdu_scaling_info["vdu"].append({
                            "name": vdur["name"],
                            "vdu_id": vdur["vdu-id-ref"],
                            "interface": []
                        })
                        for interface in vdur["interfaces"]:
                            vdu_scaling_info["vdu"][-1]["interface"].append({
                                "name": interface["name"],
                                "ip_address": interface["ip-address"],
                                "mac_address": interface.get("mac-address"),
                            })
                vdu_delete = vdu_scaling_info.pop("vdu-delete")

            # execute primitive service PRE-SCALING
            step = "Executing pre-scale vnf-config-primitive"
            if scaling_descriptor.get("scaling-config-action"):
                for scaling_config_action in scaling_descriptor["scaling-config-action"]:
                    if scaling_config_action.get("trigger") and scaling_config_action["trigger"] == "pre-scale-in" \
                            and scaling_type == "SCALE_IN":
                        vnf_config_primitive = scaling_config_action["vnf-config-primitive-name-ref"]
                        step = db_nslcmop_update["detailed-status"] = \
                            "executing pre-scale scaling-config-action '{}'".format(vnf_config_primitive)

                        # look for primitive
                        for config_primitive in db_vnfd.get("vnf-configuration", {}).get("config-primitive", ()):
                            if config_primitive["name"] == vnf_config_primitive:
                                break
                        else:
                            raise LcmException(
                                "Invalid vnfd descriptor at scaling-group-descriptor[name='{}']:scaling-config-action"
                                "[vnf-config-primitive-name-ref='{}'] does not match any vnf-configuration:config-"
                                "primitive".format(scaling_group, config_primitive))

                        vnfr_params = {"VDU_SCALE_INFO": vdu_scaling_info}
                        if db_vnfr.get("additionalParamsForVnf"):
                            vnfr_params.update(db_vnfr["additionalParamsForVnf"])

                        scale_process = "VCA"
                        db_nsr_update["config-status"] = "configuring pre-scaling"
                        result, result_detail = await self._ns_execute_primitive(
                            nsr_deployed, vnf_index, None, None, None, vnf_config_primitive,
                            self._map_primitive_params(config_primitive, {}, vnfr_params))
                        self.logger.debug(logging_text + "vnf_config_primitive={} Done with result {} {}".format(
                            vnf_config_primitive, result, result_detail))
                        if result == "FAILED":
                            raise LcmException(result_detail)
                        db_nsr_update["config-status"] = old_config_status
                        scale_process = None

            if RO_scaling_info:
                scale_process = "RO"
                RO = ROclient.ROClient(self.loop, **self.ro_config)
                RO_desc = await RO.create_action("ns", RO_nsr_id, {"vdu-scaling": RO_scaling_info})
                db_nsr_update["_admin.scaling-group.{}.nb-scale-op".format(admin_scale_index)] = nb_scale_op
                db_nsr_update["_admin.scaling-group.{}.time".format(admin_scale_index)] = time()
                # wait until ready
                RO_nslcmop_id = RO_desc["instance_action_id"]
                db_nslcmop_update["_admin.deploy.RO"] = RO_nslcmop_id

                RO_task_done = False
                step = detailed_status = "Waiting RO_task_id={} to complete the scale action.".format(RO_nslcmop_id)
                detailed_status_old = None
                self.logger.debug(logging_text + step)

                deployment_timeout = 1 * 3600   # One hour
                while deployment_timeout > 0:
                    if not RO_task_done:
                        desc = await RO.show("ns", item_id_name=RO_nsr_id, extra_item="action",
                                             extra_item_id=RO_nslcmop_id)
                        ns_status, ns_status_info = RO.check_action_status(desc)
                        if ns_status == "ERROR":
                            raise ROclient.ROClientException(ns_status_info)
                        elif ns_status == "BUILD":
                            detailed_status = step + "; {}".format(ns_status_info)
                        elif ns_status == "ACTIVE":
                            RO_task_done = True
                            step = detailed_status = "Waiting ns ready at RO. RO_id={}".format(RO_nsr_id)
                            self.logger.debug(logging_text + step)
                        else:
                            assert False, "ROclient.check_action_status returns unknown {}".format(ns_status)
                    else:
                        desc = await RO.show("ns", RO_nsr_id)
                        ns_status, ns_status_info = RO.check_ns_status(desc)
                        if ns_status == "ERROR":
                            raise ROclient.ROClientException(ns_status_info)
                        elif ns_status == "BUILD":
                            detailed_status = step + "; {}".format(ns_status_info)
                        elif ns_status == "ACTIVE":
                            step = detailed_status = \
                                "Waiting for management IP address reported by the VIM. Updating VNFRs"
                            if not vnfr_scaled:
                                self.scale_vnfr(db_vnfr, vdu_create=vdu_create, vdu_delete=vdu_delete)
                                vnfr_scaled = True
                            try:
                                desc = await RO.show("ns", RO_nsr_id)
                                # nsr_deployed["nsr_ip"] = RO.get_ns_vnf_info(desc)
                                self.ns_update_vnfr({db_vnfr["member-vnf-index-ref"]: db_vnfr}, desc)
                                break
                            except LcmExceptionNoMgmtIP:
                                pass
                        else:
                            assert False, "ROclient.check_ns_status returns unknown {}".format(ns_status)
                    if detailed_status != detailed_status_old:
                        detailed_status_old = db_nslcmop_update["detailed-status"] = detailed_status
                        self.update_db_2("nslcmops", nslcmop_id, db_nslcmop_update)

                    await asyncio.sleep(5, loop=self.loop)
                    deployment_timeout -= 5
                if deployment_timeout <= 0:
                    raise ROclient.ROClientException("Timeout waiting ns to be ready")

                # update VDU_SCALING_INFO with the obtained ip_addresses
                if vdu_scaling_info["scaling_direction"] == "OUT":
                    for vdur in reversed(db_vnfr["vdur"]):
                        if vdu_scaling_info["vdu-create"].get(vdur["vdu-id-ref"]):
                            vdu_scaling_info["vdu-create"][vdur["vdu-id-ref"]] -= 1
                            vdu_scaling_info["vdu"].append({
                                "name": vdur["name"],
                                "vdu_id": vdur["vdu-id-ref"],
                                "interface": []
                            })
                            for interface in vdur["interfaces"]:
                                vdu_scaling_info["vdu"][-1]["interface"].append({
                                    "name": interface["name"],
                                    "ip_address": interface["ip-address"],
                                    "mac_address": interface.get("mac-address"),
                                })
                    del vdu_scaling_info["vdu-create"]

            scale_process = None
            if db_nsr_update:
                self.update_db_2("nsrs", nsr_id, db_nsr_update)

            # execute primitive service POST-SCALING
            step = "Executing post-scale vnf-config-primitive"
            if scaling_descriptor.get("scaling-config-action"):
                for scaling_config_action in scaling_descriptor["scaling-config-action"]:
                    if scaling_config_action.get("trigger") and scaling_config_action["trigger"] == "post-scale-out" \
                            and scaling_type == "SCALE_OUT":
                        vnf_config_primitive = scaling_config_action["vnf-config-primitive-name-ref"]
                        step = db_nslcmop_update["detailed-status"] = \
                            "executing post-scale scaling-config-action '{}'".format(vnf_config_primitive)

                        vnfr_params = {"VDU_SCALE_INFO": vdu_scaling_info}
                        if db_vnfr.get("additionalParamsForVnf"):
                            vnfr_params.update(db_vnfr["additionalParamsForVnf"])

                        # look for primitive
                        for config_primitive in db_vnfd.get("vnf-configuration", {}).get("config-primitive", ()):
                            if config_primitive["name"] == vnf_config_primitive:
                                break
                        else:
                            raise LcmException("Invalid vnfd descriptor at scaling-group-descriptor[name='{}']:"
                                               "scaling-config-action[vnf-config-primitive-name-ref='{}'] does not "
                                               "match any vnf-configuration:config-primitive".format(scaling_group,
                                                                                                     config_primitive))
                        scale_process = "VCA"
                        db_nsr_update["config-status"] = "configuring post-scaling"

                        result, result_detail = await self._ns_execute_primitive(
                            nsr_deployed, vnf_index, None, None, None, vnf_config_primitive,
                            self._map_primitive_params(config_primitive, {}, vnfr_params))
                        self.logger.debug(logging_text + "vnf_config_primitive={} Done with result {} {}".format(
                            vnf_config_primitive, result, result_detail))
                        if result == "FAILED":
                            raise LcmException(result_detail)
                        db_nsr_update["config-status"] = old_config_status
                        scale_process = None

            db_nslcmop_update["operationState"] = nslcmop_operation_state = "COMPLETED"
            db_nslcmop_update["statusEnteredTime"] = time()
            db_nslcmop_update["detailed-status"] = "done"
            db_nsr_update["detailed-status"] = ""  # "scaled {} {}".format(scaling_group, scaling_type)
            db_nsr_update["operational-status"] = "running" if old_operational_status == "failed" \
                else old_operational_status
            db_nsr_update["config-status"] = old_config_status
            return
        except (ROclient.ROClientException, DbException, LcmException) as e:
            self.logger.error(logging_text + "Exit Exception {}".format(e))
            exc = e
        except asyncio.CancelledError:
            self.logger.error(logging_text + "Cancelled Exception while '{}'".format(step))
            exc = "Operation was cancelled"
        except Exception as e:
            exc = traceback.format_exc()
            self.logger.critical(logging_text + "Exit Exception {} {}".format(type(e).__name__, e), exc_info=True)
        finally:
            if exc:
                if db_nslcmop:
                    db_nslcmop_update["detailed-status"] = "FAILED {}: {}".format(step, exc)
                    db_nslcmop_update["operationState"] = nslcmop_operation_state = "FAILED"
                    db_nslcmop_update["statusEnteredTime"] = time()
                if db_nsr:
                    db_nsr_update["operational-status"] = old_operational_status
                    db_nsr_update["config-status"] = old_config_status
                    db_nsr_update["detailed-status"] = ""
                    db_nsr_update["_admin.nslcmop"] = None
                    if scale_process:
                        if "VCA" in scale_process:
                            db_nsr_update["config-status"] = "failed"
                        if "RO" in scale_process:
                            db_nsr_update["operational-status"] = "failed"
                        db_nsr_update["detailed-status"] = "FAILED scaling nslcmop={} {}: {}".format(nslcmop_id, step,
                                                                                                     exc)
            try:
                if db_nslcmop and db_nslcmop_update:
                    self.update_db_2("nslcmops", nslcmop_id, db_nslcmop_update)
                if db_nsr:
                    db_nsr_update["_admin.nslcmop"] = None
                    self.update_db_2("nsrs", nsr_id, db_nsr_update)
            except DbException as e:
                self.logger.error(logging_text + "Cannot update database: {}".format(e))
            if nslcmop_operation_state:
                try:
                    await self.msg.aiowrite("ns", "scaled", {"nsr_id": nsr_id, "nslcmop_id": nslcmop_id,
                                                             "operationState": nslcmop_operation_state},
                                            loop=self.loop)
                    # if cooldown_time:
                    #     await asyncio.sleep(cooldown_time)
                    # await self.msg.aiowrite("ns","scaled-cooldown-time", {"nsr_id": nsr_id, "nslcmop_id": nslcmop_id})
                except Exception as e:
                    self.logger.error(logging_text + "kafka_write notification Exception {}".format(e))
            self.logger.debug(logging_text + "Exit")
            self.lcm_tasks.remove("ns", nsr_id, nslcmop_id, "ns_scale")
