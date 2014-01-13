#!/usr/bin/env python
#
# Copyright 2013 cloudysunny14.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging
from ryu.base import app_manager
from ryu.ofproto.ofproto_v1_2 import OFPG_ANY
from ryu.controller import handler
from ryu.ofproto.ether import ETH_TYPE_ARP
from ryu.lib.packet import arp
from ryu.lib.packet.ethernet import ethernet
from ryu.lib.packet.packet import Packet
from ryu.ofproto import ether
from ryu import utils
from ryu.controller import ofp_event
from ryu.controller.handler import CONFIG_DISPATCHER
from ryu.controller.handler import MAIN_DISPATCHER
from ryu.controller.handler import HANDSHAKE_DISPATCHER
import event
import db
from topology_util import PathList
from ryu.controller.handler import set_ev_cls

LOG = logging.getLogger(__name__)

class SwitchState(object):
    def __init__(self, switch):
        self.switch = switch
        self.linked_status = dict([(port.port_no, False) \
                                  for port in switch.ports])

class SwitchEventHandler(app_manager.RyuApp):

    ARP_PACKET_LEN = ethernet._MIN_LEN + arp.arp._MIN_LEN

    def __init__(self, *args, **kwargs):
        super(SwitchEventHandler, self).__init__(*args, **kwargs)
        db.clean_tables()
        self.switches = {}
        self.link_list = []

    def send_default_flow(self, datapath):
        parser = datapath.ofproto_parser
        ofproto = datapath.ofproto
        eth_IP = ether.ETH_TYPE_IP
        match = parser.OFPMatch(eth_type=ETH_TYPE_ARP)
        output = parser.OFPActionOutput(ofproto.OFPP_CONTROLLER,
                                        self.ARP_PACKET_LEN)
        write = parser.OFPInstructionActions(ofproto.OFPIT_WRITE_ACTIONS,
                                             [output])
        instructions = [write]
        flow_mod = self.create_flow_mod(datapath, 0, 0,
                                        match, instructions)
        datapath.send_msg(flow_mod)
        match = parser.OFPMatch(eth_type=eth_IP)
        instructions = [parser.OFPInstructionGotoTable(1)]
        flow_mod = self.create_flow_mod(datapath, 0, 0,
                                        match, instructions)
        datapath.send_msg(flow_mod)

    @handler.set_ev_cls(event.EventSwitchEnter)
    def switch_enter_handler(self, event):
        switch = event.switch
        datapath = switch.dp
        self.switches[datapath.id] = SwitchState(switch)
        self.send_default_flow(datapath)

    @set_ev_cls(ofp_event.EventOFPErrorMsg,
                    [HANDSHAKE_DISPATCHER, CONFIG_DISPATCHER, MAIN_DISPATCHER])
    def error_msg_handler(self, ev):
        msg = ev.msg
        self.logger.debug('OFPErrorMsg received: type=0x%02x code=0x%02x '
                              'message=%s',
                              msg.type, msg.code, utils.hex_array(msg.data))

    @handler.set_ev_cls(event.EventLinkAdd)
    def link_add_handler(self, link):
        self.link_list.append(link.link)
        port_src = link.link.src
        port_dst = link.link.dst
        switch_src = self.switches[port_src.dpid]
        switch_dst = self.switches[port_dst.dpid]
        switch_src.linked_status[port_src.port_no] = True
        switch_dst.linked_status[port_dst.port_no] = True

    def send_port_mod(datapath, port_no, config):
        ofp_parser = datapath.ofproto_parser
        req = ofp_parser.OFPPortMod(datapath, port_no, config=config)
        datapath.send_msg(req)

    @handler.set_ev_cls(event.EventLinkDelete)
    def link_del_handler(self, link):
        port_src = link.link.src
        group_mods = db.detect_require_modify_paths(port_src.dpid, port_src.port_no)
        for group_mod in group_mods:
            group_id = group_mod['group_id']
            buckets = group_mod['buckets']
            dpid = group_mod['dpid']
            target_switch = self.switches[dpid].switch
            datapath = target_switch.dp
            parser = datapath.ofproto_parser
            ofp = datapath.ofproto
            buckets_flow = []
            for bucket in buckets:
                watch = bucket['watch']
                label = bucket['label']
                actions = self.create_push_mpls_actions(datapath, label)
                ofp_bucket = parser.OFPBucket(
                              0, watch, ofp.OFPG_ANY, actions)
                buckets_flow.append(ofp_bucket)
            if not len(buckets_flow):
                continue
            mod = parser.OFPGroupMod(datapath, ofp.OFPFC_MODIFY,
                                     ofp.OFPGT_FF, group_id, buckets_flow)
            datapath.send_msg(mod)

    def dscp_to_exp_mapping(self, dp):
        parser = dp.ofproto_parser
        eth_MPLS = ether.ETH_TYPE_MPLS
        dscp_to_table_values = {0:2, 8:2, 16:2, 24:3, 32:3, 40:3, 48:4, 56:4}
        tc = 1
        for table_id in set(dscp_to_table_values.values()):
            match = parser.OFPMatch(eth_type=eth_MPLS)
            actions = [parser.OFPActionSetField(mpls_tc=tc)]
            insts = [dp.ofproto_parser.OFPInstructionActions(
                     dp.ofproto.OFPIT_APPLY_ACTIONS, actions),
                     dp.ofproto_parser.OFPInstructionGotoTable(5)]
            flow_mod = self.create_flow_mod(dp, 100, table_id, match, insts)
            dp.send_msg(flow_mod)
            tc += 1

        eth_IP = ether.ETH_TYPE_IP
        for dscp, table in dscp_to_table_values.items():
            match = parser.OFPMatch(eth_type=eth_IP, ip_dscp=dscp)
            actions = [parser.OFPActionPushMpls(eth_MPLS)]
            insts = [dp.ofproto_parser.OFPInstructionActions(
                     dp.ofproto.OFPIT_APPLY_ACTIONS, actions),
                     dp.ofproto_parser.OFPInstructionGotoTable(table)]
            flow_mod = self.create_flow_mod(dp, 50, 1, match, insts)
            dp.send_msg(flow_mod)

    def create_push_mpls_flow(self, dp, group_id, dst_port):
        self.create_exp_queue_mapping(dp, group_id, dst_port)
        self.dscp_to_exp_mapping(dp)

    def create_swap_label_flow(self, dp, prev_label, label, out_port):
        parser = dp.ofproto_parser
        ofproto = dp.ofproto
        eth_MPLS = ether.ETH_TYPE_MPLS
        eth_IP = ether.ETH_TYPE_IP
        match = parser.OFPMatch(eth_type=eth_MPLS,
                                mpls_label=prev_label)
        actions = [parser.OFPActionPushMpls(eth_MPLS),
                   parser.OFPActionSetField(mpls_label=label),
                   parser.OFPActionSetQueue(1),
                   parser.OFPActionOutput(out_port, 0)]
        actions_apply = [parser.OFPActionPopMpls(eth_IP)]
        insts = [parser.OFPInstructionActions(ofproto.OFPIT_WRITE_ACTIONS,
                                              actions),
                 parser.OFPInstructionActions(dp.ofproto.OFPIT_APPLY_ACTIONS,
                                              actions_apply)]
        flow_mod = self.create_flow_mod(dp, 0, 0, match, insts)
        dp.send_msg(flow_mod)

    def create_pop_label_flow(self, dp, label):
        parser = dp.ofproto_parser
        eth_IP = ether.ETH_TYPE_IP
        eth_MPLS = ether.ETH_TYPE_MPLS
        match = parser.OFPMatch(eth_type=eth_MPLS,
                                mpls_label=label)
        actions = [parser.OFPActionPopMpls(eth_IP)]
        insts = [parser.OFPInstructionActions(dp.ofproto.OFPIT_APPLY_ACTIONS,
                                              actions),
                 parser.OFPInstructionGotoTable(1)]
        flow_mod = self.create_flow_mod(dp, 0, 0, match, insts)
        dp.send_msg(flow_mod)

    def create_exp_queue_mapping(self, dp, group_id, dst_port):
        dscp_values = {1:1, 2:2, 3:3}
        parser = dp.ofproto_parser
        ofproto = dp.ofproto
        eth_MPLS = ether.ETH_TYPE_MPLS
        for dscp, queue in dscp_values.items():
            match = parser.OFPMatch(eth_type=eth_MPLS, mpls_tc=dscp,
                                    eth_dst=dst_port[2])
            actions = [parser.OFPActionSetQueue(queue),
                       parser.OFPActionGroup(group_id=group_id)]
            insts = [parser.OFPInstructionActions(
                    ofproto.OFPIT_APPLY_ACTIONS, actions)]
            flow_mod = self.create_flow_mod(dp, 100, 5, match, insts)
            dp.send_msg(flow_mod)

    def create_push_mpls_actions(self, dp, label):
        parser = dp.ofproto_parser
        actions = [parser.OFPActionSetField(mpls_label=label[1]),
                   parser.OFPActionOutput(label[0], 0)]
        return actions

    def create_meter_entry(self, dp):
        parser = dp.ofproto_parser
        ofproto = dp.ofproto
        band = parser.OFPMeterBandDscpRemark(rate=1000, burst_size=250, prec_level=1)
        req = parser.OFPMeterMod(dp, ofproto.OFPMC_ADD, ofproto.OFPMF_KBPS, 1, [band])
        dp.send_msg(req)

    def send_group_flow(self, group, dst_port):
        dpid = group['dpid']
        target_switch = self.switches[dpid].switch
        datapath = target_switch.dp
        parser = datapath.ofproto_parser
        ofp = datapath.ofproto
        group_id = group['group_id']
        buckets = group['buckets']
        buckets_flow = []
        for bucket in buckets:
            watch = bucket['watch']
            label = bucket['label']
            actions = self.create_push_mpls_actions(datapath, label)
            ofp_bucket = parser.OFPBucket(
                          0, watch, ofp.OFPG_ANY, actions)
            buckets_flow.append(ofp_bucket)
        mod = parser.OFPGroupMod(datapath, ofp.OFPFC_ADD,
                                  ofp.OFPGT_FF, group_id, buckets_flow)
        datapath.send_msg(mod)
        self.create_push_mpls_flow(datapath, group_id, dst_port)

    def process_route(self, src_port, dst_port):
        path_list = PathList(self.link_list)
        paths = path_list.createWholePath(src_port[0], dst_port[0])
        path_ids = db.handle_paths(paths, src_port, dst_port)
        try:
            paths = []
            for path in path_ids:
                paths.append(path[0])
            group_flow = db.fetch_group_flows(paths)
            group = group_flow['group_flow']
            self.send_group_flow(group, dst_port)
            label_flows = group_flow['label_flow']
            last_labels = group_flow['last_label']
        except db.GroupAlreadyExistException:
            return
        for label_flow in label_flows:
            target_switch = self.switches[label_flow[1]].switch
            #Swap label entry
            self.create_swap_label_flow(target_switch.dp,
                                        label_flow[6], label_flow[5],
                                        label_flow[2])
        #Pop label entry
        for last_label in last_labels:
            if last_label != -1:
                target_switch = self.switches[dst_port[0]].switch
                self.create_pop_label_flow(target_switch.dp, last_label)

    def broadcast_to_end_nodes(self, msg):
        for switch in self.switches.values():
            for port_no, status in switch.linked_status.items():
                if status == False:
                    self.arp_packet_out(switch.switch.dp, port_no, msg.data)

    def process_end_hw_addr_flows(self, port):
        eth_IP = ether.ETH_TYPE_IP
        target_switch = self.switches[port[0]]
        datapath = target_switch.switch.dp
        hw_addr = port[2]
        ofproto = datapath.ofproto
        actions = [datapath.ofproto_parser.OFPActionSetQueue(1),
            datapath.ofproto_parser.OFPActionOutput(port[1])]
        match = datapath.ofproto_parser.OFPMatch(eth_type=eth_IP, eth_dst=hw_addr)
        inst = [datapath.ofproto_parser.OFPInstructionActions(
                ofproto.OFPIT_APPLY_ACTIONS, actions)]
        flow_mod = self.create_flow_mod(datapath, 100, 1, match, inst)
        datapath.send_msg(flow_mod)

    @handler.set_ev_cls(event.EventArpReceived)
    def arp_received_handler(self, ev):
        msg = ev.ev.msg
        datapath = msg.datapath
        in_port = msg.match['in_port']
        packet = Packet(msg.data)
        arppkt = packet.get_protocol(arp.arp)
        if arppkt.opcode == arp.ARP_REQUEST:
            self.broadcast_to_end_nodes(msg)

        try:
            src_port, dst_port = db.handle_arp_packet(arppkt, datapath.id, in_port)
            self.process_end_hw_addr_flows(src_port)
            self.process_end_hw_addr_flows(dst_port)
            if src_port[0] != dst_port[0]:
                self.process_route(src_port, dst_port)
                self.process_route(dst_port, src_port)
            if arppkt.opcode == arp.ARP_REPLY:
                target_switch = self.switches[dst_port[0]].switch
                self.arp_packet_out(target_switch.dp, dst_port[1], msg.data)
        except db.ArpTableNotFoundException:
            pass

    def arp_packet_out(self, datapath, port_no, data):
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        output_port = parser.OFPActionOutput(port_no,
                                            ofproto.OFPCML_NO_BUFFER)
        packet_out = parser.OFPPacketOut(datapath, ofproto.OFPP_ANY,
                                          ofproto.OFPP_CONTROLLER,
                                          [output_port], data)
        datapath.send_msg(packet_out)

    def create_flow_mod(self, datapath, priority,
                        table_id, match, instructions):
        """Create OFP flow mod message."""
        ofproto = datapath.ofproto
        flow_mod = datapath.ofproto_parser.OFPFlowMod(datapath, 0, 0, table_id,
                                                      ofproto.OFPFC_ADD, 0, 0,
                                                      priority,
                                                      ofproto.OFPCML_NO_BUFFER,
                                                      ofproto.OFPP_ANY,
                                                      OFPG_ANY, 0,
                                                      match, instructions)
        return flow_mod

