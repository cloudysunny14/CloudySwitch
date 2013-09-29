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
from ryu.topology import event
from ryu.ofproto.ether import ETH_TYPE_ARP
from ryu.lib.packet import arp
from ryu.lib.packet.ethernet import ethernet
from ryu.lib.packet.packet import Packet
import db
from topology_util import PathList

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
        self.arp_table = {}
        self.link_list = []

    @handler.set_ev_cls(event.EventSwitchEnter)
    def switch_enter_handler(self, event):
        switch = event.switch
        datapath = switch.dp
        self.switches[datapath.id] = SwitchState(switch)
        #send flow_mod_message
        parser = datapath.ofproto_parser
        ofproto = datapath.ofproto
        match = parser.OFPMatch()
        match.set_dl_type(ETH_TYPE_ARP)
        output = parser.OFPActionOutput(ofproto.OFPP_CONTROLLER,
                                        self.ARP_PACKET_LEN)
        write = parser.OFPInstructionActions(ofproto.OFPIT_WRITE_ACTIONS,
                                             [output])
        instructions = [write]
        flow_mod = self.create_flow_mod(datapath, 0, 0,
                                        match, instructions)
        datapath.send_msg(flow_mod)
        #db.insert()

    @handler.set_ev_cls(event.EventLinkAdd)
    def link_add_handler(self, link):
        self.link_list.append(link.link)
        port_src = link.link.src
        port_dst = link.link.dst
        switch_src = self.switches[port_src.dpid] 
        switch_dst = self.switches[port_dst.dpid]
        switch_src.linked_status[port_src.port_no] = True
        switch_dst.linked_status[port_dst.port_no] = True

    def handle_arp_packet(self, arppkt, dpid, port_no):
        #Fetch from dst_ip
        src_port = db.fetch('SELECT * FROM arp_table WHERE mac_addr = \'%s\''\
                            % (arppkt.src_mac))
        if not len(src_port):
            src_port = (dpid, port_no, arppkt.src_mac, arppkt.src_ip)
            db.execute('INSERT INTO arp_table (dpid, port_no, mac_addr,\
                        ip_addr) VALUES (\'%s\', \'%s\', \'%s\', \'%s\')'\
                        % src_port)
            src_port = [src_port]
            
        dst_port = db.fetch('SELECT * FROM arp_table WHERE ip_addr = \'%s\''\
                            % (arppkt.dst_ip))
        if len(dst_port):
            self.process_route(src_port[0], dst_port[0])

    def process_route(self, src_port, dst_port):
        path_list = PathList(self.link_list)
        LOG.info('%s, %s' % (src_port, dst_port))
        paths = path_list.createWholePath(src_port[0], dst_port[0])
        LOG.info(paths)

    @handler.set_ev_cls(event.EventArpReceived)
    def arp_received_handler(self, ev):
        LOG.info("ARP_RECEIVED")
        msg = ev.ev.msg
        datapath = msg.datapath
        in_port = msg.match['in_port']
        packet = Packet(msg.data)
        efm = packet.next()
        arppkt = packet.next()
        if arppkt.opcode == arp.ARP_REQUEST or\
           arppkt.opcode == arp.ARP_REPLY:
            self.handle_arp_packet(arppkt, datapath.id, in_port)

        for switch in self.switches.values():
            for port_no, status in switch.linked_status.items():
                if status == False:
                    self.arp_packet_out(switch.switch.dp, port_no, msg.data)

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
 
