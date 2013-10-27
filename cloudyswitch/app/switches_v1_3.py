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
import time

from ryu.base.app_manager import RyuApp
from ryu.controller import ofp_event
from ryu.controller.handler import set_ev_cls
from ryu.controller.handler import CONFIG_DISPATCHER, MAIN_DISPATCHER
from ryu.ofproto.ofproto_v1_2 import OFPG_ANY
from ryu.ofproto.ofproto_v1_3 import OFP_VERSION
from ryu.lib.mac import DONTCARE_STR
from ryu.ofproto.ether import ETH_TYPE_LLDP
from ryu.ofproto import ether
from ryu.lib.packet.packet import Packet
from ryu.topology.switches import LLDPPacket
from ryu.topology.switches import Port, PortState
from ryu.topology.switches import PortDataState
from ryu.topology.switches import Link, LinkState
from ryu.topology.switches import Switch
from ryu.lib import hub

import event

LOG = logging.getLogger("switches_v1_3")

class L2Switch(RyuApp):
    _EVENTS = [event.EventSwitchEnter, event.EventSwitchLeave,
               event.EventPortAdd, event.EventPortDelete,
               event.EventPortModify,
               event.EventLinkAdd, event.EventLinkDelete,
               event.EventArpReceived]

    OFP_VERSIONS = [OFP_VERSION]
    
    DEFAULT_TTL = 120  # unused. ignored.
    LLDP_PACKET_LEN = len(LLDPPacket.lldp_packet(0, 0, DONTCARE_STR, 0))

    LLDP_SEND_GUARD = .05
    LLDP_SEND_PERIOD_PER_PORT = .9
    TIMEOUT_CHECK_PERIOD = 15.
    LINK_TIMEOUT = TIMEOUT_CHECK_PERIOD * 2
    LINK_LLDP_DROP = 5

    def __init__(self, *args, **kwargs):
        super(L2Switch, self).__init__(*args, **kwargs)
        self.name = 'switches'
        self.dps = {}                 # datapath_id => Datapath class
        self.port_state = {}          # datapath_id => ports
        self.ports = PortDataState()  # Port class -> PortData class
        self.links = LinkState()      # Link class -> timestamp
        self.is_active = True
        self.lldp_event = hub.Event()
        self.link_event = hub.Event()
        self.threads.append(hub.spawn(self.lldp_loop))
        self.threads.append(hub.spawn(self.link_loop))
        self.link_discovery = True
    
    def close(self):
        self.is_active = False
        if self.link_discovery:
            self.lldp_event.set()
            self.link_event.set()
            hub.joinall(self.threads)

    def send_flow_stats_request(self, datapath):
        ofp = datapath.ofproto
        ofp_parser = datapath.ofproto_parser

        cookie = cookie_mask = 0
        match = ofp_parser.OFPMatch()
        req = ofp_parser.OFPFlowStatsRequest(datapath, 0,
                                         ofp.OFPTT_ALL,
                                         ofp.OFPP_ANY, ofp.OFPG_ANY,
                                         cookie, cookie_mask,
                                         match)
        if datapath.id == 4:
            datapath.send_msg(req)

    @set_ev_cls(ofp_event.EventOFPFlowStatsReply, MAIN_DISPATCHER)
    def flow_stats_reply_handler(self, ev):
        flows = []
        for stat in ev.msg.body:
            flows.append('table_id=%s '
                     'duration_sec=%d duration_nsec=%d '
                     'priority=%d '
                     'idle_timeout=%d hard_timeout=%d flags=0x%04x '
                     'cookie=%d packet_count=%d byte_count=%d '
                     'match=%s instructions=%s' %
                     (stat.table_id,
                      stat.duration_sec, stat.duration_nsec,
                      stat.priority,
                      stat.idle_timeout, stat.hard_timeout, stat.flags,
                      stat.cookie, stat.packet_count, stat.byte_count,
                      stat.match, stat.instructions))
        LOG.debug('FlowStats: %s', flows)

    def lldp_loop(self):
        while self.is_active:
            self.lldp_event.clear()
            now = time.time()
            timeout = None
            ports_now = []
            ports = []
            for (key, data) in self.ports.items():
                if data.timestamp is None:
                    ports_now.append(key)
                    continue

                expire = data.timestamp + self.LLDP_SEND_PERIOD_PER_PORT
                if expire <= now:
                    ports.append(key)
                    continue

                timeout = expire - now
                break
            for port in ports_now:
                self.send_lldp_packet(port.dpid, 
                                      port.port_no,
                                      port.hw_addr)
            for port in ports:
                self.send_lldp_packet(port.dpid,
                                      port.port_no,
                                      port.hw_addr)
                hub.sleep(self.LLDP_SEND_GUARD)      # don't burst

            if timeout is not None and ports:
                timeout = 0     # We have already slept
            self.lldp_event.wait(timeout=timeout)
  
    def link_loop(self):
        while self.is_active:
            self.link_event.clear()

            now = time.time()
            deleted = []
            for (link, timestamp) in self.links.items():
                if timestamp + self.LINK_TIMEOUT < now:
                    src = link.src
                    if src in self.ports:
                        port_data = self.ports.get_port(src)
                        if port_data.lldp_dropped() > self.LINK_LLDP_DROP:
                            deleted.append(link)
                for dp in self.dps.values():
                    self.send_flow_stats_request(dp)

            for link in deleted:
                self.links.link_down(link)
                self.send_event_to_observers(event.EventLinkDelete(link))

                dst = link.dst
                rev_link = Link(dst, link.src)
                if rev_link not in deleted:
                    # It is very likely that the reverse link is also
                    # disconnected. Check it early.
                    expire = now - self.LINK_TIMEOUT
                    self.links.rev_link_set_timestamp(rev_link, expire)
                    if dst in self.ports:
                        self.ports.move_front(dst)
                        self.lldp_event.set()

            self.link_event.wait(timeout=self.TIMEOUT_CHECK_PERIOD)

    def _get_switch(self, dpid):
        if dpid in self.dps:
            switch = Switch(self.dps[dpid])
            for ofpport in self.port_state[dpid].itervalues():
                switch.add_port(ofpport)
            return switch

    def _get_port(self, dpid, port_no):
        switch = self._get_switch(dpid)
        if switch:
            for p in switch.ports:
                if p.port_no == port_no:
                    return p

    def _port_added(self, port):
        lldp_data = LLDPPacket.lldp_packet(
            port.dpid, port.port_no, port.hw_addr, self.DEFAULT_TTL)
        self.ports.add_port(port, lldp_data)
        LOG.debug('_port_added dpid=%s, port_no=%s, live=%s',
                 port.dpid, port.port_no, port.is_live())

    def _register(self, dp):
        assert dp.id is not None
        assert dp.id not in self.dps

        self.dps[dp.id] = dp
        self.port_state[dp.id] = PortState()
        for port in dp.ports.values():
            self.port_state[dp.id].add(port.port_no, port)
        
    def _link_down(self, port):
        try:
            dst, rev_link_dst = self.links.port_deleted(port)
        except KeyError:
            # LOG.debug('key error. src=%s, dst=%s',
            #           port, self.links.get_peer(port))
            return
        link = Link(port, dst)
        self.send_event_to_observers(event.EventLinkDelete(link))
        if rev_link_dst:
            rev_link = Link(dst, rev_link_dst)
            self.send_event_to_observers(event.EventLinkDelete(rev_link))
        self.ports.move_front(dst)

    @set_ev_cls(ofp_event.EventOFPPortStatus, MAIN_DISPATCHER)
    def port_status_handler(self, ev):
        msg = ev.msg
        reason = msg.reason
        dp = msg.datapath
        ofpport = msg.desc

        if reason == dp.ofproto.OFPPR_ADD:
            LOG.debug('A port was added.' +
                      '(datapath id = %s, port number = %s)',
                      dp.id, ofpport.port_no)
            self.port_state[dp.id].add(ofpport.port_no, ofpport)
            self.send_event_to_observers(
                event.EventPortAdd(Port(dp.id, dp.ofproto, ofpport)))

            if not self.link_discovery:
                return

            port = self._get_port(dp.id, ofpport.port_no)
            if port and not port.is_reserved():
                self._port_added(port)
                self.lldp_event.set()

        elif reason == dp.ofproto.OFPPR_DELETE:
            LOG.debug('A port was deleted.' +
                      '(datapath id = %s, port number = %s)',
                      dp.id, ofpport.port_no)
            self.port_state[dp.id].remove(ofpport.port_no)
            self.send_event_to_observers(
                event.EventPortDelete(Port(dp.id, dp.ofproto, ofpport)))

            if not self.link_discovery:
                return

            port = self._get_port(dp.id, ofpport.port_no)
            if port and not port.is_reserved():
                self.ports.del_port(port)
                self._link_down(port)
                self.lldp_event.set()

        else:
            assert reason == dp.ofproto.OFPPR_MODIFY
            LOG.debug('A port was modified.' +
                      '(datapath id = %s, port number = %s)',
                      dp.id, ofpport.port_no)
            self.port_state[dp.id].modify(ofpport.port_no, ofpport)
            self.send_event_to_observers(
                event.EventPortModify(Port(dp.id, dp.ofproto, ofpport)))

            if not self.link_discovery:
                return

            port = self._get_port(dp.id, ofpport.port_no)
            if port and not port.is_reserved():
                if self.ports.set_down(port):
                    self._link_down(port)
                    self.lldp_event.set()

    @set_ev_cls(ofp_event.EventOFPSwitchFeatures, CONFIG_DISPATCHER)
    def switch_features_handler(self, ev):
        """Handle switch features reply to install table miss flow entries."""
        datapath = ev.msg.datapath
        self.install_table_miss(datapath, 0)

    def create_match(self, parser, fields):
        """Create OFP match struct from the list of fields."""
        match = parser.OFPMatch()
        for a in fields:
            match.append_field(*a)
        return match

    def send_barrier_request(self, datapath):
        ofp_parser = datapath.ofproto_parser
        req = ofp_parser.OFPBarrierRequest(datapath)
        datapath.send_msg(req)

    def send_lldp_packet(self, datapath_id, port_no, dl_addr):
        datapath = self.dps.get(datapath_id, None)
        if datapath is None:
            #datapath was already deleted
            return
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        lldp_data = LLDPPacket.lldp_packet(datapath.id,
                        port_no,
                        dl_addr,
                        self.DEFAULT_TTL)
        output_port = parser.OFPActionOutput(port_no,
                                            ofproto.OFPCML_NO_BUFFER)
        packet_out = parser.OFPPacketOut(datapath, ofproto.OFPP_ANY,
                                          ofproto.OFPP_CONTROLLER,
                                          [output_port], lldp_data)
        datapath.send_msg(packet_out)

    @set_ev_cls(ofp_event.EventOFPBarrierReply, MAIN_DISPATCHER)
    def barrier_reply_handler(self, ev):
        msg = ev.msg
        datapath = msg.datapath
        self._register(datapath)
        switch = self._get_switch(datapath.id)
        self.send_event_to_observers(event.EventSwitchEnter(switch))
        for port in switch.ports:
            if not port.is_reserved():
                self._port_added(port)
        self.lldp_event.set()
        
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
    
    def install_table_miss(self, datapath, table_id):
        """Create and install table miss flow entries."""
        parser = datapath.ofproto_parser
        ofproto = datapath.ofproto
        match = parser.OFPMatch()
        match.set_dl_type(ETH_TYPE_LLDP)
        output = parser.OFPActionOutput(ofproto.OFPP_CONTROLLER,
                                        self.LLDP_PACKET_LEN)
        write = parser.OFPInstructionActions(ofproto.OFPIT_WRITE_ACTIONS,
                                             [output])
        instructions = [write]
        flow_mod = self.create_flow_mod(datapath, 0, table_id,
                                        match, instructions)
        datapath.send_msg(flow_mod)
        self.send_barrier_request(datapath)
    
    @set_ev_cls(ofp_event.EventOFPPacketIn, MAIN_DISPATCHER)
    def _packet_in_handler(self, ev):
        """Handle packet_in events."""
        if not self.link_discovery:
            return
        msg = ev.msg
        in_port = msg.match['in_port']
        packet = Packet(msg.data)
        efm = packet.next()
        if efm.ethertype == ether.ETH_TYPE_ARP:
            self.send_event_to_observers(event.EventArpReceived(ev))

        try:
            src_dpid, src_port_no = LLDPPacket.lldp_parse(msg.data)
        except LLDPPacket.LLDPUnknownFormat as e:
            # This handler can receive all the packtes which can be
            # not-LLDP packet. Ignore it silently
            return
        else:
            dst_dpid = msg.datapath.id
            dst_port_no = in_port

            src = self._get_port(src_dpid, src_port_no)
            if not src or src.dpid == dst_dpid:
                return

            dst = self._get_port(dst_dpid, dst_port_no)
            if not dst:
                return

            old_peer = self.links.get_peer(src)
            #LOG.debug("Packet-In")
            #LOG.debug("  src=%s", src)
            #LOG.debug("  dst=%s", dst)
            #LOG.debug("  old_peer=%s", old_peer)
            if old_peer and old_peer != dst:
                old_link = Link(src, old_peer)
                self.send_event_to_observers(event.EventLinkDelete(old_link))

            link = Link(src, dst)
            if not link in self.links:
                self.send_event_to_observers(event.EventLinkAdd(link))

            if not self.links.update_link(src, dst):
                # reverse link is not detected yet.
                # So schedule the check early because it's very likely it's up
                try:
                    self.ports.lldp_received(dst)
                except KeyError as e:
                    # There are races between EventOFPPacketIn and
                    # EventDPPortAdd. So packet-in event can happend before
                    # port add event. In that case key error can happend.
                    # LOG.debug('lldp_received: KeyError %s', e)
                    pass
                else:
                    self.ports.move_front(dst)
                    self.lldp_event.set()

