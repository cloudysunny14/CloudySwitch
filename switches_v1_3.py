"""Simple layer-2 learning switch logic using OpenFlow Protocol v1.3."""

import logging
import time

from ryu.base.app_manager import RyuApp
from ryu.controller import ofp_event
from ryu.controller.handler import set_ev_cls
from ryu.controller.handler import CONFIG_DISPATCHER
from ryu.controller.handler import MAIN_DISPATCHER
from ryu.ofproto.ofproto_v1_2 import OFPG_ANY
from ryu.ofproto.ofproto_v1_3 import OFP_VERSION
from ryu.lib.mac import haddr_to_str, DONTCARE
from ryu.lib.port_no import port_no_to_str
from ryu.lib.dpid import dpid_to_str
from ryu.ofproto.ether import ETH_TYPE_LLDP
from ryu.topology.switches import LLDPPacket
from ryu.topology import event
from ryu.lib import hub

LOG = logging.getLogger("swiches_v1_3")

class L2Switch(RyuApp):
    _EVENTS = [event.EventSwitchEnter, event.EventSwitchLeave,
               event.EventPortAdd, event.EventPortDelete,
               event.EventPortModify,
               event.EventLinkAdd, event.EventLinkDelete]

    DEFAULT_TTL = 120
    OFP_VERSIONS = [OFP_VERSION]
    LLDP_PACKET_LEN = len(LLDPPacket.lldp_packet(0, 0, DONTCARE, 0))
    LLDP_SEND_GUARD = .05
    LLDP_SEND_PERIOD_PER_PORT = .9
    TIMEOUT_CHECK_PERIOD = 30.
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
                self.send_lldp_packet(port)
            for port in ports:
                self.send_lldp_packet(port)
                hub.sleep(self.LLDP_SEND_GUARD)      # don't burst

            if timeout is not None and ports:
                timeout = 0     # We have already slept
            LOG.debug('lldp sleep %s', timeout)
            self.lldp_event.wait(timeout=timeout)
  

    def link_loop(self):
        while self.is_active:
            self.link_event.clear()

            now = time.time()
            deleted = []
            for (link, timestamp) in self.links.items():
                LOG.debug('%s timestamp %d (now %d)', link, timestamp, now)
                if timestamp + self.LINK_TIMEOUT < now:
                    src = link.src
                    if src in self.ports:
                        port_data = self.ports.get_port(src)
                        if port_data.lldp_dropped() > self.LINK_LLDP_DROP:
                            deleted.append(link)

            for link in deleted:
                self.links.link_down(link)
                LOG.debug('delete %s', link)
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
        self._register(datapath)
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

    def send_lldp_packet(self, datapath, port_no, dl_addr):
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

    @set_ev_cls(ofp_event.EventOFPPortDescStatsReply, MAIN_DISPATCHER)
    def port_desc_stats_reply_handler(self, ev):
        msg = ev.msg
        datapath = msg.datapath
        self.show_port_desc(msg.body, datapath)
    
    def show_port_desc(self, body, datapath):
        self.port_state[datapath.id] = PortState()
        for p in body:
            self.port_state[datapath.id].add(p.port_no, p)
            self.send_lldp_packet(datapath, p.port_no, haddr_to_str(p.hw_addr))
            port = self._get_port(datapath.id, p.port_no)
            if port and not port.is_reserved():
                self._port_added(port)
                self.lldp_event.set()
            
    def send_port_desc_stats_request(self, datapath):
        ofp_parser = datapath.ofproto_parser
        req = ofp_parser.OFPPortDescStatsRequest(datapath, 0)
        datapath.send_msg(req)
    
    @set_ev_cls(ofp_event.EventOFPBarrierReply, MAIN_DISPATCHER)
    def barrier_reply_handler(self, ev):
        msg = ev.msg
        datapath = msg.datapath
        self.send_port_desc_stats_request(datapath)

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
        fields = msg.match.fields
        datapath = msg.datapath
        ofproto = datapath.ofproto
        
        for f in fields:
            if f.header == ofproto.OXM_OF_IN_PORT:
                in_port = f.value
        
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
            LOG.debug("Packet-In")
            LOG.debug("  src=%s", src)
            LOG.debug("  dst=%s", dst)
            LOG.debug("  old_peer=%s", old_peer)
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

class PortState(dict):
    # dict: int port_no -> OFPPort port
    # OFPPort is defined in ryu.ofproto.ofproto_v1_X_parser
    def __init__(self):
        super(PortState, self).__init__()

    def add(self, port_no, port):
        self[port_no] = port

    def remove(self, port_no):
        del self[port_no]

    def modify(self, port_no, port):
        self[port_no] = port

class Port(object):
    # This is data class passed by EventPortXXX
    def __init__(self, dpid, ofproto, ofpport):
        super(Port, self).__init__()

        self.dpid = dpid
        self._ofproto = ofproto
        self._config = ofpport.config
        self._state = ofpport.state

        self.port_no = ofpport.port_no
        self.hw_addr = ofpport.hw_addr
        self.name = ofpport.name

    def is_reserved(self):
        return self.port_no > self._ofproto.OFPP_MAX

    def is_down(self):
        return (self._state & self._ofproto.OFPPS_LINK_DOWN) > 0 \
            or (self._config & self._ofproto.OFPPC_PORT_DOWN) > 0

    def is_live(self):
        # NOTE: OF1.2 has OFPPS_LIVE state
        #       return (self._state & self._ofproto.OFPPS_LIVE) > 0
        return not self.is_down()

    def to_dict(self):
        return {'dpid': dpid_to_str(self.dpid),
                'port_no': port_no_to_str(self.port_no),
                'hw_addr': haddr_to_str(self.hw_addr),
                'name': self.name.rstrip('\0')}

    # for Switch.del_port()
    def __eq__(self, other):
        return self.dpid == other.dpid and self.port_no == other.port_no

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        return hash((self.dpid, self.port_no))

    def __str__(self):
        LIVE_MSG = {False: 'DOWN', True: 'LIVE'}
        return 'Port<dpid=%s, port_no=%s, %s>' % \
            (self.dpid, self.port_no, LIVE_MSG[self.is_live()])

class PortData(object):
    def __init__(self, is_down, lldp_data):
        super(PortData, self).__init__()
        self.is_down = is_down
        self.lldp_data = lldp_data
        self.timestamp = None
        self.sent = 0

    def lldp_sent(self):
        self.timestamp = time.time()
        self.sent += 1

    def lldp_received(self):
        self.sent = 0

    def lldp_dropped(self):
        return self.sent

    def clear_timestamp(self):
        self.timestamp = None

    def set_down(self, is_down):
        self.is_down = is_down

    def __str__(self):
        return 'PortData<live=%s, timestamp=%s, sent=%d>' \
            % (not self.is_down, self.timestamp, self.sent)

class PortDataState(dict):
    # dict: Port class -> PortData class
    # slimed down version of OrderedDict as python 2.6 doesn't support it.
    _PREV = 0
    _NEXT = 1
    _KEY = 2

    def __init__(self):
        super(PortDataState, self).__init__()
        self._root = root = []          # sentinel node
        root[:] = [root, root, None]    # [_PREV, _NEXT, _KEY]
                                        # doubly linked list
        self._map = {}

    def _remove_key(self, key):
        link_prev, link_next, key = self._map.pop(key)
        link_prev[self._NEXT] = link_next
        link_next[self._PREV] = link_prev

    def _append_key(self, key):
        root = self._root
        last = root[self._PREV]
        last[self._NEXT] = root[self._PREV] = self._map[key] = [last, root,
                                                                key]

    def _prepend_key(self, key):
        root = self._root
        first = root[self._NEXT]
        first[self._PREV] = root[self._NEXT] = self._map[key] = [root, first,
                                                                 key]

    def _move_last_key(self, key):
        self._remove_key(key)
        self._append_key(key)

    def _move_front_key(self, key):
        self._remove_key(key)
        self._prepend_key(key)

    def add_port(self, port, lldp_data):
        if port not in self:
            self._prepend_key(port)
            self[port] = PortData(port.is_down(), lldp_data)
        else:
            self[port].is_down = port.is_down()

    def lldp_sent(self, port):
        port_data = self[port]
        port_data.lldp_sent()
        self._move_last_key(port)
        return port_data

    def lldp_received(self, port):
        self[port].lldp_received()

    def move_front(self, port):
        port_data = self.get(port, None)
        if port_data is not None:
            port_data.clear_timestamp()
            self._move_front_key(port)

    def set_down(self, port):
        is_down = port.is_down()
        port_data = self[port]
        port_data.set_down(is_down)
        port_data.clear_timestamp()
        if not is_down:
            self._move_front_key(port)
        return is_down

    def get_port(self, port):
        return self[port]

    def del_port(self, port):
        del self[port]
        self._remove_key(port)

    def __iter__(self):
        root = self._root
        curr = root[self._NEXT]
        while curr is not root:
            yield curr[self._KEY]
            curr = curr[self._NEXT]

    def clear(self):
        for node in self._map.itervalues():
            del node[:]
        root = self._root
        root[:] = [root, root, None]
        self._map.clear()
        dict.clear(self)

    def items(self):
        'od.items() -> list of (key, value) pairs in od'
        return [(key, self[key]) for key in self]

    def iteritems(self):
        'od.iteritems -> an iterator over the (key, value) pairs in od'
        for k in self:
            yield (k, self[k])

class LinkState(dict):
    # dict: Link class -> timestamp
    def __init__(self):
        super(LinkState, self).__init__()
        self._map = {}

    def get_peer(self, src):
        return self._map.get(src, None)

    def update_link(self, src, dst):
        link = Link(src, dst)

        self[link] = time.time()
        self._map[src] = dst

        # return if the reverse link is also up or not
        rev_link = Link(dst, src)
        return rev_link in self

    def link_down(self, link):
        del self[link]
        del self._map[link.src]

    def rev_link_set_timestamp(self, rev_link, timestamp):
        # rev_link may or may not in LinkSet
        if rev_link in self:
            self[rev_link] = timestamp

    def port_deleted(self, src):
        dst = self.get_peer(src)
        if dst is None:
            raise KeyError()

        link = Link(src, dst)
        rev_link = Link(dst, src)
        del self[link]
        del self._map[src]
        # reverse link might not exist
        self.pop(rev_link, None)
        rev_link_dst = self._map.pop(dst, None)

        return dst, rev_link_dst

class Switch(object):
    # This is data class passed by EventSwitchXXX
    def __init__(self, dp):
        super(Switch, self).__init__()

        self.dp = dp
        self.ports = []

    def add_port(self, ofpport):
        port = Port(self.dp.id, self.dp.ofproto, ofpport)
        if not port.is_reserved():
            self.ports.append(port)

    def del_port(self, ofpport):
        self.ports.remove(Port(ofpport))

    def to_dict(self):
        d = {'dpid': dpid_to_str(self.dp.id),
             'ports': [port.to_dict() for port in self.ports]}
        return d

    def __str__(self):
        msg = 'Switch<dpid=%s, ' % self.dp.id
        for port in self.ports:
            msg += str(port) + ' '

        msg += '>'
        return msg

class Link(object):
    # This is data class passed by EventLinkXXX
    def __init__(self, src, dst):
        super(Link, self).__init__()
        self.src = src
        self.dst = dst

    def to_dict(self):
        d = {'src': self.src.to_dict(),
             'dst': self.dst.to_dict()}
        return d

    # this type is used for key value of LinkState
    def __eq__(self, other):
        return self.src == other.src and self.dst == other.dst

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        return hash((self.src, self.dst))

    def __str__(self):
        return 'Link: %s to %s' % (self.src, self.dst)

