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

from ryu.ofproto import ofproto_v1_3_parser
from ryu.ofproto import ofproto_v1_3
from ryu.topology.switches import Port, Link

def createPort(dpid, port_no):
    hw_addr = 'c0:26:53:c4:29:e2'
    name = 'name'.ljust(16)
    config = 2226555987
    state = 1678244809
    curr = 2850556459
    advertised = 2025421682
    supported = 2120575149
    peer = 2757463021
    curr_speed = 2641353507
    max_speed = 1797291672

    ofpport = ofproto_v1_3_parser.OFPPort(port_no, hw_addr, name, config,\
                                          state, curr, advertised, supported,\
                                          peer, curr_speed, max_speed)
    port = Port(dpid, ofproto_v1_3, ofpport)
    return port

def createLink(src_dpid, src_port_no, dst_dpid, dst_port_no):
    port_src = createPort(src_dpid, src_port_no)
    port_dst = createPort(dst_dpid, dst_port_no)
    return Link(port_src, port_dst)

def getLinkList(links):
    link_list = []
    for (src_dpid, src_port, dst_dpid, dst_port) in links:
        link = createLink(src_dpid, src_port, dst_dpid, dst_port)
        link_list.append(link)
    return link_list
