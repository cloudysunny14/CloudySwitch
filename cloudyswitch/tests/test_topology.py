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

import unittest
import logging
from nose.tools import eq_
from struct import *
from ryu.ofproto import ofproto_v1_3_parser
from ryu.ofproto import ofproto_v1_3
from ryu.topology.switches import Port, Link

from app.topology_util import PathList 

LOG = logging.getLogger(__name__)

class Test_topology_util(unittest.TestCase):
    """ Test case for ryu.lib.addrconv
    """

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def _createPort(self, dpid, port_no):
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

    def _createLink(self, src_dpid, src_port_no, dst_dpid, dst_port_no):
        port_src = self._createPort(src_dpid, src_port_no)
        port_dst = self._createPort(dst_dpid, dst_port_no)
        return Link(port_src, port_dst)

    def _getLinkList(self, links):
        link_list = []
        for (src_dpid, src_port, dst_dpid, dst_port) in links:
            link = self._createLink(src_dpid, src_port, dst_dpid, dst_port)
            link_list.append(link)
        return link_list

    def testLink(self):
        links = [(3, 1, 1, 2), (2, 4, 5, 2), (2, 3, 4, 2), (5, 2, 2, 4),\
                 (1, 4, 5, 1), (2, 2, 3, 2), (3, 2, 2, 2), (1, 2, 3, 1),\
                 (1, 3, 4, 1), (4, 1, 1, 3), (4, 2, 2, 3), (1, 1, 2, 1),\
                 (5, 1, 1, 4), (2, 1, 1, 1)]
        link_list = self._getLinkList(links)
        src_port = self._createPort(4, 3)
        dst_port = self._createPort(5, 3)
        path_list = PathList(link_list)
        paths = path_list.createWholePath(src_port.dpid, dst_port.dpid) 
        expected_pathes = [[(4, 1), (1, 3), (1, 4), (5, 1)],
                           [(4, 2), (2, 3), (2, 4), (5, 2)],
                           [(4, 1), (1, 3), (1, 1), (2, 1), (2, 4), (5, 2)],
                           [(4, 2), (2, 3), (2, 1), (1, 1), (1, 4), (5, 1)],
                           [(4, 1), (1, 3), (1, 2), (3, 1), (3, 2), (2, 2),\
                            (2, 4), (5, 2)],
                           [(4, 2), (2, 3), (2, 2), (3, 2), (3, 1), (1, 2),\
                            (1, 4), (5, 1)]]
        for index in range(len(paths)):
            path = paths[index]
            expected_path = expected_pathes[index]
            for port_index in range(len(path)):
                eq_(expected_path[port_index][0], path[port_index].dpid)
                eq_(expected_path[port_index][1], path[port_index].port_no)

if __name__ == '__main__':
    unittest.main()
