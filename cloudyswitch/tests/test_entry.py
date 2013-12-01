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
from ryu.ofproto import ofproto_v1_3_parser
from ryu.ofproto import ofproto_v1_3

from app import db
from app.topology_util import PathList
import test_util

LOG = logging.getLogger(__name__)

class _Datapath(object):
    ofproto = ofproto_v1_3
    ofproto_parser = ofproto_v1_3_parser
    
class Test_entry(unittest.TestCase):
    """ Test case for cloudyswitch.entry
    """

    def setUp(self):
        db.clean_tables()
        db.fetch('select setval (\'label_table_label_seq\', 1, false)')
        db.fetch('select setval (\'path_table_path_id_seq\', 1, false)')
        db.fetch('select setval (\'group_table_group_id_seq\', 1, false)')
        pass

    def tearDown(self):
        pass

    def _createSwitches(self):
        return None

    def _compareMatchField(self, match):
        jsondict = match.to_jsondict()
        # from_jsondict
        match2 = match.from_jsondict(jsondict["OFPMatch"])
        buf2 = bytearray()
        match2.serialize(buf2, 0)
        eq_(str(match), str(match2))
        
    def _add_flow(self, dp, match, actions):
        inst = [dp.ofproto_parser.OFPInstructionActions(
            dp.ofproto.OFPIT_APPLY_ACTIONS, actions)]

        mod = dp.ofproto_parser.OFPFlowMod(
            dp, cookie=0, cookie_mask=0, table_id=0,
            command=dp.ofproto.OFPFC_ADD, idle_timeout=0, hard_timeout=0,
            priority=0xff, buffer_id=0xffffffff,
            out_port=dp.ofproto.OFPP_ANY, out_group=dp.ofproto.OFPG_ANY,
            flags=0, match=match, instructions=inst)
        return mod

    def _createPaths(self, src_port, dst_port):
        links = [(3, 1, 1, 2), (2, 4, 5, 2), (2, 3, 4, 2), (5, 2, 2, 4),\
                 (1, 4, 5, 1), (2, 2, 3, 2), (3, 2, 2, 2), (1, 2, 3, 1),\
                 (1, 3, 4, 1), (4, 1, 1, 3), (4, 2, 2, 3), (1, 1, 2, 1),\
                 (5, 1, 1, 4), (2, 1, 1, 1)]
        link_list = test_util.getLinkList(links)
        path_list = PathList(link_list)
        paths = path_list.createWholePath(src_port.dpid, dst_port.dpid)
        return paths
    
    def testLabelFlowEntry(self):
        src_port = test_util.createPort(4, 3)
        dst_port = test_util.createPort(5, 3)
        paths = self._createPaths(src_port, dst_port)
        src_port_arp_table = (4, 3, '62:1e:dd:aa:41:9e', '10.0.0.2')
        dst_port_arp_table = (5, 3, '96:06:4d:e3:70:50', '10.0.0.3')
        path_ids = db.handle_paths(paths, src_port_arp_table,
                                   dst_port_arp_table)
        label_flows, last_label = db.fetch_label_flows(path_ids[0][0])
        eq_([(1, 4, 1, 1, 3, 1, -1, 5), (1, 1, 4, 5, 1, 2, 1, 5)],
            label_flows)
        paths = self._createPaths(dst_port, src_port)
        path_ids = db.handle_paths(paths, dst_port_arp_table,
                                   src_port_arp_table)
        label_flows, last_label = db.fetch_label_flows(path_ids[0][0])
        eq_([(7, 5, 2, 2, 4, 3, -1, 4), (7, 2, 3, 4, 2, 4, 3, 4)],
            label_flows)

        src_port = test_util.createPort(4, 3)
        dst_port = test_util.createPort(3, 3)
        paths = self._createPaths(src_port, dst_port)
        src_port_arp_table = (4, 3, '62:1e:dd:aa:41:9e', '10.0.0.2')
        dst_port_arp_table = (3, 3, '96:06:4d:e3:70:53', '10.0.0.4')
        path_ids = db.handle_paths(paths, src_port_arp_table,
                                   dst_port_arp_table)
        label_flows, last_label = db.fetch_label_flows(path_ids[0][0])
        eq_([(13, 4, 1, 1, 3, 5, -1, 3), (13, 1, 2, 3, 1, 6, 5, 3)],
            label_flows)

        src_port = test_util.createPort(5, 3)
        dst_port = test_util.createPort(3, 3)
        paths = self._createPaths(src_port, dst_port)
        src_port_arp_table = (5, 3, '96:06:4d:e3:70:50', '10.0.0.3')
        dst_port_arp_table = (3, 3, '96:06:4d:e3:70:53', '10.0.0.4')
        path_ids = db.handle_paths(paths, src_port_arp_table,
                                   dst_port_arp_table)
        label_flows, label_label = db.fetch_label_flows(path_ids[1][0])
        eq_([(20, 5, 1, 1, 4, 7, -1, 3), (20, 1, 2, 3, 1, 8, 7, 3)],
            label_flows)

    def testGroupFlowEntry(self):
        src_port = test_util.createPort(4, 3)
        dst_port = test_util.createPort(5, 3)
        paths = self._createPaths(src_port, dst_port)
        src_port_arp_table = (4, 3, '62:1e:dd:aa:41:9e', '10.0.0.2')
        dst_port_arp_table = (5, 3, '96:06:4d:e3:70:50', '10.0.0.3')
        path_ids = db.handle_paths(paths, src_port_arp_table,
                                   dst_port_arp_table)
        p_path = path_ids[0][0]
        b_path = path_ids[1][0]
        group_flows = db.fetch_group_flows((p_path, b_path))
        expect_flows = {'last_label': [2L, 4L], 
                        'group_flow': {'buckets': [{'watch': 1, 'label': (1, 1, -1)},
                        {'watch': 2, 'label': (2, 3, -1)}], 'group_id': 1L, 'dpid': 4},
                        'label_flow': [(1, 1, 4, 5, 1, 2, 1, 5),
                          (2, 2, 4, 5, 2, 4, 3, 5)]}
        eq_(group_flows, expect_flows)

    def testDetectRequireModifyPaths(self):
        src_port = test_util.createPort(4, 3)
        dst_port = test_util.createPort(5, 3)
        paths = self._createPaths(src_port, dst_port)
        src_port_arp_table = (4, 3, '62:1e:dd:aa:41:9e', '10.0.0.2')
        dst_port_arp_table = (5, 3, '96:06:4d:e3:70:50', '10.0.0.3')
        #Assume that already created paths
        path_ids = db.handle_paths(paths, src_port_arp_table,
                                   dst_port_arp_table)
        p_path = path_ids[0][0]
        b_path = path_ids[1][0]
        db.fetch_group_flows((p_path, b_path))
        flow_mod = db.detect_require_modify_paths(1, 4)
        expect_flow = [{'buckets': [{'watch': 2, 'label': (2, 3, -1)}],
                         'group_id': 1, 'dpid': 4}]
        eq_(flow_mod, expect_flow)

if __name__ == '__main__':
    unittest.main()

