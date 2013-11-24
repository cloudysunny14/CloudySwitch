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
import psyco_eventlet
from ryu.exception import RyuException

psyco_eventlet.make_psycopg_green()

import psycopg2

LOG = logging.getLogger("db")
#TODO Required to configure for db connecting.
conn = psycopg2.connect("dbname=ryu user=postgres")
#ISOLATION_LEVE_READ_COMMITTED is default setting.
#conn.set_isolation_level(ISOLATION_LEVEL_READ_COMMITTED)

class ArpTableNotFoundException(RyuException):
    message = '%(msg)s'

class GroupAlreadyExistException(RyuException):
    message = '%(msg)s'

def fetch(query):
    cur = conn.cursor()
    cur.execute(query)
    return cur.fetchall()

def execute(cmd):
    curs = conn.cursor()
    curs.execute(cmd)

def commit():
    conn.commit()

def clean_tables():
    curs = conn.cursor()
    curs.execute('DELETE FROM arp_table')
    curs.execute('DELETE FROM path_desc_table')
    curs.execute('DELETE FROM label_table')
    curs.execute('DELETE FROM path_table')
    curs.execute('DELETE FROM group_table')
    commit()

def handle_arp_packet(arppkt, dpid, port_no):
    #Fetch from dst_ip
    src_port = fetch('SELECT * FROM arp_table WHERE mac_addr = \'%s\''\
                        % (arppkt.src_mac))
    if not len(src_port):
        src_port = (dpid, port_no, arppkt.src_mac, arppkt.src_ip)
        execute('INSERT INTO arp_table (dpid, port_no, mac_addr,\
                 ip_addr) VALUES (%d, %d, \'%s\', \'%s\')'\
                 % src_port)
        commit()
        src_port = [src_port]
            
    dst_port = fetch('SELECT * FROM arp_table WHERE ip_addr = \'%s\''\
                     % (arppkt.dst_ip))
    if len(dst_port):
        return src_port[0], dst_port[0]
    else:
        raise ArpTableNotFoundException(
              msg='NotFound dst_port in arptable')

def fetch_path_id(src_port, dst_port):
    paths = fetch('SELECT path_id FROM path_table WHERE src_dpid = %d AND\
                  src_port_no = %d AND dst_dpid = %d AND\
                  dst_port_no = %d' % (src_port[0], src_port[1],
                  dst_port[0], dst_port[1]))
    return paths 

def register_path(path, src_port, dst_port):
    execute('INSERT INTO path_table (src_dpid, src_port_no, \
             dst_dpid, dst_port_no, cost)\
             VALUES (%d, %d, %d, %d, %d)' %
             (src_port[0], src_port[1],
              dst_port[0], dst_port[1], len(path)))
    path_id = fetch('SELECT currval(\'path_table_path_id_seq\')')
    path_seq = 0
    for port in path:
        execute('INSERT INTO path_desc_table (path_id, path_seq,\
                 dpid, port_no)\
                 VALUES (%d, %d, %d, %d)' % 
                 (path_id[0][0], path_seq, port.dpid, port.port_no))
        path_seq += 1
    return path_id[0]

def handle_paths(path_list, src_port, dst_port, is_detect_exists=False):
    #Confirm paths that already exists
    path_ids = fetch_path_id(src_port, dst_port)
    if not len(path_ids):
        for path in path_list:
            path_id = register_path(path, src_port, dst_port)
            path_ids.append(path_id)
        commit()
    return path_ids

def register_label(path_id, src_port, dst_port, prev_label, target_dst_dpid):
    registered_label = fetch('SELECT * FROM label_table WHERE\
                              src_dpid = %d AND src_port_no = %d AND\
                              dst_dpid = %d AND dst_port_no = %d AND\
                              target_dst_dpid = %d AND prev_label = %d' % 
                              (src_port[2], src_port[3], dst_port[2], dst_port[3],
                               target_dst_dpid, prev_label))
    #if not len(registered_label):
    execute('INSERT INTO label_table (path_id, src_dpid, src_port_no, \
                 dst_dpid, dst_port_no, prev_label, target_dst_dpid)\
                 VALUES (%d, %d, %d, %d, %d, %d, %d)' %
                 (path_id, src_port[2], src_port[3],
                  dst_port[2], dst_port[3], prev_label, target_dst_dpid))
    label = fetch('SELECT currval(\'label_table_label_seq\')')
    registered_label = (path_id, src_port[2], src_port[3], dst_port[2],
                            dst_port[3], label[0][0], prev_label, False, target_dst_dpid)
    #else:
    #    registered_label = registered_label[0]
        #common path found
    #    if registered_label[6] != -1 and prev_label != -1:
    #        execute('UPDATE label_table SET label = %d WHERE label = %d AND\
    #                 path_id = %d' %
    #                 (registered_label[6], prev_label, path_id))
    return registered_label
        
def create_port_set(path_ports, num = 2):
  if not path_ports:
      return []
  return [path_ports[:num]] + create_port_set(path_ports[num:], num)
    
def fetch_label_flows(path_id):
    path = fetch('SELECT * FROM path_table WHERE\
                  path_id = %d' % (path_id))
    path_desc = fetch('SELECT * FROM path_desc_table WHERE\
                       path_id = %d ORDER BY path_seq' %
                       (path_id))
    prev_label = -1
    registered_label = []
    for port_set in create_port_set(path_desc):
        src_port = port_set[0]
        dst_port = port_set[1]
        label_entry = register_label(path_id, src_port,
                                     dst_port, prev_label, path[0][3])
        registered_label.append(label_entry)
        prev_label = label_entry[5]
    commit()
    label_flows = fetch('SELECT * FROM label_table WHERE\
                        path_id = %d' % (path_id))
    if not len(label_flows):
        #Already registered labels, but first entry required.
        label_flows.append(registered_label[0])
        prev_label = -1
    return label_flows, prev_label

def fetch_group_flows(paths):
    last_labels = []
    weights = []
    path_ids = []
    for path_id, weight in paths:
        #TODO In this case whold not Fetch label.
        ignore, last_label = fetch_label_flows(path_id)
        last_labels.append(last_label)
        weights.append(weight)
        path_ids.append(path_id)
    try:
        path_ids = ','.join(str(i) for i in path_ids)
    except TypeError:
        path_ids = str(path_ids[0])
    grouping_label  = fetch('SELECT * FROM label_table WHERE\
                             prev_label = -1 and path_id IN (%s)' %
                             (path_ids,))
    group_is_exist = fetch('SELECT COUNT(*) FROM group_table WHERE\
                            including_path = \'%s\'' % (path_ids))
    if group_is_exist[0][0] == 1:
            raise GroupAlreadyExistException(
              msg='Group entry is alread exsit.')
    #All labels are owns same dpid.
    dpid = grouping_label[0][1]
    execute('INSERT INTO group_table (dpid, including_path) VALUES \
             (%d, \'%s\')' %
             (dpid, path_ids,))

    group_id = fetch('SELECT currval(\'group_table_group_id_seq\')')
    group_flow = {}
    group = {}
    group['group_id'] = group_id[0][0]
    group['dpid'] = dpid
    buckets = []
    grouped_label = []
    for label in grouping_label:
        watch = {}
        watch['watch'] = (label[2])
        watch['label'] = (label[2], label[5], label[6])
        grouped_label.append(label[5])
        buckets.append(watch)
    group['buckets'] = buckets
    try:
        grouped_label = ','.join(str(i) for i in grouped_label)
    except TypeError:
        grouped_label = str(path_ids[0])
    single_labels = fetch('SELECT * FROM label_table WHERE path_id IN (%s) AND \
                           label NOT IN (%s)' % (path_ids, grouped_label,))
    group_flow['group_flow'] = group
    group_flow['label_flow'] = single_labels
    group_flow['last_label'] = last_labels
    return group_flow

def pathsSrcFromPort(dpid, port_no):
    unavailable_paths = fetch('SELECT path_id FROM path_desc_table WHERE dpid = %s AND \
                               port_no = %s' % (dpid, port_no))
    group_id_dict = {}
    for path in unavailable_paths:
        group_ids = fetch('SELECT group_id, dpid, including_path FROM group_table WHERE \
                            including_path LIKE \'%%%s%%\'' % (path[0],))
        for group in group_ids:
            group_id_dict[group[0]] = group[2]
        execute('DELETE FROM label_table WHERE path_id = %s' % (path[0],))
    group_flows = []
    for group_id, paths in group_id_dict.items():
        path_list = paths.split(',')
        new_path_list = []
        for path in path_list:
            if path not in unavailable_paths[0]:
                new_path_list.append(path)
        try:
            new_path_ids = ','.join(str(i) for i in new_path_list)
        except TypeError:
            new_path_ids = str(new_path_list[0])
        execute('UPDATE group_table SET including_path = \'%s\' WHERE group_id = %d'%
                (new_path_ids, group_id))
        grouping_label  = fetch('SELECT * FROM label_table WHERE\
                                 prev_label = -1 and path_id IN (%s)' %
                                 (new_path_ids,))
        buckets = []
        group = {}
        for label in grouping_label:
            watch = {}
            watch['watch'] = (label[2])
            watch['label'] = (label[2], label[5], label[6])
            buckets.append(watch)
            dpid = label[1]
        group['group_id'] = group_id
        group['dpid'] = dpid
        group['buckets'] = buckets
        group_flows.append(group)
    commit()
    return group_flows
