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
import psyco_eventlet
from ryu.exception import RyuException

psyco_eventlet.make_psycopg_green()

import psycopg2

#TODO Required to configure for db connecting.
conn = psycopg2.connect("dbname=ryu user=postgres")
#ISOLATION_LEVE_READ_COMMITTED is default setting.
#conn.set_isolation_level(ISOLATION_LEVEL_READ_COMMITTED)

class ArpTableNotFoundException(RyuException):
    message = '%(msg)s'

class RequiredReFetchException(RyuException):
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

def handle_paths(path_list, src_port, dst_port):
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
                              target_dst_dpid = %d' % 
                              (src_port[2], src_port[3], dst_port[2], dst_port[3],
                               target_dst_dpid))
    if not len(registered_label):
        execute('INSERT INTO label_table (path_id, src_dpid, src_port_no, \
                 dst_dpid, dst_port_no, prev_label, entered, target_dst_dpid)\
                 VALUES (%d, %d, %d, %d, %d, %d, FALSE, %d)' %
                 (path_id, src_port[2], src_port[3],
                  dst_port[2], dst_port[3], prev_label, target_dst_dpid))
        label = fetch('SELECT currval(\'label_table_label_seq\')')
        registered_label = (path_id, src_port[2], src_port[3], dst_port[2],
                            dst_port[3], label[0][0], prev_label, False, target_dst_dpid)
    else:
        #common path found
        execute('UPDATE label_table SET label = %d WHERE label = %d AND\
                 path_id = %d' %
                (registered_label[0][6], prev_label, path_id))
        raise RequiredReFetchException(
              msg='Prev label is update. Require to reretch.') 
        
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
    label_flows = []
    prev_label = -1
    for port_set in create_port_set(path_desc):
        src_port = port_set[0]
        dst_port = port_set[1]
        try:
            label_entry = register_label(path_id, src_port,
                                       dst_port, prev_label, path[0][3])
            label_flows.append(label_entry)
            prev_label = label_entry[5]
        except RequiredReFetchException:
            pass
    commit()
    label_flows = []
    label_flow = fetch('SELECT * FROM label_table WHERE\
                        path_id = %d' % (path_id))
    for label in label_flow:
        label_flows.append(label)
    return label_flows

