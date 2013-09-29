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

conn = psycopg2.connect("dbname=ryu user=postgres")

class ArpTableNotFoundException(RyuException):
    message = '%(msg)s'

def fetch(query):
    cur = conn.cursor()
    cur.execute(query)
    return cur.fetchall()

def execute(cmd):
    with conn.cursor() as curs:
        curs.execute(cmd)

def clean_tables():
    with conn.cursor() as curs:
        curs.execute('DELETE FROM arp_table')

def handle_arp_packet(arppkt, dpid, port_no):
    #Fetch from dst_ip
    src_port = fetch('SELECT * FROM arp_table WHERE mac_addr = \'%s\''\
                        % (arppkt.src_mac))
    if not len(src_port):
        src_port = (dpid, port_no, arppkt.src_mac, arppkt.src_ip)
        execute('INSERT INTO arp_table (dpid, port_no, mac_addr,\
                 ip_addr) VALUES (\'%d\', \'%d\', \'%s\', \'%s\')'\
                 % src_port)
        src_port = [src_port]
            
    dst_port = fetch('SELECT * FROM arp_table WHERE ip_addr = \'%s\''\
                     % (arppkt.dst_ip))
    if len(dst_port):
        return src_port[0], dst_port[0]
    else:
        raise ArpTableNotFoundException(
              msg='NotFound dst_port from arptable')

