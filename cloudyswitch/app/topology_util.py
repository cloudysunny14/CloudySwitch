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
from ryu.exception import RyuException

def find_all_paths(graph, start, end, path=[]):
    path = path + [start]
    if start == end:
        return [path]
    if not graph.has_key(start):
        return []
    paths = []
    for node in graph[start]:
        if node not in path:
            newpaths = find_all_paths(graph, node, end, path)
            for newpath in newpaths:
                paths.append(newpath)
    paths.sort(key = len)
    return paths

class LinkedPorts(object):
    
    def __init__(self):
        self.link = {}

    def addLink(self, link):
        link_roots = self.link.get(link.src.dpid, [])
        link_roots.append(link)
        self.link[link.src.dpid] = link_roots

    def getLink(self, src_dpid, dst_dpid):
        link_roots = self.link[src_dpid]
        for link in link_roots:
          if link.dst.dpid == dst_dpid:
              return link
        return None

class PathList(object):

    class IllegalLink(RyuException):
        message = '%(msg)s'

    def __init__(self, link_list):
        self.link_list = link_list
        self.ports = {}
        self.linked_ports = LinkedPorts()

    def _createGraph(self, link_list):
        graph = {}
        for link in link_list:
            self.linked_ports.addLink(link)
            src_dpid = link.src.dpid
            dst_dpid = link.dst.dpid
            linked_nodes = graph.get(src_dpid, [])
            linked_nodes.append(dst_dpid)
            graph[src_dpid] = linked_nodes
        return graph

    def createWholePath(self, src_dpid, dst_dpid):
        graph = self._createGraph(self.link_list)
        paths = find_all_paths(graph, src_dpid, dst_dpid)
        path_ports = []
        for path in paths:
            ports = []
            for index in range(len(path)-1):
                link = self.linked_ports.getLink(path[index],
                                                 path[index+1])
                if link is None:
                    raise PathList.IllegalLink(
                          msg='Illegal link found. Can\'t create paths %s' % link)
                else:
                    ports.append(link.src)
                    ports.append(link.dst)
            path_ports.append(ports)
        return path_ports

