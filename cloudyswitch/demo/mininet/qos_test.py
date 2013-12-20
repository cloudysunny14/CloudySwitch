"""Custom topology example

Two directly connected switches plus a host for each switch:
Adding the 'topos' dict with a key/value pair to generate our newly defined
topology enables one to pass in '--topo=mytopo' from the command line.
"""
import logging
from mininet.net import Mininet
from mininet.cli import CLI
from mininet.node import UserSwitch as Switch
from mininet.node import RemoteController
from mininet.link import TCLink
from mininet.util import custom

from sample_topology import MyTopo 
LOG = logging.getLogger("qos_test")

def run(net):
    s1 = net.getNodeByName('s1')
    s1.cmdPrint('dpctl unix:/tmp/s1 queue-mod 1 1 1')
    s1.cmdPrint('dpctl unix:/tmp/s1 queue-mod 2 1 1')
    s1.cmdPrint('dpctl unix:/tmp/s1 queue-mod 3 1 1')
    s1.cmdPrint('dpctl unix:/tmp/s1 queue-mod 4 1 1')
    s2 = net.getNodeByName('s2')
    s2.cmdPrint('dpctl unix:/tmp/s2 queue-mod 1 1 1')
    s2.cmdPrint('dpctl unix:/tmp/s2 queue-mod 2 1 1')
    s2.cmdPrint('dpctl unix:/tmp/s2 queue-mod 3 1 1')
    s3 = net.getNodeByName('s3')
    s3.cmdPrint('dpctl unix:/tmp/s3 queue-mod 1 1 1')
    s3.cmdPrint('dpctl unix:/tmp/s3 queue-mod 2 1 1')
    s3.cmdPrint('dpctl unix:/tmp/s3 queue-mod 3 1 1')
    s4 = net.getNodeByName('s4')
    s4.cmdPrint('dpctl unix:/tmp/s4 queue-mod 1 1 1')
    s4.cmdPrint('dpctl unix:/tmp/s4 queue-mod 2 1 1')
    s4.cmdPrint('dpctl unix:/tmp/s4 queue-mod 3 1 1')
    s4.cmdPrint('dpctl unix:/tmp/s4 queue-mod 4 1 1')

def genericTest(topo):
    link = custom(TCLink, bw=50)
    net = Mininet(topo=topo, switch=Switch, link=link, controller=RemoteController)
    net.start()
    run(net)
    CLI(net)
    net.stop()

def main():
    topo = MyTopo()
    genericTest(topo)


if __name__ == '__main__':
    main()

