import logging
from mininet.net import Mininet
from mininet.cli import CLI
from mininet.node import UserSwitch as Switch
from mininet.node import RemoteController

from sample_topology import MyTopo
LOG = logging.getLogger("qos_test")

def run(net):
    s1 = net.getNodeByName('s1')
    s1.cmdPrint('dpctl unix:/tmp/s1 queue-mod 1 1 100')
    s1.cmdPrint('dpctl unix:/tmp/s1 queue-mod 1 2 200')
    s1.cmdPrint('dpctl unix:/tmp/s1 queue-mod 1 3 300')
    s1.cmdPrint('dpctl unix:/tmp/s1 queue-mod 2 1 100')
    s1.cmdPrint('dpctl unix:/tmp/s1 queue-mod 2 2 200')
    s1.cmdPrint('dpctl unix:/tmp/s1 queue-mod 2 3 300')
    s1.cmdPrint('dpctl unix:/tmp/s1 queue-mod 3 1 100')
    s1.cmdPrint('dpctl unix:/tmp/s1 queue-mod 3 2 200')
    s1.cmdPrint('dpctl unix:/tmp/s1 queue-mod 3 3 300')
    s1.cmdPrint('dpctl unix:/tmp/s1 queue-mod 4 1 100')
    s1.cmdPrint('dpctl unix:/tmp/s1 queue-mod 4 2 200')
    s1.cmdPrint('dpctl unix:/tmp/s1 queue-mod 4 3 300')
    s2 = net.getNodeByName('s2')
    s2.cmdPrint('dpctl unix:/tmp/s2 queue-mod 1 1 100')
    s2.cmdPrint('dpctl unix:/tmp/s2 queue-mod 1 2 100')
    s2.cmdPrint('dpctl unix:/tmp/s2 queue-mod 1 3 100')
    s2.cmdPrint('dpctl unix:/tmp/s2 queue-mod 2 1 100')
    s2.cmdPrint('dpctl unix:/tmp/s2 queue-mod 2 2 100')
    s2.cmdPrint('dpctl unix:/tmp/s2 queue-mod 2 3 100')
    s2.cmdPrint('dpctl unix:/tmp/s2 queue-mod 3 1 100')
    s2.cmdPrint('dpctl unix:/tmp/s2 queue-mod 3 2 100')
    s2.cmdPrint('dpctl unix:/tmp/s2 queue-mod 3 3 100')
    s3 = net.getNodeByName('s3')
    s3.cmdPrint('dpctl unix:/tmp/s3 queue-mod 1 1 100')
    s3.cmdPrint('dpctl unix:/tmp/s3 queue-mod 1 2 100')
    s3.cmdPrint('dpctl unix:/tmp/s3 queue-mod 1 3 100')
    s3.cmdPrint('dpctl unix:/tmp/s3 queue-mod 2 1 100')
    s3.cmdPrint('dpctl unix:/tmp/s3 queue-mod 2 2 100')
    s3.cmdPrint('dpctl unix:/tmp/s3 queue-mod 2 3 100')
    s3.cmdPrint('dpctl unix:/tmp/s3 queue-mod 3 1 100')
    s3.cmdPrint('dpctl unix:/tmp/s3 queue-mod 3 2 100')
    s3.cmdPrint('dpctl unix:/tmp/s3 queue-mod 3 3 100')
    s4 = net.getNodeByName('s4')
    s4.cmdPrint('dpctl unix:/tmp/s4 queue-mod 1 1 100')
    s4.cmdPrint('dpctl unix:/tmp/s4 queue-mod 1 2 100')
    s4.cmdPrint('dpctl unix:/tmp/s4 queue-mod 1 3 100')
    s4.cmdPrint('dpctl unix:/tmp/s4 queue-mod 2 1 100')
    s4.cmdPrint('dpctl unix:/tmp/s4 queue-mod 2 2 100')
    s4.cmdPrint('dpctl unix:/tmp/s4 queue-mod 2 3 100')
    s4.cmdPrint('dpctl unix:/tmp/s4 queue-mod 3 1 100')
    s4.cmdPrint('dpctl unix:/tmp/s4 queue-mod 3 2 100')
    s4.cmdPrint('dpctl unix:/tmp/s4 queue-mod 3 3 100')
    s4.cmdPrint('dpctl unix:/tmp/s4 queue-mod 4 1 100')
    s4.cmdPrint('dpctl unix:/tmp/s4 queue-mod 4 2 100')
    s4.cmdPrint('dpctl unix:/tmp/s4 queue-mod 4 3 100')

def genericTest(topo):
    net = Mininet(topo=topo, switch=Switch, controller=RemoteController)
    net.start()
    run(net)
    CLI(net)
    net.stop()

def main():
    topo = MyTopo()
    genericTest(topo)


if __name__ == '__main__':
    main()
