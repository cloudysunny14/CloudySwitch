"""Custom topology example

Two directly connected switches plus a host for each switch:
Adding the 'topos' dict with a key/value pair to generate our newly defined
topology enables one to pass in '--topo=mytopo' from the command line.
"""
import logging
from mininet.topo import Topo

LOG = logging.getLogger("switches_v1_3")

class MyTopo( Topo ):
    "Simple topology example."

    def full_mesh_connect(self, switches, bw=10):
        connected_switch = []
        for switch in switches:
            connected_switch.append(switch)
            for connectSwitch in switches:
                if connectSwitch not in connected_switch:
                    self.addLink(switch, connectSwitch, bw=bw)
    
    def __init__( self ):
        "Create custom topo."
        # Initialize topology
        Topo.__init__( self )
        # Add hosts and switches
        host01 = self.addHost('h1')
        host02 = self.addHost('h2')
        switch01 = self.addSwitch('s1')
        switch02 = self.addSwitch('s2')
        switch03 = self.addSwitch('s3')
        switch04 = self.addSwitch('s4')
        switch05 = self.addSwitch('s5')
        switch06 = self.addSwitch('s6')
        self.full_mesh_connect([switch01, switch02, switch03,
                                switch04, switch05], bw = 10)
        # Add links
        self.addLink(host01, switch01, bw=50)
        self.addLink(host01, switch02, bw=50)
        self.addLink(host02, switch03, bw=50)
        self.addLink(host02, switch04, bw=50)

topos = { 'mytopo': ( lambda: MyTopo() )}