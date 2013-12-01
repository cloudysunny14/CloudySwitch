CloudySwitch
============

CloudySwitch is OpenFlow controller using [Ryu](https://github.com/osrg/ryu) controller. Runs on OpenFlow 1.3

Getting Started
============

## Prerequisites and settings
* Ryu and Mininet setup  Please refer to [Ryu version of OpenFlow Tutorial](https://github.com/osrg/ryu/wiki/OpenFlow_Tutorial)

* CloudySwitch setup  You will need the following installed in your environment that runs CloudySwitch
  * postgresql  Please refer to [installation page](http://www.postgresql.org/) for install and settings postgresql
  * psycopg2  Please refer to [installation page](http://initd.org/psycopg/install/) for install psycopg2

## Download
    git clone https://github.com/cloudysunny14/CloudySwitch.git

## Running cloudyswitch

    $ cd CloudySwitch/cloudyswitch/demo/
    $ sudo ./cloudyrun_sample.sh


    mininet> h1 ping h2 -c 3
    EVENT ofp_event->switches EventOFPPacketIn
    EVENT switches->SwitchEventHandler EventArpReceived
    EVENT ofp_event->switches EventOFPPacketIn
    EVENT switches->SwitchEventHandler EventArpReceived
    PING 10.0.0.2 (10.0.0.2) 56(84) bytes of data.
    64 bytes from 10.0.0.2: icmp_req=2 ttl=64 time=0.588 ms
    64 bytes from 10.0.0.2: icmp_req=3 ttl=64 time=0.556 ms
    --- 10.0.0.2 ping statistics ---
    3 packets transmitted, 2 received, 33% packet loss, time 2007ms
    rtt min/avg/max/mdev = 0.556/0.572/0.588/0.016 ms
    mininet> h1 ping h2 -c 3
    PING 10.0.0.2 (10.0.0.2) 56(84) bytes of data.
    64 bytes from 10.0.0.2: icmp_req=1 ttl=64 time=0.503 ms
    64 bytes from 10.0.0.2: icmp_req=2 ttl=64 time=0.568 ms
    64 bytes from 10.0.0.2: icmp_req=3 ttl=64 time=0.619 ms
    --- 10.0.0.2 ping statistics ---
    3 packets transmitted, 3 received, 0% packet loss, time 1998ms
    rtt min/avg/max/mdev = 0.503/0.563/0.619/0.051 ms

※There is known issue only the first ping is not reach.

## Features
* Packet forwading using MPLS on OpenFlow 1.3

The code is a work in progress, and this is an early release.

##Roadmap
* REST API
* GUI(Like Floodlight)

## Get Involved
It’s all Apache 2.0 licensed.  Check out our Git repository and feel free to post patches on the issue tracker.

