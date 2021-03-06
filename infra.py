import json
from random import choice as rc
import random

from link import *
from constants import *


class Infra:
    mb = 1000000

    def __init__(self, folder):
        random.seed(1)
        # Read the file
        with open(folder + '/platform.json', 'r') as file:
            input_file = json.load(file)

        # Get raw data
        self.mobos_to_id = input_file['mobos_numeric_ids']
        self.qboxes_tree = input_file['qboxes']

        # Compute mirror data
        self.id_to_mobos = {}
        self.qmobo_to_qbox = {}

        for qmobo in self.mobos_to_id:
            modo_id = self.mobos_to_id[qmobo]
            self.id_to_mobos[modo_id] = qmobo

        for qbox in self.qboxes_tree:
            for qrad in qbox['qrads']:
                for qmobo in qrad['qmobos']:
                    self.qmobo_to_qbox[qmobo] = qbox["id"]

        # Extract ceph bw, latencies
        self.ceph_net = {}

        # Extract qbox locations
        self.qbox_loc = {}

        # List of qboxes
        self.qboxes = []

        for qbox in self.qboxes_tree:
            self.qboxes.append(qbox["id"])

            # Bandwidth always in MBps
            bw = int(qbox['wan_bw'][:-4])
            if qbox['wan_bw'][-4] == 'G':
                bw = bw*1000
            bw = bw/8

            # Extract latency in ms
            self.ceph_net[qbox['id']] = (bw, int(qbox['wan_lat'][:-2]))

            # Extract site
            self.qbox_loc[qbox['id']] = qbox['site']

        # Find average bw, latencies
        self.mean_wan_bw = 0
        self.mean_wan_lat = 0

        for qbox in self.ceph_net:
            bw, lat = self.ceph_net[qbox]
            self.mean_wan_bw = self.mean_wan_bw + bw
            self.mean_wan_lat = self.mean_wan_lat + lat

        self.mean_wan_bw = self.mean_wan_bw/len(self.ceph_net)
        self.mean_wan_lat = self.mean_wan_lat/len(self.ceph_net)

    def get_network_for_link(self, link):
        """
        Get the bandwidth and the latency for the link
        :param link: qbox1, qbox2
        :return: bandwidth, latency of the link
        """
        if link[0] == link[1]:
            return 100000000000, 0
        elif link[0] == 'ceph':
            return self.ceph_net[link[1]]
        elif self.qbox_loc[link[0]] == self.qbox_loc[link[1]]:
            bw = self.mean_wan_bw*BW_P2P_LOCAL
            lat = self.mean_wan_lat*LATENCY_P2P_LOCAL
            return bw, lat
        else:
            bw = self.mean_wan_bw*BW_P2P_NOT_LOCAL
            lat = self.mean_wan_lat*LATENCY_P2P_NOT_LOCAL
            return bw, lat
        pass

    def get_qbox_by_mobo_id(self, mobo_id):
        if mobo_id not in self.id_to_mobos:
            return -1
        qmobo = self.id_to_mobos[mobo_id]

        if qmobo not in self.qmobo_to_qbox:
            return -1
        return self.qmobo_to_qbox[qmobo]

    def get_time_for_link_transfer(self, link, size, overload=1):
        """
        Gets the time required for p2p transfer fro qbox1 to qbox2
        Return 0 if the qbox are same
        :param link: (qbox1, qbox2) link
        :param size: size of the dataset to transfer
        :param overload: number of transfers happenning on the links
        :return: Time required for transfer
        """
        if link[0] == link[1]:
            return 0
        else:
            bw, lat = self.get_network_for_link(link)
            return (size*overload)/(bw*self.mb)

    def get_size_for_link_time(self, link, time, overload=1):
        """
        Gets the size consumed for a link in the time for a single transfer among the overloaded
        :param link: (qbox1, qbox2)
        :param time: time of transfer
        :param overload: amount of parallel transfers on that link
        :return: size consumed
        """
        if link[0] == link[1]:
            return 100000000000000
        else:
            bw = self.get_network_for_link(link)[0]
            return bw*time/overload

    def get_p2p_mesh(self, datasets):
        links = {}
        for qbox1 in self.qboxes:
            for qbox2 in self.qboxes:
                links[(qbox1, qbox2)] = Link(self, datasets, (qbox1, qbox2))
        return links

    def get_ceph_network(self, datasets):
        links = {}
        for qbox in self.qboxes:
            links[('ceph', qbox)] = Link(self, datasets, ('ceph', qbox))
        return links

    def get_random_qbox(self):
        return rc(self.qboxes)
