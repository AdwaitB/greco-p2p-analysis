import json


class Infra:
    def __init__(self, folder):
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

        # Extract ceph latencies
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

    def get_qbox_by_mobo_id(self, mobo_id):
        if mobo_id not in self.id_to_mobos:
            return -1
        qmobo = self.id_to_mobos[mobo_id]

        if qmobo not in self.qmobo_to_qbox:
            return -1
        return self.qmobo_to_qbox[qmobo]

    def get_time_for_data_transfer(self, qbox, size):
        # Size is in Bytes
        speed, lat = self.ceph_net[qbox]
        return lat + (size/(speed*1000000))

