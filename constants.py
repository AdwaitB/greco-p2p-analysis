DATA_FOLDERS = {
    #-1: 'data/test',
    0: 'data/3d_09-05-2019',
    1: 'data/1w_03-05-2019',
    2: 'data/1w_10-05-2019',
    3: 'data/1w_17-05-2019',
    4: 'data/2w_01-05-2019'
}


"""
Assmuptions
For the same geo location
1. P2P bandwidth = BW_P2P_LOCAL * CEPH_AVERAGE_BW
2. P2P Latency = LATENCY_P2P_LOCAL * CEPH_AVERAGE_LAT

For different geo location
1. P2P BW = BW_P2P_NOT_LOCAL * CEPH_AVERAGE_BW
2. P2P Lat = LATENCY_P2P_NOT_LOCAL * CEPH_AVERAGE_LAT

For every link, equal bandwidth is given to every transfer.
But the time taken for this and the time taken for stacked, but independent queries is same.
So it is implemented in the 2nd way
"""

LATENCY_P2P_LOCAL = 0
LATENCY_P2P_NOT_LOCAL = 0
BW_P2P_LOCAL = 0.5
BW_P2P_NOT_LOCAL = 0.5
JOBS_SCALE = (0, 1, 5, 6, 7)
DATA_SIZE_SCALING = (64, 512)
