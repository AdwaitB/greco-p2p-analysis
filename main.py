from datasets import *
from constants import *
from utils import *
from workload import *
from infra import *
from heap import *

from copy import deepcopy


def init(folder):
    i = Infra(folder)
    d = Dataset(folder)
    w = Workload(folder, i)

    write_data(d.data, DATA_FOLDERS[0], 'datasets_new.json')
    write_data(w.raw_workloads, DATA_FOLDERS[0], 'workload_new.json')

    return i, d, w


def get_clean_data_staging_jobs(s):
    infra, datasets, workloads = s

    # Heap to store the data staging jobs
    data_staging = Heap([])

    count = 0

    # Now add all the data staging job requests
    for job_id in workloads.raw_workloads:
        count = count + 1
        job = workloads.raw_workloads[job_id]

        for data_id in job['datasets']:
            # Datasets are not scaled here
            time = job['start'] - infra.get_time_for_ceph_transfer(job['qbox'], datasets.get_size(data_id))

            data_staging.push((time, count, job['qbox'], data_id))

    write_data({'data_staging': data_staging.heap}, DATA_FOLDERS[0], 'data_staging.json')

    # Initialize the cache
    cache = {}

    # Initialize the qboxes
    for qbox in infra.qboxes:
        cache[qbox] = {}

    # Now clean the redundant datasets
    data_staging_all = deepcopy(data_staging.heap)

    data_staging.clear()

    for ds_job in data_staging_all:
        qbox = ds_job[2]
        data_id = ds_job[3]

        if data_id not in cache[qbox]:
            data_staging.push(ds_job)
            cache[qbox][data_id] = data_id[0]

    write_data({'data_staging_clean': data_staging.heap}, DATA_FOLDERS[0], 'data_staging_clean.json')

    return data_staging


def worst_case_analysis(queue, ses):
    total_size, total_time = 0, 0

    for job in queue.heap:
        qbox = job[2]
        data_id = job[3]
        size = ses[1].get_size(data_id)

        total_size = total_size + size
        total_size = total_size + size
        total_time = total_time + ses[0].get_time_for_ceph_transfer(qbox, size)

    return total_size, total_time


session = init(DATA_FOLDERS[0])

data_staging_clean = get_clean_data_staging_jobs(session)
print("Data staging jobs for analysis : {}".format(len(data_staging_clean.heap)))

session[1].scale_datasets(1)

worst_case = worst_case_analysis(data_staging_clean, session)
print(worst_case)
