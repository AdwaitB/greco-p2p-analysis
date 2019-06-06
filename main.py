from __future__ import print_function

from datasets import *
from utils import *
from workload import *
from infra import *
from analysis import *

from copy import deepcopy

import pandas as pd


def init(folder):
    i = Infra(folder)
    d = Dataset(folder)
    w = Workload(folder, i)

    write_data(d.data, folder, 'datasets_new.json')
    write_data(w.raw_workloads, folder, 'workload_new.json')

    return i, d, w


def get_clean_data_staging_jobs(s, folder):
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
            time = job['start'] - infra.get_time_for_link_transfer(('ceph', job['qbox']), datasets.get_size(data_id))

            data_staging.push((time, count, job['qbox'], data_id))

    write_data({'data_staging': data_staging.heap}, folder, 'data_staging.json')

    # Initialize the cache
    cache = {}

    # Initialize the qboxes
    for qbox in infra.qboxes:
        cache[qbox] = {}

    # Now clean the redundant datasets
    data_staging_all = Heap(deepcopy(data_staging.heap))

    data_staging.clear()

    while not data_staging_all.empty():
        ds_job = data_staging_all.pop()

        qbox = ds_job[2]
        data_id = ds_job[3]

        if data_id not in cache[qbox]:
            data_staging.push(ds_job)
            cache[qbox][data_id] = data_id[0]

    write_data({'data_staging_clean': data_staging.heap}, folder, 'data_staging_clean.json')

    return data_staging


def main():
    df = pd.DataFrame(columns=['data', 'job_scaling', 'data_scaling',
                               'transfer_size_worst', 'transfer_time_worst', 'transfer_size_improvement'
                               'transfer_size_p2p', 'transfer_time_p2p', 'transfer_time_improvement'
                               'average_time_worst', 'average_time_p2p', 'average_time_improvement'])

    for index in DATA_FOLDERS:
        data = DATA_FOLDERS[index]

        print("{}".format(data))

        for job_scale in JOBS_SCALE:
            for data_scale in DATA_SIZE_SCALING:

                print(str(job_scale) + ' ' + str(data_scale), end=' ')

                session = init(data)
                print("=", end='')

                # Add random datasets to jobs
                session[2].add_random_datasets_to_job(session[1], job_scale)
                print("=", end='')

                # Clean the entries for cached datasets
                data_staging_clean = get_clean_data_staging_jobs(session, data)
                print("=", end='')

                # Scale the datasets
                session[1].scale_datasets(data_scale)
                print("-", end='')

                # Get the worst_case and the p2p execution
                worst_case, traces = worst_case_analysis(data_staging_clean, session)
                print("+", end=' : ')

                p2p, traces = p2p_analysis(data_staging_clean, session)

                print(get_percent(worst_case[0], p2p[0]), end=' ')
                print(get_percent(worst_case[1], p2p[1]), end=' ')
                print(get_percent(worst_case[2], p2p[2]))

                entry = {
                    'data': data,
                    'job_scaling': job_scale,
                    'data_scaling': data_scale,
                    'transfer_size_worst': worst_case[0],
                    'transfer_size_p2p': p2p[0],
                    'transfer_size_improvement': get_percent(worst_case[0], p2p[0]),
                    'transfer_time_worst': worst_case[1],
                    'transfer_time_p2p': p2p[1],
                    'transfer_time_improvement': get_percent(worst_case[1], p2p[1]),
                    'average_time_worst': worst_case[2],
                    'average_time_p2p': p2p[2],
                    'average_time_improvement': get_percent(worst_case[2], p2p[2])
                }

                df = df.append(entry, ignore_index=True)
            print("")
        print("")
    df.to_csv('output_traces/output_{}={}={}={}={}={}.csv'.format(
        BW_P2P_LOCAL, BW_P2P_NOT_LOCAL, LATENCY_P2P_LOCAL, LATENCY_P2P_NOT_LOCAL,
        get_str(JOBS_SCALE), get_str(DATA_SIZE_SCALING)
    ))


main()
