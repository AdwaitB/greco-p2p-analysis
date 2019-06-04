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
    data_staging_all = Heap(deepcopy(data_staging.heap))

    data_staging.clear()

    while not data_staging_all.empty():
        ds_job = data_staging_all.pop()

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
        total_time = total_time + ses[0].get_time_for_ceph_transfer(qbox, size)

    return total_size, total_time


def p2p_analysis(queue, ses):
    jobs = Heap([])

    staging_clean = deepcopy(queue.heap)
    for staging_clean_job in staging_clean:
        jobs.push(staging_clean_job + ("REQ",))

    # Stores the network links
    links = ses[0].get_links(ses[1])

    total_size = 0
    total_time = 0

    # Stores the location of the datasets in the system
    dataset_loc = ses[1].get_dataset_locs()

    while not jobs.empty():
        job = jobs.pop()

        # Check job type
        # For POP, pop transfers from that link
        if job[4] == 'POP':
            links[(job[2], job[3])].pop()
        # For REQ type, check if dataset is present in system or not
        else:
            qbox = job[2]
            data_id = job[3]

            if qbox in dataset_loc[data_id]:
                continue

            # If the dataset is not present in the system
            if len(dataset_loc[data_id]) == 0:
                # Create a random source
                src = ses[0].get_random_qbox()
                while src == qbox:
                    src = ses[0].get_random_qbox()
            else:
                # Find sources
                sources = dataset_loc[data_id]

                # Get nearest Source
                src = sources[0]
                min_time = links[(src, qbox)].get_time(data_id, job[0])

                for source in sources:
                    new_time = links[(source, qbox)].get_time(data_id, job[0])

                    if new_time < min_time:
                        src = source
                        min_time = new_time

            # Transfer from that link
            tracking, transfer_time = links[(src, qbox)].push(data_id, job[0])

            # Add a POP job for that link
            jobs.push(tracking)

            # Update the sources
            dataset_loc[data_id].append(qbox)
            total_size += ses[1].get_size(data_id)
            total_time += transfer_time
    return total_size, total_time


def main():
    for index in DATA_FOLDERS:
        data = DATA_FOLDERS[index]
        print(data + " ================================")
        session = init(data)

        data_staging_clean = get_clean_data_staging_jobs(session)

        for scaling in (1, 2, 4, 8, 16, 32, 64, 128):
            session[1].scale_datasets(scaling)

            worst_case = worst_case_analysis(data_staging_clean, session)
            p2p = p2p_analysis(data_staging_clean, session)

            print("{} : {} , {}".format(
                scaling,
                get_percent(worst_case[0], p2p[0]),
                get_percent(worst_case[1], p2p[1])
            ))

        print("")

main()