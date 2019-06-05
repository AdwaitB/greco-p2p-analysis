from heap import *

import random
from copy import deepcopy


def p2p_analysis(queue, ses):
    random.seed(1)

    links = ses[0].get_p2p_mesh(ses[1])

    jobs = Heap([])

    staging_clean = deepcopy(queue.heap)
    for staging_clean_job in staging_clean:
        jobs.push(staging_clean_job + ("REQ",))

    total_size = 0
    total_time = 0
    average_time = 0

    # Stores the location of the datasets in the system
    dataset_loc = ses[1].get_dataset_locs()

    traces = []

    while not jobs.empty():
        job = jobs.pop()
        traces.append(job)

        # Check job type
        # For POP, tell the link to complete transfers
        if job[4] == 'POP':
            pop_entry, history = links[(job[2], job[3])].complete_transfer(job)

            if pop_entry is not None:
                jobs.push(pop_entry)
                average_time += history[1]
        # For REQ type, check if dataset is present in system or not
        else:
            qbox = job[2]
            data_id = job[3]

            if qbox in dataset_loc[data_id]:
                continue

            # If the dataset is not present in the system, find the source
            if len(dataset_loc[data_id]) == 0:
                # Create a random source
                src = ses[0].get_random_qbox()
            else:
                # Find sources
                sources = dataset_loc[data_id]

                # Get nearest Source
                src = list(dataset_loc[data_id])[0]
                max_bandwidth = links[(src, qbox)].get_bandwidth()

                for source in sources:
                    new_bw = links[(source, qbox)].get_bandwidth()

                    if new_bw > max_bandwidth:
                        src = source
                        max_bandwidth = new_bw

            # Transfer from that link
            pop_entry = links[(src, qbox)].add_transfer(data_id, job[0])

            # Add a POP job for that link
            jobs.push(pop_entry)

            # Update the sources
            dataset_loc[data_id].add(src)
            dataset_loc[data_id].add(qbox)

            total_size += ses[1].get_size(data_id)
            total_time += ses[0].get_time_for_link_transfer((src, qbox), ses[1].get_size(data_id))

    return (total_size, total_time, average_time), traces


def worst_case_analysis(queue, ses):
    links = ses[0].get_ceph_network(ses[1])

    jobs = Heap([])

    staging_clean = deepcopy(queue.heap)
    for staging_clean_job in staging_clean:
        jobs.push(staging_clean_job + ("REQ",))

    total_size = 0
    total_time = 0
    average_time = 0

    traces = []

    while not jobs.empty():
        job = jobs.pop()
        traces.append(job)

        # Check job type
        # For POP, tell the link to complete transfers
        if job[4] == 'POP':
            pop_entry, history = links[(job[2], job[3])].complete_transfer(job)

            if pop_entry is not None:
                jobs.push(pop_entry)
                average_time += history[1]
        # For REQ type, check if dataset is present in system or not
        else:
            qbox = job[2]
            data_id = job[3]

            # Transfer from that link
            pop_entry = links[('ceph', qbox)].add_transfer(data_id, job[0])

            # Add a POP job for that link
            jobs.push(pop_entry)

            total_size += ses[1].get_size(data_id)
            total_time += ses[0].get_time_for_link_transfer(('ceph', qbox), ses[1].get_size(data_id))

    return (total_size, total_time, average_time), traces
