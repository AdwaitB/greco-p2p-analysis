from datasets import *
from constants import *
from utils import *
from workload import *
from infra import *
from heap import *

from copy import deepcopy

infra = Infra(DATA_FOLDER)
datasets = Dataset(DATA_FOLDER)
workloads = Workload(DATA_FOLDER, infra)


write_data(datasets.data, DATA_FOLDER, 'datasets_new.json')
write_data(workloads.raw_workloads, DATA_FOLDER, 'workload_new.json')

# Heap to store the data staging jobs
data_staging = Heap([])

count = 0

# Now add all the data staging job requests
for job_id in workloads.raw_workloads:
    count = count + 1
    job = workloads.raw_workloads[job_id]

    for data_id in job['datasets']:
        # Datasets are not scaled here
        time = job['start'] - infra.get_time_for_data_transfer(job['qbox'], datasets.get_size(data_id))

        data_staging.push((time, count, job['qbox'], data_id))

write_data({'data_staging': data_staging.heap}, DATA_FOLDER, 'data_staging.json')


# Initialize the cache
cache = {}

# Initialize the qboxes
for qbox in infra.qboxes:
    cache[qbox] = {}

# Now clean

# Now scale the datasets
Dataset.scale_datasets(datasets, scale)

