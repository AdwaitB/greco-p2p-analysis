from datasets import *
from constants import *
from utils import *
from workload import *
from infra import *


infra = Infra(data)
datasets = Dataset.read_datasets(data)
Dataset.scale_datasets(datasets, scale)
write_data(datasets, data, 'datasets_new.json')

workloads = Workload.read_workload(data, infra)
write_data(workloads, data, 'workload_new.json')


