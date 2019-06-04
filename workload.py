import json


class Workload:
    def __init__(self, folder, infra):
        with open(folder + '/workload.json', 'r') as file:
            input_file = json.load(file)

        self.raw_workloads = {}

        for job in input_file["jobs"]:
            job_id = job["id"]

            # Check if data is valid
            qmobo = job['real_allocation']
            if qmobo == -1:
                continue
            qbox = infra.get_qbox_by_mobo_id(qmobo)
            if qbox == -1:
                continue
            if input_file['profiles'][job['profile']]['datasets'] is None:
                continue

            self.raw_workloads[job_id] = {}
            self.raw_workloads[job_id]['start'] = job['real_start_time']
            self.raw_workloads[job_id]['end'] = job['real_finish_time']
            self.raw_workloads[job_id]['qbox'] = qbox
            self.raw_workloads[job_id]['datasets'] = input_file['profiles'][job['profile']]['datasets']

    def add_random_datasets_to_job(self, dataset, count):
        for job_id in self.raw_workloads:
            for i in range(count):
                data_id = dataset.get_random_dataset()
                if data_id not in self.raw_workloads[job_id]:
                    self.raw_workloads[job_id]['datasets'].append(data_id)
