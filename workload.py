import json


class Workload:
    @staticmethod
    def read_workload(folder, infra):
        with open(folder + '/workload.json', 'r') as file:
            input_file = json.load(file)

        input_file = Workload.parse_workload(input_file, infra)
        Workload.clean_workload(input_file)

        return input_file

    @staticmethod
    def parse_workload(input_file, infra):
        ret = {}

        for job in input_file["jobs"]:
            job_id = job["id"]

            qmobo = job['real_allocation']
            if qmobo == -1:
                continue
            qbox = infra.get_qbox_by_mobo_id(qmobo)
            if qbox == -1:
                continue

            ret[job_id] = {}
            ret[job_id]['start'] = job['real_start_time']
            ret[job_id]['end'] = job['real_finish_time']
            ret[job_id]['qbox'] = qbox
            ret[job_id]['datasets'] = input_file['profiles'][job['profile']]['datasets']

        return ret

    @staticmethod
    def clean_workload(input_file):
        pass
