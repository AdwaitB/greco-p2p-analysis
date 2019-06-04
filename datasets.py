import json


class Dataset:
    def __init__(self, folder):
        self.data = {}
        self.scale = 1

        with open(folder + '/datasets.json', 'r') as file:
            # Iterate over every line (which is a json object)
            for cnt, line in enumerate(file):
                # Parse this object
                parsed = json.loads(line)

                # Add this to the dataset
                self.data[parsed['id']] = parsed['size']

    def scale_datasets(self, val):
        self.scale = val

    def get_size(self, dataset_id):
        return self.data[dataset_id]*self.scale

    def get_dataset_locs(self):
        ret = {}
        for dataset in self.data:
            ret[dataset] = []
        return ret
