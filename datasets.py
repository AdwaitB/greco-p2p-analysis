import json


class Dataset:
    @staticmethod
    def read_datasets(folder):
        ret = {}

        with open(folder + '/datasets.json', 'r') as file:
            # Iterate over every line (which is a json object)
            for cnt, line in enumerate(file):
                # Parse this object
                parsed = json.loads(line)

                # Add this to the dataset
                ret[parsed['id']] = parsed['size']

        return ret

    @staticmethod
    def scale_datasets(datasets, val):
        for d in datasets:
            datasets[d] = datasets[d]*val
