import json


def write_data(datasets, folder, name):
    with open(folder + '/' + name, 'w') as file:
        json.dump(datasets, file)


def get_percent(a, b):
    return (a - b)*100/a
