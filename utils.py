import json


def write_data(datasets, folder, name):
    with open(folder + '/' + name, 'w') as file:
        json.dump(datasets, file)


def get_percent(a, b):
    return (a - b)*100/a


def get_str(l):
    ret = ""
    for e in l:
        ret += '_' + str(e)
    return ret
