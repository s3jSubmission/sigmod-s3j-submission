import json
import random
import pyhdfs


machines = [0, 13, 26, 39, 52, 65, 78, 91, 104, 117]

group_keys = [
    (1, 0, 0),
    (2, 0, 0),
    (2, 13, 13),
    (1, 13, 13),
    (1, 26, 26),
    (2, 26, 26),
    (1, 39, 39),
    (3, 39, 39),
    (2, 39, 39),
    (2, 52, 52),
    (1, 52, 52),
    (1, 65, 65),
    (2, 65, 65),
    (2, 78, 78),
    (3, 78, 78),
    (1, 78, 78),
    (1, 91, 91),
    (1, 104, 104),
    (2, 104, 104),
    (1, 117, 117),
    (2, 117, 117)
]

if __name__ == "__main__":

    hdfs_client = pyhdfs.HdfsClient(hosts='localhost:9870')

    group_allocation = dict()
    x = random.Random()
    x.seed(42)

    for group in group_keys:
        group_allocation[str(group)] = x.choice(machines)

    print(hdfs_client.listdir("/"))
    print(hdfs_client.list_status("/"))

    hdfs_client.create(path="/test/test_input.json", data=json.dumps(group_allocation))
    # lines = hdfs_client.open(path="/test/test_input.json").readlines()
    # print(lines)
