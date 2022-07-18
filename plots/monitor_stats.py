import json
import time

import requests

if __name__ == "__main__":

    metrics = "Sink__Unnamed.KafkaProducer.record-send-rate"

    stats_jobid_request = requests.get(f'http://coordinator:5000/get_stats_jobid')
    stats_id = stats_jobid_request.text

    r = requests.get(f'http://flink-rest:8081/jobs/{stats_id}/plan')
    vertices = []
    for node in r.json()["plan"]["nodes"]:
        if "Sink" in node["description"]:
            vertices.append(node["id"])

    # print(json.dumps(r.json(), indent=4))
    # print(vertices)

    while 1:
        zero = True
        for vertex in vertices:
            b = requests.get(f'http://flink-rest:8081/jobs/{stats_id}/vertices/{vertex}/subtasks/metrics'
                             f'?get={metrics}')
            # print(json.dumps(b.json(), indent=4))
            if b.json():
                if b.json()[0]["sum"] != 0.0:
                    zero = False
            else:
                zero = False
        if zero:
            break
        time.sleep(5)
