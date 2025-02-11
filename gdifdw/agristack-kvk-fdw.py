from multicorn import ForeignDataWrapper
import requests
import json


def run():
    r = requests.get("https://kvk.icar.gov.in/api/api/KMS/getKVKDetails")
    resp = r.json()
    id = 0
    for i in resp:
        yield {
            **i,
            "geom": json.dumps(
                {
                    "type": "Point",
                    "coordinates": [i["KVK_Latitude"], i["KVK_Longitude"]],
                }
            ),
            "id": id,
        }
        id = id + 1


class AgriStackKvkFdw(ForeignDataWrapper):

    def __init__(self, options, columns):
        super(AgriStackKvkFdw, self).__init__(options, columns)
        self.columns = columns

    def execute(self, quals, columns):
        return run()
