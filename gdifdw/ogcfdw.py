from multicorn import ForeignDataWrapper
import requests
import json


def run():
    r = requests.get("https://ogc-compliance.iudx.io/collections/1e7f3be1-5d07-4cba-9c8c-5c3a2fd5c82a/items")
    resp = r.json()['features']
    for i in resp:
        yield {
                **i['properties'],
                "id": i["id"],
                "geom":json.dumps(i["geometry"]),
        }


class OgcFdw(ForeignDataWrapper):

    def __init__(self, options, columns):
        super(OgcFdw, self).__init__(options, columns)
        self.columns = columns

    def execute(self, quals, columns):
        return run()
