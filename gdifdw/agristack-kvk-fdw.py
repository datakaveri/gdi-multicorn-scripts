from multicorn import ForeignDataWrapper
from requests_cache import CachedSession
from multicorn.utils import log_to_postgres
from logging import ERROR
from requests_cache import CachedSession
import requests
from requests import (
    RequestException,
    HTTPError,
    Timeout,
    ConnectionError,
    TooManyRedirects,
    JSONDecodeError,
)
import json

REQUESTS_CACHE_FILENAME = "requests-cache"
API = "https://kvk.icar.gov.in/api/api/KMS/getKVKDetails"
RESPONSE_SORT_KEY = "kvk_id"
TIMEOUT_SEC = 60


class AgriStackKvkFdw(ForeignDataWrapper):

    def __init__(self, options, columns):
        super(AgriStackKvkFdw, self).__init__(options, columns)
        self.session = CachedSession(
            db_path=REQUESTS_CACHE_FILENAME, backend="sqlite", use_temp=True
        )
        self.columns = columns

    def execute(self, quals, columns):
        api_response = []
        try:
            r = self.session.get(API, timeout=TIMEOUT_SEC)
            r.raise_for_status()

            # API returns a JSON array of JSON objects
            api_response = sorted(r.json(), key=lambda d: d["kvk_id"])
        except (
            RequestException,
            HTTPError,
            Timeout,
            ConnectionError,
            TooManyRedirects,
            JSONDecodeError,
        ) as e:
            log_to_postgres(
                f"Failed to get data from KVK API due to python-requests error {e}",
                ERROR,
            )
        except Exception as exp:
            log_to_postgres(
                f"Failed to get data from KVK API due to exception {exp}", ERROR
            )

        id = 1
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
