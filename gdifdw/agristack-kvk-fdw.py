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
API = "https://ufsi.agristack.gov.in/nm/kvkRegistrySearch"
RESPONSE_SORT_KEY = "kvk_id"
RESPONSE_CACHE_TIMEOUT_SECONDS = 172800  # 2 days
TIMEOUT_SEC = 60


class AgriStackKvkFdw(ForeignDataWrapper):

    def __init__(self, options, columns):
        super(AgriStackKvkFdw, self).__init__(options, columns)
        self.session = CachedSession(
            db_path=REQUESTS_CACHE_FILENAME,
            backend="sqlite",
            use_temp=True,
            expire_after=RESPONSE_CACHE_TIMEOUT_SECONDS,
        )
        self.columns = columns

    def execute(self, quals, columns):
        api_response = []
        try:
            r = self.session.get(API, timeout=TIMEOUT_SEC)
            r.raise_for_status()
            json_output = r.json()

            # API returns a JSON object containing a JSON array called 'data' of JSON objects
            api_response = sorted(json_output['data'], key=lambda d: d["kvk_id"])
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
        for i in api_response:
            yield {
                **i,
                "geom": json.dumps(
                    {
                        "type": "Point",
                        "coordinates": [i["longitude_of_kvk"], i["latitude_of_kvk"]],
                    }
                ),
                "id": id,
            }
            id = id + 1
