from multicorn import ForeignDataWrapper
from requests_cache import CachedSession
from multicorn.utils import log_to_postgres
from logging import ERROR, INFO, WARNING
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
import jwt
import time

REQUESTS_CACHE_FILENAME = "assam-cadastral-cache"
API = "https://landhub.assam.gov.in/api/index.php/NicApi/VillageGeoJsonGDPDC"
RESPONSE_CACHE_TIMEOUT_SECONDS = 172800  # 2 days
TIMEOUT_SEC = 25
MULTICORN_REQUEST_PG_ERROR_HINT = "MULTICORN_REQUEST_ERR"
MULTICORN_API_PG_ERROR_HINT = "MULTICORN_API_ERR"
VILLAGE_LGD_CODE = "villageLgdCode"


class AssamVillageCadastralFdw(ForeignDataWrapper):

    def __init__(self, options, columns):
        super(AssamVillageCadastralFdw, self).__init__(options, columns)
        self.session = CachedSession(
            db_path=REQUESTS_CACHE_FILENAME,
            backend="sqlite",
            use_temp=True,
            expire_after=RESPONSE_CACHE_TIMEOUT_SECONDS,
        )

        self.jwt_key = options.get("jwt_key", None)
        if self.jwt_key is None:
            log_to_postgres(
                "The API's JWT key must be set with the foreign table option 'jwt_key'",
                ERROR,
            )

        self.https_proxy = options.get("https_proxy", None)
        if self.https_proxy is None:
            log_to_postgres(
                "HTTPS proxy is not set. Set the HTTPS proxy using foreign table option 'https_proxy'",
                WARNING,
            )

        self.columns = columns

    def execute(self, quals, columns):
        # check if id='<feature ID>' kind of query is run. This indicates that an `/items/<featureId>` API call is being made.
        feature_id = [
            qual for qual in quals if qual.field_name == "id" and qual.operator == "="
        ]

        if feature_id:
            log_to_postgres(
                "This dataset does not support the `/items/{itemId}` API",
                ERROR,
                MULTICORN_REQUEST_PG_ERROR_HINT,
            )

        # check if villageLdgCode param present in quals
        village_code_lst = [
            qual
            for qual in quals
            if qual.field_name == VILLAGE_LGD_CODE and qual.operator == "="
        ]

        if not village_code_lst or not isinstance(village_code_lst[0].value, int):
            log_to_postgres(
                "Must include `villageLgdCode` query parameter to get data. Visit lgdirectory.gov.in to get village codes for Assam",
                ERROR,
                MULTICORN_REQUEST_PG_ERROR_HINT,
            )

        village_code = village_code_lst[0].value

        iat = int(time.time())
        exp = int(time.time()) + 60
        encoded_jwt = jwt.encode(
            {"sub": "API_Call", "iat": iat, "exp": exp}, self.jwt_key, algorithm="HS256"
        )

        api_response = []
        try:
            body = {"villageLgdCode": str(village_code)}
            r = self.session.post(
                API,
                timeout=TIMEOUT_SEC,
                data=body,
                headers={"Authorization": "Bearer " + encoded_jwt},
                proxies={"https": self.https_proxy},
            )
            r.raise_for_status()

            if r.text == "No data found for this lgd parameters":
                yield {}
                return

            json_output = r.json()

            if isinstance(json_output, str):
                log_to_postgres(
                    f"Failed to get data from API. Obtained string error message {json_output}",
                    ERROR,
                    MULTICORN_API_PG_ERROR_HINT,
                )

            if "features" not in json_output:
                log_to_postgres(
                    f"Failed to get expected data from API. Response is {json_output}",
                    ERROR,
                    MULTICORN_API_PG_ERROR_HINT,
                )

            # API returns a JSON object containing a JSON array called 'data' of JSON objects
            api_response = sorted(
                json_output["features"], key=lambda d: d["properties"]["kide"]
            )
        except (
            RequestException,
            HTTPError,
            Timeout,
            ConnectionError,
            TooManyRedirects,
            JSONDecodeError,
        ) as e:
            log_to_postgres(
                f"Failed to get data from API due to python-requests error {e}",
                ERROR,
                MULTICORN_API_PG_ERROR_HINT,
            )
        except Exception as exp:
            log_to_postgres(
                f"Failed to get data from API due to exception {exp}",
                ERROR,
                MULTICORN_API_PG_ERROR_HINT,
            )

        id = 1
        for i in api_response:
            yield {
                "fid": i["id"],
                "prop_id": i["properties"]["id"],
                "info": i["properties"]["info"],
                "kide": i["properties"]["kide"],
                "pniu": i["properties"]["pniu"],
                "giscode": i["properties"]["giscode"],
                "geom": json.dumps(i["geometry"]),
                "villageLgdCode": village_code,
                "id": id,
            }
            id = id + 1
