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
import shapely
from shapely.geometry import shape, mapping

REQUESTS_CACHE_FILENAME = "iwai-cache"
API = "https://ntcpwcit.in/vesseltracker/api/gdpdc/national-waterways"
RESPONSE_CACHE_TIMEOUT_SECONDS = 172800  # 2 days
TIMEOUT_SEC = 25
MULTICORN_REQUEST_PG_ERROR_HINT = "MULTICORN_REQUEST_ERR"
MULTICORN_API_PG_ERROR_HINT = "MULTICORN_API_ERR"
# VILLAGE_LGD_CODE = "villageLgdCode"


class IwaiFdw(ForeignDataWrapper):

    def __init__(self, options, columns):
        super(IwaiFdw, self).__init__(options, columns)
        self.session = CachedSession(
            db_path=REQUESTS_CACHE_FILENAME,
            backend="sqlite",
            use_temp=True,
            expire_after=RESPONSE_CACHE_TIMEOUT_SECONDS,
        )

        self.jwt_key = options.get("jwt_key", None)
        if self.jwt_key is None:
            log_to_postgres(
                "The API's API key must be set with the foreign table option 'jwt_key'",
                ERROR,
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

        # check if iwai_waterway_id param present in quals
        waterway_name_list = [
            qual
            for qual in quals
            if qual.field_name == "name" and qual.operator == "="
        ]
        
        if not waterway_name_list or not isinstance(waterway_name_list[0].value, str):
            log_to_postgres(
                "Must include `name` query parameter to get data. Use https://shipmin.gov.in/en/list-of-waterways to find the name of the waterway",
                ERROR,
                MULTICORN_REQUEST_PG_ERROR_HINT,
            )

        waterway_name = waterway_name_list[0].value

        api_response = []
        try:
            r = self.session.get(
                API+'/'+waterway_name,
                timeout=TIMEOUT_SEC,
                headers={"Authorization": "Bearer " + self.jwt_key},
            )
            r.raise_for_status()

            json_output = r.json()

            if "features" not in json_output:
                log_to_postgres(
                    f"Failed to get expected data from API. Response is {json_output}",
                    ERROR,
                    MULTICORN_API_PG_ERROR_HINT,
                )

            # API returns a JSON object containing a JSON array called 'data' of JSON objects
            api_response = json_output["features"]
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
            geom = shape(i["geometry"])
            if geom.has_z:
                geom = shapely.force_2d(geom)
            yield {
                "nw_name": i["properties"].get("NW_Name"),
                "river_name": i["properties"].get("River_Name"),
                "length": i["properties"].get("Length"),
                "start_lat": i["properties"].get("Start_Lat"),
                "start_long": i["properties"].get("Start_Long"),
                "end_lat": i["properties"].get("End_Lat"),
                "end_long": i["properties"].get("End_Long"),
                "nw_class": i["properties"].get("NW_Class"),
                "geom": json.dumps(mapping(geom)),
                "name": waterway_name,
                "id": id
            }
            id = id + 1
