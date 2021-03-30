import math
import urllib3
import json
from collections import namedtuple
from dateutil.parser import parse as dateutil_parse
import datetime

from yuntu.core.database.REST.base import RESTManager
from yuntu.core.database.REST.models import RESTModel


MODELS = [
    "recording",
]
Models = namedtuple("Models", MODELS)


class IrekuaRecording(RESTModel):
    def __init__(
        self,
        target_url,
        target_attr="results",
        page_size=1,
        auth=None,
        bucket="irekua",
        base_filter={"mime_type": 49},
    ):
        self.target_url = target_url
        self.target_attr = target_attr
        self._auth = auth
        self._page_size = page_size
        self.bucket = bucket
        self.http = urllib3.PoolManager()
        self.base_filter = base_filter

    def parse(self, datum):
        """Parse audio item from irekua REST api"""
        if self.bucket is None:
            path = datum["item_file"]
        else:
            key = "media" + datum["item_file"].split("media")[-1]
            path = f"s3://{self.bucket}/{key}"

        samplerate = datum["media_info"]["sampling_rate"]
        media_info = {
            "nchannels": datum["media_info"]["channels"],
            "sampwidth": datum["media_info"]["sampwidth"],
            "samplerate": samplerate,
            "length": datum["media_info"]["frames"],
            "filesize": datum["filesize"],
            "duration": datum["media_info"]["duration"],
        }
        spectrum = "ultrasonic" if samplerate > 50000 else "audible"

        dtime_zone = datum["captured_on_timezone"]
        dtime = dateutil_parse(datum["captured_on"])
        dtime_format = "%H:%M:%S %d/%m/%Y (%z)"
        dtime_raw = datetime.datetime.strftime(dtime, format=dtime_format)

        return {
            "id": datum["id"],
            "path": path,
            "hash": datum["hash"],
            "timeexp": 1,
            "media_info": media_info,
            "metadata": datum,
            "spectrum": spectrum,
            "time_raw": dtime_raw,
            "time_format": dtime_format,
            "time_zone": dtime_zone,
            "time_utc": dtime,
        }

    def validate_query(self, query):
        if query is None:
            return self.base_filter

        if not isinstance(query, dict):
            raise ValueError(
                "When using REST collections, queries should "
                + "be specified with a dictionary that contains "
                + "url parameters."
            )

        for key in self.base_filter:
            query[key] = self.base_filter[key]

        return query

    def count(self, query=None):
        """Request results count"""
        query = self.validate_query(query)
        query["page_size"] = 1
        query["page"] = 1

        headers = urllib3.make_headers(basic_auth=self.auth)
        res = self.http.request(
            "GET", self.target_url, fields=query, headers=headers
        )
        if res.status != 200:
            raise ValueError("Connection error!")

        res = json.loads(res.data.decode("utf-8"))
        return res["count"]

    def iter_pages(self, query=None, limit=None, offset=None):
        query = self.validate_query(query)

        page_start, page_end, page_size = self._get_pagination(
            query=query,
            limit=limit,
            offset=offset,
        )

        for page_number in range(page_start, page_end):
            query["page_size"] = page_size
            query["page"] = page_number

            headers = urllib3.make_headers(basic_auth=self.auth)

            res = self.http.request(
                "GET",
                self.target_url,
                fields=query,
                headers=headers,
            )

            if res.status != 200:
                res = self.http.request(
                    "GET",
                    self.target_url,
                    fields=query,
                    headers=headers,
                )
                if res.status != 200:
                    raise ValueError(str(res))

            yield json.loads(res.data.decode("utf-8"))

    def _get_pagination(self, query=None, limit=None, offset=None):
        total_pages = self._total_pages(query)

        if offset is not None:
            offset = offset + 1

        if limit is None and offset is None:
            return 1, total_pages, self.page_size

        if limit is None and offset is not None:
            return offset, total_pages, 1

        if limit is not None and offset is None:
            page_limit = math.ceil(float(limit) / float(self.page_size))
            page_limit = max(1, page_limit)
            return 1, page_limit, self.page_size

        return offset, offset + limit, 1

    def _total_pages(self, query=None):
        """Request results count"""
        return math.ceil(float(self.count(query)) / float(self.page_size))


class IrekuaREST(RESTManager):
    def build_models(self):
        """Construct all database entities."""
        recording = self.build_recording_model()
        models = {"recording": recording}
        return Models(**models)

    def build_recording_model(self):
        """Build REST recording model"""
        return IrekuaRecording(
            target_url=self.recordings_url,
            target_attr="results",
            page_size=self.page_size,
            auth=self.auth,
        )
