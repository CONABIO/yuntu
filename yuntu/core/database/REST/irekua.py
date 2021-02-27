import json
from collections import namedtuple
from dateutil.parser import parse as dateutil_parse
import urllib3
import datetime
import math

from yuntu.core.database.REST.base import RESTManager
from yuntu.core.database.REST.models import RESTModel

MODELS = [
    'recording',
]
MAX_PAGE_SIZE = 5000
Models = namedtuple('Models', MODELS)

def get_sync(client, url, params=None, auth=None):
    headers=None
    if auth is not None:
        headers = urllib3.make_headers(basic_auth=auth)
    res = client.request('GET',  url,
                         fields=params,
                         headers=headers
                         )
    if res.status != 200:
        res = client.request('GET', url,
                             fields=params,
                             headers=headers
                             )
        if res.status != 200:
            raise ValueError(f"Server error {res.status}")

    return res.json()


class IrekuaRecording(RESTModel):
    def __init__(self, target_url,
                 target_attr="results",
                 page_size=1, auth=None,
                 base_filter=None,
                 bucket='irekua'):
        self.target_url = target_url
        self.target_attr = target_attr
        self._auth = auth
        self._page_size = min(page_size, MAX_PAGE_SIZE)
        self._http = urllib3.PoolManager()
        self.bucket = bucket
        self.base_filter = {}
        if base_filter is not None:
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
            'nchannels': datum["media_info"]["channels"],
            'sampwidth': datum["media_info"]["sampwidth"],
            'samplerate': samplerate,
            'length': datum["media_info"]["frames"],
            'filesize': datum["filesize"],
            'duration': datum["media_info"]["duration"]
        }
        spectrum = 'ultrasonic' if samplerate > 50000 else 'audible'

        dtime_zone = datum["captured_on_timezone"]
        dtime = dateutil_parse(datum["captured_on"])
        dtime_format = "%H:%M:%S %d/%m/%Y (%z)"
        dtime_raw = datetime.datetime.strftime(dtime, format=dtime_format)

        return {
            'id': datum['id'],
            'path': path,
            'hash': datum["hash"],
            'timeexp': 1,
            'media_info': media_info,
            'metadata': datum,
            'spectrum': spectrum,
            'time_raw': dtime_raw,
            'time_format': dtime_format,
            'time_zone': dtime_zone,
            'time_utc': dtime
        }

    def validate_query(self, query):
        if query is None:
            return self.base_filter

        if not isinstance(query, dict):
            raise ValueError("When using REST collections, queries should " +
                             "be specified with a dictionary that contains " +
                             "url parameters.")
        for key in self.base_filter:
            query[key] = self.base_filter[key]

        return query

    def count(self, query=None):
        """Request results count"""
        query = self.validate_query(query)
        return self._count(query)

    def iter_pages(self, query=None, limit=None, offset=None):
        query = self.validate_query(query)
        page_start, page_end, page_size = self._get_pagination(query=query,
                                                               limit=limit,
                                                               offset=offset)
        for page in range(page_start, page_end):
            params = {key: query[key] for key in query}
            params.update({"page": page, "page_size": page_size})

            yield get_sync(self._http, self.target_url, params=params, auth=self.auth)

    def _count(self, query=None):
        query["page_size"] = 1
        query["page"] = 1

        config = {"params": query, "headers": None}
        res = get_sync(self._http, self.target_url, params=query, auth=self.auth)

        return res["count"]

    def _get_pagination(self, query=None, limit=None, offset=None):
        total_pages = self._total_pages(query)
        if offset is not None:
            offset = offset + 1
        if limit is not None:
            limit = limit + 1
        if limit is None and offset is None:
            return 1, total_pages, self.page_size
        elif limit is None and offset is not None:
            return offset, total_pages, 1
        elif limit is not None and offset is None:
            page_limit = math.ceil(float(limit)/float(self.page_size))
            page_limit = max(2, page_limit)
            return 1, page_limit, self.page_size
        else:
            return offset, offset + limit, 1

    def _total_pages(self, query=None):
        """Request results count"""
        return math.ceil(float(self._count(query))/float(self.page_size))


class IrekuaREST(RESTManager):

    def build_models(self):
        """Construct all database entities."""
        recording = self.build_recording_model()
        models = {
            'recording': recording,
        }
        return Models(**models)

    def build_recording_model(self):
        """Build REST recording model"""
        return IrekuaRecording(target_url=self.recordings_url,
                               target_attr="results",
                               page_size=self.page_size,
                               auth=self.auth,
                               bucket=self.bucket)
