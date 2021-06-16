import json
from collections import namedtuple
from dateutil.parser import parse as dateutil_parse
import urllib3
import datetime
import math

from yuntu.core.database.REST.base import RESTManager
from yuntu.core.database.REST.models import RESTModel

RMODELS = [
    'recording',
]

IMODELS = [
    'image',
]


MAX_PAGE_SIZE = 1000
RModels = namedtuple('Models', RMODELS)
IModels = namedtuple('Models', IMODELS)

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

    return json.loads(res.data.decode('utf-8'))


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
        npages = math.ceil(float(limit)/float(self.page_size))

        for page in range(npages):
            params = {key: query[key] for key in query}
            params.update({"offset": offset + page*self.page_size,
                           "limit": self.page_size})
            yield get_sync(self._http, self.target_url, params=params, auth=self.auth)

    def _count(self, query=None):
        query["limit"] = 1
        return get_sync(self._http, self.target_url,
                        params=query, auth=self.auth)["count"]



class IrekuaREST(RESTManager):

    def build_models(self):
        """Construct all database entities."""
        recording = self.build_recording_model()
        models = {
            'recording': recording,
        }
        return RModels(**models)

    def build_recording_model(self):
        """Build REST recording model"""
        return IrekuaRecording(target_url=self.recordings_url,
                               target_attr="results",
                               page_size=self.page_size,
                               base_filter=self.base_filter,
                               auth=self.auth,
                               bucket=self.bucket)

    
class IrekuaImage(RESTModel):
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
        """Parse image item from irekua REST api"""
        if self.bucket is None:
            path = datum["item_file"]
        else:
            key = "media" + datum["item_file"].split("media")[-1]
            path = f"s3://{self.bucket}/{key}"
#        samplerate = datum["media_info"]["sampling_rate"]
        media_info = {
            'nchannels': 3, # TODO
            'imgwidth': datum["media_info"]["image_width"],
            'imgheight': datum["media_info"]["image_length"],
            'filesize': datum["filesize"]
        }
        dtime_zone = datum["captured_on_timezone"]
        dtime = dateutil_parse(datum["captured_on"])
        dtime_format = "%H:%M:%S %d/%m/%Y (%z)"
        dtime_raw = datetime.datetime.strftime(dtime, format=dtime_format)

        return {
            'id': datum['id'],
            'path': path,
            'hash': datum["hash"],
            'media_info': media_info,
            'metadata': datum,
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
        npages = math.ceil(float(limit)/float(self.page_size))

        for page in range(npages):
            params = {key: query[key] for key in query}
            params.update({"offset": offset + page*self.page_size,
                           "limit": self.page_size})
            yield get_sync(self._http, self.target_url, params=params, auth=self.auth)

    def _count(self, query=None):
        query["limit"] = 1
        return get_sync(self._http, self.target_url,
                        params=query, auth=self.auth)["count"]



class ImageIrekuaREST(RESTManager):

    def build_models(self):
        """Construct all database entities."""
        image = self.build_image_model()
        models = {
            'image': image,
        }
        return IModels(**models)

    def build_image_model(self):
        """Build REST recording model"""
        return IrekuaImage(target_url=self.recordings_url,
                               target_attr="results",
                               page_size=self.page_size,
                               base_filter=self.base_filter,
                               auth=self.auth,
                               bucket=self.bucket)
