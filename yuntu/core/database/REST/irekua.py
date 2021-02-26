import math
from collections import namedtuple
import requests
import dateutil.parser
import datetime

from yuntu.core.database.REST.base import RESTManager
from yuntu.core.database.REST.models import RESTModel

MODELS = [
    'recording',
]
Models = namedtuple('Models', MODELS)


class IrekuaRecording(RESTModel):

    def parse(self, meta):
        """Parse audio item from irekua REST api"""
        path = datum["item_file"]
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
        metadata = {
            'item_url': datum["url"],
            'page_conf': datum["page_conf"]
        }

        dtime_zone = datum["captured_on_timezone"]
        dtime = parse(datum["captured_on"])
        dtime_format = "%H:%M:%S %d/%m/%Y (%z)"
        dtime_raw = datetime.datetime.strftime(dtime, format=dtime_format)

        return {
            'id': datum['id'],
            'path': path,
            'hash': datum["hash"],
            'timeexp': 1,
            'media_info': media_info,
            'metadata': metadata,
            'spectrum': spectrum,
            'time_raw': dtime_raw,
            'time_format': dtime_format,
            'time_zone': dtime_zone,
            'time_utc': dtime
        }

    def validate_query(self, query):
        if query is None:
            return {}
        if not isinstance(query, dict):
            raise ValueError("When using REST collections, queries should " +
                             "be specified with a dictionary that contains" +
                             "url parameters.")
        return query

    def count(self, query=None):
        """Request results count"""
        query = self.validate_query(query)
        query["page_size"] = 1
        query["page"] = 0

        one_page = requests.get(self.target_url,
                                params=query,
                                auth=self.auth)
        if len(one_page) == 0:
            return 0
        return one_page[0]["count"]

    def iter_pages(self, query=None, limit=None, offset=None):
        query = self.validate_query(query)
        page_start, page_end, page_size = self._get_pagination(limit, offset)
        for page_number in range(page_start, page_end):
            query["page_size"] = page_size
            query["page"] = page
            res = requests.get(self.target_url,
                               params=query,
                               auth=self.auth)

            if res.status_code != 200:
                res = requests.get(self.target_url,
                                   params=query,
                                   auth=self.auth)
                raise ValueError(str(res))

            res_json = res.json()
            res_json["page_conf"] = {
                'page_size': page_size,
                'page_number': page_number
            }
            yield res_json

    def _get_pagination(self, query=None, limit=None, offset=None):
        total_pages = self._total_pages(query)
        if limit is None and offset is None:
            return 0, total_pages, self.page_size
        elif limit is None and offset is not None:
            return offset, total_pages, 1
        elif limit is not None and offset is None:
            return 0, limit, 1
        else:
            return offset, offset + limit, 1

    def _total_pages(self, query=None):
        """Request results count"""
        return math.ceil(float(self.count(query))/float(self.page_size))


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
                               auth=self.auth)
