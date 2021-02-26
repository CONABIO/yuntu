import math
from collections import namedtuple
import requests
from dateutil.parser import parse as dateutil_parse
import datetime

from yuntu.core.database.REST.base import RESTManager
from yuntu.core.database.REST.models import RESTModel

# import textwrap
#
#
# def print_roundtrip(response, *args, **kwargs):
#     format_headers = lambda d: '\n'.join(f'{k}: {v}' for k, v in d.items())
#     print(textwrap.dedent('''
#         ---------------- request ----------------
#         {req.method} {req.url}
#         {reqhdrs}
#
#         {req.body}
#         ---------------- response ----------------
#         {res.status_code} {res.reason} {res.url}
#         {reshdrs}
#
#         {res.text}
#     ''').format(
#         req=response.request,
#         res=response,
#         reqhdrs=format_headers(response.request.headers),
#         reshdrs=format_headers(response.headers),
#     ))

MODELS = [
    'recording',
]
Models = namedtuple('Models', MODELS)


class IrekuaRecording(RESTModel):

    def parse(self, datum):
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
            return {}

        if not isinstance(query, dict):
            raise ValueError("When using REST collections, queries should " +
                             "be specified with a dictionary that contains " +
                             "url parameters.")
        return query

    def count(self, query=None):
        """Request results count"""
        query = self.validate_query(query)
        query["page_size"] = 1
        query["page"] = 1

        res = requests.get(self.target_url,
                           params=query,
                           auth=self.auth)

        if res.status_code != 200:
            raise ValueError("Connection error!")

        return res.json()["count"]

    def iter_pages(self, query=None, limit=None, offset=None):
        query = self.validate_query(query)
        page_start, page_end, page_size = self._get_pagination(query=query,
                                                               limit=limit,
                                                               offset=offset)
        for page_number in range(page_start, page_end):
            query["page_size"] = page_size
            query["page"] = page_number
            res = requests.get(self.target_url,
                               params=query,
                               auth=self.auth)

            if res.status_code != 200:
                res = requests.get(self.target_url,
                                   params=query,
                                   auth=self.auth)
                                   # hooks={'response': print_roundtrip})
                raise ValueError(str(res))

            yield res.json()

    def _get_pagination(self, query=None, limit=None, offset=None):
        total_pages = self._total_pages(query)
        if offset is not None:
            offset = offset + 1
        if limit is None and offset is None:
            return 1, total_pages, self.page_size
        elif limit is None and offset is not None:
            return offset, total_pages, 1
        elif limit is not None and offset is None:
            return 1, limit, 1
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
