import json
from collections import namedtuple
from dateutil.parser import parse as dateutil_parse
import datetime
import copy
import math
import nest_asyncio
nest_asyncio.apply()
import aiohtt
from aiohttp_retry import RetryClient
import asyncio


from yuntu.core.database.REST.base import RESTManager
from yuntu.core.database.REST.models import RESTModel


MODELS = [
    'recording',
]
Models = namedtuple('Models', MODELS)

async def get_async(client, url, params=None, headers=None):
    retry_client = RetryClient(client)
    async with retry_client.get(url, params=params, headers=headers,
                                retry_attempts=100, retry_for_statuses=[504]) as resp:
        resp = await resp.json()
        resp["params"] = params
        return resp

async def fetch_multi_async(url, configs, auth=None):
    async with aiohttp.ClientSession(auth=auth) as session:
        tasks = []
        for conf in configs:
            task = asyncio.ensure_future(get_async(session, url, conf["params"], conf["headers"]))
            tasks.append(task)
        resp = await asyncio.gather(*tasks, return_exceptions=True)
        return sorted(resp, key=lambda k: k["params"]["page"])

class IrekuaRecording(RESTModel):

    def __init__(self, target_url,
                 target_attr="results",
                 page_size=1, auth=None,
                 bucket='irekua',
                 base_filter={"mime_type": 49},
                 batch_size=10):
        self.target_url = target_url
        self.target_attr = target_attr
        self._auth = aiohttp.BasicAuth(auth[0], auth[1])
        self._page_size = page_size
        self.bucket = bucket
        self.base_filter = base_filter
        self.batch_size = batch_size

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


    def batch_pages(self, query=None, limit=None, offset=None):
        query = self.validate_query(query)
        page_start, page_end, page_size = self._get_pagination(query=query,
                                                               limit=limit,
                                                               offset=offset)
        pages = list(range(page_start, page_end))
        batched_page_numbers = ([pages[i:i+self.batch_size]
                                for i in range(0, len(pages), self.batch_size)])

        for pnumbers in batched_page_numbers:
            batch = []
            for page in pnumbers:
                params = copy.deepcopy(query)
                params.update({"page": page, "page_size": page_size})
                batch.append({"params": params, "headers": None})
            yield batch

    def iter_pages(self, query=None, limit=None, offset=None):
        page_batches = self.batch_pages(query, limit, offset)

        for batch in page_batches:
            results = (asyncio.get_event_loop()
                       .run_until_complete(
                           fetch_multi_async(self.target_url, batch, self.auth)))
            for page in results:
                yield page

    def _count(self, query=None):
        query["page_size"] = 1
        query["page"] = 1

        config = {"params": query, "headers": None}
        res = (asyncio.get_event_loop()
               .run_until_complete(
                   fetch_multi_async(self.target_url, [config], self.auth)))

        return res[0]["count"]

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
                               auth=self.auth)
