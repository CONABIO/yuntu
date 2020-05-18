"""Prebuilt t datastores for common cases."""
import glob
import os
from collections import OrderedDict
import struct
import re
from datetime import datetime

from yuntu.datastore.base import Datastore
from yuntu.core.audio.utils import hash_file

RIFF_ID_LENGTH = 4
LENGTH_OF_COMMENT = 128
PCM_FORMAT = 1

RIFF_FORMAT = f'{RIFF_ID_LENGTH}s'
uint32_t = 'I'
uint16_t = 'H'


class CustomStruct(OrderedDict):
    endian = '<'

    def __init__(self, objects):
        super().__init__(objects)

        self.struct = struct.Struct(self.collapse())
        self.size = self.struct.size

    def collapse(self):
        fmt = self.endian
        for value in self.values():
            if isinstance(value, CustomStruct):
                fmt += value.collapse().replace(self.endian, '')
            else:
                fmt += value

        return fmt

    def unpack(self, buffer):
        results = {}

        index = 0
        for key, value in self.items():
            if isinstance(value, CustomStruct):
                size = value.size
            else:
                size = struct.calcsize(value)

            subbuffer = buffer[index:index + size]

            if isinstance(value, CustomStruct):
                unpacked = value.unpack(subbuffer)
            else:
                unpacked = struct.unpack(self.endian + value, subbuffer)[0]

            results[key] = unpacked
            index += size

        return results


chunk_t = CustomStruct([
    ('id', RIFF_FORMAT),
    ('size', uint32_t)
])

icmt_t = CustomStruct([
    ('icmt', chunk_t),
    ('comment', f'{LENGTH_OF_COMMENT}s')
])

wavFormat_t = CustomStruct([
    ('format', uint16_t),
    ('numberOfChannels', uint16_t),
    ('samplesPerSecond', uint32_t),
    ('bytesPerSecond', uint32_t),
    ('bytesPerCapture', uint16_t),
    ('bitsPerSample', uint16_t),
])

wavHeader_t = CustomStruct([
    ('riff', chunk_t),
    ('format', RIFF_FORMAT),
    ('fmt', chunk_t),
    ('wavFormat', wavFormat_t),
    ('list', chunk_t),
    ('info', RIFF_FORMAT),
    ('icmt', icmt_t),
    ('data', chunk_t),
])


def read_am_header(path):
    with open(path, 'rb') as buffer:
        data = wavHeader_t.unpack(buffer.read(wavHeader_t.size))

    return data


id_regex = re.compile(r'AudioMoth ([0-9A-Z]{16})')


def get_am_id(comment):
    match = id_regex.search(comment)
    return match.group(1)


date_regex = re.compile(r'((\d{2}:\d{2}:\d{2}) (\d{2}\/\d{2}\/\d{4}) \((UTC((-|\+)(\d+))?)\))')


def get_am_datetime(comment):
    match = date_regex.search(comment)
    timezone = match.group

    raw = match.group(1)
    time = match.group(2)
    date = match.group(3)

    tz = match.group(4)

    try:
        offset_direction = match.group(6)
        offset = match.group(7)

        if len(offset) != 4:
            offset = '{:02d}00'.format(int(offset))

        new_tz = tz.replace(match.group(5), offset_direction + offset)
        raw = raw.replace(tz, new_tz)
        tz = new_tz
    except Exception:
        pass

    datetime_format = '%H:%M:%S %d/%m/%Y (%Z%z)'

    return {
        'raw': raw,
        'time': time,
        'date': date,
        'tz': tz,
        'datetime': datetime.strptime(raw, datetime_format),
        'format': datetime_format
    }


gain_regex = re.compile(r'gain setting (\d)')


def get_am_gain(comment):
    match = gain_regex.search(comment)
    return match.group(1)


battery_regex = re.compile(r'battery state was (\d.\dV)')


def get_am_battery_state(comment):
    match = battery_regex.search(comment)
    return match.group(1)


class AudioMothDatastore(Datastore):
    def __init__(self, path, tqdm=None):
        self.path = path
        self.tqdm = tqdm

    def iter(self):
        if self.tqdm is not None:
            for fname in self.tqdm(glob.glob(os.path.join(self.path,
                                                          '*.WAV'))):
                yield fname
        else:
            for fname in glob.glob(os.path.join(self.path, '*.WAV')):
                yield fname

    def iter_annotations(self, datum):
        return []

    def prepare_datum(self, datum):
        header = read_am_header(datum)

        nchannels = header['wavFormat']['numberOfChannels']
        samplerate = header['wavFormat']['samplesPerSecond']
        sampwidth = header['wavFormat']['bitsPerSample']
        filesize = header['riff']['size'] + 4
        length = int(8 * (filesize - wavHeader_t.size) /
                     (nchannels * sampwidth))

        media_info = {
            'nchannels': nchannels,
            'sampwidth': sampwidth,
            'samplerate': samplerate,
            'length': length,
            'filesize': filesize,
            'duration': length / samplerate
        }

        spectrum = 'ultrasonic' if samplerate > 50000 else 'audible'

        comment = header['icmt']['comment'].decode('utf-8').rstrip('\x00')
        battery = get_am_battery_state(comment)
        gain = get_am_gain(comment)
        am_id = get_am_id(comment)

        metadata = {
            'am_id': am_id,
            'gain': gain,
            'battery': battery,
            'comment': comment,
        }

        datetime_info = get_am_datetime(comment)
        datetime = datetime_info['datetime']

        return {
            'path': datum,
            'hash': hash_file(datum),
            'timeexp': 1,
            'media_info': media_info,
            'metadata': metadata,
            'spectrum': spectrum,
            'time_raw': datetime_info['raw'],
            'time_format': datetime_info['format'],
            'time_zone': datetime.tzinfo.tzname(datetime),
            'time_utc': datetime
        }

    def prepare_annotation(self, datum, annotation):
        pass
