"""Base classes for audio manipulation."""
from abc import ABC, abstractmethod
from pony.orm import db_session
from yuntu.core.database.base import Recording
import yuntu.core.audio.utils as audio_utils


class Media(ABC):
    """Abstract class for any media object."""

    @abstractmethod
    def build(self, meta, insert):
        """Build object from configuration input.

        This method tries insert data if 'insert' is True.
        """

    @abstractmethod
    def read(self):
        """Read media from file."""

    @abstractmethod
    def write(self, path, out_format):
        """Write media to path."""


class Audio(Media):
    """Base class for all audio."""

    db_instance = None
    path = None
    timeexp = None
    media_info = None
    read_sr = None
    mask = None
    signal = None
    samplerate = None
    meta = {}

    def __init__(self, meta, insert=False):
        self.meta = meta
        self.build(insert)

    def build(self, insert):
        if isinstance(self.meta, Recording):
            self.db_instance = self.meta
            self.media_info = self.db_instance.media_info
            self.timeexp = self.db_instance.timeexp
            self.path = self.db_instance.path
        elif "id" in self.meta:
            self.retrieve()
        elif insert:
            self.insert()
        elif "path" not in self.meta or "timeexp" not in self.meta:
            raise ValueError("Config dictionary must include both, path \
                             and time expansion.")
        if self.db_instance is None:
            self.timeexp = self.meta["timeexp"]
            self.path = self.meta["path"]
            self.media_info = audio_utils.read_info(self.meta["path"],
                                                    self.meta["timeexp"])
        self.read_sr = self.media_info["samplerate"]

    def set_read_sr(self, read_sr=None):
        """Set read sample rate for future loadings."""
        if read_sr is None:
            read_sr = self.media_info["samplerate"]
        if self.signal is not None and self.read_sr != read_sr:
            self.clear()
        self.read_sr = read_sr

    def set_mask(self, limits=None):
        """Set read mask.

        A read mask is a time interval that determines the part of
        the recording that is going to be read and affects any output that
        uses loaded data.
        """
        if limits is not None:
            offset = limits[0]
            duration = limits[1] - limits[0]
            self.mask = (offset, duration)
        else:
            self.mask = None
        self.clear()

    def unset_mask(self):
        """Unset read mask."""
        self.set_mask()

    def get_signal(self, pre_proc=None, refresh=False):
        """Read signal from file (mask sensitive, lazy loading)."""
        if self.signal is not None and not refresh:
            return self.signal
        self.read()
        if pre_proc:
            return pre_proc(self.signal)
        return self.signal

    def get_spec(self,
                 channel=0,
                 n_fft=1024,
                 hop_length=512,
                 pre_proc=None,
                 refresh=False):
        """Compute spectrogram (mask sensitive)."""
        if channel is None:
            signal = audio_utils.channel_mean(self.get_signal(pre_proc,
                                                              refresh))
        elif channel > self.media_info["nchannels"]:
            raise ValueError("Channel outside range.")
        else:
            signal = audio_utils.get_channel(self.get_signal(pre_proc,
                                                             refresh),
                                             channel,
                                             self.media_info["nchannels"])
        spec = audio_utils.spectrogram(signal,
                                       n_fft=n_fft,
                                       hop_length=hop_length)
        freqs = audio_utils.spec_frequencies(self.samplerate, n_fft)
        return spec, freqs

    def clear(self):
        """Clear cached data."""
        self.signal = None
        self.samplerate = None

    @db_session
    def insert(self):
        """Insert intself to database."""
        if self.db_instance is None:
            if "media_info" not in self.meta:
                path = self.meta["path"]
                timeexp = self.meta["timeexp"]
                self.meta["media_info"] = audio_utils.read_info(path,
                                                                timeexp)
            if "hash" not in self.meta:
                self.meta["hash"] = audio_utils.hash_file(self.meta["path"])
            self.db_instance = Recording(**self.meta)
            self.media_info = self.db_instance.media_info
            self.timeexp = self.db_instance.timeexp
            self.path = self.db_instance.path
            self.unset_mask()

    @db_session
    def retrieve(self):
        """Retrieve from database."""
        if self.db_instance is None:
            self.db_instance = Recording[self.meta["id"]]
            self.media_info = self.db_instance.media_info
            self.timeexp = self.db_instance.timeexp
            self.path = self.db_instance.path
            self.unset_mask()

    def read(self):
        """Read signal from file (mask sensitive, lazy loading)."""
        if self.mask is not None:
            offset, duration = self.mask
            self.signal, self.samplerate = audio_utils.read_media(self.path,
                                                                  self.read_sr,
                                                                  offset,
                                                                  duration)
        else:
            self.signal, self.samplerate = audio_utils.read_media(self.path,
                                                                  self.read_sr)

    def write(self,
              path,
              media_format="wav",
              samplerate=None):
        """Write media to path."""
        signal = self.get_signal()
        out_sr = self.samplerate
        if samplerate is not None:
            out_sr = samplerate
        audio_utils.write_media(self.path,
                                signal,
                                out_sr,
                                self.media_info["nchannels"],
                                media_format)
