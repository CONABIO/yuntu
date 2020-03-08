"""Base classes for audio manipulation."""
from abc import ABC, abstractmethod
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

    db_entry = None
    path = None
    timeexp = None
    media_info = None
    metadata = None
    read_sr = None
    mask = None
    signal = None
    samplerate = None
    meta = {}

    def __init__(self, meta, mask=None, insert=False):
        self.mask = mask
        self.meta = meta
        self.build(insert)

    def is_recording(self, metadata):
        if not hasattr(metadata, 'media_info'):
            return False

        if not hasattr(metadata, 'timeexp'):
            return False

        if not hasattr(metadata, 'path'):
            return False

        if not hasattr(metadata, 'metadata'):
            return False

        return True

    def build(self, insert):
        if not hasattr(self.meta, 'path') or not hasattr(self.meta, 'timeexp'):
            raise ValueError("Config dictionary must include both, path \
                             and time expansion.")
        if self.is_recording(self.meta):
            self.db_entry = self.meta
            self.timeexp = self.db_entry.timeexp
            self.path = self.db_entry.path
            self.media_info = self.db_entry.media_info
            self.metadata = self.db_entry.metadata

        if self.db_entry is None:
            self.timeexp = self.meta["timeexp"]
            self.path = self.meta["path"]
            self.media_info = audio_utils.read_info(self.meta["path"],
                                                    self.meta["timeexp"])
            if "metadata" in self.meta:
                self.metadata = self.meta["metadata"]
        self.read_sr = self.media_info["samplerate"]

    def set_read_sr(self, read_sr=None):
        """Set read sample rate for future loadings."""
        if read_sr is None:
            read_sr = self.media_info["samplerate"]
        if self.signal is not None and self.read_sr != read_sr:
            self.clear()
        self.read_sr = read_sr

    def slice(self, limits=None):
        """Should return a new Audio object with mask initialized at limits."""
        if limits is not None:
            offset = limits[0]
            duration = limits[1] - limits[0]
            mask = (offset, duration)
        else:
            mask = None
        return Audio(self.meta, mask)

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
