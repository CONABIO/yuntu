import numpy as np
import xarray

from yuntu.core.media.base import Media
from yuntu.core.media.mixins import XArrayMixin

from yuntu.core.plotters import plotter_descriptor
from yuntu.audio.plotting import AudioPlotter


class Audio(XArrayMixin, Media):

    plot = plotter_descriptor(AudioPlotter)

    def __init__(
        self,
        *args,
        start_time=None,
        end_time=None,
        time_expansion=1,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)

        self.start_time = start_time
        self.end_time = end_time
        self.time_expansion = time_expansion

        if time_expansion != 1:
            self.expand(time_expansion)

    def expand(self, time_expansion):
        self.coords["time"] = self.coords["time"] / time_expansion
        self.time_expansion = time_expansion

    def play(self, speed=1, channel=0):
        from IPython.display import Audio

        samplerate = self.media_info.samplerate
        return Audio(
            data=self.array[:, channel],
            rate=samplerate * speed,
        )

    def __repr__(self):
        kwargs = {}

        if self.path is not None:
            kwargs["path"] = self.path

        if self.start_time is not None:
            kwargs["start_time"] = self.start_time

        if self.end_time is not None:
            kwargs["end_time"] = self.end_time

        if self.time_expansion != 1:
            kwargs["time_expansion"] = 1

        kwargs_str = ", ".join(
            f"{key}={value}" for key, value in kwargs.items()
        )
        return f"Audio({kwargs_str})"

    def _get_reader_kwargs(self):
        return {
            "start_time": self.start_time * self.time_expansion,
            "end_time": self.end_time * self.time_expansion,
        }

    def _prepare_content(self, content):
        if self.start_time is None:
            start_time = 0
        else:
            start_time = self.start_time * self.time_expansion

        if self.end_time is None:
            end_time = self.media_info.duration
        else:
            end_time = self.end_time * self.time_expansion

        return xarray.DataArray(
            content,
            coords=[
                (
                    "time",
                    np.linspace(start_time, end_time, content.shape[0]),
                ),
                ("channel", range(content.shape[1])),
            ],
        )
