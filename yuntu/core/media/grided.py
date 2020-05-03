from yuntu.core.media import time
from yuntu.core.media import frequency
from yuntu.core.media import time_frequency


class GridedMediaMixin:
    def __init__(self, media, **kwargs):
        self.media = media
        super().__init__(**kwargs)


class TimeGridedMediaMixin(
        time.TimeMediaMixin,
        GridedMediaMixin):
    def __init__(self, media, window_length, hop_length=None, **kwargs):
        self.window_length = window_length
        self.hop_length = hop_length
        super().__init__(media, **kwargs)

    def compute(self):
        pass


class FrequencyGridedMediaMixin(
        frequency.FrequencyMediaMixin,
        GridedMediaMixin):
    def __init__(self, media, window_length, hop_length=None, **kwargs):
        self.window_length = window_length
        self.hop_length = hop_length
        super().__init__(media, **kwargs)


class TimeFrequencyGridedMediaMixin(
        time_frequency.TimeFrequencyMediaMixin,
        GridedMediaMixin):
    def __init__(
            self,
            media,
            time_window_length,
            freq_window_length,
            freq_hop_length=None,
            time_hop_length=None,
            **kwargs):
        self.time_window_length = time_window_length
        self.freq_window_length = freq_window_length

        self.freq_hop_length = freq_hop_length
        self.time_hop_length = time_hop_length

        super().__init__(media, **kwargs)
