from yuntu.core.plotters import get_ax
from yuntu.core.utils.plugins import FuncMount


class AudioPlotter(metaclass=FuncMount):
    pass


@AudioPlotter.register
def waveform(audio, **kwargs):
    """Generate the waveform of an audio.

    Parameters
    ----
    audio: Audio

    Returns
    -------
    Axes
    """
    ax = get_ax(**kwargs)
    ax.plot(audio.sel(dict(channel=0)))
    return ax


@AudioPlotter.register
def spectrogram(audio, **kwargs):
    ax = get_ax(**kwargs)
    ax.plot(audio.sel(dict(channel=0)))
    return ax
