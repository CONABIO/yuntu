import string
import random

import pytest
import numpy as np
import xarray as xr
import soundfile

from yuntu.audio import Audio


def random_string():
    options = string.ascii_uppercase + string.digits
    return "".join(random.choice(options) for _ in range(10))


@pytest.fixture
def random_wav_file(tmp_path):
    def wav_factory(path=None, samplerate=22100, duration=0.1, channels=1):
        if path is None:
            path = tmp_path / (random_string() + ".wav")

        frames = int(samplerate * duration)
        shape = (frames, channels)
        array = np.random.random(shape)
        soundfile.write(path, array, samplerate)
        return path

    return wav_factory


@pytest.mark.parametrize(
    "samplerate,duration,channels",
    [
        (22100, 0.1, 1),
        (22100, 0.2, 1),
        (22100, 0.1, 2),
        (192000, 0.01, 2),
        (192000, 0.01, 1),
    ],
)
def test_audio_media_info(samplerate, duration, channels, random_wav_file):
    path = random_wav_file(
        samplerate=samplerate,
        duration=duration,
        channels=channels,
    )

    audio = Audio(path)

    frames = int(duration * samplerate)

    assert audio.media_info.samplerate == samplerate
    assert audio.media_info.duration == duration
    assert audio.media_info.channels == channels
    assert audio.media_info.frames == frames
    assert audio.shape == (frames, channels)


def test_load_remote_audio():
    audio = Audio(
        "https://file-examples-com.github.io/uploads/2017/11/file_example_WAV_1MG.wav"
    )

    assert isinstance(audio.array, xr.DataArray)
    assert audio.media_info.samplerate > 0
