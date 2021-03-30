import os
import pytest
from yuntu import Audio

from tests.fixtures.audio import *


TEST_DATA = os.path.join(
    os.path.dirname(__file__),
    "test_data",
)


def test_load_filesystem_file(random_wav_file):
    audio = Audio(
        os.path.join(
            TEST_DATA,
            "selvaAltaPerennifolia_estacionLosTuxtlas_Ver_830am.wav",
        )
    )

    assert audio.media_info.nchannels == 1
    assert audio.media_info.sampwidth == 2
    assert audio.media_info.samplerate == 48000
    assert audio.media_info.length == 14400000

    audio = Audio(random_wav_file(sample_rate=16000, duration=2))

    assert audio.media_info.nchannels == 1
    assert audio.media_info.samplerate == 16000
    assert audio.media_info.length == 16000 * 2

    audio = Audio(random_wav_file(sample_rate=8000, channels=2, duration=3))

    assert audio.media_info.nchannels == 2
    assert audio.media_info.samplerate == 8000
    assert audio.media_info.length == 8000 * 3


@pytest.mark.parametrize(
    "url",
    [
        "https://freewavesamples.com/files/Ensoniq-ZR-76-01-Dope-77.wav",
        "https://www2.cs.uic.edu/~i101/SoundFiles/ImperialMarch60.wav",
        "https://www2.cs.uic.edu/~i101/SoundFiles/taunt.wav",
    ],
)
def test_load_remote_http_file(url):
    audio = Audio(url)
    assert audio.media_info.duration > 0
