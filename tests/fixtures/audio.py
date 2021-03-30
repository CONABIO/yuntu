import uuid
import wave
from io import BytesIO

import numpy as np
import pytest


def wav_bytes_from_array(
    array,
    sample_rate=44100,
    sample_width=2,
):
    fp = BytesIO()

    if len(array.shape) == 0:
        channels = 1
    else:
        channels = array.shape[0]

    with wave.open(fp, mode="wb") as wav:
        wav.setnchannels(channels)
        wav.setsampwidth(sample_width)
        wav.setframerate(sample_rate)
        wav.writeframes(array)

    fp.seek(0)
    return fp


def create_random_wav(
    sample_rate=44100,
    duration=1,
    sample_width=2,
    channels=1,
):
    if sample_width not in [2, 4]:
        raise NotImplementedError

    length = int(sample_rate * duration)
    shape = (channels, length)
    array = np.random.random(size=shape)

    exponent = 8 * sample_width - 1
    dtype = "<h" if sample_width == 2 else "<i"
    audio = (array * (2 ** exponent - 1)).astype(dtype)

    fp = BytesIO()

    with wave.open(fp, mode="wb") as wav:
        wav.setnchannels(channels)
        wav.setsampwidth(sample_width)
        wav.setframerate(sample_rate)
        wav.writeframes(audio.tobytes())

    fp.seek(0)

    return fp


@pytest.fixture
def random_wav_bytes():
    return create_random_wav


@pytest.fixture
def random_wav_file(random_wav_bytes, tmp_path):
    def create_random_wav_file(
        sample_rate=44100,
        duration=1,
        sample_width=2,
        channels=1,
        filename=None,
    ):
        if filename is None:
            filename = f"{uuid.uuid4()}.wav"

        wav_bytes = random_wav_bytes(
            sample_rate=sample_rate,
            duration=duration,
            sample_width=sample_width,
            channels=channels,
        )

        wavfile = tmp_path / filename
        wavfile.write_bytes(wav_bytes.read())

        return str(wavfile.resolve())

    return create_random_wav_file
