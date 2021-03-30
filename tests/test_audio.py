import os
import numpy as np

import pytest
from tests.fixtures.audio import *
from yuntu.core.audio import utils

TEST_DATA = os.path.join(
    os.path.dirname(__file__),
    "test_data",
)


def test_audio_bytes_to_array():
    array = np.ones(8000, dtype="<h")
    wav_bytes = wav_bytes_from_array(array, sample_rate=8000)
    params, audio_bytes = utils._read_audio_bytes(wav_bytes)
    recovered = utils._audio_bytes_to_array(
        audio_bytes,
        params,
        dtype=np.int16,
        normalize=False,
    )
    assert (array == recovered).all()

    array = np.ones(8000, dtype="<i")
    wav_bytes = wav_bytes_from_array(array, sample_width=4, sample_rate=8000)
    params, audio_bytes = utils._read_audio_bytes(wav_bytes)
    recovered = utils._audio_bytes_to_array(
        audio_bytes,
        params,
        dtype=np.int32,
        normalize=False,
    )
    assert (array == recovered).all()

    array = np.ones([2, 8000])
    array = np.array([1, 2])[:, None] * array
    array = array.astype(np.int16)
    wav_bytes = wav_bytes_from_array(array, sample_width=2, sample_rate=8000)
    params, audio_bytes = utils._read_audio_bytes(wav_bytes)
    recovered = utils._audio_bytes_to_array(
        audio_bytes,
        params,
        dtype=np.int16,
        normalize=False,
    )
    assert (array == recovered).all()

    array = np.ones([2, 8000])
    array = np.array([1, 2])[:, None] * array
    array = array.astype(np.int32)
    wav_bytes = wav_bytes_from_array(array, sample_width=4, sample_rate=8000)
    params, audio_bytes = utils._read_audio_bytes(wav_bytes)
    recovered = utils._audio_bytes_to_array(
        audio_bytes,
        params,
        dtype=np.int32,
        normalize=False,
    )
    assert (array == recovered).all()


def test_read_media_is_equal_to_librosa():
    import librosa

    path = os.path.join(
        TEST_DATA,
        "selvaAltaPerennifolia_estacionLosTuxtlas_Ver_830am.wav",
    )

    wav_librosa, sr_librosa = librosa.load(path, sr=None)
    wav_yuntu, sr_yuntu = utils.read_media(path)

    assert sr_librosa == sr_yuntu
    assert np.isclose(
        wav_librosa,
        wav_yuntu,
        rtol=0.0001,
    ).all()


@pytest.mark.parametrize(
    "samplerate,channels,samplewidth,duration",
    [
        (8000, 1, 2, 1),
        (44100, 2, 4, 1),
    ],
)
def test_read_media(
    random_wav_file,
    samplerate,
    channels,
    samplewidth,
    duration,
):
    wav, sr = utils.read_media(
        random_wav_file(
            sample_rate=samplerate,
            channels=channels,
            sample_width=samplewidth,
            duration=duration,
        ),
        samplerate,
    )
    assert wav.shape[0] == channels
    assert sr == samplerate
