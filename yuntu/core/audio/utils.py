"""Auxiliar utilities for Audio classes and methods."""
from typing import Optional
import os
import io
import hashlib
import wave
import numpy as np
import soundfile


SAMPWIDTHS = {"PCM_16": 2, "PCM_32": 4, "PCM_U8": 1}


def load_data_s3(path):
    from s3fs.core import S3FileSystem

    s3 = S3FileSystem()
    bucket = path.replace("s3://", "").split("/")[0]
    key = path.replace(f"s3://{bucket}/", "")
    return s3.open("{}/{}".format(bucket, key))


def binary_md5_s3(path, blocksize=65536):
    hasher = hashlib.md5()
    with load_data_s3(path) as media:
        buf = media.read(blocksize)
        while len(buf) > 0:
            hasher.update(buf)
            buf = media.read(blocksize)
    return hasher.hexdigest()


def load_media_info_s3(path):
    """Return media size of s3 locations."""
    from s3fs.core import S3FileSystem

    s3 = S3FileSystem()
    info = s3.info(path)
    if "size" in info:
        if info["size"] is not None:
            filesize = info["size"]
    elif "Size" in info:
        if info["Size"] is not None:
            filesize = info["Size"]
    else:
        raise ValueError(f"Could not retrieve size of file {path}")

    audio_info = soundfile.info(load_data_s3(path))

    return (
        audio_info.samplerate,
        audio_info.channels,
        audio_info.duration,
        audio_info.subtype,
        filesize,
    )


def load_media_info(path):
    """DEPRECATED: not in use. Sound file cannot handle byte objects"""
    if isinstance(path, str) and path[:5] == "s3://":
        return load_media_info_s3(path)

    audio_info = soundfile.info(path)
    filesize = media_size(path)
    return (
        audio_info.samplerate,
        audio_info.channels,
        audio_info.duration,
        audio_info.subtype,
        filesize,
    )


def binary_md5(path, blocksize=65536):
    """Hash file by blocksize."""
    if path[:5] == "s3://":
        return binary_md5_s3(path, blocksize)
    if path is None:
        raise ValueError("Path is None.")
    if not os.path.isfile(path):
        raise ValueError("Path does not exist.")
    hasher = hashlib.md5()
    with open(path, "rb") as media:
        buf = media.read(blocksize)
        while len(buf) > 0:
            hasher.update(buf)
            buf = media.read(blocksize)
    return hasher.hexdigest()


def media_size(path):
    """Return media size or None."""
    if isinstance(path, io.BytesIO):
        return len(path.getvalue())

    if path is not None:
        if os.path.isfile(path):
            return os.path.getsize(path)
    return None


def read_info(path, timeexp):
    """Read recording information form file."""
    with wave.open(path, "r") as wavfile:
        (
            nchannels,
            sampwidth,
            framerate,
            nframes,
            comptype,
            compname,
        ) = wavfile.getparams()

        return {
            "samplerate": framerate,
            "nchannels": nchannels,
            "sampwidth": sampwidth,
            "length": nframes,
            "filesize": nframes * sampwidth,
            "duration": nframes / framerate,
        }


def hash_file(path, alg="md5"):
    """Produce hash from audio recording."""
    if alg == "md5":
        return binary_md5(path)
    raise NotImplementedError("Algorithm " + alg + " is not implemented.")


def read_media(path, samplerate=None, offset=0.0, duration=None):
    """Read wav file into numpy array.

    Parameters
    ----------
    path: str or file or file-like object
    samplerate: int, optional
    offset: int or float, optional
    duration: int or float, optional
    """
    if not isinstance(offset, (int, float)):
        raise ValueError("Offset should be a number")

    offset = float(offset)
    if offset < 0:
        raise ValueError("Offset cannot be negative")

    params, audio_bytes = _read_audio_bytes(
        path,
        samplerate,
        offset,
        duration,
    )
    array = _audio_bytes_to_array(audio_bytes, params)
    return array, params[2]


def _audio_bytes_to_array(
    audio_bytes,
    params,
    normalize=True,
    dtype=np.float32,
):
    nchannels, sampwidth = params[:2]

    if sampwidth == 1:
        read_dtype = np.uint8
    elif sampwidth == 2:
        read_dtype = np.int16
    elif sampwidth == 4:
        read_dtype = np.int32
    else:
        raise NotImplementedError(
            f"WAV files with {sampwidth} sample width cannot be read"
        )

    array = np.frombuffer(audio_bytes, dtype=read_dtype)
    array = array.reshape([nchannels, -1])
    array = array.astype(dtype)

    if sampwidth == 1:
        array -= np.iinfo(np.uint8).max // 2

    if normalize:
        array /= np.iinfo(read_dtype).max

    return array


def _read_audio_bytes(path, samplerate=None, offset=0.0, duration=None):
    with wave.open(path) as wavfile:
        params = wavfile.getparams()

        if samplerate is None:
            samplerate = params[2]

        offset = int(offset * samplerate)
        tframes = params[3]

        if offset >= tframes:
            raise ValueError(
                "The offset cannot be larger than the size of the file"
            )

        if duration is None:
            frames = tframes - offset

        else:
            frames = int(duration * samplerate)

        if offset + frames > tframes:
            raise ValueError(
                "The audio is too short for the offset and duration requested"
            )

        wavfile.setpos(offset)
        audiobytes = wavfile.readframes(frames)
    return params, audiobytes


def write_media(path, signal, samplerate, nchannels, media_format="wav"):
    """Write media."""
    if media_format not in ["wav", "flac", "ogg"]:
        raise NotImplementedError(
            "Writer for " + media_format + " not implemented."
        )
    if nchannels > 1:
        signal = np.transpose(signal, (1, 0))
    soundfile.write(path, signal, samplerate, format=media_format)


def get_channel(signal, channel, nchannels):
    """Return correct channel in any case."""
    if nchannels > 1:
        return np.squeeze(signal[[channel], :])
    return signal


def resample(
    array: np.array,
    original_sr: int,
    target_sr: int,
    res_type: Optional[str] = "kaiser_best",
    fix: Optional[bool] = True,
    scale: Optional[bool] = False,
    **kwargs,
):
    """Resample audio."""
    if original_sr == target_sr:
        return y

    ratio = float(target_sr) / original_sr

    n_samples = int(np.ceil(array.shape[-1] * ratio))

    if res_type == "scipy":
        import scipy.signal

        y_hat = scipy.signal.resample(
            array,
            n_samples,
            axis=-1,
        )

    else:
        import resampy

        y_hat = resampy.resample(
            array,
            original_sr,
            target_sr,
            filter=res_type,
            axis=-1,
        )

    if scale:
        y_hat /= np.sqrt(ratio)

    return np.ascontiguousarray(y_hat, dtype=array.dtype)


def channel_mean(signal, keepdims=False):
    """Return channel mean."""
    return np.mean(signal, axis=0, keepdims=keepdims)
