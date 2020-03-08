"""Auxiliar utilities for audio classes and methods."""
import os
import hashlib
import wave
import numpy as np
import librosa
import soundfile


def binary_md5(path, blocksize=65536):
    """Hash file by blocksize"""
    if path is not None:
        if os.path.isfile(path):
            hasher = hashlib.md5()
            with open(path, "rb") as media:
                buf = media.read(blocksize)
                while len(buf) > 0:
                    hasher.update(buf)
                    buf = media.read(blocksize)
            return hasher.hexdigest()
        else:
            return None
    else:
        return None


def media_size(path):
    """Return media size or None."""
    if path is not None:
        if os.path.isfile(path):
            return os.path.getsize(path)
    return None


def read_info(path, timeexp):
    """Read recording information form file."""
    wav = wave.open(path)
    media_info = {}
    media_info["samplerate"] = wav.getframerate()
    media_info["nchannels"] = wav.getnchannels()
    media_info["sampwidth"] = wav.getsampwidth()
    media_info["length"] = wav.getnframes()
    media_info["filesize"] = media_size(path)
    media_info["duration"] = (float(media_info["length"]) /
                              float(media_info["samplerate"])) / timeexp
    return media_info


def hash_file(path, alg="md5"):
    """Produce hash from audio recording."""
    if alg == "md5":
        return binary_md5(path)
    raise NotImplementedError("Algorithm "+alg+" is not implemented.")


def read_media(path,
               samplerate,
               offset=0.0,
               duration=None):
    """Read media."""
    return librosa.load(path,
                        sr=samplerate,
                        offset=offset,
                        duration=duration,
                        mono=False)


def write_media(path,
                signal,
                samplerate,
                nchannels,
                media_format="wav"):
    """Write media."""
    if media_format not in ["wav", "flac", "ogg"]:
        raise NotImplementedError("Writer for " + media_format
                                  + " not implemented.")
    if nchannels > 1:
        signal = np.transpose(signal, (1, 0))
    soundfile.write(path,
                    signal,
                    samplerate,
                    format=media_format)


def get_channel(signal, channel, nchannels):
    """Return correct channel in any case."""
    if nchannels > 1:
        return np.squeeze(signal[[channel], :])
    return signal


def channel_mean(signal, keepdims=False):
    """Return channel mean."""
    return np.mean(signal,
                   axis=0,
                   keepdims=keepdims)


def stft(signal,
         n_fft,
         hop_length,
         win_length=None,
         window='hann',
         center=True):
    """Shor Time Fourier Transform."""
    return librosa.stft(signal,
                        n_fft,
                        hop_length,
                        win_length,
                        window,
                        center)


def spectrogram(signal,
                n_fft,
                hop_length):
    """Return standard spectrogram."""
    return np.abs(stft(signal,
                       n_fft,
                       hop_length))


def spec_frequencies(samplerate,
                     n_fft):
    """Return frequency vector for stft parameters."""
    return librosa.core.fft_frequencies(sr=samplerate,
                                        n_fft=n_fft)
