"""Auxiliar utilities for Image classes and methods."""
from typing import Optional
import os
import io
import hashlib
#import wave
import numpy as np
#import librosa
import PIL.Image
import cv2
import requests
from io import BytesIO
from urllib.parse import urlparse


SAMPWIDTHS = {
    'PCM_16': 2,
    'PCM_32': 4,
    'PCM_U8': 1
}

def load_data_s3(path):
    from s3fs.core import S3FileSystem
    s3 = S3FileSystem()
    bucket = path.replace("s3://", "").split("/")[0]
    key = path.replace(f"s3://{bucket}/", "")
    return s3.open('{}/{}'.format(bucket, key))

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

#    audio_info = soundfile.info(load_data_s3(path))
    img = cv2.imread(load_data_s3(path))
    
    return img.shape[2], img.shape[1], img.shape[0], filesize # channels, x, y, filesize

def load_media_info(path):
    if path[:5] == "s3://":
        return load_media_info_s3(path)
#    audio_info = soundfile.info(path)
#    filesize = media_size(path)
#    return audio_info.samplerate, audio_info.channels, audio_info.duration, audio_info.subtype, filesize
    img = cv2.imread(path)
    return img.shape[2], img.shape[1], img.shape[0], filesize # channels, x, y, filesize

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

#def read_info(path, timeexp):
def read_info(path):
    """Read recording information form file."""
#    samplerate, nchannels, duration, subtype, filesize = load_media_info(path)
    nchannels, x, y, filesize = load_media_info(path)
    media_info = {}
#    media_info["samplerate"] = samplerate
#    media_info["nchannels"] = nchannels
#    media_info["sampwidth"] = SAMPWIDTHS[subtype]
#    media_info["length"] = int(duration*samplerate)
#    media_info["filesize"] = filesize
#    media_info["duration"] = float(duration) / timeexp
    media_info["nchannels"] = nchannels
    media_info["imgwidth"] = x
    media_info["imgheight"] = y
    media_info["filesize"] = filesize
    return media_info

def hash_file(path, alg="md5"):
    """Produce hash from audio recording."""
    if alg == "md5":
        return binary_md5(path)
    raise NotImplementedError("Algorithm "+alg+" is not implemented.")

def read_media(path,
#               samplerate,
#               offset=0.0,
#               duration=None):
               x=None,y=None,w=None,h=None):
    """Read media."""
    if path[:5] == "s3://":
        path = load_data_s3(path)
    elif not os.path.exists(path):
        parsed = urlparse(path)
        if parsed.scheme in ['http', 'https']:
            if x == None:
                return np.array(PIL.Image.open(BytesIO(requests.get(path).content)).convert("RGB"))
            elif type(x) == float:
                img = np.array(PIL.Image.open(BytesIO(requests.get(path).content)).convert("RGB")); img = np.array(img)
                x1 = int(x*img.shape[1] + 0.5)
                y1 = int(y*img.shape[0] + 0.5)
                x2 = x1 + int(w*img.shape[1] + 0.5)
                y2 = y1 + int(h*img.shape[0] + 0.5)
                return img[y1:y2, x1:x2, :]
            elif type(x) == int:
                img = np.array(PIL.Image.open(BytesIO(requests.get(path).content)).convert("RGB")); img = np.array(img)
                return img[y:y+h, x:x+w, :]
            else:
                raise ValueError("Value of x is not valid: {}".format(x))
    else:          
        if x == None:
            return cv2.imread(path)
        elif type(x) == float:
            img = cv2.imread(path)
            x1 = int(x*img.shape[1] + 0.5)
            y1 = int(y*img.shape[0] + 0.5)
            x2 = x1 + int(w*img.shape[1] + 0.5)
            y2 = y1 + int(h*img.shape[0] + 0.5)
            return img[y1:y2, x1:x2, :]
        elif type(x) == int:
            img = cv2.imread(path)
            return img[y:y+h, x:x+w, :]
        else:
            raise ValueError("Value of x is not valid: {}".format(x))
        

        

def write_media(path,
                image):
#                samplerate,
#                nchannels,
#                media_format="jpg"):
    """Write media."""
#    if media_format not in ["wav", "flac", "ogg"]:
#        raise NotImplementedError("Writer for " + media_format
#                                  + " not implemented.")
#    if nchannels > 1:
#        signal = np.transpose(signal, (1, 0))
    cv2.imwrite(path, image)

def get_channel(signal, channel, nchannels):
    """Return correct channel in any case."""
    if nchannels > 1:
        return np.squeeze(signal[[channel], :])
    return signal


#def resample(
#        array: np.array,
#        original_sr: int,
#        target_sr: int,
#        res_type: Optional[str] = 'kaiser_best',
#        fix: Optional[bool] = True,
#        scale: Optional[bool] = False,
#        **kwargs):
#    """Resample audio."""
#    return librosa.core.resample(
#        array,
#        original_sr,
#        target_sr,
#        res_type=res_type,
#        fix=fix,
#        scale=scale,
#        **kwargs)


def channel_mean(signal, keepdims=False):
    """Return channel mean."""
    return np.mean(signal,
                   axis=0,
                   keepdims=keepdims)
