import os
import math
import wave
import librosa
import librosa.display
import soundfile as sf
import numpy as np
import matplotlib.pyplot as plt
import scipy.signal
from skimage.draw import line_aa


def getWindowPieces(window,
                    spec,
                    freqs,
                    duration,
                    limits,
                    slice_dim,
                    truncate=True):
    ntargets = len(limits)
    fmax = np.amax(freqs)
    fmin = np.amin(freqs)
    freq_range = fmax - fmin
    fbins, tbins = spec.shape
    win_tsize = 0
    win_fsize = 0
    if window is not None:
        win_tsize = min(tbins, np.ceil((window[0] / duration) * tbins))
        if len(window) > 1:
            win_fsize = min(fbins, np.ceil((window[1] / freq_range) * fbins))
    half_win_t = np.ceil(win_tsize / 2)
    half_win_f = np.ceil(win_fsize / 2)
    bin_limits = []
    for i in range(ntargets):
        start_time, end_time, min_freq, max_freq = limits[i]["limits"]
        if window is not None and truncate:
            midt = (start_time + end_time) / 2
            start_time = max(start_time, midt - window[0] / 3)
            end_time = min(end_time, midt + window[0] / 3)
            if slice_dim == 2:
                midf = (min_freq + max_freq) / 2
                min_freq = max(min_freq, midf - window[1] / 3)
                max_freq = min(max_freq, midf + window[1] / 3)
        tbin0 = int(max(math.floor((start_time / duration) * tbins), 0))
        tbin1 = int(min(math.ceil((end_time / duration) * tbins), tbins))
        fbin0 = 0
        fbin1 = fbins
        if slice_dim == 2:
            fbin0 = int(max(math.floor(
                                      ((min_freq - fmin) / freq_range)
                                      * fbins),
                            0))
            fbin1 = int(min(math.ceil(
                ((max_freq - fmin) / freq_range) * fbins),
                fbins - 1))
        if window is not None:
            if tbin1 - tbin0 < win_tsize:
                if tbin0 - half_win_t >= 0 and tbin1 + \
                        (win_tsize - half_win_t) <= tbins:
                    tbin0 = int(tbin0 - half_win_t)
                    tbin1 = int(tbin1 + (win_tsize - half_win_t))
                elif tbin0 - half_win_t >= 0:
                    tbin0 = int(max(0, int(tbin0 - half_win_t +
                                           (tbin1 + (win_tsize - half_win_t) -
                                            tbins))))
                    tbin1 = int(min(tbins, tbin0 + win_tsize))
                else:
                    tbin0 = 0
                    tbin1 = int(min(tbins, tbin0 + win_tsize))
            if slice_dim == 2:
                if fbin1 - fbin0 < win_fsize:
                    if fbin0 - half_win_f >= 0 and fbin1 + (win_fsize -
                       half_win_f) <= fbins:
                        fbin0 = int(fbin0 - half_win_f)
                        fbin1 = int(fbin1 + (win_fsize - half_win_f))
                    elif fbin0 - half_win_f >= 0:
                        fbin0 = int(max(0, int(fbin0 - half_win_f +
                                               (fbin1 + (win_fsize -
                                                half_win_f) - fbins))))
                        fbin1 = int(min(fbins, fbin0 + win_fsize))
                    else:
                        fbin0 = 0
                        fbin1 = int(min(fbins, fbin0 + win_fsize))
        bin_limits.append((tbin0, tbin1, fbin0, fbin1))
    return bin_limits


def getCoords(x,
              tbins,
              fbins,
              duration,
              freq_range,
              fmin,
              fmax):
    tbin0 = min(tbins - 1,
                int(max(round((x[0] / duration) * tbins), 0)))
    fbin0 = min(fbins - 1,
                int(max(round(((x[1] - fmin) / freq_range) * fbins), 0)))
    return [tbin0, fbin0]


def drawVect(mask,
             verts,
             tbins,
             fbins,
             duration,
             freq_range,
             fmin,
             fmax):
    for i in range(0, len(verts) - 1):
        x1 = verts[i]
        x2 = verts[i + 1]
        coords1 = getCoords(x1,
                            tbins,
                            fbins,
                            duration,
                            freq_range,
                            fmin,
                            fmax)
        coords2 = getCoords(x2,
                            tbins,
                            fbins,
                            duration,
                            freq_range,
                            fmin,
                            fmax)
        rr, cc, val = line_aa(coords1[1],
                              coords1[0],
                              coords2[1],
                              coords2[0])
        mask[rr, cc] = 1.0
    return mask


def drawGeoms(geoms,
              mask,
              duration,
              freqs):
    fbins, tbins = mask.shape
    fmin = np.amin(freqs)
    fmax = np.amax(freqs)
    freq_range = fmax - fmin
    for geom in geoms:
        if "member_geoms" in geom:
            for sub_geom in geom["member_geoms"]:
                verts = sub_geom["verts"]
                if verts is not None:
                    mask = drawVect(mask,
                                    verts,
                                    tbins,
                                    fbins,
                                    duration,
                                    freq_range,
                                    fmin,
                                    fmax)
        else:
            verts = geom["verts"]
            if verts is not None:
                mask = drawVect(mask,
                                verts,
                                tbins,
                                fbins,
                                duration,
                                freq_range,
                                fmin,
                                fmax)
    return mask


def mediaSize(path):
    if path is not None:
        if os.path.isfile(path):
            return os.path.getsize(path)
        else:
            return None
    else:
        return None


def readInfo(path):
    wav = wave.open(path)
    return wav.getframerate(), wav.getnchannels(), wav.getsampwidth(),\
        wav.getnframes(), mediaSize(path)


def read(path,
         sr,
         offset=0.0,
         duration=None):
    return librosa.load(path,
                        sr=sr,
                        offset=offset,
                        duration=duration,
                        mono=False)


def write(path,
          sig,
          sr,
          nchannels,
          media_format="wav"):
    if nchannels > 1:
        sig = np.transpose(sig, (1, 0))
    sf.write(path,
             sig,
             sr,
             format=media_format)


def sigChannel(sig,
               channel,
               nchannels):
    if nchannels > 1:
        return np.squeeze(sig[[channel], :])
    else:
        return sig


def channelMean(sig,
                keepdims=False):
    return np.mean(sig,
                   axis=0,
                   keepdims=keepdims)


def stft(sig,
         n_fft,
         hop_length,
         win_length=None,
         window='hann',
         center=True,
         pad_mode='reflect'):
    return librosa.stft(sig,
                        n_fft,
                        hop_length,
                        win_length=win_length,
                        window=window,
                        center=center,
                        pad_mode=pad_mode)


def spectrogram(sig,
                n_fft,
                hop_length):
    return np.abs(stft(sig,
                       n_fft,
                       hop_length))


def specFrequencies(sr, n_fft):
    return librosa.core.fft_frequencies(sr=sr, n_fft=n_fft)


def zeroCrossingRate(sig,
                     frame_length=2048,
                     hop_length=512,
                     center=True):
    return librosa.feature.zero_crossing_rate(sig,
                                              frame_length,
                                              hop_length,
                                              center=center)


def mfcc(sig,
         sr=22050,
         S=None,
         n_mfcc=20,
         dct_type=2,
         norm='ortho'):
    return librosa.feature.mfcc(sig, sr, S, n_mfcc, dct_type, norm)


def minMaxNorm(x):
    amin = np.amin(x)
    amax = np.amax(x)

    return (x - amin) / (amax - amin)
