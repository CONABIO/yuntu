import numpy as np
import yuntu.core.audio.utils as auUtils


def audioLoadBasicInfo(au):
    au.originalSr = au.config["samplerate"]
    au.nchannels = au.config["nchannels"]
    au.sampwidth = au.config["sampwidth"]
    au.length = au.config["length"]
    au.md5 = au.config["md5"]
    au.duration = au.config["duration"]
    au.filesize = au.config["filesize"]
    au.sr = au.originalSr
    au.mask = None
    au.signal = None

    return True


def audioReadBasicInfo(au):
    au.originalSr, au.nchannels, au.sampwidth, au.length, \
        au.filesize = auUtils.readInfo(au.path)
    au.duration = (float(au.length) / float(au.originalSr)) / au.timeexp
    au.sr = au.originalSr
    au.mask = None
    au.signal = None

    return True


def audioListen(au):
    from IPython.display import Audio
    return Audio(data=au.getSignal(), rate=au.sr)


def audioReadMedia(au):
    offset = 0.0
    duration = None

    if au.mask is not None:
        offset = au.mask[0]
        duration = au.mask[1] - au.mask[0]

    au.signal, au.sr = auUtils.read(au.path, au.readSr, offset, duration)

    return True


def audioClearMedia(au):
    au.signal = None
    au.sr = au.originalSr

    return True


def audioSetReadSr(au, sr):
    au.readSr = sr
    au.clearMedia()

    return True


def audioSetMetadata(au,
                     metadata):
    au.metadata = metadata

    return True


def audioSetMask(au,
                 startTime,
                 endTime):
    au.mask = [startTime / au.timeexp, endTime / au.timeexp]
    au.readMedia()

    return True


def audioUnsetMask(au):
    au.mask = None
    au.clearMedia()

    return True


def audioGetMediaInfo(au):
    info = {}
    info["path"] = au.path
    info["filesize"] = au.filesize
    info["timeexp"] = au.timeexp
    info["samplerate"] = au.originalSr
    info["sampwidth"] = au.sampwidth
    info["length"] = au.length
    info["nchannels"] = au.nchannels
    info["duration"] = au.duration

    return info


def audioGetSignal(au,
                   preProcess=None):
    if au.signal is None:
        au.readMedia()
        if preProcess is not None:
            if isinstance(preProcess, dict):
                au.signal = preProcess["transform"](
                    au.signal, **preProcess["kwargs"])
            else:
                au.signal = preProcess(au.signal)

    return au.signal


def audioGetZcr(au,
                channel=0,
                frame_length=1024,
                hop_length=512,
                preProcess=None):
    if channel > au.nchannels - 1:
        raise ValueError("Channel outside range.")

    sig = au.getSignal(preProcess)

    sig = auUtils.sigChannel(sig,
                             channel,
                             au.nchannels)

    return auUtils.zeroCrossingRate(sig,
                                    frame_length,
                                    hop_length)


def audioGetSpec(au,
                 channel=None,
                 n_fft=1024,
                 hop_length=512,
                 preProcess=None):
    if channel is None:
        sig = auUtils.channelMean(au.getSignal(preProcess))
    else:
        if channel > au.nchannels - 1:
            raise ValueError("Channel outside range.")

        sig = au.getSignal(preProcess)
        sig = auUtils.sigChannel(au.getSignal(
            preProcess), channel, au.nchannels)

    return auUtils.spectrogram(sig,
                               n_fft=n_fft,
                               hop_length=hop_length),\
        auUtils.specFrequencies(au.sr, n_fft)


def audioGetMfcc(au,
                 channel=0,
                 sr=22050,
                 S=None,
                 n_mfcc=20,
                 dct_type=2,
                 norm='ortho',
                 preProcess=None):
    if channel > au.nchannels - 1:
        raise ValueError("Channel outside range.")

    sig = au.getSignal(preProcess)
    sig = auUtils.sigChannel(sig,
                             channel,
                             au.nchannels)

    return auUtils.mfcc(sig,
                        sr=au.sr,
                        S=None,
                        n_mfcc=n_mfcc,
                        dct_type=dct_type,
                        norm=norm)


def audioWriteMedia(au,
                    path,
                    media_format="wav",
                    sr=None):
    if media_format in ["wav", "flac", "ogg"]:
        sig = au.getSignal()
        out_sr = au.sr

        if sr is not None:
            out_sr = sr

        auUtils.write(path, sig, out_sr, au.nchannels, media_format)

        return path
    else:
        raise ValueError("Writing to '" + media_format +
                         "' is not supported yet.")


def audioWriteChunks(au,
                     basePath,
                     chop,
                     thresh=1,
                     media_format="wav",
                     sr=None):
    duration = au.duration
    if sr is not None:
        audioSetReadSr(au, sr)

    results = []
    if chop is None:
        out = {"tlimits": [0, duration], "number": 0}
        out["path"] = audioWriteMedia(
            au, basePath + "." + media_format, media_format)
        results = [out]
    elif duration < chop:
        out = {"tlimits": [0, duration], "number": 0}
        out["path"] = audioWriteMedia(
            au, basePath + "." + media_format, media_format)
        results = [out]
    else:
        fchunks = duration / chop
        if fchunks <= thresh:
            out = {"tlimits": [0, duration], "number": 0}
            out["path"] = audioWriteMedia(
                au, basePath + "." + media_format, media_format)
            results = [out]
        else:
            nchunks = int(fchunks)
            residual = fchunks - nchunks

            if residual >= 0.5:
                nchunks += 1

            tlimits = [(float(chop * x), float(min(chop * (x + 1), duration)))
                       for x in range(nchunks)]
            for i in range(len(tlimits)):
                au.setMask(tlimits[i][0], tlimits[i][1])
                out = {"tlimits": tlimits[i], "number": i}
                out["path"] = audioWriteMedia(
                    au, basePath + "_chunk_" + str(i) + "." +
                    media_format, media_format)
                results.append(out)
                au.unsetMask()

    return results


def audioSetAnn(ann_au,
                annotations):
    for ann in annotations:
        if ann["atype"] == "single":
            ann_au.annotations[ann["id"]] = ann
        else:
            nann = {"atype": "group", "members": []}
            for sann in ann["data"]:
                ann_au.annotations[sann["id"]] = sann
                nann["members"].append(sann["id"])
            group_keys = []
            for key in ann:
                if key.split("_")[0] == "gkey":
                    group_keys.append(key)
                    nann[key] = ann[key]
            group_keys.sort()
            nann["group_code"] = tuple(group_keys)
            gcode = [nann[x] for x in group_keys]
            ann_au.groups[tuple(gcode)] = nann


def audioGetAnn(ann_au,
                noteids=None,
                groups=None):
    if noteids is not None:
        return [ann_au.annotations[nid] for nid in noteids]
    if groups is not None:
        result = []
        for gkey in groups:
            group = dict(ann_au.groups[gkey])
            group["data"] = [ann_au.annotations[nid]
                             for nid in group["members"]]
            result.append(group)
        return result
    return [ann_au.annotations[x] for x in ann_au.annotations]


def audioGetAnnLimits(ann_au,
                      noteids=None,
                      groups=None):
    if noteids is not None or (noteids is None and groups is None):
        return [{
            "noteid": x["id"],
            "limits": (x["start_time"],
                       x["end_time"],
                       x["min_freq"],
                       x["max_freq"])
        }
            for x in ann_au.getAnn(noteids)]
    group_results = []
    for gkey in groups:
        group = ann_au.groups[gkey]
        member_limits = ann_au.getAnnLimits(noteids=group["members"])
        start_time = min([x["limits"][0] for x in member_limits])
        end_time = max([x["limits"][1] for x in member_limits])
        min_freq = min([x["limits"][2] for x in member_limits])
        max_freq = max([x["limits"][3] for x in member_limits])
        glimits = {
            "group": gkey,
            "limits": (start_time, end_time, min_freq, max_freq),
            "members": group["members"]
        }
        group_results.append(glimits)
    return group_results


def audioGetAnnGeoms(ann_au,
                     noteids=None,
                     groups=None):
    if noteids is not None or (noteids is None and groups is None):
        return [{
            "noteid": x["id"],
            "verts": x["verts"]
        }
            for x in ann_au.getAnn(noteids)]
    group_results = []
    for gkey in groups:
        group = ann_au.groups[gkey]
        member_geoms = ann_au.getAnnGeoms(noteids=group["members"])
        mgeoms = {
            "group": gkey,
            "member_geoms": member_geoms
        }
        group_results.append(mgeoms)
    return group_results


def audioGetAnnMatrices(ann_au,
                        noteids=None,
                        groups=None,
                        slices=True,
                        slice_dim=1,
                        window=None,
                        mask_window=None,
                        channel=0,
                        n_fft=1024,
                        hop_length=512,
                        preProcess=None):
    limits = ann_au.getAnnLimits(noteids,
                                 groups)
    ntargets = len(limits)
    if ntargets == 0:
        return []
    spec, freqs = ann_au.getSpec(channel,
                                 n_fft,
                                 hop_length,
                                 preProcess)
    mask_exp = np.zeros_like(spec)
    geoms = ann_au.getAnnGeoms(noteids,
                               groups)
    freqs = freqs / 1000
    mask_exp = auUtils.drawGeoms(geoms, mask_exp, ann_au.duration, freqs)
    actual_pieces = auUtils.getWindowPieces(mask_window,
                                            spec,
                                            freqs,
                                            ann_au.duration,
                                            limits,
                                            slice_dim,
                                            False)
    mask_2d = np.zeros_like(spec)
    for i in range(len(actual_pieces)):
        tbin0, tbin1, fbin0, fbin1 = actual_pieces[i]
        mask_2d[fbin0:fbin1, tbin0:tbin1] = np.ones_like(mask_2d[fbin0:fbin1,
                                                                 tbin0:tbin1])
    mask_1d = np.amax(mask_2d, axis=0)
    if slices:
        print("slices", slices)
        window_pieces = auUtils.getWindowPieces(window,
                                                spec,
                                                freqs,
                                                ann_au.duration,
                                                limits,
                                                slice_dim)
        for i in range(len(window_pieces)):
            tbin0, tbin1, fbin0, fbin1 = window_pieces[i]
            limits[i]["spec"] = spec[fbin0:fbin1, tbin0:tbin1]
            limits[i]["mask2d"] = mask_2d[fbin0:fbin1, tbin0:tbin1]
            limits[i]["mask1d"] = mask_1d[tbin0:tbin1]
            limits[i]["mask_exp"] = mask_exp[fbin0:fbin1, tbin0:tbin1]
            limits[i]["freqs"] = freqs[fbin0:fbin1]
        return limits
    return {"spec": spec,
            "mask1d": mask_1d,
            "mask2d": mask_2d,
            "limits": limits,
            "freqs": freqs,
            "mask_exp": mask_exp}
