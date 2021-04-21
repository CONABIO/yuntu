from typing import BinaryIO
from typing import List
from dataclasses import dataclass

import soundfile
from yuntu.core.media.info import MediaInfoReader


@dataclass
class WavMediaInfo:
    samplerate: int
    frames: int
    duration: float
    channels: int


class WavMediaInfoReader(MediaInfoReader):
    media_info_types: List[str] = ["wav_media_info"]

    def read(self, fp: BinaryIO) -> WavMediaInfo:
        info = soundfile.info(fp)
        return WavMediaInfo(
            samplerate=info.samplerate,
            channels=info.channels,
            frames=info.frames,
            duration=info.frames / info.samplerate,
        )
