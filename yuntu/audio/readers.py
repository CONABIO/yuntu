from typing import BinaryIO

import numpy as np
import soundfile
from yuntu.core.media.readers import FileReader


class WavReader(FileReader):
    mime_types = ["audio/x-wav"]

    def read(self, fp: BinaryIO) -> np.ndarray:
        wav, _ = soundfile.read(fp)

        if wav.ndim == 1:
            # Add one dimension if mono
            wav = wav[:, None]

        return wav
