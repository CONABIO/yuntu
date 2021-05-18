from typing import BinaryIO
from typing import Optional

import numpy as np
import soundfile

from yuntu.core.media.readers import FileReader


class WavReader(FileReader):
    mime_types = ["audio/x-wav"]

    def read(
        self,
        fp: BinaryIO,
        start_time: Optional[float] = None,
        end_time: Optional[float] = None,
        **kwargs
    ) -> np.ndarray:
        with soundfile.SoundFile(fp) as sf:
            samplerate = sf.samplerate

            if start_time is None:
                start = 0
            else:
                start = int(np.floor(start_time * samplerate))

            if end_time is None:
                end = sf.frames
            else:
                end = int(np.floor(end_time * samplerate))

            if start > 0:
                sf.seek(start)

            return sf.read(end - start, fill_value=0, always_2d=True)
