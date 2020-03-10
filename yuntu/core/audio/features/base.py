"""Feature class module."""
from abc import ABC
from yuntu.core.media import Media


# pylint: disable=abstract-method
class Feature(Media, ABC):
    """Feature base class.

    This is the base class for all audio features. A feature contains
    information extracted from the audio data.
    """

    def __init__(
            self,
            audio=None,
            path: str = None,
            lazy: bool = False,
            samplerate: int = None,
            duration: float = None):
        """Construct a feature."""
        self.audio = audio

        if samplerate is not None:
            self.samplerate = samplerate

        if duration is not None:
            self.duration = duration

        if audio is not None:
            self.duration = self.audio.media_info.duration
            self.samplerate = self.audio.media_info.samplerate

        if not hasattr(self, 'duration'):
            message = (
                'All audio feature object must have duration. '
                'You should provide them explicitely when not creating this '
                'feature from an Audio object.')
            raise ValueError(message)

        if not hasattr(self, 'samplerate'):
            message = (
                'All audio feature object must have samplerate. '
                'You should provide them explicitely when not creating this '
                'feature from an Audio object.')
            raise ValueError(message)

        super().__init__(path=path, lazy=lazy)

    def has_audio(self):
        """Return if this feature is linked to an Audio instance."""
        if not hasattr(self, 'audio'):
            return False

        return self.audio is not None
