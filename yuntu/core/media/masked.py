import numpy as np

from yuntu.core.media.base import Media


class MaskedMediaMixin:
    def __init__(self, media, geometry, **kwargs):
        self.geometry = geometry
        self.media = media
        kwargs['window'] = self.media.window
        super().__init__(**kwargs)

    def to_dict(self):
        return {
            'geometry': self.geometry.to_dict(),
            'media': self.media.to_dict(),
            **super().to_dict()
        }

    def write(self, path=None, **kwargs):
        if path is None:
            path = self.path

        self.path = path

        data = {
            'mask': self.array
        }

        if self.media.path_exists():
            data['media_path'] = self.media.path

        if not self.window.is_trivial():
            window_data = {
                f'window_{key}': value
                for key, value in self.window.to_dict()
                if value is not None
            }
            data.update(window_data)

        np.savez(self.path, **data)


class MaskedMedia(MaskedMediaMixin, Media):
    pass


def masks(cls):
    def decorator(mask_cls):
        if not issubclass(mask_cls, MaskedMediaMixin):
            message = (
                f'Class {mask_cls} cannot be masked by an'
                ' object which does not inherit from MaskedMedia')
            raise ValueError(message)

        cls.mask_class = mask_cls
        return mask_cls
    return decorator
