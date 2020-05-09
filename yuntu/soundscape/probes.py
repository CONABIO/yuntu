"""Classes for audio probes."""
from abc import ABC
from abc import abstractmethod
# from scipy.signal import correlate2d
# from skimage.feature import peak_local_max


class Probe(ABC):
    """Base class for all probes.

    Given a signal, a probe is a method that tests matching
    criteria against it and returns a list of media slices that
    satisfy them.
    """

    @abstractmethod
    def apply(self, media):
        """Apply probe and return matches."""

    def __call__(self, media):
        return self.apply(media)


class TemplateProbe(Probe, ABC):
    """A probe that uses a template to find similar matches."""
    _template = None

    @property
    def template(self):
        """Return probe's template."""
        return self._template

    @abstractmethod
    def compare(self, template):
        """Compare input template with self's template."""

    @abstractmethod
    def to_template(self, mold):
        """Use mold to produce a template."""


# class CorrelationProbe(TemplateProbe):
#     """A probe that uses cross correaltion to find matches."""
#     name = "Correlation probe"
#
#     def __init__(self, mold, name=None):
#         if not isinstance(mold, Media):
#             raise TypeError("Argument 'media' must be a Media object.")
#         if name is not None:
#             self.name = name
#         self.name = name
#         self._template = self.to_template(mold)
#
#     def to_template(self, mold):
#         return mold
#
#     def compare(self, media):
#         return correlate2d(media.array,
#                            self.template,
#                            boundary="symm",
#                            mode="same")
#
#     def apply(self, media, thresh=0.0):
#         min_distance = min(*self.shape)
#         curr_d = 0
#         next_d = 1
#         if self.shape[1] == min_distance:
#             curr_d = 1
#             next_d = 0
#         corr = self.compare(media)
#         all_peaks = peak_local_max(corr,
#                                    min_distance=min_distance,
#                                    threshold_abs=thresh)
#         return all_peaks
#
#     def shape(self):
#         return self.template.shape
