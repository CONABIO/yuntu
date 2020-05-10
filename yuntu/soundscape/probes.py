"""Classes for audio probes."""
from abc import ABC
from abc import abstractmethod
import numpy as np
from skimage.feature import peak_local_max, match_template
from shapely.ops import unary_union
from yuntu.core.geometry.base import BBox, Polygon, FrequencyInterval


class Probe(ABC):
    """Base class for all probes.

    Given a signal, a probe is a method that tests matching
    criteria against it and returns a list of media slices that
    satisfy them.
    """

    @abstractmethod
    def apply(self, target, **kwargs):
        """Apply probe and return matches."""

    def __call__(self, target, **kwargs):
        return self.apply(target, **kwargs)


class TemplateProbe(Probe, ABC):
    """A probe that uses a template to find similar matches."""

    @abstractmethod
    @property
    def template(self):
        """Return probe's template."""

    @abstractmethod
    def compare(self, target):
        """Compare target with self's template."""


class CrossCorrelationProbe(TemplateProbe):
    """A probe that uses cross correaltion to match inputs with templates."""
    name = "Correlation probe"

    def __init__(self, mold):
        if not isinstance(mold, (tuple, list)):
            raise ValueError("Argument 'mold' must be a list of "
                             "time/frequency media.")
        self._template = []
        self._frequency_interval = None
        self.set_template(mold)

    @property
    def template(self):
        """Return probe's template."""
        return self._template

    def set_template(self, mold):
        for m in mold:
            self.add_template(m)

    def add_template(self, mold):
        if self._template is None:
            self._template = []
        self._template.append(mold.array)
        self._extend_interval_with(mold)

    def _extend_interval_with(self, mold):
        freqs = mold.frequencies
        if self._frequency_interval is None:
            self._frequency_interval = FrequencyInterval(min_freq=freqs[0],
                                                         max_freq=freqs[-1])
        else:
            min_freq = self._frequency_interval.min_freq
            max_freq = self._frequency_interval.max_freq
            if freqs[0] < min_freq:
                min_freq = freqs[0]
            if freqs[1] > max_freq:
                max_freq = freqs[-1]
            self._frequency_interval = FrequencyInterval(min_freq=min_freq,
                                                         max_freq=max_freq)

    def compare(self, target):
        return np.array([match_template(target.array, templ, pad_input=True)
                        for templ in self.template])


    def apply(self, target, thresh=0.5, method='mean',
              peak_distance=10, limit_freqs=True):
        if not isinstance(limit_freqs, FrequencyInterval):
            if limit_freqs:
                limit_freqs = self.frequency_interval

        min_distancex = self.shape[0]
        min_distancey = self.shape[1]

        corr = self.compare(target)
        if method == 'max':
            corr = np.amax(corr, axis=0)
        else:
            corr = np.mean(corr, axis=0)
        all_peaks = peak_local_max(corr,
                                   min_distance=peak_distance,
                                   threshold_abs=thresh)

        boxes = []
        for x, y in all_peaks:
            l1 = max(0, x - int(round(min_distancex/2)))
            l2 = min(l1 + min_distancex, corr.shape[0])-1
            if l2 - l1 > 1:
                min_freq = target.frequencies[l1]
                max_freq = target.frequencies[l2]
                if limit_freqs:
                    min_freq = max(min_freq, limit_freqs.min_freq)
                    max_freq = min(max_freq, limit_freqs.max_freq)
                m1 = max(0, y - int(round(min_distancey/2)))
                m2 = min(m1 + min_distancey, corr.shape[1]-1)
                if m2 - m1 > 1:
                    start_time = target.times[m1]
                    end_time = target.times[m2]
                    new_box = BBox(start_time, end_time,
                                   min_freq, max_freq)
                    found = False
                    for ind, box in enumerate(boxes):
                        if new_box.intersects(box):
                            disolved = unary_union([new_box.geometry,
                                                    box.geometry])
                            boxes[ind] = Polygon(geometry=disolved)
                            found = True
                            break
                    if not found:
                        boxes.append(BBox(start_time, end_time,
                                          min_freq, max_freq))

        output = []
        for box in boxes:
            corr_values = corr[target.to_mask(geometry=box)]
            peak_corr = np.amax(corr_values)
            result = {
                "peak_corr": peak_corr,
                "geometry": box
            }
            output.append(result)
        return output

    @property
    def shape(self):
        """Return probe's shape.

        The shape of a probe is the shape of a ndarray that covers all
        molds in template.
        """
        return np.amax(np.array([x.shape for x in self.template]), axis=0)

    @property
    def frequency_interval(self):
        """Return frequency interval of probe."""
        return self._frequency_interval


def probe(ptype="cross_correlation", **kwargs):
    """Create probe of type 'ptype'."""
    if ptype == "cross_correlation":
        return CrossCorrelationProbe(**kwargs)
    raise NotImplementedError(f"Probe type {ptype} not found.")
