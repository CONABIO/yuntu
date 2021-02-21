"""Classes for audio probes."""
from abc import ABC
from abc import abstractmethod
import numpy as np
from skimage.feature import peak_local_max, match_template
from shapely.ops import unary_union
from yuntu.core.geometry import BBox, Polygon, FrequencyInterval


class Probe(ABC):
    """Base class for all probes.

    Given a signal, a probe is a method that tests matching
    criteria against it and returns a list of media slices that
    satisfy them.
    """

    @abstractmethod
    def apply(self, target, **kwargs):
        """Apply probe and return matches."""
    
    def __enter__(self):
        """Behaviour for context manager"""
        return self

    @abstractmethod
    def __exit__(self, exception_type, exception_value, traceback):
        """Behaviour for context manager"""
        
    def __call__(self, target, **kwargs):
        """Call apply method."""
        return self.apply(target, **kwargs)


class TemplateProbe(Probe, ABC):
    """A probe that uses a template to find similar matches."""

    @property
    @abstractmethod
    def template(self):
        """Return probe's template."""

    @abstractmethod
    def compare(self, target):
        """Compare target with self's template."""

class ModelProbe(Probe, ABC):
    """A probe that use any kind of detection or multilabelling model."""

    @property
    @abstractmethod
    def model(self):
        """Return probe's model."""

    @abstractmethod
    def predict(self, target):
        """Return self model's raw output."""

class CrossCorrelationProbe(TemplateProbe):
    """A probe that uses cross correaltion to match inputs with templates."""
    name = "Correlation probe"

    def __init__(self, molds, tag="target"):
        if not isinstance(molds, (tuple, list)):
            raise ValueError("Argument 'mold' must be a list of "
                             "time/frequency media.")
        if not isinstance(tag, str):
            raise ValueError("Argument 'tag' must be a string.")
        self.tag = tag
        self._template = []
        self._frequency_interval = None
        self.set_template(molds)

    @property
    def template(self):
        """Return probe's template."""
        return self._template

    def set_template(self, molds):
        """Set probe's template."""
        for m in molds:
            self.add_mold(m)

    def add_mold(self, mold):
        """Append a new mold to template.

        Molds are example spectra to cross-correlate with input samples.
        """
        if self._template is None:
            self._template = []
        self._template.append(mold.array.copy())

        self._extend_interval_with(mold)

    def _build_output(self, corr, target, geom):
        corr_values = corr[target.to_mask(geometry=geom).array]
        if corr_values.size > 0:
            return {
                "tag": self.tag,
                "peak_corr": np.amax(corr_values),
                "geometry": geom
            }
        return None

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
            if freqs[-1] > max_freq:
                max_freq = freqs[-1]
            self._frequency_interval = FrequencyInterval(min_freq=min_freq,
                                                         max_freq=max_freq)

    def compare(self, target):
        results = []
        for templ in self.template:
            corr = match_template(target.array, templ, pad_input=True)
            results.append(corr)
        return np.array(results)

    def apply(self, target, thresh=0.5, method='mean',
              peak_distance=10, limit_freqs=True):
        if not isinstance(limit_freqs, FrequencyInterval):
            if limit_freqs:
                limit_freqs = self.frequency_interval

        min_distancex = self.shape_range[0][0]
        min_distancey = self.shape_range[0][1]

        if limit_freqs:
            target = target.cut(min_freq=limit_freqs.min_freq,
                                max_freq=limit_freqs.max_freq)
        corr = self.corr(target, method=method)

        all_peaks = peak_local_max(corr,
                                   min_distance=peak_distance,
                                   threshold_abs=thresh)

        boxes = []
        for x, y in all_peaks:
            xind1 = max(0, x - int(round(min_distancex/2)))
            xind2 = min(xind1 + min_distancex, corr.shape[0])-1
            if xind2 - xind1 > 1:
                min_freq = target.frequencies[xind1]
                max_freq = target.frequencies[xind2]
                yind1 = max(0, y - int(round(min_distancey/2)))
                yind2 = min(yind1 + min_distancey, corr.shape[1]-1)
                if yind2 - yind1 > 1:
                    start_time = target.times[yind1]
                    end_time = target.times[yind2]
                    new_box = BBox(start_time, end_time,
                                   min_freq, max_freq)
                    boxes.append(new_box)

        boxes = unary_union([b.geometry for b in boxes])
        if boxes.geom_type == 'MultiPolygon':
            boxes = [Polygon(geometry=geom) for geom in boxes]
        else:
            boxes = [Polygon(geometry=boxes)]

        output = []
        for box in boxes:
            if box.geometry.geom_type == 'MultiPolygon':
                for poly in box.geometry:
                    new_box = Polygon(geometry=poly)
                    result = self._build_output(corr, target, new_box)
                    if result is not None:
                        output.append(result)
            else:
                result = self._build_output(corr, target, box)
                if result is not None:
                    output.append(result)
        return output

    def corr(self, target, method='mean'):
        corr = self.compare(target)
        if len(self.template) > 0:
            if method == 'max':
                corr = np.amax(corr, axis=0)
            elif method == 'median':
                corr = np.median(corr, axis=0)
            else:
                corr = np.mean(corr, axis=0)
        return corr

    @property
    def shape_range(self):
        """Return probe's shape.

        The shape of a probe is the shape of a ndarray that covers all
        molds in template.
        """
        return (np.amin(np.array([x.shape for x in self.template]), axis=0),
                np.amax(np.array([x.shape for x in self.template]), axis=0))

    @property
    def frequency_interval(self):
        """Return frequency interval of probe."""
        return self._frequency_interval

    def __exit__(self, exception_type, exception_value, traceback):
        del self._template[:]
        self._frequency_interval = None


def probe(ptype="cross_correlation", **kwargs):
    """Create probe of type 'ptype'."""
    if ptype == "cross_correlation":
        return CrossCorrelationProbe(**kwargs)
    raise NotImplementedError(f"Probe type {ptype} not found.")
