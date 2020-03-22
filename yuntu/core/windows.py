"""Windows module."""
from typing import Optional
from abc import ABC
from abc import abstractmethod


class Window(ABC):
    """A window is an object used to select portions of data."""
    def cut(self, other):
        """Use window to cut out object."""
        return other.cut(window=self)

    @abstractmethod
    def to_dict(self):
        """Return a dictionary representation of the window."""

    @classmethod
    def from_dict(cls, data):
        """Rebuild the window from dictionary data."""
        if 'type' not in data:
            raise ValueError('Window data does not have a type.')

        window_type = data.pop('type')
        if window_type == 'TimeWindow':
            return TimeWindow(**data)

        if window_type == 'FrequencyWindow':
            return FrequencyWindow(**data)

        if window_type == 'TimeFrequencyWindow':
            return TimeFrequencyWindow(**data)

        message = (
            f'Window type {window_type} is incorrect. Valid options: '
            'TimeWindow, FrequencyWindow, TimeFrequencyWindow')
        raise ValueError(message)


class TimeWindow(Window):
    """Time window class.

    Used to cut a time interval.
    """

    def __init__(
            self,
            start: Optional[float] = None,
            end: Optional[float] = None,
            **kwargs):
        """Construct a time window.

        Parameters
        ----------
        start: float
            Interval starting time in seconds.
        end:
            Interval ending time in seconds.
        """
        self.start = start
        self.end = end
        super().__init__(**kwargs)

    def to_dict(self):
        """Get dictionary representation of window."""
        return {
            'type': 'TimeWindow',
            'start': self.start,
            'end': self.end
        }

    def is_trivial(self):
        """Return if window is trivial."""
        if self.start is not None:
            return False

        if self.end is not None:
            return False

        return True

    def __repr__(self):
        """Get string representation of window."""
        return f'TimeWindow(start={self.start}, end={self.end})'


class FrequencyWindow(Window):
    """Frequency window class.

    Used to cut a range of frequencies.
    """

    # pylint: disable=redefined-builtin
    def __init__(
            self,
            min: Optional[float] = None,
            max: Optional[float] = None,
            **kwargs):
        """Construct a frequency window.

        Parameters
        ----------
        min: float
            Interval starting frequency in hertz.
        max:
            Interval ending frequency in hertz.
        """
        self.min = min
        self.max = max
        super().__init__(**kwargs)

    def to_dict(self):
        """Get dictionary representation of window."""
        return {
            'type': 'FrequencyWindow',
            'min': self.min,
            'max': self.max
        }

    def is_trivial(self):
        """Return if window is trivial."""
        if self.min is not None:
            return False

        if self.max is not None:
            return False

        return True

    def __repr__(self):
        """Get string representation of window."""
        return f'FrequencyWindow(min={self.min}, max={self.max})'


class TimeFrequencyWindow(TimeWindow, FrequencyWindow):
    """Time and Frequency window class.

    Used to cut a range of frequencies and times.
    """

    # pylint: disable=redefined-builtin
    def __init__(
            self,
            start: Optional[float] = None,
            end: Optional[float] = None,
            min: Optional[float] = None,
            max: Optional[float] = None,
            **kwargs):
        """Construct a time frequency window.

        Parameters
        ----------
        start: float
            Interval starting time in seconds.
        end:
            Interval ending time in seconds.
        min: float
            Interval starting frequency in hertz.
        max:
            Interval ending frequency in hertz.
        """
        super().__init__(start=start, end=end, min=min, max=max)

    def to_dict(self):
        """Get dictionary representation of window."""
        return {
            'type': 'TimeFrequencyWindow',
            'start': self.start,
            'end': self.end,
            'min': self.min,
            'max': self.max
        }

    def is_trivial(self):
        """Return if window is trivial."""
        if self.start is not None:
            return False

        if self.end is not None:
            return False

        if self.min is not None:
            return False

        if self.max is not None:
            return False

        return True

    def __repr__(self):
        """Get string representation of window."""
        return (
            'TimeFrequencyWindow('
            f'start={self.start}, end={self.end}, '
            f'min={self.min}, max={self.max})')
