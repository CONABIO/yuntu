"""Windows module."""
from typing import Optional


class Window:
    """A window is an object used to select portions of data."""

    def cut(self, other):
        """Use window to cut out object."""
        return other.cut(window=self)


class TimeWindow(Window):
    """Time window class.

    Used to cut a time interval.
    """

    def __init__(
            self,
            start: Optional[float] = None,
            end: Optional[float] = None):
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


class FrequencyWindow(Window):
    """Frequency window class.

    Used to cut a range of frequencies.
    """

    # pylint: disable=redefined-builtin
    def __init__(
            self,
            min: Optional[float] = None,
            max: Optional[float] = None):
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


class TimeFrequencyWindow(Window):
    """Time and Frequency window class.

    Used to cut a range of frequencies and times.
    """

    # pylint: disable=redefined-builtin
    def __init__(
            self,
            start: Optional[float] = None,
            end: Optional[float] = None,
            min: Optional[float] = None,
            max: Optional[float] = None):
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
        self.start = start
        self.end = end
        self.min = min
        self.max = max
