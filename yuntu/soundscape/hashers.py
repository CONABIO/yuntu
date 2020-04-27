"""Row hashers for category assingment."""
from abc import ABC
from abc import abstractmethod
import numpy as np
from yuntu.soundscape.utils import aware_time
import datetime


TIME_ZONE = "America/Mexico_city"
TIME_FORMAT = "%Y-%m-%d %H:%M:%S"
TIME_START = "2000-01-01 00:00:00"
TIME_UNIT = 3600
TIME_MODULE = 24
TIME_COLUMN = "time_raw"
TIME_FORMAT_COLUMN = "time_format"
TIME_ZONE_COLUMN = "time_zone"
AWARE_START = aware_time(TIME_START, TIME_ZONE, TIME_FORMAT)


class Hasher(ABC):
    columns = "__all__"

    @abstractmethod
    def hash(self, row, out_name="hash"):
        """Return row hash."""

    @abstractmethod
    def unhash(self, hashed):
        """Invert hash."""

    def validate(self, df):
        """Check it self can be applied to dataframe."""
        if self.columns != "__all__":
            df_cols = df.columns
            for col in self.columns:
                if col not in df_cols:
                    return False
        return True

    @property
    @abstractmethod
    def dtype(self):
        """Return row hash."""

    def __call__(self, row, **kwargs):
        """Hash object."""
        return self.hash(row, **kwargs)


class GenericHasher(Hasher, ABC):
    def __init__(self, hash_method, unhash_method=None, columns="__all__"):
        if not hasattr(hash_method, "__call__"):
            raise ValueError("Argument 'hash_method' must"
                             "be a callable object.")
        if unhash_method is not None:
            if not hasattr(hash_method, "__call__"):
                raise ValueError("Argument 'unhash_method' must be a"
                                 "callable object.")
        if columns != "__all__":
            if not isinstance(columns, (tuple, list)):
                raise ValueError("Argument 'columns' must be a list of "
                                 "column names.")
        self.columns = columns
        self._hash_method = hash_method
        self._unhash_method = unhash_method

    def hash(self, row, out_name="hash", **kwargs):
        """Return row hash."""
        row[out_name] = self._hash_method(row, **kwargs)
        return row

    def unhash(self, hashed, **kwargs):
        """Invert hash (if possible)."""
        if self._unhash_method is not None:
            return self._unhash_method(hashed, **kwargs)
        raise NotImplementedError("Hasher has no inverse.")


class StrHasher(GenericHasher):

    @property
    def dtype(self):
        return np.dtype('str')


class IntHasher(GenericHasher):

    def __init__(self, *args, precision=32, **kwargs):
        if precision not in [8, 16, 32, 64]:
            raise ValueError("Argument precision must be 8, 16, 32 or 64.")
        super().__init__(*args, **kwargs)
        self.precision = precision

    @property
    def dtype(self):
        strtype = f"int{self.precision}"
        return np.dtype(strtype)


class CronoHasher(Hasher):

    def __init__(self,
                 time_column=TIME_COLUMN,
                 tzone_column=TIME_ZONE_COLUMN,
                 format_column=TIME_FORMAT_COLUMN,
                 aware_start=AWARE_START,
                 start_time=TIME_START,
                 start_tzone=TIME_ZONE,
                 start_format=TIME_FORMAT,
                 time_unit=TIME_UNIT,
                 time_module=TIME_MODULE):

        if not isinstance(time_column, str):
            raise ValueError("Argument 'time_column' must be a string.")
        if not isinstance(tzone_column, str):
            raise ValueError("Argument 'tzone_column' must be a string.")
        if not isinstance(tzone_column, str):
            raise ValueError("Argument 'tzone_column' must be a string.")
        if not isinstance(format_column, str):
            raise ValueError("Argument 'format_column' must be a string.")

        if not isinstance(time_unit, (int, float)):
            raise ValueError("Argument 'time_unit' must be a number.")
        if not isinstance(time_module, (int, float)):
            raise ValueError("Argument 'time_module' must be a number.")

        if aware_start is not None:
            if not isinstance(aware_start, datetime.datetime):
                raise ValueError("Argument 'standard_start' must "
                                 "be a datetime.")
            self.start = aware_start

        else:
            if not isinstance(start_tzone, str):
                raise ValueError("Argument 'start_tzone' must be a string.")
            if not isinstance(start_time, str):
                raise ValueError("Argument 'start_time' must be a string.")
            if not isinstance(start_tzone, str):
                raise ValueError("Argument 'start_tzone' must be a string.")
            if not isinstance(start_format, str):
                raise ValueError("Argument 'start_format' must be a string.")
            self.start = aware_time(start_time, start_tzone, start_format)

        self.time_column = time_column
        self.tzone_column = tzone_column
        self.format_column = format_column
        self.columns = [time_column, tzone_column, format_column]
        self.unit = datetime.timedelta(seconds=time_unit)
        self.module = datetime.timedelta(seconds=time_unit * time_module)

    def hash(self, row, out_name="crono_hash"):
        """Return rotating integer hash according to unit, module and start."""
        strtime = row[self.time_column]
        timezone = row[self.tzone_column]
        timeformat = row[self.format_column]

        atime = aware_time(strtime, timezone, timeformat)
        delta_from_start = self.start - atime

        remainder = delta_from_start % self.module
        row[out_name] = np.int64(int(round(remainder/self.unit)))
        return row

    def unhash(self, hashed):
        """Produce datetime object according to input integer hash."""
        unit_seconds = self.unit.total_seconds()
        hash_delta = datetime.timedelta(seconds=hashed * unit_seconds)
        return self.start + hash_delta

    @property
    def dtype(self):
        return np.dtype('int64')


def hasher(hash_type, hash_method=None, unhash_method=None, **kwargs):
    """Return hasher instance by name."""
    if hash_type in ["string", "integer"]:
        if hash_method is None:
            raise ValueError("Hash method can not be undefined for generic"
                             " string and integer hashers.")
    if hash_type == "string":
        return StrHasher(hash_method, unhash_method=unhash_method, **kwargs)
    if hash_type == "integer":
        return IntHasher(hash_method, unhash_method=unhash_method, **kwargs)
    if hash_type == "crono":
        return CronoHasher(**kwargs)

    raise NotImplementedError(f"Hasher type unknown.")
