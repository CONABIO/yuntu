from yuntu.core import windows
from yuntu.core.media.base import Media
from yuntu.core.axis import XAxis, YAxis

class ImageItem(Media):
    def __init__(self, image, *args, **kwargs):
        self.image = image
        super().__init__(*args, **kwargs)

    def load(self, path):
        pass

    def write(self, path):
        pass

    def plot(self, ax=None, **kwargs):
        pass

    

class ImageMediaMixin:
    x_axis_index = 0
    y_axis_index = 0
    X_axis_class = XAxis
    Y_axis_class = YAxis
    image_item_class = ImageItem
    window_class = windows.ImageWindow

    plot_xlabel = 'x (pixels)'
    plot_ylabel = 'y (pixels)'

    def __init__(
            self,
            xstart=0,
            ystart=0,
            xlen=None,
            ylen=None,
            resolution=None,
            x_axis=None,
            y_axis=None,
            **kwargs):

        if x_axis is None:
            x_axis = self.X_axis_class(resolution=resolution, **kwargs)

        if y_axis is None:
            y_axis = self.Y_axis_class(resolution=resolution, **kwargs)

        if not isinstance(x_axis, self.X_axis_class):
            x_axis = self.X_axis_class.from_dict(x_axis)
        self.x_axis = x_axis

        if not isinstance(y_axis, self.Y_axis_class):
            y_axis = self.Y_axis_class.from_dict(y_axis)
        self.y_axis = y_axis

#        if xsize is None:
#            xsize = self.x_size()

#        if ysize is None:
#            ysize = self.y_size()

        if 'window' not in kwargs:
            kwargs['window'] = windows.ImageWindow(
            x= xstart,
            w= xlen,
            y= ystart,
            h= ylen)

        super().__init__(**kwargs)

    @property
    def x_size(self):
        if self.is_empty():
            return self.x_axis.get_size(window=self.window)
        return self.array.shape[self.x_axis_index]

    @property
    def y_size(self):
        if self.is_empty():
            return self.y_axis.get_size(window=self.window)
        return self.array.shape[self.y_axis_index]


    def to_dict(self):
        return {
            'x_axis': self.x_axis.to_dict(),
            'y_axis': self.y_axis.to_dict(),
            **super().to_dict()
        }

    def get_image_item_kwargs(self):
        return {'window': self.window.copy()}

    def _get_x_start(self):
        return self.x_axis.get_start(window=self.window)

    def _get_x_size(self):
        return self.x_axis.get_size(window=self.window)

    def _get_y_start(self):
        return self.y_axis.get_start(window=self.window)

    def _get_y_size(self):
        return self.y_axis.get_size(window=self.window)


    def _copy_dict(self):
        return {
            'x_axis': self.x_axis.copy(),
            'y_axis': self.y_axis.copy(),
            **super()._copy_dict()
        }

    def cut(
            self,
            x: int = None,
            y: int = None,
            w: int = None,
            h: int = None,
            window: windows.ImageWindow = None,
            lazy=False,
            pad=False,
            pad_mode='constant',
            constant_values=0):
        """Get a window to the media data.

        Parameters
        ----------
        x1: int, optional
            Window starting time in seconds. If not provided
            it will default to the beggining of the recording.
        x2: int, optional
            Window ending time in seconds. If not provided
            it will default to the duration of the recording.
        window: TimeWindow, optional
            A window object to use for cutting.
        lazy: bool, optional
            Boolean flag that determines if the fragment loads
            its data lazily.

        Returns
        -------
        Media
            The resulting media object with the correct window set.
        """
        current_x = self._get_x()
        current_y = self._get_y()
        current_w = self._get_w()
        current_h = self._get_h()

        if x is None:
            if window is None:
                x = current_x
            else:
                x = (
                    window.x
                    if window.x is not None
                    else current_x)

        if y is None:
            if window is None:
                y = current_y
            else:
                y = (
                    window.y
                    if window.y is not None
                    else current_y)

        if w is None:
            if window is None:
                w = current_w
            else:
                w = (
                    window.w
                    if window.w is not None
                    else current_w)

        if h is None:
            if window is None:
                h = current_h
            else:
                h = (
                    window.h
                    if window.h is not None
                    else current_h)

        if w < 1 or h < 1:
            message = 'Window is empty'
            raise ValueError(message)

        bounded_x = max(min(x, current_x+current_w), current_x)
        bounded_w = max(min(x+w, current_x+current_w), current_x) - bounded_x
        bounded_y = max(min(y, current_y+current_h), current_y)
        bounded_h = max(min(y+h, current_y+current_h), current_y) - bounded_y

        kwargs_dict = self._copy_dict()
        kwargs_dict['window'] = windows.ImageWindow(
            x=x if pad else bounded_x,
            w=w if pad else bounded_w,
            y=y if pad else bounded_y,
            h=h if pad else bounded_h)

        if lazy:
            # TODO: No lazy cutting for now. The compute method does not take
            # into acount possible cuts and thus might not give the correct
            # result.
            lazy = False
        kwargs_dict['lazy'] = lazy

        if not lazy:
            start = self.get_index_from_time(bounded_start_time)
            end = self.get_index_from_time(bounded_end_time)

            slices = self._build_slices(start, end)
            array = self.array[slices]

            if pad:
                start_pad = self.time_axis.get_bin_nums(
                    start_time, bounded_start_time)
                end_pad = self.time_axis.get_bin_nums(
                    bounded_end_time, end_time)
                pad_widths = self._build_pad_widths(start_pad, end_pad)
                array = pad_array(
                    array,
                    pad_widths,
                    mode=pad_mode,
                    constant_values=constant_values)

            kwargs_dict['array'] = array

        return type(self)(**kwargs_dict)




class AtemporalMedia(ImageMediaMixin,Media):
    def _get_x(self):
        return self.x_axis.get_start(window=self.window)

    def _get_y(self):
        return self.y_axis.get_start(window=self.window)

    def _get_w(self):
        return self.x_axis.get_size(window=self.window)

    def _get_h(self):
        return self.y_axis.get_size(window=self.window)
