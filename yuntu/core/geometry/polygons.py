import shapely.geometry as shapely_geometry

import yuntu.core.geometry.base as base
import yuntu.core.geometry.utils as utils
import yuntu.core.geometry.mixins as mixins


class Polygon(mixins.Geometry2DMixin, base.Geometry):
    name = base.Geometry.Types.Polygon

    def __init__(self, wkt=None, shell=None, holes=None, geometry=None):
        if geometry is None:
            if wkt is not None:
                geometry = utils.geom_from_wkt(wkt)

            elif shell is not None:
                if holes is None:
                    holes = []

                geometry = utils.polygon_geometry(shell, holes)

        super().__init__(geometry=geometry)

        self.wkt = self.geometry.wkt

    def to_dict(self):
        data = super().to_dict()
        data['wkt'] = self.wkt
        return data

    def plot(self, ax=None, **kwargs):
        ax = super().plot(ax=ax, **kwargs)

        lineplot, = ax.plot(
            *self.geometry.exterior.xy,
            linewidth=kwargs.get('linewidth', 1),
            color=kwargs.get('color', None),
            linestyle=kwargs.get('linestyle', '--'),
        )

        color = lineplot.get_color()

        for interior in self.geometry.interiors:
            ax.plot(
                *interior.xy,
                linewidth=kwargs.get('linewidth', None),
                color=color,
                linestyle=kwargs.get('linestyle', '--'))

        if kwargs.get('fill', True):
            if self.geometry.exterior.is_ccw:
                coords = self.geometry.exterior.coords
            else:
                coords = self.geometry.exterior.coords[::-1]

            xcoords, ycoords = map(list, zip(*coords))
            xstart = xcoords[0]
            ystart = ycoords[0]

            for interior in self.geometry.interiors:
                if interior.is_ccw:
                    coords = interior.coords[::-1]
                else:
                    coords = interior.coords

                intxcoords, intycoords = map(list, zip(*coords))
                xcoords += intxcoords
                ycoords += intycoords

                xcoords.append(xstart)
                ycoords.append(ystart)

            ax.fill(
                xcoords,
                ycoords,
                color=color,
                alpha=kwargs.get('alpha', 0.5),
                linewidth=0,
                label=kwargs.get('label', None))

        return ax


class MultiPolygon(
        mixins.MultiGeometryMixin,
        mixins.Geometry2DMixin,
        base.Geometry):
    """Polygon collection geometry."""

    name = base.Geometry.Types.MultiPolygon

    def __init__(self, polygons=None, geometry=None):
        if geometry is None:
            geoms = []
            if polygons is not None:
                for geom in polygons:
                    if isinstance(geom, Polygon):
                        geoms.append(geom.geometry)
                    elif isinstance(geom, shapely_geometry.Polygon):
                        geoms.append(geom)
                    else:
                        raise ValueError("All elements of input polygon list"
                                         " must be polygons.")
                geometry = shapely_geometry.MultiPolygon(geoms)
        super().__init__(geometry)
