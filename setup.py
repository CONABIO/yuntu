"""Instalation script."""
import os
import version

from setuptools import find_packages, setup

with open(os.path.join(os.path.dirname(__file__), 'README.md')) as readme:
    README = readme.read()

# allow setup.py to be run from any path
os.chdir(os.path.normpath(os.path.join(os.path.abspath(__file__), os.pardir)))

setup(
    name='yuntu',
    version=version.__version__,
    packages=find_packages(exclude=['tests']),
    include_package_data=True,
    license='BSD License',
    description='Acoustic Analysis tools for Conabio',
    long_description=README,
    url='https://github.com/CONABIO-audio/yuntu',
    author=(
        'CONABIO, '
        'Dalia Camacho García Formentí, '
        'Santiago Martínez Balvanera, '
        'Everardo Gustavo Robredo Esquivelzeta'

    ),
    author_email=(
        'dcamacho@conabio.gob.mx, '
        'smartinez@conabio.gob.mx, '
        'erobredo@conabio.gob.mx'
    ),
    install_requires=['numpy',
                      'pony',
                      'dill',
                      'pickle-mixin',
                      'matplotlib',
                      'librosa',
                      'scikit-image',
                      'shapely',
                      'requests',
                      'tqdm',
                      'dask[complete]',
                      'fastparquet',
                      'pygraphviz',
                      'pymongo'],
    classifiers=[
        'Programming Language :: Python :: 3.6',
    ],
)
