"""Yuntu utilities."""
import os
import tempfile
import sys
import subprocess
from contextlib import contextmanager

import requests
from tqdm import tqdm


TMP_DIR = os.path.join(tempfile.gettempdir(), 'yuntu')


@contextmanager
def tmp_file(basename):
    filename = os.path.join(TMP_DIR, basename)

    dirname = os.path.dirname(filename)
    if not os.path.exists(dirname):
        os.makedirs(dirname)

    with open(filename, 'wb') as tmpfile:
        yield filename, tmpfile


def download_file(url, file):
    r = requests.get(url, stream=True)
    file_size = int(r.headers['Content-Length'])
    chunk_size = 1024
    num_bars = int(file_size / chunk_size)
    iterable = tqdm(
        r.iter_content(chunk_size=chunk_size),
        total=num_bars,
        desc=url,
        leave=True,
        unit='KB')
    for chunk in iterable:
        file.write(chunk)


def scp_file(src, dest):
    filename = os.path.join(TMP_DIR, dest)
    print(f'Downloading file {src}...', end='')
    subprocess.run(['scp', src, filename], check=True)
    print(' done.')
    return filename
