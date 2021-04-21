import pytest

from yuntu.core.media.readers import get_reader
from yuntu.core.media.readers import TextReader


def test_get_reader(tmp_path):
    text_file = tmp_path / "text_file.txt"
    random_file = tmp_path / "wav_file.random"

    reader = get_reader(text_file)
    assert isinstance(reader, TextReader)

    with pytest.raises(NotImplementedError):
        get_reader(random_file)


def test_text_reader(tmp_path):
    content = "This is a test, with non ascii characters ñ"
    text_file = tmp_path / "text_file.txt"
    text_file.write_text(content)

    reader = TextReader()
    with open(text_file, "rb") as fp:
        assert reader.read(fp) == content

    ascii_reader = TextReader(encoding="ascii")
    with pytest.raises(UnicodeDecodeError):
        with open(text_file, "rb") as fp:
            assert ascii_reader.read(fp) == content
