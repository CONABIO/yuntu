import pytest
from yuntu.core.media.base import Media

TEST_CONTENT = b"test"
TEST_PATH = "tmp.txt"


@pytest.fixture
def random_media(tmp_path):
    def test_media(path=TEST_PATH, content=TEST_CONTENT):
        tmp_file = tmp_path / path
        tmp_file.write_bytes(content)
        return Media(path=tmp_file)

    return test_media


def test_media_content(random_media):
    m = random_media()
    assert m.bytes == TEST_CONTENT


def test_clean(random_media):
    m = random_media()

    assert not m.has_bytes()
    m.bytes
    assert m.has_bytes()
    m.clean_bytes()
    assert not m.has_bytes()
