from pathlib import Path
import pytest
from web_monitoring.content_type import detect_binary_type


@pytest.mark.parametrize('filename, binary_type', [
    ('simple.pdf', 'application/pdf'),
    ('edgi_logo.png', 'image/png'),
    ('edgi_background.jpg', 'image/jpeg'),
    ('unknown_encoding.html', None),
    ('empty.txt', None),
    ('poorly_encoded_utf8.txt', None)
    ])
def test_detect_binary_type(filename, binary_type):
    """Read file as binary stream. Detect its type
    """
    fname = Path(__file__).resolve().parent / 'fixtures' / filename
    with open(fname, 'rb') as fp:
        data = fp.read(1024)
        assert detect_binary_type(data) == binary_type
